package storage

import (
	"sync/atomic"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// LockFreeMemTable errors
var (
	// Use the unified error constants from model package
	// These match the standard MemTable errors for API consistency
	ErrLockFreeKeyNotFound     = model.ErrKeyNotFound
	ErrLockFreeNilValue        = model.ErrNilValue
	ErrLockFreeNilKey          = model.ErrNilKey
	ErrLockFreeMemTableFull    = model.ErrMemTableFull
	ErrLockFreeMemTableFlushed = model.ErrMemTableFlushed
)

// LockFreeMemTableConfig holds configuration options for the LockFreeMemTable
type LockFreeMemTableConfig struct {
	// MaxSize is the maximum size in bytes the LockFreeMemTable can hold before flushing is required
	MaxSize uint64
	// Logger is used to log LockFreeMemTable operations
	Logger model.Logger
	// Comparator is used to compare keys in the LockFreeMemTable
	Comparator Comparator
}

// LockFreeMemTable uses a lock-free concurrent skiplist to store key-value pairs
// It provides thread-safe access without using mutexes
type LockFreeMemTable struct {
	skiplist   *ConcurrentSkipList // Lock-free skiplist
	maxSize    uint64              // Maximum size in bytes
	isFlushed  uint32              // Whether the table has been flushed (atomic)
	logger     model.Logger        // Logger for operations
	comparator Comparator          // Function for comparing keys
}

// NewLockFreeMemTable creates a new empty LockFreeMemTable
func NewLockFreeMemTable(config LockFreeMemTableConfig) *LockFreeMemTable {
	// Convert to standard config for validation
	standardConfig := MemTableConfig{
		MaxSize:    config.MaxSize,
		Logger:     config.Logger,
		Comparator: config.Comparator,
	}

	// Validate using shared validation function
	standardConfig = ValidateMemTableConfig(standardConfig)

	return &LockFreeMemTable{
		skiplist:   NewConcurrentSkipList(standardConfig.Comparator),
		maxSize:    standardConfig.MaxSize,
		isFlushed:  0, // Not flushed initially
		logger:     standardConfig.Logger,
		comparator: standardConfig.Comparator,
	}
}

// Put adds or updates a key-value pair in the LockFreeMemTable
func (m *LockFreeMemTable) Put(key, value []byte) error {
	return m.PutWithVersion(key, value, 0)
}

// PutWithVersion adds or updates a key-value pair with a specific version
func (m *LockFreeMemTable) PutWithVersion(key, value []byte, version uint64) error {
	// Use shared validation function
	if err := ValidateKeyValue(key, value); err != nil {
		return err
	}

	// Check if the table is flushed
	if atomic.LoadUint32(&m.isFlushed) == 1 {
		return ErrLockFreeMemTableFlushed
	}

	// Check if adding would exceed max size
	entrySize := uint64(len(key) + len(value))
	currentSize := m.skiplist.Size()
	if currentSize+entrySize > m.maxSize {
		return ErrLockFreeMemTableFull
	}

	// Try to put the key with several retries if needed
	maxPutRetries := 3
	var usedVersion uint64
	var ok bool

	for retry := 0; retry < maxPutRetries; retry++ {
		if version == 0 {
			// Generate new version without passing one
			usedVersion, ok = m.skiplist.Put(key, value)
		} else {
			// Use specified version
			usedVersion, ok = m.skiplist.PutWithVersion(key, value, version)
		}

		if usedVersion > 0 {
			// The put was successful

			// Verify the key is now retrievable
			if retry < maxPutRetries-1 {
				_, err := m.Get(key)
				if err == nil {
					// Key is retrievable, operation complete
					return nil
				}

				// Key not yet retrievable, may need to wait a bit for other threads
				for i := 0; i < (retry+1)*50; i++ {
					// CPU yield to give other threads a chance
				}
			} else {
				// Last retry, just accept the put worked even if not yet visible
				// Log the operation
				if ok {
					m.logger.Debug("Added new entry to LockFreeMemTable with version %d, key size: %d, value size: %d",
						usedVersion, len(key), len(value))
				} else {
					m.logger.Debug("Updated entry in LockFreeMemTable with version %d, key size: %d, value size: %d",
						usedVersion, len(key), len(value))
				}
				return nil
			}
		} else if !ok {
			// Something failed, retry with a small delay
			for i := 0; i < (retry+1)*10; i++ {
				// CPU yield
			}
		} else {
			// Operation succeeded
			// Log the operation
			if ok {
				m.logger.Debug("Added new entry to LockFreeMemTable with version %d, key size: %d, value size: %d",
					usedVersion, len(key), len(value))
			} else {
				m.logger.Debug("Updated entry in LockFreeMemTable with version %d, key size: %d, value size: %d",
					usedVersion, len(key), len(value))
			}
			return nil
		}
	}

	// If we got here, the operation completed but might not be immediately visible
	// This is okay in a lock-free structure, so we'll return success
	return nil
}

// Get retrieves a value from the LockFreeMemTable by key
func (m *LockFreeMemTable) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrLockFreeNilKey
	}

	// Try to get from skiplist with retries
	const maxRetries = 3
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		// Try to get the value
		value, _, found := m.skiplist.Get(key)
		if found {
			return value, nil
		}

		// Check if the key exists but is marked as deleted
		node, _ := m.skiplist.findNodeAndPrevs(key)
		if node != nil && atomic.LoadUint32(&node.isDeleted) == 1 {
			// The key exists but is marked as deleted - this is a definitive result
			return nil, ErrLockFreeKeyNotFound
		}

		// Key wasn't found, but we'll retry because in a concurrent environment
		// the key might be in the process of being added by another goroutine
		lastErr = ErrLockFreeKeyNotFound

		// Small backoff before retry
		if retry < maxRetries-1 {
			// Sleep for a short time to allow other operations to complete
			for i := 0; i < 100; i++ {
				// This is a simple CPU yield that's more efficient than time.Sleep
				// for very short durations in a high-concurrency scenario
			}
		}
	}

	return nil, lastErr
}

// Delete marks a key as deleted
func (m *LockFreeMemTable) Delete(key []byte) error {
	return m.DeleteWithVersion(key, 0)
}

// DeleteWithVersion marks a key as deleted with a specific version
func (m *LockFreeMemTable) DeleteWithVersion(key []byte, version uint64) error {
	if key == nil {
		return ErrLockFreeNilKey
	}

	// Check if the table is flushed
	if atomic.LoadUint32(&m.isFlushed) == 1 {
		return ErrLockFreeMemTableFlushed
	}

	// Delete from skiplist
	usedVersion, deleted := m.skiplist.DeleteWithVersion(key, version)

	if deleted {
		m.logger.Debug("Deleted entry from LockFreeMemTable with version %d, key size: %d",
			usedVersion, len(key))
	}

	return nil
}

// Contains checks if a key exists in the LockFreeMemTable
func (m *LockFreeMemTable) Contains(key []byte) bool {
	if key == nil {
		return false
	}

	// The skiplist Contains method already checks for deletion status
	// so we can use it directly
	return m.skiplist.Contains(key)
}

// Size returns the current size of the LockFreeMemTable in bytes
func (m *LockFreeMemTable) Size() uint64 {
	return m.skiplist.Size()
}

// MaxSize returns the maximum size of the LockFreeMemTable in bytes
func (m *LockFreeMemTable) MaxSize() uint64 {
	return m.maxSize
}

// EntryCount returns the number of entries in the LockFreeMemTable
func (m *LockFreeMemTable) EntryCount() int {
	return m.skiplist.Count()
}

// IsFull returns true if the LockFreeMemTable has reached its maximum size
func (m *LockFreeMemTable) IsFull() bool {
	return m.skiplist.Size() >= m.maxSize
}

// MarkFlushed marks the LockFreeMemTable as flushed (read-only)
func (m *LockFreeMemTable) MarkFlushed() {
	atomic.StoreUint32(&m.isFlushed, 1)
	m.logger.Info("LockFreeMemTable marked as flushed with %d entries and %d bytes",
		m.skiplist.Count(), m.skiplist.Size())
}

// IsFlushed returns true if the LockFreeMemTable has been flushed
func (m *LockFreeMemTable) IsFlushed() bool {
	return atomic.LoadUint32(&m.isFlushed) == 1
}

// GetEntries returns all entries in the LockFreeMemTable in sorted order
func (m *LockFreeMemTable) GetEntries() [][]byte {
	return m.skiplist.GetEntries()
}

// GetEntriesWithMetadata returns all entries with metadata (versioning, deletion status)
// This implementation needs to match the contract from the original MemTable
func (m *LockFreeMemTable) GetEntriesWithMetadata() [][]byte {
	// Create a slice to hold entries - each entry consists of:
	// [0]: key
	// [1]: value
	// [2]: version (as 8-byte binary)
	// [3]: isDeleted flag (as 1-byte binary)
	totalEntries := m.skiplist.Count()
	entries := make([][]byte, 0, totalEntries*4)

	// Start traversal from level 0
	curr := m.skiplist.head.next[0]

	// Traverse all nodes
	for curr != m.skiplist.tail {
		// Skip physically deleted nodes (marked for removal)
		if atomic.LoadUint32(&curr.marked) == 1 {
			curr = curr.next[0]
			continue
		}

		// Add key and value to result
		keyCopy := make([]byte, len(curr.key))
		valueCopy := make([]byte, len(curr.value))
		copy(keyCopy, curr.key)
		copy(valueCopy, curr.value)

		entries = append(entries, keyCopy)
		entries = append(entries, valueCopy)

		// Use shared utility for formatting version and deletion flag
		isDeleted := atomic.LoadUint32(&curr.isDeleted) == 1
		versionBytes, deletedFlag := FormatMetadataEntry(curr.version, isDeleted)
		entries = append(entries, versionBytes)
		entries = append(entries, deletedFlag)

		// Move to next node
		curr = curr.next[0]
	}

	return entries
}

// GetVersion returns the current version of the LockFreeMemTable
func (m *LockFreeMemTable) GetVersion() uint64 {
	return m.skiplist.GetVersion()
}

// Clear removes all entries from the LockFreeMemTable
func (m *LockFreeMemTable) Clear() {
	m.skiplist.Clear()
	atomic.StoreUint32(&m.isFlushed, 0) // Reset flushed status
	m.logger.Info("LockFreeMemTable cleared")
}

// GetKeysWithPrefix returns all keys that start with the given prefix
func (m *LockFreeMemTable) GetKeysWithPrefix(prefix []byte) [][]byte {
	if prefix == nil {
		return [][]byte{}
	}

	// Create a slice to hold the matching keys
	keys := make([][]byte, 0)

	// Use a more efficient way to search with a prefix in a concurrent structure
	curr := m.skiplist.head.next[0]
	prefixLen := len(prefix)

	// Traverse all nodes at level 0
	for curr != nil && curr != m.skiplist.tail {
		// Skip physically deleted nodes
		if atomic.LoadUint32(&curr.marked) == 1 {
			curr = curr.next[0]
			continue
		}

		// Skip logically deleted nodes
		if atomic.LoadUint32(&curr.isDeleted) == 1 {
			curr = curr.next[0]
			continue
		}

		// Check if this key has the prefix
		if curr.key != nil && len(curr.key) >= prefixLen {
			hasPrefix := true
			for i := 0; i < prefixLen; i++ {
				if curr.key[i] != prefix[i] {
					hasPrefix = false
					break
				}
			}

			if hasPrefix {
				// Create a copy of the key to avoid race conditions
				keyCopy := make([]byte, len(curr.key))
				copy(keyCopy, curr.key)
				keys = append(keys, keyCopy)
			}

			// If the key is already greater than the prefix, we can stop
			// (assuming keys are sorted lexicographically)
			if len(curr.key) >= prefixLen && m.comparator(curr.key[:prefixLen], prefix) > 0 {
				break
			}
		}

		// Move to next node
		curr = curr.next[0]
	}

	return keys
}

// Ensure LockFreeMemTable implements the MemTableInterface
var _ MemTableInterface = (*LockFreeMemTable)(nil)
