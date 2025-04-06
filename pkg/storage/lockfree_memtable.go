package storage

import (
	"encoding/binary"
	"errors"
	"sync/atomic"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// LockFreeMemTable errors
var (
	ErrLockFreeKeyNotFound     = errors.New("key not found in LockFreeMemTable")
	ErrLockFreeNilValue        = errors.New("cannot add nil value to LockFreeMemTable")
	ErrLockFreeNilKey          = errors.New("cannot use nil key in LockFreeMemTable")
	ErrLockFreeMemTableFull    = errors.New("LockFreeMemTable is full")
	ErrLockFreeMemTableFlushed = errors.New("LockFreeMemTable has been flushed and is read-only")
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
	if config.Logger == nil {
		config.Logger = model.DefaultLoggerInstance
	}
	if config.Comparator == nil {
		config.Comparator = DefaultComparator
	}
	if config.MaxSize == 0 {
		config.MaxSize = 32 * 1024 * 1024 // 32MB default
	}

	return &LockFreeMemTable{
		skiplist:   NewConcurrentSkipList(config.Comparator),
		maxSize:    config.MaxSize,
		isFlushed:  0, // Not flushed initially
		logger:     config.Logger,
		comparator: config.Comparator,
	}
}

// Put adds or updates a key-value pair in the LockFreeMemTable
func (m *LockFreeMemTable) Put(key, value []byte) error {
	if key == nil {
		return ErrLockFreeNilKey
	}
	if value == nil {
		return ErrLockFreeNilValue
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

	// Generate new version without passing one
	m.skiplist.Put(key, value)
	return nil
}

// PutWithVersion adds or updates a key-value pair with a specific version
func (m *LockFreeMemTable) PutWithVersion(key, value []byte, version uint64) error {
	if key == nil {
		return ErrLockFreeNilKey
	}
	if value == nil {
		return ErrLockFreeNilValue
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

	// Add to the skiplist
	usedVersion, isNew := m.skiplist.PutWithVersion(key, value, version)

	// Log the operation
	if isNew {
		m.logger.Debug("Added new entry to LockFreeMemTable with version %d, key size: %d, value size: %d",
			usedVersion, len(key), len(value))
	} else {
		m.logger.Debug("Updated entry in LockFreeMemTable with version %d, key size: %d, value size: %d",
			usedVersion, len(key), len(value))
	}

	return nil
}

// Get retrieves a value from the LockFreeMemTable by key
func (m *LockFreeMemTable) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrLockFreeNilKey
	}

	// Try to get from skiplist
	value, _, found := m.skiplist.Get(key)
	if !found {
		// Check if the key exists but is marked as deleted
		// The skiplist's Get method already checks this internally,
		// but we need to specifically identify deleted keys to return the correct error
		node, _ := m.skiplist.findNodeAndPrevs(key)
		if node != nil && atomic.LoadUint32(&node.isDeleted) == 1 {
			// The key exists but is marked as deleted
			return nil, ErrLockFreeKeyNotFound
		}
		return nil, ErrLockFreeKeyNotFound
	}

	return value, nil
}

// Delete marks a key as deleted
func (m *LockFreeMemTable) Delete(key []byte) error {
	if key == nil {
		return ErrLockFreeNilKey
	}

	// Check if the table is flushed
	if atomic.LoadUint32(&m.isFlushed) == 1 {
		return ErrLockFreeMemTableFlushed
	}

	// Delete in skiplist
	m.skiplist.Delete(key)
	return nil
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

		// Convert version to 8-byte array
		versionBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(versionBytes, curr.version)
		entries = append(entries, versionBytes)

		// Convert deletion flag to 1-byte array
		var deletedFlag byte = 0
		if atomic.LoadUint32(&curr.isDeleted) == 1 {
			deletedFlag = 1
		}
		entries = append(entries, []byte{deletedFlag})

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

// Ensure LockFreeMemTable implements the same interface as MemTable
var _ MemTableInterface = (*LockFreeMemTable)(nil)

// MemTableInterface defines the common interface that both MemTable and LockFreeMemTable implement
type MemTableInterface interface {
	Put(key, value []byte) error
	PutWithVersion(key, value []byte, version uint64) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	DeleteWithVersion(key []byte, version uint64) error
	Contains(key []byte) bool
	Size() uint64
	MaxSize() uint64
	EntryCount() int
	IsFull() bool
	MarkFlushed()
	IsFlushed() bool
	GetEntries() [][]byte
	GetEntriesWithMetadata() [][]byte
	GetVersion() uint64
	Clear()
}
