package storage

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sync"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// MemTable errors
var (
	// Use the unified error constants from model package
	ErrKeyNotFound     = model.ErrKeyNotFound
	ErrNilValue        = model.ErrNilValue
	ErrNilKey          = model.ErrNilKey
	ErrMemTableFull    = model.ErrMemTableFull
	ErrMemTableFlushed = model.ErrMemTableFlushed
)

// Comparator is a function type that compares two byte slices
type Comparator func(a, b []byte) int

// DefaultComparator compares two byte slices lexicographically
func DefaultComparator(a, b []byte) int {
	return bytes.Compare(a, b)
}


// Config holds configuration options for the MemTable
type MemTableConfig struct {
	// MaxSize is the maximum size in bytes the MemTable can hold before flushing is required
	MaxSize uint64
	// Logger is used to log MemTable operations
	Logger model.Logger
	// Comparator is used to compare keys in the MemTable
	Comparator Comparator
	// MaxHeight is the maximum height of the skip list (optional)
	MaxHeight int
}

// DefaultConfig returns a default configuration for MemTable
func DefaultConfig() MemTableConfig {
	return MemTableConfig{
		MaxSize:    32 * 1024 * 1024, // 32MB
		Logger:     model.DefaultLoggerInstance,
		Comparator: DefaultComparator,
		MaxHeight:  12, // Good for millions of entries
	}
}

// ValidateMemTableConfig ensures a MemTable configuration has valid values
// and fills in defaults where needed - common to all MemTable implementations
func ValidateMemTableConfig(config MemTableConfig) MemTableConfig {
	if config.Logger == nil {
		config.Logger = model.DefaultLoggerInstance
	}
	if config.Comparator == nil {
		config.Comparator = DefaultComparator
	}
	if config.MaxSize == 0 {
		config.MaxSize = DefaultConfig().MaxSize
	}
	if config.MaxHeight <= 0 {
		config.MaxHeight = DefaultConfig().MaxHeight
	}
	return config
}

// ValidateKeyValue validates key and value parameters for MemTable operations
// Returns appropriate errors if invalid
func ValidateKeyValue(key, value []byte) error {
	if key == nil {
		return ErrNilKey
	}
	if value == nil {
		return ErrNilValue
	}
	return nil
}

// FormatMetadataEntry formats version and deletion flag into byte slices for metadata entries
func FormatMetadataEntry(version uint64, isDeleted bool) ([]byte, []byte) {
	// Convert version to 8-byte array
	versionBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(versionBytes, version)

	// Convert deletion flag to 1-byte array
	var deletedFlag byte = 0
	if isDeleted {
		deletedFlag = 1
	}

	return versionBytes, []byte{deletedFlag}
}

// skipNode represents a node in the skip list
type skipNode struct {
	key       []byte
	value     []byte
	size      uint64 // size in bytes
	forward   []*skipNode
	isDeleted bool   // Marker for lazy deletion
	version   uint64 // Version number for this record
}

// MemTable uses a skip list to store key-value pairs in memory in a sorted order
// It is used to buffer writes before they are flushed to disk as SSTables
// This implementation offers O(log n) insertion and lookup complexity
// Optimized for single-writer multiple-reader model per transaction
type MemTable struct {
	mu             sync.RWMutex // Used to protect concurrent access
	head           *skipNode    // Pointer to the head (sentinel) node
	maxHeight      int          // Maximum height of skip list nodes
	currentHeight  int          // Current height of the skip list
	size           uint64       // Total size in bytes
	count          int          // Number of entries
	maxSize        uint64       // Maximum size in bytes
	isFlushed      bool         // Whether the MemTable has been flushed
	logger         model.Logger // Logger for operations
	comparator     Comparator   // Function for comparing keys
	currentVersion uint64       // Version counter for tracking changes
	txContext      uint64       // Current transaction context ID
}

// NewMemTable creates a new empty MemTable using a skip list data structure
func NewMemTable(config MemTableConfig) *MemTable {
	// Validate and apply defaults to config
	config = ValidateMemTableConfig(config)

	// Create a head node with the maximum height
	head := &skipNode{
		forward: make([]*skipNode, config.MaxHeight),
	}

	return &MemTable{
		head:           head,
		maxHeight:      config.MaxHeight,
		currentHeight:  1,
		size:           0,
		count:          0,
		maxSize:        config.MaxSize,
		isFlushed:      false,
		logger:         config.Logger,
		comparator:     config.Comparator,
		currentVersion: 1,
		txContext:      0, // No transaction context initially
	}
}

// SetTransactionContext assigns a transaction context ID to this memtable
// This is used to track which transaction is operating on this memtable
func (m *MemTable) SetTransactionContext(txID uint64) {
	m.txContext = txID
}

// GetTransactionContext returns the current transaction context ID
func (m *MemTable) GetTransactionContext() uint64 {
	return m.txContext
}

// randomHeight generates a random height for a new node
// Uses a probabilistic distribution to ensure ~1/4 nodes have height 1,
// ~1/16 have height 2, etc.
func (m *MemTable) randomHeight() int {
	const probability = 0.25 // Probability to increase height
	height := 1

	// With probability 1/4, increase height until reaching max height
	for height < m.maxHeight && rand.Float64() < probability {
		height++
	}

	return height
}

// findNodeAndPrevs searches for a key in the skip list
// Returns the node with the key (or nil if not found) and an array of predecessor nodes
func (m *MemTable) findNodeAndPrevs(key []byte) (*skipNode, []*skipNode) {
	// Initialize an array to store the previous nodes at each level
	prevs := make([]*skipNode, m.maxHeight)
	current := m.head

	// Start from the highest level of the skip list
	for i := m.currentHeight - 1; i >= 0; i-- {
		// Move forward at the current level as long as the key is greater
		for current.forward[i] != nil && m.comparator(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		// Record the predecessor at this level
		prevs[i] = current
	}

	// If we found a matching key at level 0, return the node
	var node *skipNode
	if current.forward[0] != nil && m.comparator(current.forward[0].key, key) == 0 {
		node = current.forward[0]
	}

	return node, prevs
}

// Put adds or updates a key-value pair in the MemTable
func (m *MemTable) Put(key, value []byte) error {
	return m.PutWithVersion(key, value, 0)
}

// PutWithVersion adds or updates a key-value pair in the MemTable with a specific version
// If version is 0, it will use the next auto-incremented version
// This implementation is optimized for the single-writer model
func (m *MemTable) PutWithVersion(key, value []byte, version uint64) error {
	if err := ValidateKeyValue(key, value); err != nil {
		return err
	}

	// In single-writer model, we still need a write lock to protect readers
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isFlushed {
		return ErrMemTableFlushed
	}

	entrySize := uint64(len(key) + len(value))

	// Find the node and its predecessors at each level
	node, prevs := m.findNodeAndPrevs(key)

	// Determine version to use - in single-writer model, we can use the
	// transaction context directly if provided
	currentVersion := version
	if currentVersion == 0 {
		// Use transaction context if available, or auto-increment
		if m.txContext > 0 {
			currentVersion = m.txContext
		} else {
			// Auto-increment version if no transaction context and no version specified
			m.currentVersion++
			currentVersion = m.currentVersion
		}
	} else if currentVersion > m.currentVersion {
		// If a higher version is manually specified, update our counter
		m.currentVersion = currentVersion
	}

	// If the key already exists
	if node != nil {
		// Handle existing nodes marked as deleted
		if node.isDeleted {
			// Unmark as deleted and update value with new version
			node.isDeleted = false
			node.value = append([]byte{}, value...) // Create a copy
			node.version = currentVersion
			oldSize := node.size
			node.size = entrySize
			m.size = m.size - oldSize + entrySize
			m.count++ // Increment count since we're reusing a previously deleted node
			m.logger.Debug("Reactivated deleted entry in MemTable with tx %d, version %d, key size: %d, value size: %d",
				m.txContext, currentVersion, len(key), len(value))
			return nil
		}

		// Normal update case
		oldSize := node.size
		// Check if the new size exceeds max size
		if m.size-oldSize+entrySize > m.maxSize {
			return ErrMemTableFull
		}

		// Update the value, size, and version
		node.value = append([]byte{}, value...) // Create a copy
		node.version = currentVersion
		node.size = entrySize
		m.size = m.size - oldSize + entrySize
		m.logger.Debug("Updated entry in MemTable with tx %d, version %d, key size: %d, value size: %d",
			m.txContext, currentVersion, len(key), len(value))
		return nil
	}

	// Check if adding this entry would exceed max size
	if m.size+entrySize > m.maxSize {
		return ErrMemTableFull
	}

	// Create a new node with random height
	height := m.randomHeight()
	newNode := &skipNode{
		key:     append([]byte{}, key...),   // Create a copy
		value:   append([]byte{}, value...), // Create a copy
		size:    entrySize,
		forward: make([]*skipNode, height),
		version: currentVersion,
	}

	// Update the skip list height if necessary
	if height > m.currentHeight {
		for i := m.currentHeight; i < height; i++ {
			prevs[i] = m.head
		}
		m.currentHeight = height
	}

	// Insert the new node by updating the forward pointers
	for i := 0; i < height; i++ {
		newNode.forward[i] = prevs[i].forward[i]
		prevs[i].forward[i] = newNode
	}

	// Update size and count
	m.size += entrySize
	m.count++

	m.logger.Debug("Added new entry to MemTable with tx %d, version %d, key size: %d, value size: %d",
		m.txContext, currentVersion, len(key), len(value))
	return nil
}

// Get retrieves a value from the MemTable by key
// In single-writer model, we still need read locks to ensure readers
// don't see partial updates from the writer
func (m *MemTable) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}

	// Read lock to protect against concurrent writer
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find the node with the key
	node, _ := m.findNodeAndPrevs(key)
	if node == nil || node.isDeleted {
		return nil, ErrKeyNotFound
	}

	// Return a copy of the value to ensure isolation
	return append([]byte{}, node.value...), nil
}

// Delete marks a key as deleted by setting the isDeleted flag
// In this implementation, we use lazy deletion to avoid reorganizing the skip list
// We also assign a new version number to the deletion to track its order in the version history
func (m *MemTable) Delete(key []byte) error {
	return m.DeleteWithVersion(key, 0)
}

// DeleteWithVersion marks a key as deleted with a specific version number
// If version is 0, it will use the next auto-incremented version
// Optimized for single-writer model but with reader protection
func (m *MemTable) DeleteWithVersion(key []byte, version uint64) error {
	if key == nil {
		return ErrNilKey
	}

	// Write lock to protect concurrent readers
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isFlushed {
		return ErrMemTableFlushed
	}

	// Find the node with the key
	node, _ := m.findNodeAndPrevs(key)
	if node == nil || node.isDeleted {
		// Key doesn't exist or already deleted, nothing to do
		return nil
	}

	// Determine version to use
	currentVersion := version
	if currentVersion == 0 {
		// Use transaction context if available, or auto-increment
		if m.txContext > 0 {
			currentVersion = m.txContext
		} else {
			// Auto-increment version if no transaction context and no version specified
			m.currentVersion++
			currentVersion = m.currentVersion
		}
	} else if currentVersion > m.currentVersion {
		// If a higher version is manually specified, update our counter
		m.currentVersion = currentVersion
	}

	// Mark the node as deleted, update version, and update size and count
	node.isDeleted = true
	node.version = currentVersion
	m.size -= node.size
	m.count--

	m.logger.Debug("Deleted entry from MemTable with tx %d, version %d, key size: %d", 
		m.txContext, currentVersion, len(key))
	return nil
}

// Contains checks if a key exists in the MemTable
func (m *MemTable) Contains(key []byte) bool {
	if key == nil {
		return false
	}

	// Read lock to ensure we don't see partial updates
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find the node with the key
	node, _ := m.findNodeAndPrevs(key)
	return node != nil && !node.isDeleted
}

// Size returns the current size of the MemTable in bytes
func (m *MemTable) Size() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.size
}

// MaxSize returns the maximum size of the MemTable in bytes
// This is a config value that doesn't change, so no locking needed
func (m *MemTable) MaxSize() uint64 {
	return m.maxSize
}

// EntryCount returns the number of entries in the MemTable
func (m *MemTable) EntryCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.count
}

// IsFull returns true if the MemTable has reached its maximum size
func (m *MemTable) IsFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.size >= m.maxSize
}

// MarkFlushed marks the MemTable as flushed
// After being marked as flushed, the MemTable becomes read-only
func (m *MemTable) MarkFlushed() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.isFlushed = true
	m.logger.Info("MemTable marked as flushed with %d entries and %d bytes", m.count, m.size)
}

// IsFlushed returns true if the MemTable has been flushed
func (m *MemTable) IsFlushed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.isFlushed
}

// GetEntries returns all entries in the MemTable in sorted order
// with only key-value pairs (for backward compatibility with tests)
// This does not include deleted entries
func (m *MemTable) GetEntries() [][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Create a slice to hold key-value pairs (2 entries per key-value pair)
	entries := make([][]byte, 0, m.count*2)

	// Traverse the skip list at level 0 (which contains all nodes)
	current := m.head.forward[0]
	for current != nil {
		if !current.isDeleted {
			// Add key and value to the results
			entries = append(entries, append([]byte{}, current.key...))
			entries = append(entries, append([]byte{}, current.value...))
		}
		current = current.forward[0]
	}

	return entries
}

// GetEntriesWithMetadata returns all entries in the MemTable in sorted order
// including metadata like version and deletion flag
// This is used for the new versioned record implementation
func (m *MemTable) GetEntriesWithMetadata() [][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Create a slice to hold entries - each entry consists of:
	// [0]: key
	// [1]: value
	// [2]: version (as 8-byte binary)
	// [3]: isDeleted flag (as 1-byte binary)
	// So 4 entries per key-value pair, not just 2
	totalEntries := m.count

	// Count all nodes, including deleted ones (that haven't been flushed yet)
	if m.head != nil {
		totalEntries = 0
		current := m.head.forward[0]
		for current != nil {
			totalEntries++
			current = current.forward[0]
		}
	}

	entries := make([][]byte, 0, totalEntries*4)

	// Traverse the skip list at level 0 (which contains all nodes)
	current := m.head.forward[0]
	for current != nil {
		// Add key, value, version, and deletion flag to the results
		entries = append(entries, append([]byte{}, current.key...))
		entries = append(entries, append([]byte{}, current.value...))

		// Format and add version and deletion flag
		versionBytes, deletedFlag := FormatMetadataEntry(current.version, current.isDeleted)
		entries = append(entries, versionBytes)
		entries = append(entries, deletedFlag)

		current = current.forward[0]
	}

	return entries
}

// GetVersion returns the current version of the MemTable
// The version increases whenever the MemTable is modified
func (m *MemTable) GetVersion() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return m.currentVersion
}

// Clear removes all entries from the MemTable
func (m *MemTable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Reset the skip list
	head := &skipNode{
		forward: make([]*skipNode, m.maxHeight),
	}

	m.head = head
	m.currentHeight = 1
	m.size = 0
	m.count = 0
	m.isFlushed = false
	m.currentVersion++
	
	// Reset transaction context
	m.txContext = 0

	m.logger.Info("MemTable cleared")
}

// GetAllKeys returns all keys in the MemTable
// This is mainly for testing and maintenance operations
func (m *MemTable) GetAllKeys() [][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	result := make([][]byte, 0, m.count)
	
	// Start from level 0 (all nodes appear at level 0)
	current := m.head.forward[0]
	for current != nil {
		// Skip deleted keys
		if !current.isDeleted {
			// Make a copy of the key to ensure isolation
			key := make([]byte, len(current.key))
			copy(key, current.key)
			result = append(result, key)
		}
		current = current.forward[0]
	}
	
	return result
}

// GetKeysWithPrefix returns all keys that start with the given prefix
func (m *MemTable) GetKeysWithPrefix(prefix []byte) [][]byte {
	if prefix == nil {
		return [][]byte{}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	
	// Create a slice to hold the matching keys
	keys := make([][]byte, 0)

	// Start at the lowest level which contains all nodes
	current := m.head.forward[0]
	prefixLen := len(prefix)

	// Traverse the skip list at level 0 (which contains all nodes)
	for current != nil {
		// Skip deleted nodes
		if current.isDeleted {
			current = current.forward[0]
			continue
		}

		// Check if this key has the prefix
		if current.key != nil && len(current.key) >= prefixLen {
			hasPrefix := true
			for i := 0; i < prefixLen; i++ {
				if current.key[i] != prefix[i] {
					hasPrefix = false
					break
				}
			}

			if hasPrefix {
				// Add a copy of the key to avoid race conditions
				keyCopy := make([]byte, len(current.key))
				copy(keyCopy, current.key)
				keys = append(keys, keyCopy)
			}

			// If the key is already lexicographically greater than any key that could start with the prefix,
			// we can stop (assuming keys are sorted)
			// This block is commented out because it's causing issues with prefix scans
			// The condition was terminating scanning too early when a key partially matched the prefix
			// but had subsequent characters that would make it sort after the prefix
			// 
			// We'll let the loop continue until we reach the end of the list instead of
			// trying to optimize with early termination
			// if len(current.key) >= prefixLen && m.comparator(current.key[:prefixLen], prefix) > 0 {
			// 	break
			// }
		}

		current = current.forward[0]
	}

	return keys
}

// Clone creates a deep copy of the MemTable for use in snapshots
func (m *MemTable) Clone() *MemTable {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a new empty MemTable with the same configuration
	clone := &MemTable{
		head:           &skipNode{forward: make([]*skipNode, m.maxHeight)},
		maxHeight:      m.maxHeight,
		currentHeight:  1,
		size:           0,
		count:          0,
		maxSize:        m.maxSize,
		isFlushed:      m.isFlushed,
		logger:         m.logger,
		comparator:     m.comparator,
		currentVersion: m.currentVersion,
		txContext:      m.txContext, // Copy transaction context
	}

	// IMPROVED IMPLEMENTATION: Directly copy only non-deleted entries
	// This ensures snapshots don't see deleted keys, which is critical
	// for proper isolation especially during transaction rollback
	current := m.head.forward[0]
	for current != nil {
		// Only copy non-deleted entries
		if !current.isDeleted {
			// Make copies of the key and value to ensure isolation
			keyClone := make([]byte, len(current.key))
			copy(keyClone, current.key)
			
			valueClone := make([]byte, len(current.value))
			copy(valueClone, current.value)
			
			// Add to the clone, preserving the version
			clone.PutWithVersion(keyClone, valueClone, current.version)
		}
		
		current = current.forward[0]
	}
	
	// For debugging/metrics
	m.logger.Debug("Cloned MemTable: original had %d entries, clone has %d entries (only non-deleted)",
		m.count, clone.count)

	return clone
}

