package storage

import (
	"encoding/binary"
	"math/rand"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// SingleWriterMemTable is a MemTable optimized for the single-writer model.
// It retains the same interface as MemTable but removes concurrency controls
// and optimizes for the scenario where only one writer can be active at a time.
type SingleWriterMemTable struct {
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
	txContext      uint64       // Transaction context ID (0 if not in transaction)
}

// NewSingleWriterMemTable creates a new MemTable optimized for single-writer model
func NewSingleWriterMemTable(config MemTableConfig) *SingleWriterMemTable {
	// Validate and apply defaults to config
	config = ValidateMemTableConfig(config)

	// Create a head node with the maximum height
	head := &skipNode{
		forward: make([]*skipNode, config.MaxHeight),
	}

	return &SingleWriterMemTable{
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
		txContext:      0,
	}
}

// SetTransactionContext assigns a transaction context to this memtable
// Used to track which transaction a memtable belongs to for transaction awareness
func (m *SingleWriterMemTable) SetTransactionContext(txID uint64) {
	m.txContext = txID
}

// GetTransactionContext retrieves the transaction context ID
func (m *SingleWriterMemTable) GetTransactionContext() uint64 {
	return m.txContext
}

// randomHeight generates a random height for a new node
// Uses a probabilistic distribution to ensure ~1/4 nodes have height 1,
// ~1/16 have height 2, etc.
func (m *SingleWriterMemTable) randomHeight() int {
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
func (m *SingleWriterMemTable) findNodeAndPrevs(key []byte) (*skipNode, []*skipNode) {
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
func (m *SingleWriterMemTable) Put(key, value []byte) error {
	return m.PutWithVersion(key, value, 0)
}

// PutWithVersion adds or updates a key-value pair in the MemTable with a specific version
// If version is 0, it will use the next auto-incremented version
func (m *SingleWriterMemTable) PutWithVersion(key, value []byte, version uint64) error {
	if err := ValidateKeyValue(key, value); err != nil {
		return err
	}

	if m.isFlushed {
		return ErrMemTableFlushed
	}

	entrySize := uint64(len(key) + len(value))

	// Find the node and its predecessors at each level
	node, prevs := m.findNodeAndPrevs(key)

	// Determine version to use
	currentVersion := version
	if currentVersion == 0 {
		// Auto-increment version if not specified
		m.currentVersion++
		currentVersion = m.currentVersion
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
			m.logger.Debug("Reactivated deleted entry in SingleWriterMemTable with version %d, key size: %d, value size: %d",
				currentVersion, len(key), len(value))
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
		m.logger.Debug("Updated entry in SingleWriterMemTable with version %d, key size: %d, value size: %d",
			currentVersion, len(key), len(value))
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

	m.logger.Debug("Added new entry to SingleWriterMemTable with version %d, key size: %d, value size: %d",
		currentVersion, len(key), len(value))
	return nil
}

// Get retrieves a value from the MemTable by key
func (m *SingleWriterMemTable) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}

	// Find the node with the key
	node, _ := m.findNodeAndPrevs(key)
	if node == nil || node.isDeleted {
		return nil, ErrKeyNotFound
	}

	// Return a copy of the value
	return append([]byte{}, node.value...), nil
}

// Delete marks a key as deleted by setting the isDeleted flag
// In this implementation, we use lazy deletion to avoid reorganizing the skip list
// We also assign a new version number to the deletion to track its order in the version history
func (m *SingleWriterMemTable) Delete(key []byte) error {
	return m.DeleteWithVersion(key, 0)
}

// DeleteWithVersion marks a key as deleted with a specific version number
// If version is 0, it will use the next auto-incremented version
func (m *SingleWriterMemTable) DeleteWithVersion(key []byte, version uint64) error {
	if key == nil {
		return ErrNilKey
	}

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
		// Auto-increment version if not specified
		m.currentVersion++
		currentVersion = m.currentVersion
	} else if currentVersion > m.currentVersion {
		// If a higher version is manually specified, update our counter
		m.currentVersion = currentVersion
	}

	// Mark the node as deleted, update version, and update size and count
	node.isDeleted = true
	node.version = currentVersion
	m.size -= node.size
	m.count--

	m.logger.Debug("Deleted entry from SingleWriterMemTable with version %d, key size: %d", currentVersion, len(key))
	return nil
}

// Contains checks if a key exists in the MemTable
func (m *SingleWriterMemTable) Contains(key []byte) bool {
	if key == nil {
		return false
	}

	// Find the node with the key
	node, _ := m.findNodeAndPrevs(key)
	return node != nil && !node.isDeleted
}

// Size returns the current size of the MemTable in bytes
func (m *SingleWriterMemTable) Size() uint64 {
	return m.size
}

// MaxSize returns the maximum size of the MemTable in bytes
func (m *SingleWriterMemTable) MaxSize() uint64 {
	return m.maxSize
}

// EntryCount returns the number of entries in the MemTable
func (m *SingleWriterMemTable) EntryCount() int {
	return m.count
}

// IsFull returns true if the MemTable has reached its maximum size
func (m *SingleWriterMemTable) IsFull() bool {
	return m.size >= m.maxSize
}

// MarkFlushed marks the MemTable as flushed
// After being marked as flushed, the MemTable becomes read-only
func (m *SingleWriterMemTable) MarkFlushed() {
	m.isFlushed = true
	m.logger.Info("SingleWriterMemTable marked as flushed with %d entries and %d bytes", m.count, m.size)
}

// IsFlushed returns true if the MemTable has been flushed
func (m *SingleWriterMemTable) IsFlushed() bool {
	return m.isFlushed
}

// GetEntries returns all entries in the MemTable in sorted order
// with only key-value pairs (for backward compatibility with tests)
// This does not include deleted entries
func (m *SingleWriterMemTable) GetEntries() [][]byte {
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
func (m *SingleWriterMemTable) GetEntriesWithMetadata() [][]byte {
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
func (m *SingleWriterMemTable) GetVersion() uint64 {
	return m.currentVersion
}

// Clear removes all entries from the MemTable
func (m *SingleWriterMemTable) Clear() {
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

	m.logger.Info("SingleWriterMemTable cleared")
}

// GetKeysWithPrefix returns all keys that start with the given prefix
func (m *SingleWriterMemTable) GetKeysWithPrefix(prefix []byte) [][]byte {
	if prefix == nil {
		return [][]byte{}
	}

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

			// If the key is already greater than the prefix, we can stop
			// (assuming keys are sorted)
			if len(current.key) >= prefixLen && m.comparator(current.key[:prefixLen], prefix) > 0 {
				break
			}
		}

		current = current.forward[0]
	}

	return keys
}

// AddReadCache implements an optimization for read-heavy workloads
// by providing a cache for frequently accessed keys
// This is a noop in the basic implementation and can be extended in subclasses
func (m *SingleWriterMemTable) AddReadCache(key []byte, value []byte) {
	// This is a no-op in the base implementation
	// Subclasses can override this to implement caching
}

// GetFromReadCache attempts to retrieve a value from the read cache
// Returns nil, false if not found in cache (cache miss)
func (m *SingleWriterMemTable) GetFromReadCache(key []byte) ([]byte, bool) {
	// This is a no-op in the base implementation
	// Subclasses can override this to implement caching
	return nil, false
}

// Clone creates a deep copy of the MemTable for use in snapshots
func (m *SingleWriterMemTable) Clone() *SingleWriterMemTable {
	// Create a new empty MemTable with the same configuration
	clone := &SingleWriterMemTable{
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
		txContext:      m.txContext,
	}

	// Go through all entries and add them to the clone
	entries := m.GetEntriesWithMetadata()
	
	// Process entries in groups of 4 (key, value, version, deletionFlag)
	for i := 0; i < len(entries); i += 4 {
		if i+3 < len(entries) {
			key := entries[i]
			value := entries[i+1]
			versionBytes := entries[i+2]
			deletedFlag := entries[i+3]
			
			// Extract version from version bytes
			version := binary.LittleEndian.Uint64(versionBytes)
			
			// Extract deletion flag
			isDeleted := deletedFlag[0] == 1
			
			// Create a new node with the same data
			height := m.randomHeight() // We'll use a new random height for each node
			newNode := &skipNode{
				key:       append([]byte{}, key...),
				value:     append([]byte{}, value...),
				size:      uint64(len(key) + len(value)),
				forward:   make([]*skipNode, height),
				isDeleted: isDeleted,
				version:   version,
			}
			
			// Find the insertion point in the clone's skip list
			prevs := make([]*skipNode, clone.maxHeight)
			current := clone.head
			
			for i := clone.currentHeight - 1; i >= 0; i-- {
				for current.forward[i] != nil && clone.comparator(current.forward[i].key, key) < 0 {
					current = current.forward[i]
				}
				prevs[i] = current
			}
			
			// Update height if necessary
			if height > clone.currentHeight {
				for i := clone.currentHeight; i < height; i++ {
					prevs[i] = clone.head
				}
				clone.currentHeight = height
			}
			
			// Insert the node
			for i := 0; i < height; i++ {
				newNode.forward[i] = prevs[i].forward[i]
				prevs[i].forward[i] = newNode
			}
			
			// Update size and count if not deleted
			if !isDeleted {
				clone.size += newNode.size
				clone.count++
			}
		}
	}

	return clone
}

