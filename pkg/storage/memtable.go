package storage

import (
	"bytes"
	"errors"
	"math/rand"
	"sync"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// MemTable errors
var (
	ErrKeyNotFound     = errors.New("key not found in MemTable")
	ErrNilValue        = errors.New("cannot add nil value to MemTable")
	ErrNilKey          = errors.New("cannot use nil key in MemTable")
	ErrMemTableFull    = errors.New("MemTable is full")
	ErrMemTableFlushed = errors.New("MemTable has been flushed and is read-only")
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

// skipNode represents a node in the skip list
type skipNode struct {
	key       []byte
	value     []byte
	size      uint64 // size in bytes
	forward   []*skipNode
	isDeleted bool // Marker for lazy deletion
}

// MemTable uses a skip list to store key-value pairs in memory in a sorted order
// It is used to buffer writes before they are flushed to disk as SSTables
// This implementation offers O(log n) insertion and lookup complexity
type MemTable struct {
	mu             sync.RWMutex
	head           *skipNode     // Pointer to the head (sentinel) node
	maxHeight      int           // Maximum height of skip list nodes
	currentHeight  int           // Current height of the skip list
	size           uint64        // Total size in bytes
	count          int           // Number of entries
	maxSize        uint64        // Maximum size in bytes
	isFlushed      bool          // Whether the MemTable has been flushed
	logger         model.Logger  // Logger for operations
	comparator     Comparator    // Function for comparing keys
	currentVersion uint64        // Version counter for tracking changes
}

// NewMemTable creates a new empty MemTable using a skip list data structure
func NewMemTable(config MemTableConfig) *MemTable {
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
	}
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
	if key == nil {
		return ErrNilKey
	}
	if value == nil {
		return ErrNilValue
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isFlushed {
		return ErrMemTableFlushed
	}

	entrySize := uint64(len(key) + len(value))
	
	// Find the node and its predecessors at each level
	node, prevs := m.findNodeAndPrevs(key)

	// If the key already exists
	if node != nil {
		// Handle existing nodes marked as deleted
		if node.isDeleted {
			// Unmark as deleted and update value
			node.isDeleted = false
			node.value = append([]byte{}, value...) // Create a copy
			oldSize := node.size
			node.size = entrySize
			m.size = m.size - oldSize + entrySize
			m.count++  // Increment count since we're reusing a previously deleted node
			m.currentVersion++
			m.logger.Debug("Reactivated deleted entry in MemTable, key size: %d, value size: %d", len(key), len(value))
			return nil
		}

		// Normal update case
		oldSize := node.size
		// Check if the new size exceeds max size
		if m.size - oldSize + entrySize > m.maxSize {
			return ErrMemTableFull
		}
		
		// Update the value and size
		node.value = append([]byte{}, value...) // Create a copy
		node.size = entrySize
		m.size = m.size - oldSize + entrySize
		m.currentVersion++
		m.logger.Debug("Updated entry in MemTable, key size: %d, value size: %d", len(key), len(value))
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
	m.currentVersion++
	
	m.logger.Debug("Added new entry to MemTable, key size: %d, value size: %d", len(key), len(value))
	return nil
}

// Get retrieves a value from the MemTable by key
func (m *MemTable) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

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
func (m *MemTable) Delete(key []byte) error {
	if key == nil {
		return ErrNilKey
	}

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

	// Mark the node as deleted and update size and count
	node.isDeleted = true
	m.size -= node.size
	m.count--
	m.currentVersion++
	
	m.logger.Debug("Deleted entry from MemTable with key size: %d", len(key))
	return nil
}

// Contains checks if a key exists in the MemTable
func (m *MemTable) Contains(key []byte) bool {
	if key == nil {
		return false
	}

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
// This is typically used when flushing the MemTable to an SSTable
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
	
	m.logger.Info("MemTable cleared")
}