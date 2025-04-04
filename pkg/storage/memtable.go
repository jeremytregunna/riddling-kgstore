package storage

import (
	"bytes"
	"errors"
	"sort"
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

// MemTable stores key-value pairs in memory in a sorted order
// It is used to buffer writes before they are flushed to disk as SSTables
type MemTable struct {
	mu             sync.RWMutex
	entries        []entry
	size           uint64
	maxSize        uint64
	isFlushed      bool
	logger         model.Logger
	comparator     Comparator
	currentVersion uint64
}

// entry represents a key-value pair in the MemTable
type entry struct {
	key   []byte
	value []byte
	size  uint64 // size in bytes
}

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
}

// DefaultConfig returns a default configuration for MemTable
func DefaultConfig() MemTableConfig {
	return MemTableConfig{
		MaxSize:    32 * 1024 * 1024, // 32MB
		Logger:     model.DefaultLoggerInstance,
		Comparator: DefaultComparator,
	}
}

// NewMemTable creates a new empty MemTable
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

	return &MemTable{
		entries:        make([]entry, 0),
		size:           0,
		maxSize:        config.MaxSize,
		isFlushed:      false,
		logger:         config.Logger,
		comparator:     config.Comparator,
		currentVersion: 1,
	}
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
	
	// Check if adding this entry would exceed max size
	if m.size+entrySize > m.maxSize {
		return ErrMemTableFull
	}

	// Check if the key already exists
	idx := m.findKey(key)
	if idx >= 0 {
		// Replace the existing entry
		oldSize := m.entries[idx].size
		m.size = m.size - oldSize + entrySize
		m.entries[idx] = entry{
			key:   append([]byte{}, key...),   // Create a copy
			value: append([]byte{}, value...), // Create a copy
			size:  entrySize,
		}
		m.currentVersion++
		m.logger.Debug("Updated entry in MemTable, key size: %d, value size: %d", len(key), len(value))
		return nil
	}

	// Insert new entry (will be sorted later)
	m.entries = append(m.entries, entry{
		key:   append([]byte{}, key...),   // Create a copy
		value: append([]byte{}, value...), // Create a copy
		size:  entrySize,
	})
	m.size += entrySize
	m.currentVersion++
	
	// Sort the entries whenever we add a new key
	sort.Slice(m.entries, func(i, j int) bool {
		return m.comparator(m.entries[i].key, m.entries[j].key) < 0
	})

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

	idx := m.findKey(key)
	if idx < 0 {
		return nil, ErrKeyNotFound
	}

	// Return a copy of the value
	return append([]byte{}, m.entries[idx].value...), nil
}

// Delete marks a key as deleted by storing a tombstone value
// In this implementation, we simply remove the key from the MemTable
func (m *MemTable) Delete(key []byte) error {
	if key == nil {
		return ErrNilKey
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isFlushed {
		return ErrMemTableFlushed
	}

	idx := m.findKey(key)
	if idx < 0 {
		// Key doesn't exist, nothing to delete
		return nil
	}

	// Remove the entry
	m.size -= m.entries[idx].size
	m.entries = append(m.entries[:idx], m.entries[idx+1:]...)
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

	return m.findKey(key) >= 0
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
	
	return len(m.entries)
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
	m.logger.Info("MemTable marked as flushed with %d entries and %d bytes", len(m.entries), m.size)
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
	
	// Create a slice to hold key-value pairs
	entries := make([][]byte, len(m.entries)*2)
	
	// Populate the slice with keys and values
	for i, e := range m.entries {
		// Keys are at even indices
		entries[i*2] = append([]byte{}, e.key...)
		// Values are at odd indices
		entries[i*2+1] = append([]byte{}, e.value...)
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

// findKey returns the index of the entry with the given key or -1 if not found
// This uses binary search since entries are kept sorted
// The caller must hold at least a read lock
func (m *MemTable) findKey(key []byte) int {
	// Binary search
	i, j := 0, len(m.entries)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow
		if m.comparator(m.entries[h].key, key) < 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	
	if i < len(m.entries) && m.comparator(m.entries[i].key, key) == 0 {
		return i
	}
	
	return -1
}

// Clear removes all entries from the MemTable
func (m *MemTable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.entries = make([]entry, 0)
	m.size = 0
	m.isFlushed = false
	m.currentVersion++
	
	m.logger.Info("MemTable cleared")
}