package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

func TestSingleWriterMemTableBasicOperations(t *testing.T) {
	// Create a new SingleWriterMemTable with a small max size for testing
	memTable := NewSingleWriterMemTable(MemTableConfig{
		MaxSize:    1024, // 1KB
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Test empty state
	if memTable.EntryCount() != 0 {
		t.Errorf("New SingleWriterMemTable should have 0 entries, got %d", memTable.EntryCount())
	}
	if memTable.Size() != 0 {
		t.Errorf("New SingleWriterMemTable should have size 0, got %d", memTable.Size())
	}
	if memTable.IsFull() {
		t.Error("New SingleWriterMemTable should not be full")
	}
	if memTable.IsFlushed() {
		t.Error("New SingleWriterMemTable should not be flushed")
	}

	// Test Put and Get
	key1 := []byte("key1")
	val1 := []byte("value1")

	err := memTable.Put(key1, val1)
	if err != nil {
		t.Errorf("Failed to put key-value pair: %v", err)
	}

	// Check transaction context
	if memTable.GetTransactionContext() != 0 {
		t.Errorf("Expected default transaction context to be 0, got %d", memTable.GetTransactionContext())
	}
	
	// Set and check transaction context
	testTxID := uint64(12345)
	memTable.SetTransactionContext(testTxID)
	if memTable.GetTransactionContext() != testTxID {
		t.Errorf("Expected transaction context to be %d, got %d", testTxID, memTable.GetTransactionContext())
	}

	// Check entry count and size
	if memTable.EntryCount() != 1 {
		t.Errorf("SingleWriterMemTable should have 1 entry, got %d", memTable.EntryCount())
	}
	expectedSize := uint64(len(key1) + len(val1))
	if memTable.Size() != expectedSize {
		t.Errorf("SingleWriterMemTable size should be %d, got %d", expectedSize, memTable.Size())
	}

	// Test Get
	retrieved, err := memTable.Get(key1)
	if err != nil {
		t.Errorf("Failed to get value for key: %v", err)
	}
	if !bytes.Equal(retrieved, val1) {
		t.Errorf("Retrieved value %q does not match expected %q", retrieved, val1)
	}

	// Test Contains
	if !memTable.Contains(key1) {
		t.Errorf("SingleWriterMemTable should contain key %q", key1)
	}
	if memTable.Contains([]byte("nonexistent")) {
		t.Error("SingleWriterMemTable should not contain nonexistent key")
	}

	// Test updating a value
	newVal1 := []byte("newvalue1")
	err = memTable.Put(key1, newVal1)
	if err != nil {
		t.Errorf("Failed to update value: %v", err)
	}

	// Check that entry count is still 1
	if memTable.EntryCount() != 1 {
		t.Errorf("SingleWriterMemTable should still have 1 entry after update, got %d", memTable.EntryCount())
	}

	// Check that we get the updated value
	retrieved, err = memTable.Get(key1)
	if err != nil {
		t.Errorf("Failed to get updated value: %v", err)
	}
	if !bytes.Equal(retrieved, newVal1) {
		t.Errorf("Retrieved updated value %q does not match expected %q", retrieved, newVal1)
	}

	// Test Delete
	err = memTable.Delete(key1)
	if err != nil {
		t.Errorf("Failed to delete key: %v", err)
	}

	// Check entry count after delete
	if memTable.EntryCount() != 0 {
		t.Errorf("SingleWriterMemTable should have 0 entries after delete, got %d", memTable.EntryCount())
	}

	// Check that the key no longer exists
	if memTable.Contains(key1) {
		t.Error("SingleWriterMemTable should not contain deleted key")
	}

	// Try to get deleted key
	_, err = memTable.Get(key1)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestSingleWriterMemTableReadCache(t *testing.T) {
	// Test the read cache implementation
	memTable := NewSingleWriterMemTable(MemTableConfig{
		MaxSize:    1024,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Add some data
	key1 := []byte("key1")
	val1 := []byte("value1")
	memTable.Put(key1, val1)

	// Try the cache functions (should be no-ops in base implementation)
	memTable.AddReadCache(key1, val1)
	
	// Get from cache should always miss in base implementation
	cachedVal, found := memTable.GetFromReadCache(key1)
	if found {
		t.Error("Base implementation should not find anything in ReadCache")
	}
	if cachedVal != nil {
		t.Errorf("Expected nil value from GetFromReadCache, got %v", cachedVal)
	}
}

func TestSingleWriterMemTableSorting(t *testing.T) {
	// Create a new SingleWriterMemTable
	memTable := NewSingleWriterMemTable(MemTableConfig{
		MaxSize:    1024,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Add keys in non-sorted order
	keys := [][]byte{
		[]byte("zzz"),
		[]byte("aaa"),
		[]byte("mmm"),
		[]byte("ccc"),
	}

	for i, key := range keys {
		value := []byte{byte(i)}
		err := memTable.Put(key, value)
		if err != nil {
			t.Errorf("Failed to put key %q: %v", key, err)
		}
	}

	// Check entry count
	if memTable.EntryCount() != len(keys) {
		t.Errorf("Expected %d entries, got %d", len(keys), memTable.EntryCount())
	}

	// Get all entries and check order
	entries := memTable.GetEntries()
	if len(entries) != len(keys)*2 {
		t.Errorf("Expected %d entries (key-value pairs), got %d", len(keys)*2, len(entries))
	}

	// Check that keys are in sorted order
	var retrievedKeys [][]byte
	for i := 0; i < len(entries); i += 2 {
		retrievedKeys = append(retrievedKeys, entries[i])
	}

	// Expected sorted keys
	expectedKeys := [][]byte{
		[]byte("aaa"),
		[]byte("ccc"),
		[]byte("mmm"),
		[]byte("zzz"),
	}

	for i, expectedKey := range expectedKeys {
		if !bytes.Equal(retrievedKeys[i], expectedKey) {
			t.Errorf("At position %d, expected key %q, got %q", i, expectedKey, retrievedKeys[i])
		}
	}
}

func TestSingleWriterMemTableGetKeysWithPrefix(t *testing.T) {
	// Create a new SingleWriterMemTable
	memTable := NewSingleWriterMemTable(MemTableConfig{
		MaxSize:    1024,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Add keys with different prefixes
	keys := [][]byte{
		[]byte("user:1"),
		[]byte("user:2"),
		[]byte("user:3"),
		[]byte("product:1"),
		[]byte("product:2"),
	}

	for i, key := range keys {
		value := []byte(fmt.Sprintf("value-%d", i))
		err := memTable.Put(key, value)
		if err != nil {
			t.Errorf("Failed to put key %q: %v", key, err)
		}
	}

	// Test prefix search for "user:"
	userPrefix := []byte("user:")
	userKeys := memTable.GetKeysWithPrefix(userPrefix)
	
	// Should find 3 user keys
	if len(userKeys) != 3 {
		t.Errorf("Expected 3 keys with prefix 'user:', got %d", len(userKeys))
	}
	
	// Test prefix search for "product:"
	productPrefix := []byte("product:")
	productKeys := memTable.GetKeysWithPrefix(productPrefix)
	
	// Should find 2 product keys
	if len(productKeys) != 2 {
		t.Errorf("Expected 2 keys with prefix 'product:', got %d", len(productKeys))
	}
	
	// Test prefix search for non-existent prefix
	nonExistentPrefix := []byte("nonexistent:")
	nonExistentKeys := memTable.GetKeysWithPrefix(nonExistentPrefix)
	
	// Should find 0 keys
	if len(nonExistentKeys) != 0 {
		t.Errorf("Expected 0 keys with prefix 'nonexistent:', got %d", len(nonExistentKeys))
	}
}

func TestSingleWriterMemTableClone(t *testing.T) {
	// Create a source memtable and populate it
	srcMemTable := NewSingleWriterMemTable(MemTableConfig{
		MaxSize:    1024,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})
	
	// Set transaction context
	srcMemTable.SetTransactionContext(12345)
	
	// Add some keys
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
	}
	values := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
	}
	
	for i := range keys {
		err := srcMemTable.Put(keys[i], values[i])
		if err != nil {
			t.Errorf("Failed to put key %q: %v", keys[i], err)
		}
	}
	
	// Clone the memtable
	cloneMemTable := srcMemTable.Clone()
	
	// Check the transaction context was preserved
	if cloneMemTable.GetTransactionContext() != srcMemTable.GetTransactionContext() {
		t.Errorf("Clone did not preserve transaction context. Expected %d, got %d", 
			srcMemTable.GetTransactionContext(), cloneMemTable.GetTransactionContext())
	}
	
	// Check that entry count matches
	if cloneMemTable.EntryCount() != srcMemTable.EntryCount() {
		t.Errorf("Clone entry count doesn't match. Expected %d, got %d", 
			srcMemTable.EntryCount(), cloneMemTable.EntryCount())
	}
	
	// Check that size matches
	if cloneMemTable.Size() != srcMemTable.Size() {
		t.Errorf("Clone size doesn't match. Expected %d, got %d", 
			srcMemTable.Size(), cloneMemTable.Size())
	}
	
	// Check that all keys are present and have correct values
	for i, key := range keys {
		value, err := cloneMemTable.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %q from clone: %v", key, err)
		}
		if !bytes.Equal(value, values[i]) {
			t.Errorf("Value for key %q doesn't match. Expected %q, got %q", 
				key, values[i], value)
		}
	}
	
	// Modify the clone and ensure it doesn't affect the original
	cloneMemTable.Delete(keys[0])
	
	// Original should still have the key
	_, err := srcMemTable.Get(keys[0])
	if err != nil {
		t.Errorf("Original memtable should still have key %q, but got error: %v", keys[0], err)
	}
	
	// Clone should not have the key
	_, err = cloneMemTable.Get(keys[0])
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for deleted key in clone, got %v", err)
	}
}

// Simple benchmark for comparing the SingleWriterMemTable to the original MemTable
func BenchmarkSingleWriterMemTablePut(b *testing.B) {
	// Create a SingleWriterMemTable with sufficient capacity
	memTable := NewSingleWriterMemTable(MemTableConfig{
		MaxSize:    uint64(b.N * 100), // Ensure we don't hit size limits
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err := memTable.Put(key, value)
		if err != nil {
			b.Fatalf("Failed to put key: %v", err)
		}
	}
}

// Benchmark retrieval
func BenchmarkSingleWriterMemTableGet(b *testing.B) {
	// Create a SingleWriterMemTable and populate it
	keyCount := 100000
	memTable := NewSingleWriterMemTable(MemTableConfig{
		MaxSize:    uint64(keyCount * 100), // Ensure we don't hit size limits
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Insert a fixed set of keys
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err := memTable.Put(key, value)
		if err != nil {
			b.Fatalf("Failed to setup SingleWriterMemTable: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Pick a random key from the inserted set
		idx := i % keyCount
		key := []byte(fmt.Sprintf("key-%d", idx))
		_, err := memTable.Get(key)
		if err != nil {
			b.Fatalf("Failed to get key: %v", err)
		}
	}
}

// Comparison benchmark between SingleWriterMemTable and MemTable
func BenchmarkMemTableImplementations(b *testing.B) {
	// Test the same operations on different implementations
	b.Run("Put_SingleWriter", func(b *testing.B) {
		// Create a SingleWriterMemTable with sufficient capacity
		memTable := NewSingleWriterMemTable(MemTableConfig{
			MaxSize:    uint64(b.N * 100), // Ensure we don't hit size limits
			Logger:     model.NewNoOpLogger(),
			Comparator: DefaultComparator,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%d", i))
			value := []byte(fmt.Sprintf("value-%d", i))
			err := memTable.Put(key, value)
			if err != nil {
				b.Fatalf("Failed to put key: %v", err)
			}
		}
	})

	b.Run("Put_Original", func(b *testing.B) {
		// Create a regular MemTable with sufficient capacity
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    uint64(b.N * 100), // Ensure we don't hit size limits
			Logger:     model.NewNoOpLogger(),
			Comparator: DefaultComparator,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%d", i))
			value := []byte(fmt.Sprintf("value-%d", i))
			err := memTable.Put(key, value)
			if err != nil {
				b.Fatalf("Failed to put key: %v", err)
			}
		}
	})
	
	// Similar benchmarks for Get
	const keyCount = 10000
	
	b.Run("Get_SingleWriter", func(b *testing.B) {
		memTable := NewSingleWriterMemTable(MemTableConfig{
			MaxSize:    uint64(keyCount * 100),
			Logger:     model.NewNoOpLogger(),
			Comparator: DefaultComparator,
		})
		
		// Setup - populate memtable
		for i := 0; i < keyCount; i++ {
			key := []byte(fmt.Sprintf("key-%d", i))
			value := []byte(fmt.Sprintf("value-%d", i))
			memTable.Put(key, value)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx := i % keyCount
			key := []byte(fmt.Sprintf("key-%d", idx))
			_, _ = memTable.Get(key)
		}
	})
	
	b.Run("Get_Original", func(b *testing.B) {
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    uint64(keyCount * 100),
			Logger:     model.NewNoOpLogger(),
			Comparator: DefaultComparator,
		})
		
		// Setup - populate memtable
		for i := 0; i < keyCount; i++ {
			key := []byte(fmt.Sprintf("key-%d", i))
			value := []byte(fmt.Sprintf("value-%d", i))
			memTable.Put(key, value)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx := i % keyCount
			key := []byte(fmt.Sprintf("key-%d", idx))
			_, _ = memTable.Get(key)
		}
	})
}

// Test the cache-optimized version of SingleWriterMemTable
type CachedSingleWriterMemTable struct {
	SingleWriterMemTable
	cache     map[string][]byte
	cacheLock sync.RWMutex
}

// Create a new cached version of the memtable
func NewCachedSingleWriterMemTable(config MemTableConfig) *CachedSingleWriterMemTable {
	baseTable := NewSingleWriterMemTable(config)
	return &CachedSingleWriterMemTable{
		SingleWriterMemTable: *baseTable,
		cache:                make(map[string][]byte),
	}
}

// Override AddReadCache to implement caching
func (m *CachedSingleWriterMemTable) AddReadCache(key []byte, value []byte) {
	m.cacheLock.Lock()
	defer m.cacheLock.Unlock()
	
	// Add to cache with string key for map access
	m.cache[string(key)] = append([]byte{}, value...)
}

// Override GetFromReadCache to check cache
func (m *CachedSingleWriterMemTable) GetFromReadCache(key []byte) ([]byte, bool) {
	m.cacheLock.RLock()
	defer m.cacheLock.RUnlock()
	
	value, found := m.cache[string(key)]
	if found {
		// Return a copy to avoid cache corruption
		return append([]byte{}, value...), true
	}
	return nil, false
}

// Override Get to use cache when possible
func (m *CachedSingleWriterMemTable) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}

	// First check cache
	if cachedValue, found := m.GetFromReadCache(key); found {
		return cachedValue, nil
	}
	
	// Fallback to standard lookup
	value, err := m.SingleWriterMemTable.Get(key)
	if err == nil {
		// Cache the value for future lookups
		m.AddReadCache(key, value)
	}
	return value, err
}

// Test the cached implementation
func TestCachedSingleWriterMemTable(t *testing.T) {
	memTable := NewCachedSingleWriterMemTable(MemTableConfig{
		MaxSize:    1024,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})
	
	// Add some test data
	key1 := []byte("cacheKey1")
	val1 := []byte("cacheValue1")
	err := memTable.Put(key1, val1)
	if err != nil {
		t.Fatalf("Failed to put key: %v", err)
	}
	
	// First get should add to cache
	_, err = memTable.Get(key1)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	
	// Get from cache directly
	cachedVal, found := memTable.GetFromReadCache(key1)
	if !found {
		t.Error("Value should be in cache after first get")
	}
	if !bytes.Equal(cachedVal, val1) {
		t.Errorf("Cached value %q does not match expected %q", cachedVal, val1)
	}
	
	// Update the value in the table
	val2 := []byte("updatedValue")
	err = memTable.Put(key1, val2)
	if err != nil {
		t.Fatalf("Failed to update key: %v", err)
	}
	
	// Add the new value to cache
	memTable.AddReadCache(key1, val2)
	
	// Get should now return the updated value from cache
	cachedVal, found = memTable.GetFromReadCache(key1)
	if !found {
		t.Error("Updated value should be in cache")
	}
	if !bytes.Equal(cachedVal, val2) {
		t.Errorf("Updated cached value %q does not match expected %q", cachedVal, val2)
	}
}

// Benchmark the cached implementation vs non-cached
func BenchmarkCachedVsRegularMemTable(b *testing.B) {
	const keyCount = 10000
	const repeatedReadCount = 100 // How many times each key is read
	
	b.Run("WithCache", func(b *testing.B) {
		memTable := NewCachedSingleWriterMemTable(MemTableConfig{
			MaxSize:    uint64(keyCount * 100),
			Logger:     model.NewNoOpLogger(),
			Comparator: DefaultComparator,
		})
		
		// Setup - populate memtable
		for i := 0; i < keyCount; i++ {
			key := []byte(fmt.Sprintf("key-%d", i))
			value := []byte(fmt.Sprintf("value-%d", i))
			memTable.Put(key, value)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Reading a small subset of keys repeatedly to benefit from caching
			idx := (i % (keyCount / repeatedReadCount)) * repeatedReadCount
			key := []byte(fmt.Sprintf("key-%d", idx))
			_, _ = memTable.Get(key)
		}
	})
	
	b.Run("WithoutCache", func(b *testing.B) {
		memTable := NewSingleWriterMemTable(MemTableConfig{
			MaxSize:    uint64(keyCount * 100),
			Logger:     model.NewNoOpLogger(),
			Comparator: DefaultComparator,
		})
		
		// Setup - populate memtable
		for i := 0; i < keyCount; i++ {
			key := []byte(fmt.Sprintf("key-%d", i))
			value := []byte(fmt.Sprintf("value-%d", i))
			memTable.Put(key, value)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Same reading pattern as the cached version
			idx := (i % (keyCount / repeatedReadCount)) * repeatedReadCount
			key := []byte(fmt.Sprintf("key-%d", idx))
			_, _ = memTable.Get(key)
		}
	})
}