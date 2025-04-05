package storage

import (
	"bytes"
	"fmt"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

func TestMemTableBasicOperations(t *testing.T) {
	// Create a new MemTable with a small max size for testing
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    1024, // 1KB
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Test empty state
	if memTable.EntryCount() != 0 {
		t.Errorf("New MemTable should have 0 entries, got %d", memTable.EntryCount())
	}
	if memTable.Size() != 0 {
		t.Errorf("New MemTable should have size 0, got %d", memTable.Size())
	}
	if memTable.IsFull() {
		t.Error("New MemTable should not be full")
	}
	if memTable.IsFlushed() {
		t.Error("New MemTable should not be flushed")
	}

	// Test Put and Get
	key1 := []byte("key1")
	val1 := []byte("value1")

	err := memTable.Put(key1, val1)
	if err != nil {
		t.Errorf("Failed to put key-value pair: %v", err)
	}

	// Check entry count and size
	if memTable.EntryCount() != 1 {
		t.Errorf("MemTable should have 1 entry, got %d", memTable.EntryCount())
	}
	expectedSize := uint64(len(key1) + len(val1))
	if memTable.Size() != expectedSize {
		t.Errorf("MemTable size should be %d, got %d", expectedSize, memTable.Size())
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
		t.Errorf("MemTable should contain key %q", key1)
	}
	if memTable.Contains([]byte("nonexistent")) {
		t.Error("MemTable should not contain nonexistent key")
	}

	// Test updating a value
	newVal1 := []byte("newvalue1")
	err = memTable.Put(key1, newVal1)
	if err != nil {
		t.Errorf("Failed to update value: %v", err)
	}

	// Check that entry count is still 1
	if memTable.EntryCount() != 1 {
		t.Errorf("MemTable should still have 1 entry after update, got %d", memTable.EntryCount())
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
		t.Errorf("MemTable should have 0 entries after delete, got %d", memTable.EntryCount())
	}

	// Check that the key no longer exists
	if memTable.Contains(key1) {
		t.Error("MemTable should not contain deleted key")
	}

	// Try to get deleted key
	_, err = memTable.Get(key1)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestMemTableSorting(t *testing.T) {
	// Create a new MemTable
	memTable := NewMemTable(MemTableConfig{
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

func TestMemTableFlushing(t *testing.T) {
	// Create a new MemTable
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    1024,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Add a key-value pair
	key := []byte("key")
	value := []byte("value")
	err := memTable.Put(key, value)
	if err != nil {
		t.Errorf("Failed to put key-value pair: %v", err)
	}

	// Mark as flushed
	memTable.MarkFlushed()

	// Check flushed state
	if !memTable.IsFlushed() {
		t.Error("MemTable should be marked as flushed")
	}

	// Try to modify after flushing
	err = memTable.Put([]byte("newkey"), []byte("newvalue"))
	if err != ErrMemTableFlushed {
		t.Errorf("Expected ErrMemTableFlushed, got %v", err)
	}

	err = memTable.Delete(key)
	if err != ErrMemTableFlushed {
		t.Errorf("Expected ErrMemTableFlushed, got %v", err)
	}

	// Reading should still work
	retrieved, err := memTable.Get(key)
	if err != nil {
		t.Errorf("Failed to get value after flushing: %v", err)
	}
	if !bytes.Equal(retrieved, value) {
		t.Errorf("Retrieved value %q does not match expected %q", retrieved, value)
	}

	// Clear the MemTable
	memTable.Clear()

	// Check state after clearing
	if memTable.EntryCount() != 0 {
		t.Errorf("Cleared MemTable should have 0 entries, got %d", memTable.EntryCount())
	}
	if memTable.Size() != 0 {
		t.Errorf("Cleared MemTable should have size 0, got %d", memTable.Size())
	}
	if memTable.IsFlushed() {
		t.Error("Cleared MemTable should not be flushed")
	}
}

func TestMemTableSizeLimit(t *testing.T) {
	// Create a new MemTable with a small max size
	maxSize := uint64(100) // 100 bytes
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    maxSize,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Add a key-value pair that fits
	key1 := []byte("key1")
	val1 := []byte("value1") // Total: 4 + 6 = 10 bytes

	err := memTable.Put(key1, val1)
	if err != nil {
		t.Errorf("Failed to put small key-value pair: %v", err)
	}

	// Try to add a key-value pair that would exceed the max size
	largeKey := []byte("largekey")
	largeVal := make([]byte, 95) // This would make total size 8 + 95 = 103 bytes

	err = memTable.Put(largeKey, largeVal)
	if err != ErrMemTableFull {
		t.Errorf("Expected ErrMemTableFull, got %v", err)
	}

	// Check that the large key was not added
	if memTable.Contains(largeKey) {
		t.Error("MemTable should not contain the large key that exceeded size limits")
	}

	// Calculate how much space we have left
	remainingSpace := maxSize - memTable.Size()

	// Check IsFull when we're close to the limit
	closeKey := []byte("close")
	// Make a value that will fit within the remaining space (leaving 10 bytes)
	closeVal := make([]byte, int(remainingSpace) - len(closeKey) - 10)

	err = memTable.Put(closeKey, closeVal)
	if err != nil {
		t.Errorf("Failed to put key-value pair close to size limit: %v", err)
	}

	if memTable.IsFull() {
		t.Error("MemTable should not be full yet")
	}

	// Add one more key to push it over the threshold
	finalKey := []byte("final")
	// Make a value that will exceed the remaining space
	finalVal := make([]byte, 20) // This should push it over the limit

	// Since this should exceed the limit, we expect an error
	err = memTable.Put(finalKey, finalVal)
	if err != ErrMemTableFull {
		t.Errorf("Expected ErrMemTableFull, got %v", err)
	}

	// Check if MemTable is full based on its current size
	if !(memTable.Size() >= memTable.MaxSize() * 9 / 10) {
		t.Error("MemTable should be at least 90% full now")
	}
}

func TestMemTableErrorCases(t *testing.T) {
	// Create a new MemTable
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    1024,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Test nil key
	err := memTable.Put(nil, []byte("value"))
	if err != ErrNilKey {
		t.Errorf("Expected ErrNilKey, got %v", err)
	}

	// Test nil value
	err = memTable.Put([]byte("key"), nil)
	if err != ErrNilValue {
		t.Errorf("Expected ErrNilValue, got %v", err)
	}

	// Test Get with nil key
	_, err = memTable.Get(nil)
	if err != ErrNilKey {
		t.Errorf("Expected ErrNilKey for Get with nil key, got %v", err)
	}

	// Test Get with nonexistent key
	_, err = memTable.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}

	// Test Delete with nil key
	err = memTable.Delete(nil)
	if err != ErrNilKey {
		t.Errorf("Expected ErrNilKey for Delete with nil key, got %v", err)
	}

	// Test Contains with nil key
	if memTable.Contains(nil) {
		t.Error("Contains with nil key should return false")
	}
}

func TestMemTableVersioning(t *testing.T) {
	// Create a new MemTable
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    1024,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Initial version should be 1
	if v := memTable.GetVersion(); v != 1 {
		t.Errorf("Initial version should be 1, got %d", v)
	}

	// Version should increase after Put
	key := []byte("key")
	value := []byte("value")

	memTable.Put(key, value)
	if v := memTable.GetVersion(); v != 2 {
		t.Errorf("Version after Put should be 2, got %d", v)
	}

	// Version should increase after Delete
	memTable.Delete(key)
	if v := memTable.GetVersion(); v != 3 {
		t.Errorf("Version after Delete should be 3, got %d", v)
	}

	// Version should increase after Clear
	memTable.Clear()
	if v := memTable.GetVersion(); v != 4 {
		t.Errorf("Version after Clear should be 4, got %d", v)
	}

	// Version should not increase after read operations
	memTable.Put(key, value)
	initialVersion := memTable.GetVersion()

	memTable.Get(key)
	if v := memTable.GetVersion(); v != initialVersion {
		t.Errorf("Version should not change after Get, expected %d, got %d", initialVersion, v)
	}

	memTable.Contains(key)
	if v := memTable.GetVersion(); v != initialVersion {
		t.Errorf("Version should not change after Contains, expected %d, got %d", initialVersion, v)
	}

	memTable.EntryCount()
	if v := memTable.GetVersion(); v != initialVersion {
		t.Errorf("Version should not change after EntryCount, expected %d, got %d", initialVersion, v)
	}

	memTable.Size()
	if v := memTable.GetVersion(); v != initialVersion {
		t.Errorf("Version should not change after Size, expected %d, got %d", initialVersion, v)
	}

	memTable.GetEntries()
	if v := memTable.GetVersion(); v != initialVersion {
		t.Errorf("Version should not change after GetEntries, expected %d, got %d", initialVersion, v)
	}
}

func TestCustomComparator(t *testing.T) {
	// Create a reverse comparator (descending order)
	reverseComparator := func(a, b []byte) int {
		return -bytes.Compare(a, b)
	}

	// Create a MemTable with the reverse comparator
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    1024,
		Logger:     model.NewNoOpLogger(),
		Comparator: reverseComparator,
	})

	// Add keys in non-sorted order
	keys := [][]byte{
		[]byte("aaa"),
		[]byte("mmm"),
		[]byte("ccc"),
		[]byte("zzz"),
	}

	for i, key := range keys {
		value := []byte{byte(i)}
		err := memTable.Put(key, value)
		if err != nil {
			t.Errorf("Failed to put key %q: %v", key, err)
		}
	}

	// Get all entries and check order
	entries := memTable.GetEntries()

	// Check that keys are in reverse sorted order
	var retrievedKeys [][]byte
	for i := 0; i < len(entries); i += 2 {
		retrievedKeys = append(retrievedKeys, entries[i])
	}

	// Expected reverse sorted keys
	expectedKeys := [][]byte{
		[]byte("zzz"),
		[]byte("mmm"),
		[]byte("ccc"),
		[]byte("aaa"),
	}

	for i, expectedKey := range expectedKeys {
		if !bytes.Equal(retrievedKeys[i], expectedKey) {
			t.Errorf("At position %d, expected key %q, got %q", i, expectedKey, retrievedKeys[i])
		}
	}
}

// Benchmarks for the MemTable

// BenchmarkMemTablePut benchmarks putting new keys into the MemTable
func BenchmarkMemTablePut(b *testing.B) {
	// Create a MemTable with sufficient capacity
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
}

// BenchmarkMemTablePutExisting benchmarks updating existing keys
func BenchmarkMemTablePutExisting(b *testing.B) {
	// Create a MemTable with sufficient capacity
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    uint64(b.N * 100), // Ensure we don't hit size limits
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Insert initial keys first
	key := []byte("same-key")
	value := []byte("initial-value")
	memTable.Put(key, value)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		valueUpdate := []byte(fmt.Sprintf("updated-value-%d", i))
		err := memTable.Put(key, valueUpdate)
		if err != nil {
			b.Fatalf("Failed to update key: %v", err)
		}
	}
}

// BenchmarkMemTableGet benchmarks retrieving keys from the MemTable
func BenchmarkMemTableGet(b *testing.B) {
	// Create a MemTable and populate it
	keyCount := 100000
	memTable := NewMemTable(MemTableConfig{
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
			b.Fatalf("Failed to setup MemTable: %v", err)
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

// BenchmarkMemTableContains benchmarks checking for key existence
func BenchmarkMemTableContains(b *testing.B) {
	// Create a MemTable and populate it
	keyCount := 100000
	memTable := NewMemTable(MemTableConfig{
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
			b.Fatalf("Failed to setup MemTable: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Mix of existing and non-existing keys
		exists := i % 2 == 0
		var key []byte
		if exists {
			idx := i % keyCount
			key = []byte(fmt.Sprintf("key-%d", idx))
		} else {
			key = []byte(fmt.Sprintf("nonexistent-key-%d", i))
		}
		memTable.Contains(key)
	}
}

// BenchmarkMemTableDelete benchmarks deleting keys
func BenchmarkMemTableDelete(b *testing.B) {
	// Create a MemTable and populate it
	keyCount := b.N
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    uint64(keyCount * 100), // Ensure we don't hit size limits
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Insert all keys first
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err := memTable.Put(key, value)
		if err != nil {
			b.Fatalf("Failed to setup MemTable: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		err := memTable.Delete(key)
		if err != nil {
			b.Fatalf("Failed to delete key: %v", err)
		}
	}
}

// BenchmarkMemTableGetEntries benchmarks retrieving all entries
func BenchmarkMemTableGetEntries(b *testing.B) {
	// We'll create MemTables of different sizes and benchmark retrieval of entries
	benchSizes := []int{100, 1000, 10000}

	for _, size := range benchSizes {
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			// Create and populate a MemTable
			memTable := NewMemTable(MemTableConfig{
				MaxSize:    uint64(size * 100), // Ensure we don't hit size limits
				Logger:     model.NewNoOpLogger(),
				Comparator: DefaultComparator,
			})

			// Insert the specified number of keys
			for i := 0; i < size; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				value := []byte(fmt.Sprintf("value-%d", i))
				err := memTable.Put(key, value)
				if err != nil {
					b.Fatalf("Failed to setup MemTable: %v", err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entries := memTable.GetEntries()
				if len(entries) != size*2 {
					b.Fatalf("Expected %d entries, got %d", size*2, len(entries))
				}
			}
		})
	}
}
