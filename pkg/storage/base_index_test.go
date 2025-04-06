package storage

import (
	"bytes"
	"os"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// setupTestStorageEngine creates a real storage engine for testing
func setupTestStorageEngine(t *testing.T) (*StorageEngine, func()) {
	// Create a temporary directory for the database
	dbDir, err := os.MkdirTemp("", "base_index_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a storage engine
	config := EngineConfig{
		DataDir:              dbDir,
		MemTableSize:         4096,
		SyncWrites:           true,
		Logger:               model.NewNoOpLogger(),
		Comparator:           DefaultComparator,
		BackgroundCompaction: false,
		BloomFilterFPR:       0.01,
	}

	engine, err := NewStorageEngine(config)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}

	// Return the engine and a cleanup function
	cleanup := func() {
		engine.Close()
		os.RemoveAll(dbDir)
	}

	return engine, cleanup
}

// Test SerializeIDs and DeserializeIDs
func TestIDSerialization(t *testing.T) {
	// Test data
	ids := [][]byte{
		[]byte("id1"),
		[]byte("id2"),
		[]byte("id3"),
	}

	// Serialize
	serialized, err := SerializeIDs(ids)
	if err != nil {
		t.Fatalf("SerializeIDs failed: %v", err)
	}

	// Deserialize
	deserialized, err := DeserializeIDs(serialized)
	if err != nil {
		t.Fatalf("DeserializeIDs failed: %v", err)
	}

	// Check count
	if len(deserialized) != len(ids) {
		t.Errorf("Expected %d IDs, got %d", len(ids), len(deserialized))
	}

	// Check content
	for i, id := range ids {
		if !bytes.Equal(id, deserialized[i]) {
			t.Errorf("ID at position %d doesn't match. Expected %s, got %s", i, id, deserialized[i])
		}
	}

	// Test edge cases
	// Empty list
	emptyIDs := [][]byte{}
	serialized, err = SerializeIDs(emptyIDs)
	if err != nil {
		t.Fatalf("SerializeIDs failed with empty list: %v", err)
	}
	deserialized, err = DeserializeIDs(serialized)
	if err != nil {
		t.Fatalf("DeserializeIDs failed with empty list: %v", err)
	}
	if len(deserialized) != 0 {
		t.Errorf("Expected empty list, got %d items", len(deserialized))
	}

	// Corrupted data
	_, err = DeserializeIDs([]byte{1, 2, 3}) // Too short
	if err == nil {
		t.Error("DeserializeIDs should fail with corrupted data")
	}
}

// Test BaseIndex functionality
func TestBaseIndex(t *testing.T) {
	storage, cleanup := setupTestStorageEngine(t)
	defer cleanup()

	logger := model.NewNoOpLogger()
	prefix := []byte("test:")
	indexType := IndexType(42)

	// Create a base index
	baseIndex := *NewBaseIndex(storage, logger, prefix, indexType)

	// Test MakeKey
	key := []byte("testkey")
	fullKey := baseIndex.MakeKey(key)
	expectedKey := append(prefix, key...)

	if !bytes.Equal(fullKey, expectedKey) {
		t.Errorf("MakeKey output mismatch. Expected %v, got %v", expectedKey, fullKey)
	}

	// Test IsOpen
	if !baseIndex.IsOpen() {
		t.Error("BaseIndex should be open by default")
	}

	// Test GetIndexType
	if baseIndex.GetIndexType() != indexType {
		t.Errorf("GetIndexType returned %d, expected %d", baseIndex.GetIndexType(), indexType)
	}

	// Test Close
	err := baseIndex.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
	if baseIndex.IsOpen() {
		t.Error("BaseIndex should be closed after Close()")
	}

	// Test calling Close on an already closed index
	err = baseIndex.Close()
	if err != nil {
		t.Errorf("Closing an already closed index should not error: %v", err)
	}
}

// testIndex implements Index for testing BaseIndex functionality
type testIndex struct {
	*BaseIndex
}

func (idx *testIndex) Put(key, value []byte) error {
	if !idx.isOpen {
		return ErrIndexClosed
	}
	fullKey := idx.MakeKey(key)
	return idx.storage.Put(fullKey, value)
}

func (idx *testIndex) Get(key []byte) ([]byte, error) {
	if !idx.isOpen {
		return nil, ErrIndexClosed
	}
	fullKey := idx.MakeKey(key)
	return idx.storage.Get(fullKey)
}

func (idx *testIndex) GetAll(key []byte) ([][]byte, error) {
	val, err := idx.Get(key)
	if err != nil {
		return nil, err
	}
	return [][]byte{val}, nil
}

func (idx *testIndex) Delete(key []byte) error {
	if !idx.isOpen {
		return ErrIndexClosed
	}
	fullKey := idx.MakeKey(key)
	return idx.storage.Delete(fullKey)
}

func (idx *testIndex) DeleteValue(key, _ []byte) error {
	return idx.Delete(key)
}

func (idx *testIndex) Contains(key []byte) (bool, error) {
	if !idx.isOpen {
		return false, ErrIndexClosed
	}
	fullKey := idx.MakeKey(key)
	return idx.storage.Contains(fullKey)
}

func (idx *testIndex) GetType() IndexType {
	return idx.indexType
}

// Test Index implementation that uses BaseIndex
func TestIndexImplementation(t *testing.T) {
	storage, cleanup := setupTestStorageEngine(t)
	defer cleanup()

	logger := model.NewNoOpLogger()
	prefix := []byte("test:")
	indexType := IndexType(42)

	// Create a base index
	base := NewBaseIndex(storage, logger, prefix, indexType)

	// Create a test index using the base index
	index := &testIndex{BaseIndex: base}

	// Test index operations
	key := []byte("testkey")
	value := []byte("testvalue")

	// Test Put
	err := index.Put(key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	retrievedValue, err := index.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(retrievedValue, value) {
		t.Errorf("Retrieved value doesn't match. Expected %s, got %s", value, retrievedValue)
	}

	// Test GetAll
	allValues, err := index.GetAll(key)
	if err != nil {
		t.Fatalf("GetAll failed: %v", err)
	}
	if len(allValues) != 1 || !bytes.Equal(allValues[0], value) {
		t.Errorf("GetAll returned incorrect values: %v", allValues)
	}

	// Test Contains
	exists, err := index.Contains(key)
	if err != nil {
		t.Fatalf("Contains failed: %v", err)
	}
	if !exists {
		t.Error("Contains returned false, expected true")
	}

	// Test Delete
	err = index.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	exists, err = index.Contains(key)
	if err != nil {
		t.Fatalf("Contains failed after deletion: %v", err)
	}
	if exists {
		t.Error("Contains returned true after deletion, expected false")
	}

	// Test operations on closed index
	index.Close()

	// These should fail with ErrIndexClosed
	operations := []struct {
		name string
		op   func() error
	}{
		{"Put", func() error { return index.Put(key, value) }},
		{"Get", func() error { _, err := index.Get(key); return err }},
		{"GetAll", func() error { _, err := index.GetAll(key); return err }},
		{"Delete", func() error { return index.Delete(key) }},
		{"DeleteValue", func() error { return index.DeleteValue(key, value) }},
		{"Contains", func() error { _, err := index.Contains(key); return err }},
	}

	for _, op := range operations {
		err := op.op()
		if err != ErrIndexClosed {
			t.Errorf("%s on closed index should return ErrIndexClosed, got: %v", op.name, err)
		}
	}
}

// Test IndexCache functionality
func TestIndexCache(t *testing.T) {
	cache := NewIndexCache(3) // Cache with max size 3

	// Keys and values for testing
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
	}

	values := [][][]byte{
		{[]byte("value1-1"), []byte("value1-2")},
		{[]byte("value2-1")},
		{[]byte("value3-1"), []byte("value3-2"), []byte("value3-3")},
		{[]byte("value4-1")},
	}

	// Test Put and Get
	for i := 0; i < 3; i++ {
		cache.Put(keys[i], values[i])
	}

	// Check values were cached correctly
	for i := 0; i < 3; i++ {
		cachedValues, found := cache.Get(keys[i])
		if !found {
			t.Errorf("Key %s should be in cache", keys[i])
			continue
		}

		if len(cachedValues) != len(values[i]) {
			t.Errorf("Cached values count mismatch for key %s. Expected %d, got %d",
				keys[i], len(values[i]), len(cachedValues))
			continue
		}

		for j, val := range values[i] {
			if !bytes.Equal(cachedValues[j], val) {
				t.Errorf("Cached value mismatch at key %s, position %d. Expected %s, got %s",
					keys[i], j, val, cachedValues[j])
			}
		}
	}

	// Test eviction when size limit is reached
	cache.Put(keys[3], values[3])

	// One of the first 3 keys should have been evicted
	evictionCount := 0
	for i := 0; i < 3; i++ {
		_, found := cache.Get(keys[i])
		if !found {
			evictionCount++
		}
	}

	if evictionCount != 1 {
		t.Errorf("Expected exactly 1 key to be evicted, but %d were evicted", evictionCount)
	}

	// The 4th key should be in the cache
	_, found := cache.Get(keys[3])
	if !found {
		t.Errorf("Key %s should be in cache", keys[3])
	}

	// Test Invalidate
	for i := 0; i < 4; i++ {
		if _, found := cache.Get(keys[i]); found {
			cache.Invalidate(keys[i])
			if _, stillFound := cache.Get(keys[i]); stillFound {
				t.Errorf("Key %s should be invalidated", keys[i])
			}
		}
	}

	// Test Clear
	for i := 0; i < 3; i++ {
		cache.Put(keys[i], values[i])
	}

	cache.Clear()

	for i := 0; i < 3; i++ {
		if _, found := cache.Get(keys[i]); found {
			t.Errorf("Key %s should be cleared", keys[i])
		}
	}

	// Test hit and miss count
	// Create a fresh cache for this specific test
	hitMissCache := NewIndexCache(2)

	// Put some keys
	hitMissCache.Put(keys[0], values[0])
	hitMissCache.Put(keys[1], values[1])

	// Generate exact number of hits
	hitMissCache.Get(keys[0])
	hitMissCache.Get(keys[0])
	hitMissCache.Get(keys[0])

	// Generate exact number of misses
	hitMissCache.Get(keys[3])
	hitMissCache.Get(keys[3])

	size, hits, misses := hitMissCache.GetStats()
	if size != 2 {
		t.Errorf("Expected cache size 2, got %d", size)
	}
	if hits != 3 {
		t.Errorf("Expected 3 hits, got %d", hits)
	}
	if misses != 2 {
		t.Errorf("Expected 2 misses, got %d", misses)
	}
}
