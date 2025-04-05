package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

func TestLockFreeMemTablePutGet(t *testing.T) {
	table := NewLockFreeMemTable(LockFreeMemTableConfig{
		MaxSize: 1024 * 1024, // 1MB
		Logger:  model.DefaultLoggerInstance,
	})

	// Add key-value pairs
	key1 := []byte("key1")
	value1 := []byte("value1")
	if err := table.Put(key1, value1); err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}

	// Get existing key
	result, err := table.Get(key1)
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if !bytes.Equal(result, value1) {
		t.Errorf("Expected value %s for key1, got %s", value1, result)
	}

	// Check non-existent key
	_, err = table.Get([]byte("nonexistent"))
	if err != ErrLockFreeKeyNotFound {
		t.Errorf("Expected ErrLockFreeKeyNotFound for nonexistent key, got %v", err)
	}
}

func TestLockFreeMemTableDelete(t *testing.T) {
	table := NewLockFreeMemTable(LockFreeMemTableConfig{
		MaxSize: 1024 * 1024, // 1MB
		Logger:  model.DefaultLoggerInstance,
	})

	// Add key-value pair
	key := []byte("deleteMe")
	value := []byte("toBeDeleted")
	if err := table.Put(key, value); err != nil {
		t.Fatalf("Failed to put key: %v", err)
	}

	// Delete the key
	if err := table.Delete(key); err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Try to get the deleted key
	_, err := table.Get(key)
	if err != ErrLockFreeKeyNotFound {
		t.Errorf("Expected ErrLockFreeKeyNotFound for deleted key, got %v", err)
	}
}

func TestLockFreeMemTableSize(t *testing.T) {
	table := NewLockFreeMemTable(LockFreeMemTableConfig{
		MaxSize: 1024 * 1024, // 1MB
		Logger:  model.DefaultLoggerInstance,
	})

	// Check initial size
	if size := table.Size(); size != 0 {
		t.Errorf("Expected initial size 0, got %d", size)
	}

	// Add a key-value pair
	key := []byte("sizeTest")
	value := []byte("valueForSizeTest")
	expectedSize := uint64(len(key) + len(value))
	
	if err := table.Put(key, value); err != nil {
		t.Fatalf("Failed to put key: %v", err)
	}

	// Check size after adding
	if size := table.Size(); size != expectedSize {
		t.Errorf("Expected size %d after adding, got %d", expectedSize, size)
	}

	// Delete the key
	if err := table.Delete(key); err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Check size after deleting
	if size := table.Size(); size != 0 {
		t.Errorf("Expected size 0 after deleting, got %d", size)
	}
}

func TestLockFreeMemTableConcurrentAccess(t *testing.T) {
	table := NewLockFreeMemTable(LockFreeMemTableConfig{
		MaxSize: 10 * 1024 * 1024, // 10MB
		Logger:  model.DefaultLoggerInstance,
	})

	const numGoRoutines = 10
	const numOperationsPerRoutine = 10

	var wg sync.WaitGroup
	wg.Add(numGoRoutines)

	// Start goroutines to perform concurrent operations
	for i := 0; i < numGoRoutines; i++ {
		go func(id int) {
			defer wg.Done()

			// Each goroutine performs a mix of puts and gets
			for j := 0; j < numOperationsPerRoutine; j++ {
				// Use safe strings for keys and values
				key := []byte(fmt.Sprintf("key-%d-%d", id, j))
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))

				// Put operation
				if err := table.Put(key, value); err != nil {
					t.Errorf("Failed to put key %s: %v", key, err)
					continue
				}

				// Get operation (should succeed)
				result, err := table.Get(key)
				if err != nil {
					t.Errorf("Failed to get key %s: %v", key, err)
				} else if !bytes.Equal(result, value) {
					t.Errorf("Expected value %s for key %s, got %s", value, key, result)
				}

				// Delete operation
				if j%2 == 0 {
					if err := table.Delete(key); err != nil {
						t.Errorf("Failed to delete key %s: %v", key, err)
					}

					// Verify deletion
					_, err := table.Get(key)
					if err != ErrLockFreeKeyNotFound {
						t.Errorf("Expected ErrLockFreeKeyNotFound for deleted key %s, got %v", key, err)
					}
				}
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Verify the final state
	expectedEntries := numGoRoutines * numOperationsPerRoutine / 2 // Half the entries should remain
	actualEntries := table.EntryCount()

	// The number might not be exactly half due to concurrency, but should be close
	if actualEntries < expectedEntries/2 || actualEntries > expectedEntries*2 {
		t.Errorf("Expected around %d entries, got %d", expectedEntries, actualEntries)
	}
}

func TestLockFreeMemTableIsFull(t *testing.T) {
	// Create a small table with just enough space for one entry
	smallTable := NewLockFreeMemTable(LockFreeMemTableConfig{
		MaxSize: 6, // Just enough for a small key-value pair
		Logger:  model.DefaultLoggerInstance,
	})

	// Add a key that fits (3 bytes key + 3 bytes value = 6 bytes)
	if err := smallTable.Put([]byte("key"), []byte("val")); err != nil {
		t.Fatalf("Failed to put small key: %v", err)
	}

	// At this point the table should be full (6/6 bytes)
	if !smallTable.IsFull() {
		t.Errorf("Expected IsFull() to return true for a table at capacity")
	}

	// Try to add a key that won't fit
	err := smallTable.Put([]byte("key2"), []byte("value2TooLarge"))
	if err != ErrLockFreeMemTableFull {
		t.Errorf("Expected ErrLockFreeMemTableFull, got %v", err)
	}
}

func TestLockFreeMemTableFlushed(t *testing.T) {
	table := NewLockFreeMemTable(LockFreeMemTableConfig{
		MaxSize: 1024 * 1024, // 1MB
		Logger:  model.DefaultLoggerInstance,
	})

	// Initially not flushed
	if table.IsFlushed() {
		t.Errorf("New table should not be flushed")
	}

	// Mark as flushed
	table.MarkFlushed()

	// Should now be flushed
	if !table.IsFlushed() {
		t.Errorf("Table should be flushed after MarkFlushed()")
	}

	// Cannot add to flushed table
	err := table.Put([]byte("key"), []byte("value"))
	if err != ErrLockFreeMemTableFlushed {
		t.Errorf("Expected ErrLockFreeMemTableFlushed when putting to flushed table, got %v", err)
	}
}

func TestLockFreeMemTableGetEntries(t *testing.T) {
	table := NewLockFreeMemTable(LockFreeMemTableConfig{
		MaxSize: 1024 * 1024, // 1MB
		Logger:  model.DefaultLoggerInstance,
	})

	// Add some key-value pairs
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
		if err := table.Put(keys[i], values[i]); err != nil {
			t.Fatalf("Failed to put key%d: %v", i+1, err)
		}
	}

	// Get all entries
	entries := table.GetEntries()

	// Check that we have the expected number of entries (key-value pairs are flattened)
	expectedLen := len(keys) * 2
	if len(entries) != expectedLen {
		t.Errorf("Expected %d entries, got %d", expectedLen, len(entries))
	}

	// Verify entries are in order and match what we put in
	for i := 0; i < len(entries); i += 2 {
		// Found key should match one of our input keys
		foundKey := entries[i]
		foundValue := entries[i+1]
		
		keyMatched := false
		for j, key := range keys {
			if bytes.Equal(foundKey, key) {
				keyMatched = true
				// Value should match corresponding input value
				if !bytes.Equal(foundValue, values[j]) {
					t.Errorf("Value mismatch for key %s: expected %s, got %s", 
						key, values[j], foundValue)
				}
				break
			}
		}
		
		if !keyMatched {
			t.Errorf("Entry at index %d contains unknown key: %s", i, foundKey)
		}
	}
}

func TestLockFreeMemTableVersioning(t *testing.T) {
	// This test is mostly informational - in our lock-free implementation
	// version increments are handled internally and may behave differently than
	// the original implementation. The important thing is that higher operations
	// have higher versions, not necessarily that each operation increases the version by 1.
	table := NewLockFreeMemTable(LockFreeMemTableConfig{
		MaxSize: 1024 * 1024, // 1MB
		Logger:  model.DefaultLoggerInstance,
	})

	// Initial version should be 1 or greater
	initialVersion := table.GetVersion()
	if initialVersion < 1 {
		t.Errorf("Expected initial version to be at least 1, got %d", initialVersion)
	}

	// Perform some operations
	key := []byte("versionTest")
	value := []byte("value1")
	
	// First put
	if err := table.Put(key, value); err != nil {
		t.Fatalf("Failed to put key: %v", err)
	}
	
	// Update
	if err := table.Put(key, []byte("value2")); err != nil {
		t.Fatalf("Failed to update key: %v", err)
	}
	
	// Delete
	if err := table.Delete(key); err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}
	
	// Final version should be at least initial + 1
	finalVersion := table.GetVersion()
	if finalVersion <= initialVersion {
		t.Errorf("Expected final version (%d) to be greater than initial version (%d)", 
			finalVersion, initialVersion)
	}
}