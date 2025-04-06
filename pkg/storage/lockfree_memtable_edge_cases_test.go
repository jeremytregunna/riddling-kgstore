package storage

import (
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TestLockFreeMemTableEdgeCases tests various edge cases and boundary conditions
func TestLockFreeMemTableEdgeCases(t *testing.T) {
	table := NewLockFreeMemTable(LockFreeMemTableConfig{
		MaxSize: 1024, // Very small, just 1KB
		Logger:  model.DefaultLoggerInstance,
	})
	
	// In Go, an empty byte slice is not nil
	// For our test purposes, we'll consider this a valid key
	emptyKey := []byte{}
	err := table.Put(emptyKey, []byte("value"))
	if err != nil {
		t.Errorf("Failed to put empty key: %v", err)
	}
	
	// Test nil key (should fail)
	err = table.Put(nil, []byte("value"))
	if err != ErrLockFreeNilKey {
		t.Errorf("Expected error for nil key, got: %v", err)
	}
	
	// Test nil value (should fail)
	err = table.Put([]byte("key"), nil)
	if err != ErrLockFreeNilValue {
		t.Errorf("Expected error for nil value, got: %v", err)
	}
	
	// Test empty value (should succeed)
	emptyValue := []byte{}
	err = table.Put([]byte("empty-value-key"), emptyValue)
	if err != nil {
		t.Errorf("Failed to put empty value: %v", err)
	}
	
	// Test retrieving empty value
	result, err := table.Get([]byte("empty-value-key"))
	if err != nil {
		t.Errorf("Failed to get empty value: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("Expected empty value, got %d bytes", len(result))
	}
	
	// Test boundary around max size
	largeKey := []byte("large-key")
	// Create a value that will cause the table to reach its max size
	largeValue := make([]byte, 900) // Just under our 1KB limit
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	
	// This should succeed (we're under the limit)
	err = table.Put(largeKey, largeValue)
	if err != nil {
		t.Errorf("Failed to put large value: %v", err)
	}
	
	// Now try to put another large value, should fail with ErrLockFreeMemTableFull
	err = table.Put([]byte("another-large-key"), largeValue)
	if err != ErrLockFreeMemTableFull {
		t.Errorf("Expected ErrLockFreeMemTableFull, got: %v", err)
	}
	
	// Test deletion of non-existent key
	err = table.Delete([]byte("nonexistent-key"))
	if err != nil {
		t.Errorf("Expected no error when deleting non-existent key, got: %v", err)
	}
	
	// Test operations on a flushed table
	table.MarkFlushed()
	
	// Put should fail on flushed table
	err = table.Put([]byte("after-flush"), []byte("value"))
	if err != ErrLockFreeMemTableFlushed {
		t.Errorf("Expected ErrLockFreeMemTableFlushed, got: %v", err)
	}
	
	// Delete should fail on flushed table
	err = table.Delete(largeKey)
	if err != ErrLockFreeMemTableFlushed {
		t.Errorf("Expected ErrLockFreeMemTableFlushed for delete, got: %v", err)
	}
	
	// Get should still work on flushed table
	_, err = table.Get(largeKey)
	if err != nil {
		t.Errorf("Get should work on flushed table: %v", err)
	}
}