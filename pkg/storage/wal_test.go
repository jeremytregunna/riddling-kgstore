package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

func TestWALBasicOperations(t *testing.T) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "test.wal")

	// Create a new WAL
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Test that the WAL file was created
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Fatal("WAL file was not created")
	}

	// Test recording operations
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for key, value := range testData {
		err := wal.RecordPut([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to record put operation: %v", err)
		}
	}

	// Test recording a delete
	err = wal.RecordDelete([]byte("key2"))
	if err != nil {
		t.Errorf("Failed to record delete operation: %v", err)
	}

	// Close the WAL
	err = wal.Close()
	if err != nil {
		t.Errorf("Failed to close WAL: %v", err)
	}

	// Verify the WAL is closed
	if wal.IsOpen() {
		t.Error("WAL should be closed")
	}

	// Test operations on closed WAL
	err = wal.RecordPut([]byte("key4"), []byte("value4"))
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}

	err = wal.RecordDelete([]byte("key4"))
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}

	err = wal.Sync()
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}

func TestWALReplayOperations(t *testing.T) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "wal_replay_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "replay.wal")

	// Create a new WAL
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Record some operations
	operations := []struct {
		op    string
		key   string
		value string
	}{
		{"put", "key1", "value1"},
		{"put", "key2", "value2"},
		{"put", "key3", "value3"},
		{"delete", "key2", ""},
		{"put", "key4", "value4"},
		{"put", "key1", "updated1"}, // Update key1
	}

	for _, op := range operations {
		if op.op == "put" {
			err := wal.RecordPut([]byte(op.key), []byte(op.value))
			if err != nil {
				t.Errorf("Failed to record put operation: %v", err)
			}
		} else if op.op == "delete" {
			err := wal.RecordDelete([]byte(op.key))
			if err != nil {
				t.Errorf("Failed to record delete operation: %v", err)
			}
		}
	}

	// Create a MemTable to replay the WAL into
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    1024 * 1024,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Replay the WAL
	err = wal.Replay(memTable)
	if err != nil {
		t.Errorf("Failed to replay WAL: %v", err)
	}

	// Verify that the MemTable contains the expected keys
	expectedData := map[string]string{
		"key1": "updated1", // Note this is the updated value
		"key3": "value3",
		"key4": "value4",
	}

	// key2 should not be present because it was deleted
	if memTable.Contains([]byte("key2")) {
		t.Error("key2 should not be present in MemTable after replay")
	}

	for key, expectedValue := range expectedData {
		value, err := memTable.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %q from MemTable: %v", key, err)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("For key %q, expected value %q, got %q", key, expectedValue, string(value))
		}
	}

	// Verify the entry count
	expectedCount := len(expectedData)
	if memTable.EntryCount() != expectedCount {
		t.Errorf("Expected %d entries in MemTable, got %d", expectedCount, memTable.EntryCount())
	}

	// Close the WAL
	err = wal.Close()
	if err != nil {
		t.Errorf("Failed to close WAL: %v", err)
	}
}

func TestWALTruncate(t *testing.T) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "wal_truncate_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "truncate.wal")

	// Create a new WAL
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Record some operations
	for i := 1; i <= 5; i++ {
		key := []byte("key" + string(rune('0'+i)))
		value := []byte("value" + string(rune('0'+i)))
		err := wal.RecordPut(key, value)
		if err != nil {
			t.Errorf("Failed to record put operation: %v", err)
		}
	}

	// Get the file size before truncation
	fileInfo, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to get file info: %v", err)
	}
	originalSize := fileInfo.Size()

	// Truncate the WAL
	err = wal.Truncate()
	if err != nil {
		t.Errorf("Failed to truncate WAL: %v", err)
	}

	// Get the file size after truncation
	fileInfo, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to get file info after truncation: %v", err)
	}
	newSize := fileInfo.Size()

	// The new size should be smaller, just containing the header
	if newSize >= originalSize {
		t.Errorf("WAL was not truncated: original size %d, new size %d", originalSize, newSize)
	}

	// Replay the WAL to verify it's empty
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    1024 * 1024,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	err = wal.Replay(memTable)
	if err != nil {
		t.Errorf("Failed to replay truncated WAL: %v", err)
	}

	// The MemTable should be empty
	if memTable.EntryCount() != 0 {
		t.Errorf("Expected 0 entries in MemTable after replay, got %d", memTable.EntryCount())
	}

	// Record a new operation after truncation
	err = wal.RecordPut([]byte("new-key"), []byte("new-value"))
	if err != nil {
		t.Errorf("Failed to record put operation after truncation: %v", err)
	}

	// Replay again
	memTable.Clear()
	err = wal.Replay(memTable)
	if err != nil {
		t.Errorf("Failed to replay WAL after truncation: %v", err)
	}

	// The MemTable should now have 1 entry
	if memTable.EntryCount() != 1 {
		t.Errorf("Expected 1 entry in MemTable after replay, got %d", memTable.EntryCount())
	}

	// Verify the entry
	value, err := memTable.Get([]byte("new-key"))
	if err != nil {
		t.Errorf("Failed to get new key from MemTable: %v", err)
	} else if !bytes.Equal(value, []byte("new-value")) {
		t.Errorf("Expected value %q, got %q", "new-value", string(value))
	}

	// Close the WAL
	err = wal.Close()
	if err != nil {
		t.Errorf("Failed to close WAL: %v", err)
	}
}

func TestWALSync(t *testing.T) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "wal_sync_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "sync.wal")

	// Create a new WAL with syncOnWrite disabled
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: false,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Record some operations
	for i := 1; i <= 3; i++ {
		key := []byte("key" + string(rune('0'+i)))
		value := []byte("value" + string(rune('0'+i)))
		err := wal.RecordPut(key, value)
		if err != nil {
			t.Errorf("Failed to record put operation: %v", err)
		}
	}

	// Manually sync the WAL
	err = wal.Sync()
	if err != nil {
		t.Errorf("Failed to sync WAL: %v", err)
	}

	// Close the WAL
	err = wal.Close()
	if err != nil {
		t.Errorf("Failed to close WAL: %v", err)
	}

	// Reopen the WAL and verify the records are there
	wal, err = NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: false,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	// Replay into a MemTable
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    1024 * 1024,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	err = wal.Replay(memTable)
	if err != nil {
		t.Errorf("Failed to replay WAL: %v", err)
	}

	// Verify all three records are present
	for i := 1; i <= 3; i++ {
		key := []byte("key" + string(rune('0'+i)))
		expectedValue := []byte("value" + string(rune('0'+i)))

		value, err := memTable.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %q from MemTable: %v", key, err)
			continue
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("For key %q, expected value %q, got %q", key, expectedValue, value)
		}
	}

	// Close the WAL
	err = wal.Close()
	if err != nil {
		t.Errorf("Failed to close WAL: %v", err)
	}
}
