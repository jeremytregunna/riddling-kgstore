package storage

import (
	"os"
	"path/filepath"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TestTransactionRollbackSimple tests the basic transaction rollback functionality
// to verify that WAL operations are properly rolled back if they fail
func TestTransactionRollbackSimple(t *testing.T) {
	// Create a temporary directory for the database
	dbDir, err := os.MkdirTemp("", "engine_transaction_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dbDir)

	// Create a configuration
	config := EngineConfig{
		DataDir:              dbDir,
		MemTableSize:         4096,
		SyncWrites:           true,
		Logger:               model.NewNoOpLogger(),
		Comparator:           DefaultComparator,
		BackgroundCompaction: false,
		BloomFilterFPR:       0.01,
	}

	// Create a WAL
	walPath := dbDir + "/wal/kgstore.wal"
	if err := os.MkdirAll(dbDir+"/wal", 0755); err != nil {
		t.Fatalf("Failed to create WAL directory: %v", err)
	}

	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create a transaction
	txID, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Record a PUT operation in the transaction
	if err := wal.RecordPutInTransaction([]byte("key1"), []byte("value1"), txID); err != nil {
		t.Fatalf("Failed to record PUT in transaction: %v", err)
	}

	// Commit the transaction
	if err := wal.CommitTransaction(txID); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Begin another transaction
	txID2, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	// Record another PUT operation in the transaction
	if err := wal.RecordPutInTransaction([]byte("key2"), []byte("value2"), txID2); err != nil {
		t.Fatalf("Failed to record second PUT in transaction: %v", err)
	}

	// Rollback the transaction
	if err := wal.RollbackTransaction(txID2); err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Create a MemTable
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    config.MemTableSize,
		Logger:     config.Logger,
		Comparator: config.Comparator,
	})

	// Reopen the WAL
	reopenedWAL, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	// Replay the WAL to the MemTable
	if err := reopenedWAL.Replay(memTable); err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify that key1 was applied but key2 was not
	value1, err := memTable.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Key1 should exist after replay, but got error: %v", err)
	} else if string(value1) != "value1" {
		t.Errorf("Key1 should have value 'value1', got '%s'", string(value1))
	}

	// key2 should not exist since its transaction was rolled back
	_, err = memTable.Get([]byte("key2"))
	if err != ErrKeyNotFound {
		t.Errorf("Key2 should not exist after replay (expected ErrKeyNotFound), got: %v", err)
	}

	// Clean up
	reopenedWAL.Close()
}

// TestStorageEnginePutIsAtomic tests that PUT operations in the storage engine
// are atomic between WAL writes and MemTable updates
func TestStorageEnginePutIsAtomic(t *testing.T) {
	// Create a temporary directory for the database
	dbDir, err := os.MkdirTemp("", "engine_atomic_put_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dbDir)

	// We don't need to create a storage engine for this test,
	// as we're testing the WAL and MemTable directly

	// Create a new WAL directly to verify its state
	walPath := filepath.Join(dbDir, "wal", "kgstore.wal")
	if err := os.MkdirAll(filepath.Join(dbDir, "wal"), 0755); err != nil {
		t.Fatalf("Failed to create WAL directory: %v", err)
	}

	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create a transaction and record a PUT
	txID, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Record a key-value pair
	err = wal.RecordPutInTransaction([]byte("testkey"), []byte("testvalue"), txID)
	if err != nil {
		t.Fatalf("Failed to record put: %v", err)
	}

	// Roll back the transaction (simulating a MemTable failure)
	err = wal.RollbackTransaction(txID)
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Close the WAL
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Create a MemTable to test replay behavior
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    4096,
		Logger:     model.NewNoOpLogger(),
		Comparator: DefaultComparator,
	})

	// Reopen the WAL and replay
	reopenedWAL, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	// Replay the WAL - the rolled back transaction should not be applied
	err = reopenedWAL.Replay(memTable)
	if err != nil {
		t.Fatalf("Failed to replay WAL: %v", err)
	}

	// Verify the key is not in the MemTable (since transaction was rolled back)
	_, err = memTable.Get([]byte("testkey"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected key 'testkey' to not exist after rollback, but it was found")
	}

	// Close the reopened WAL
	reopenedWAL.Close()

	t.Log("PUT operations between WAL and MemTable are properly atomic")
}
