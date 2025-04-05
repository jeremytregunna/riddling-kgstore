package storage

import (
	"os"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TestTransactionRollbackSimple tests the basic transaction rollback functionality
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