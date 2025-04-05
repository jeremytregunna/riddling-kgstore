package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

func TestTransactionManagerBasic(t *testing.T) {
	// Create a temporary directory for the transaction files
	tempDir, err := os.MkdirTemp("", "tx_manager_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create WAL for transaction boundaries
	walPath := filepath.Join(tempDir, "tx_test.wal")
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create transaction manager
	txManager, err := NewTransactionManager(tempDir, model.NewNoOpLogger(), wal)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	// Test basic transaction lifecycle
	t.Run("TransactionLifecycle", func(t *testing.T) {
		// Begin a transaction
		tx := txManager.Begin()
		if tx == nil {
			t.Fatal("Failed to begin transaction")
		}

		if tx.IsCommitted() {
			t.Error("New transaction should not be committed")
		}

		// Add some operations
		tx.AddOperation(TransactionOperation{
			Type:   "add",
			Target: "sstable",
			ID:     1001,
		})

		tx.AddOperation(TransactionOperation{
			Type:   "remove",
			Target: "sstable",
			ID:     1002,
		})

		// Commit the transaction
		err := tx.Commit()
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}

		if !tx.IsCommitted() {
			t.Error("Transaction should be marked as committed")
		}

		// Try to add another operation after commit (should be ignored)
		tx.AddOperation(TransactionOperation{
			Type:   "add",
			Target: "sstable",
			ID:     1003,
		})

		// Try to commit again (should be no-op)
		err = tx.Commit()
		if err != nil {
			t.Errorf("Second commit should be a no-op: %v", err)
		}
	})

	// Test transaction rollback
	t.Run("TransactionRollback", func(t *testing.T) {
		// Begin a transaction
		tx := txManager.Begin()
		if tx == nil {
			t.Fatal("Failed to begin transaction")
		}
		
		txID := tx.id

		// Add some operations
		tx.AddOperation(TransactionOperation{
			Type:   "add",
			Target: "sstable",
			ID:     2001,
		})

		// Rollback the transaction
		err := tx.Rollback()
		if err != nil {
			t.Errorf("Failed to rollback transaction: %v", err)
		}

		// We need to check that the transaction has been marked as inactive
		// but we shouldn't rely on IsCommitted since that's not necessarily
		// meant to handle rolled back transactions
		
		// Verify the transaction was removed from active transactions
		txManager.mu.RLock()
		_, stillActive := txManager.activeTransactions[txID]
		txManager.mu.RUnlock()
		
		if stillActive {
			t.Error("Transaction should be removed from active transactions after rollback")
		}

		// Try to use the transaction again (should be rejected or be a no-op)
		tx.AddOperation(TransactionOperation{
			Type:   "add",
			Target: "sstable", 
			ID:     2002,
		})
		
		// Start a new transaction to ensure we can still use the transaction manager
		newTx := txManager.Begin()
		if newTx == nil {
			t.Fatal("Failed to begin new transaction after rollback")
		}
		
		// Clean up
		newTx.Rollback()
	})

	// Close the transaction manager
	if err := txManager.Close(); err != nil {
		t.Errorf("Failed to close transaction manager: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Errorf("Failed to close WAL: %v", err)
	}
}

func TestTransactionManagerWithWAL(t *testing.T) {
	// Create a temporary directory for the transaction files
	tempDir, err := os.MkdirTemp("", "tx_manager_wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Skip this test if we're having issues with it in the current environment
	// t.Skip("Skipping transaction manager with WAL test")

	// Use separate subdirectories for each test to avoid file conflicts
	walIntegrationDir := filepath.Join(tempDir, "integration")
	crashRecoveryDir := filepath.Join(tempDir, "recovery")
	
	// Create subdirectories
	if err := os.MkdirAll(walIntegrationDir, 0755); err != nil {
		t.Fatalf("Failed to create integration directory: %v", err)
	}
	if err := os.MkdirAll(crashRecoveryDir, 0755); err != nil {
		t.Fatalf("Failed to create recovery directory: %v", err)
	}

	// Test that simple transaction operations are properly recorded in WAL
	t.Run("SimpleWALIntegration", func(t *testing.T) {
		// Create a test WAL path and ensure clean state
		walPath := filepath.Join(walIntegrationDir, "simple_test.wal")
		os.Remove(walPath) // Remove any existing file

		// Create a fresh WAL
		wal, err := NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      model.NewNoOpLogger(),
		})
		if err != nil {
			t.Fatalf("Failed to create WAL: %v", err)
		}
		defer wal.Close()

		// Perform a simple transaction directly against the WAL
		txID, err := wal.BeginTransaction()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Record a key-value pair
		testKey := []byte("integration-test-key")
		testValue := []byte("integration-test-value")
		err = wal.RecordPutInTransaction(testKey, testValue, txID)
		if err != nil {
			t.Errorf("Failed to record put in transaction: %v", err)
		}

		// Commit the transaction
		err = wal.CommitTransaction(txID)
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}

		// Close the WAL
		wal.Close()

		// Reopen to test replay
		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      model.NewNoOpLogger(),
		})
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}
		defer wal.Close()

		// Create a MemTable to replay into
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

		// Verify the key-value pair was correctly replayed
		value, err := memTable.Get(testKey)
		if err != nil {
			t.Errorf("Failed to get test key: %v", err)
		} else if !bytes.Equal(value, testValue) {
			t.Errorf("Expected value %q, got %q", testValue, value)
		}

		// Create a second transaction to verify IDs
		txID2, err := wal.BeginTransaction()
		if err != nil {
			t.Fatalf("Failed to begin second transaction: %v", err)
		}

		if txID2 <= txID {
			t.Errorf("Expected txID2 %d to be > txID %d", txID2, txID)
		}

		// Clean up
		wal.RollbackTransaction(txID2)
	})

	// Test recovery of a simulated crash during transaction
	t.Run("SimpleCrashRecovery", func(t *testing.T) {
		// Create a WAL path for crash test
		walPath := filepath.Join(crashRecoveryDir, "crash_test.wal")
		os.Remove(walPath) // Ensure clean state

		// Create a fresh WAL
		wal, err := NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      model.NewNoOpLogger(),
		})
		if err != nil {
			t.Fatalf("Failed to create WAL: %v", err)
		}

		// Begin a transaction but don't commit or rollback (simulate crash)
		txID, err := wal.BeginTransaction()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Add a key-value pair
		crashKey := []byte("crash-test-key")
		crashValue := []byte("crash-test-value")
		err = wal.RecordPutInTransaction(crashKey, crashValue, txID)
		if err != nil {
			t.Errorf("Failed to record put in transaction: %v", err)
		}

		// Close without committing to simulate crash
		wal.Close()

		// Reopen the WAL to test recovery
		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      model.NewNoOpLogger(),
		})
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}
		defer wal.Close()

		// Create a MemTable for replay
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

		// Verify the key from uncommitted transaction is not present
		if memTable.Contains(crashKey) {
			t.Error("Key from uncommitted transaction should not be present after replay")
		}

		// Begin a new transaction to check IDs
		newTxID, err := wal.BeginTransaction()
		if err != nil {
			t.Fatalf("Failed to begin new transaction: %v", err)
		}

		if newTxID <= txID {
			t.Errorf("Expected new txID %d to be > old txID %d", newTxID, txID)
		}

		// Clean up
		wal.RollbackTransaction(newTxID)
	})
}

func TestTransactionFileOperations(t *testing.T) {
	// Create a temporary directory for transaction files
	tempDir, err := os.MkdirTemp("", "tx_file_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test SSTable file structure
	sstableDir := filepath.Join(tempDir, "sstables")
	if err := os.MkdirAll(sstableDir, 0755); err != nil {
		t.Fatalf("Failed to create SSTable directory: %v", err)
	}

	// Create some dummy SSTable files
	dummyIDs := []uint64{5001, 5002, 5003}
	for _, id := range dummyIDs {
		// Create the SSTable files
		dataFile := filepath.Join(sstableDir, fmt.Sprintf("%d.data", id))
		indexFile := filepath.Join(sstableDir, fmt.Sprintf("%d.index", id))
		filterFile := filepath.Join(sstableDir, fmt.Sprintf("%d.filter", id))
		
		createDummyFile(t, dataFile)
		createDummyFile(t, indexFile)
		createDummyFile(t, filterFile)
	}

	// Create WAL for transaction boundaries
	walPath := filepath.Join(tempDir, "tx_file_test.wal")
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create transaction manager
	txManager, err := NewTransactionManager(tempDir, model.NewNoOpLogger(), wal)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	// Test SSTable file removal
	t.Run("SSTableFileRemoval", func(t *testing.T) {
		// First verify that the files exist
		for _, id := range dummyIDs[:2] { // check first 2 files
			dataFile := filepath.Join(sstableDir, fmt.Sprintf("%d.data", id))
			if _, err := os.Stat(dataFile); os.IsNotExist(err) {
				t.Errorf("SSTable data file %d should exist", id)
			}
		}

		// Begin a transaction
		tx := txManager.Begin()
		
		// Add operation to remove SSTable 5001
		tx.AddOperation(TransactionOperation{
			Type:   "remove",
			Target: "sstable",
			ID:     5001,
		})

		// Commit transaction
		err := tx.Commit()
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}

		// Verify that file 5001 is removed
		dataFile := filepath.Join(sstableDir, fmt.Sprintf("%d.data", dummyIDs[0]))
		if _, err := os.Stat(dataFile); !os.IsNotExist(err) {
			t.Errorf("SSTable data file %d should be removed", dummyIDs[0])
		}

		// Verify that file 5002 still exists
		dataFile = filepath.Join(sstableDir, fmt.Sprintf("%d.data", dummyIDs[1]))
		if _, err := os.Stat(dataFile); os.IsNotExist(err) {
			t.Errorf("SSTable data file %d should still exist", dummyIDs[1])
		}
	})

	// Test file rename operation
	t.Run("SSTableFileRename", func(t *testing.T) {
		// Create a rename operation
		tx := txManager.Begin()
		
		// Add operation to rename SSTable 5002 to 6000
		tx.AddOperation(TransactionOperation{
			Type:   "rename",
			Target: "sstable",
			ID:     5002,
			Data:   []uint64{5002, 6000}, // from, to
		})

		// Commit transaction
		err := tx.Commit()
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}

		// Verify original files don't exist
		dataFile := filepath.Join(sstableDir, fmt.Sprintf("%d.data", 5002))
		if _, err := os.Stat(dataFile); !os.IsNotExist(err) {
			t.Errorf("Original SSTable data file %d should be renamed", 5002)
		}

		// Verify new files exist
		dataFile = filepath.Join(sstableDir, fmt.Sprintf("%d.data", 6000))
		if _, err := os.Stat(dataFile); os.IsNotExist(err) {
			t.Errorf("Renamed SSTable data file %d should exist", 6000)
		}
	})

	// Close the transaction manager
	if err := txManager.Close(); err != nil {
		t.Errorf("Failed to close transaction manager: %v", err)
	}

	// Close the WAL
	if err := wal.Close(); err != nil {
		t.Errorf("Failed to close WAL: %v", err)
	}
}

// createDummyFile creates a dummy file with some basic content
func createDummyFile(t *testing.T, path string) {
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Failed to create dummy file %s: %v", path, err)
	}
	defer file.Close()

	_, err = file.WriteString("Dummy file content for testing")
	if err != nil {
		t.Fatalf("Failed to write to dummy file %s: %v", path, err)
	}
}