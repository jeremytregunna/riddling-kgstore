package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
	"sync"
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

func TestWALTransactions(t *testing.T) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "wal_tx_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "tx.wal")

	// Create a new WAL with syncOnWrite enabled for tests
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Test 1: Basic transaction lifecycle (begin, commit)
	t.Run("BasicTransactionLifecycle", func(t *testing.T) {
		// Begin a new transaction
		txID, err := wal.BeginTransaction()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		if txID == 0 {
			t.Fatal("Expected non-zero transaction ID")
		}

		// Add operations to the transaction
		err = wal.RecordPutInTransaction([]byte("tx-key1"), []byte("tx-value1"), txID)
		if err != nil {
			t.Errorf("Failed to record put in transaction: %v", err)
		}

		err = wal.RecordPutInTransaction([]byte("tx-key2"), []byte("tx-value2"), txID)
		if err != nil {
			t.Errorf("Failed to record put in transaction: %v", err)
		}

		err = wal.RecordDeleteInTransaction([]byte("tx-key1"), txID)
		if err != nil {
			t.Errorf("Failed to record delete in transaction: %v", err)
		}

		// Commit the transaction
		err = wal.CommitTransaction(txID)
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}

		// Verify that trying to use the same transaction ID fails
		err = wal.RecordPutInTransaction([]byte("tx-key3"), []byte("tx-value3"), txID)
		if err == nil {
			t.Error("Expected error when using committed transaction ID")
		}
	})

	// Test 2: Transaction replay
	t.Run("TransactionReplay", func(t *testing.T) {
		// Create a new transaction
		txID, err := wal.BeginTransaction()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Add operations to the transaction
		err = wal.RecordPutInTransaction([]byte("replay-key1"), []byte("replay-value1"), txID)
		if err != nil {
			t.Errorf("Failed to record put in transaction: %v", err)
		}

		err = wal.RecordPutInTransaction([]byte("replay-key2"), []byte("replay-value2"), txID)
		if err != nil {
			t.Errorf("Failed to record put in transaction: %v", err)
		}

		// Add a non-transactional operation
		err = wal.RecordPut([]byte("no-tx-key"), []byte("no-tx-value"))
		if err != nil {
			t.Errorf("Failed to record non-transactional put: %v", err)
		}

		// Commit the transaction
		err = wal.CommitTransaction(txID)
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}

		// Close the WAL and reopen it to test replay
		err = wal.Close()
		if err != nil {
			t.Errorf("Failed to close WAL: %v", err)
		}

		// Reopen the WAL
		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
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

		// Verify committed transaction data is present
		value, err := memTable.Get([]byte("replay-key1"))
		if err != nil {
			t.Errorf("Failed to get replay-key1: %v", err)
		} else if string(value) != "replay-value1" {
			t.Errorf("Expected 'replay-value1', got '%s'", string(value))
		}

		value, err = memTable.Get([]byte("replay-key2"))
		if err != nil {
			t.Errorf("Failed to get replay-key2: %v", err)
		} else if string(value) != "replay-value2" {
			t.Errorf("Expected 'replay-value2', got '%s'", string(value))
		}

		// Verify non-transactional data is present
		value, err = memTable.Get([]byte("no-tx-key"))
		if err != nil {
			t.Errorf("Failed to get no-tx-key: %v", err)
		} else if string(value) != "no-tx-value" {
			t.Errorf("Expected 'no-tx-value', got '%s'", string(value))
		}

		// Verify tx-key2 from the first test is also present
		value, err = memTable.Get([]byte("tx-key2"))
		if err != nil {
			t.Errorf("Failed to get tx-key2: %v", err)
		} else if string(value) != "tx-value2" {
			t.Errorf("Expected 'tx-value2', got '%s'", string(value))
		}

		// tx-key1 should not be present because it was deleted
		if memTable.Contains([]byte("tx-key1")) {
			t.Error("tx-key1 should not be present in MemTable after replay")
		}
	})

	// Test 3: Transaction rollback
	t.Run("TransactionRollback", func(t *testing.T) {
		// Create a new transaction
		txID, err := wal.BeginTransaction()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Add operations to the transaction
		err = wal.RecordPutInTransaction([]byte("rollback-key1"), []byte("rollback-value1"), txID)
		if err != nil {
			t.Errorf("Failed to record put in transaction: %v", err)
		}

		err = wal.RecordPutInTransaction([]byte("rollback-key2"), []byte("rollback-value2"), txID)
		if err != nil {
			t.Errorf("Failed to record put in transaction: %v", err)
		}

		// Rollback the transaction
		err = wal.RollbackTransaction(txID)
		if err != nil {
			t.Errorf("Failed to rollback transaction: %v", err)
		}

		// Create a new MemTable for replay
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     model.NewNoOpLogger(),
			Comparator: DefaultComparator,
		})

		// Close and reopen the WAL to test rollback persistence
		err = wal.Close()
		if err != nil {
			t.Errorf("Failed to close WAL: %v", err)
		}

		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      model.NewNoOpLogger(),
		})
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}

		// Replay the WAL
		err = wal.Replay(memTable)
		if err != nil {
			t.Errorf("Failed to replay WAL: %v", err)
		}

		// Verify that rolled back transaction data is NOT present
		if memTable.Contains([]byte("rollback-key1")) {
			t.Error("rollback-key1 should not be present after rollback")
		}

		if memTable.Contains([]byte("rollback-key2")) {
			t.Error("rollback-key2 should not be present after rollback")
		}
	})

	// Test 4: Concurrent transactions
	t.Run("ConcurrentTransactions", func(t *testing.T) {
		var wg sync.WaitGroup
		const numTransactions = 5
		const numOperationsPerTx = 10

		// Channel to collect transaction IDs
		txIDs := make(chan uint64, numTransactions)

		// Start concurrent transactions
		for i := 0; i < numTransactions; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				// Begin a transaction
				txID, err := wal.BeginTransaction()
				if err != nil {
					t.Errorf("Failed to begin transaction %d: %v", idx, err)
					return
				}
				txIDs <- txID

				// Add operations to the transaction
				for j := 0; j < numOperationsPerTx; j++ {
					key := []byte(fmt.Sprintf("conc-tx%d-key%d", idx, j))
					value := []byte(fmt.Sprintf("conc-tx%d-value%d", idx, j))

					err = wal.RecordPutInTransaction(key, value, txID)
					if err != nil {
						t.Errorf("Failed to record put in transaction %d: %v", idx, err)
					}
				}

				// Commit the transaction
				err = wal.CommitTransaction(txID)
				if err != nil {
					t.Errorf("Failed to commit transaction %d: %v", idx, err)
				}
			}(i)
		}

		// Wait for all transactions to complete
		wg.Wait()
		close(txIDs)

		// Close and reopen the WAL to test concurrent transaction replay
		err = wal.Close()
		if err != nil {
			t.Errorf("Failed to close WAL: %v", err)
		}

		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      model.NewNoOpLogger(),
		})
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}

		// Create a new MemTable for replay
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

		// Verify all concurrent transaction data is present
		for i := 0; i < numTransactions; i++ {
			for j := 0; j < numOperationsPerTx; j++ {
				key := []byte(fmt.Sprintf("conc-tx%d-key%d", i, j))
				expectedValue := []byte(fmt.Sprintf("conc-tx%d-value%d", i, j))

				value, err := memTable.Get(key)
				if err != nil {
					t.Errorf("Failed to get key %s: %v", key, err)
					continue
				}

				if !bytes.Equal(value, expectedValue) {
					t.Errorf("For key %s, expected value %s, got %s", key, expectedValue, value)
				}
			}
		}
	})

	// Close the WAL
	err = wal.Close()
	if err != nil {
		t.Errorf("Failed to close WAL: %v", err)
	}
}
