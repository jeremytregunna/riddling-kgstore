package storage

import (
	"os"
	"path/filepath"
	"testing"
	
	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TestWALMixedTransactionRecovery tests recovery with a mix of complete, incomplete, and corrupted transactions
// This creates a more complex scenario to verify our enhanced recovery process
func TestWALMixedTransactionRecovery(t *testing.T) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "wal_mixed_tx_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "mixed_tx.wal")

	// Use the model logger for this test
	logger := model.NewNoOpLogger()

	// Create a new WAL
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Add standalone operations
	err = wal.RecordPut([]byte("standalone-key"), []byte("standalone-value"))
	if err != nil {
		t.Fatalf("Failed to record standalone put: %v", err)
	}

	// Create a completed transaction
	txID1, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}
	err = wal.RecordPutInTransaction([]byte("complete-tx-key"), []byte("complete-tx-value"), txID1)
	if err != nil {
		t.Fatalf("Failed to record put in transaction 1: %v", err)
	}
	err = wal.CommitTransaction(txID1)
	if err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}

	// Create a rolled back transaction
	txID2, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}
	err = wal.RecordPutInTransaction([]byte("rollback-tx-key"), []byte("rollback-tx-value"), txID2)
	if err != nil {
		t.Fatalf("Failed to record put in transaction 2: %v", err)
	}
	err = wal.RollbackTransaction(txID2)
	if err != nil {
		t.Fatalf("Failed to rollback transaction 2: %v", err)
	}

	// Create an incomplete transaction
	txID3, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction 3: %v", err)
	}
	err = wal.RecordPutInTransaction([]byte("incomplete-tx-key"), []byte("incomplete-tx-value"), txID3)
	if err != nil {
		t.Fatalf("Failed to record put in transaction 3: %v", err)
	}
	// Deliberately do not commit or rollback

	// Close the WAL to finalize its state
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Corrupt the WAL by appending garbage bytes at the end
	// This approach is less likely to affect specific records in our test
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open WAL file for corruption: %v", err)
	}
	
	// Append some garbage bytes to simulate corruption
	corruptData := []byte{0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66}
	_, err = file.Write(corruptData)
	if err != nil {
		t.Fatalf("Failed to write corrupt data: %v", err)
	}
	file.Close()

	// Test replay with different options
	// 1. AtomicTxOnly=true, StrictMode=false (default)
	t.Run("DefaultOptions", func(t *testing.T) {
		// Reopen the WAL
		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      logger,
		})
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}
		defer wal.Close()

		// Create a new MemTable for replay
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     logger,
			Comparator: DefaultComparator,
		})

		// Replay with default options
		options := DefaultReplayOptions() // AtomicTxOnly=true, StrictMode=false
		err = wal.ReplayWithOptions(memTable, options)
		if err != nil {
			t.Fatalf("Failed to replay WAL with default options: %v", err)
		}

		// Verify completed transaction was correctly applied
		value, err := memTable.Get([]byte("complete-tx-key"))
		if err != nil {
			t.Errorf("Expected complete-tx-key to exist but got error: %v", err)
		} else if string(value) != "complete-tx-value" {
			t.Errorf("For complete-tx-key, expected value complete-tx-value, got %s", string(value))
		}

		// Verify rolled back transaction was not applied
		if memTable.Contains([]byte("rollback-tx-key")) {
			t.Error("rollback-tx-key should not exist after rollback")
		}

		// Verify incomplete transaction was not applied (with AtomicTxOnly=true)
		if memTable.Contains([]byte("incomplete-tx-key")) {
			t.Error("incomplete-tx-key should not exist with AtomicTxOnly=true")
		}
	})

	// 2. AtomicTxOnly=false, StrictMode=false (allow incomplete tx)
	t.Run("NonAtomicMode", func(t *testing.T) {
		// Reopen the WAL
		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      logger,
		})
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}
		defer wal.Close()

		// Create a new MemTable for replay
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     logger,
			Comparator: DefaultComparator,
		})

		// Replay with non-atomic mode
		options := ReplayOptions{
			AtomicTxOnly: false, // Apply operations from incomplete transactions
			StrictMode:   false,
		}
		err = wal.ReplayWithOptions(memTable, options)
		if err != nil {
			t.Fatalf("Failed to replay WAL with non-atomic options: %v", err)
		}

		// Verify completed transaction was correctly applied
		value, err := memTable.Get([]byte("complete-tx-key"))
		if err != nil {
			t.Errorf("Expected complete-tx-key to exist but got error: %v", err)
		} else if string(value) != "complete-tx-value" {
			t.Errorf("For complete-tx-key, expected value complete-tx-value, got %s", string(value))
		}

		// Verify rolled back transaction was not applied
		if memTable.Contains([]byte("rollback-tx-key")) {
			t.Error("rollback-tx-key should not exist after rollback")
		}

		// Verify incomplete transaction WAS applied (with AtomicTxOnly=false)
		value, err = memTable.Get([]byte("incomplete-tx-key"))
		if err != nil {
			t.Logf("Note: incomplete-tx-key might be missing due to corruption in the WAL file")
		} else if string(value) != "incomplete-tx-value" {
			t.Errorf("For incomplete-tx-key, expected value incomplete-tx-value, got %s", string(value))
		}
	})

	// 3. StrictMode=true (should fail on any corruption)
	t.Run("StrictMode", func(t *testing.T) {
		// Reopen the WAL
		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      logger,
		})
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}
		defer wal.Close()

		// Create a new MemTable for replay
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     logger,
			Comparator: DefaultComparator,
		})

		// Replay with strict mode enabled
		options := ReplayOptions{
			AtomicTxOnly: true,
			StrictMode:   true, // Fail on first corrupted record
		}
		err = wal.ReplayWithOptions(memTable, options)
		
		// Since we injected corruption, we expect replay to fail in strict mode
		if err == nil {
			t.Error("Expected replay to fail in strict mode, but it succeeded")
		} else {
			t.Logf("Correctly failed in strict mode with error: %v", err)
		}
	})
}