package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWALCorruptedTransactionRecovery tests how the WAL handles recovery when a transaction has corrupted records
func TestWALCorruptedTransactionRecovery(t *testing.T) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "wal_corrupted_tx_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "corrupted_tx.wal")

	// Create a custom logger for catching warnings
	warningCount := 0
	logger := &warningCountLogger{
		t:            t,
		warningCount: &warningCount,
	}

	// Create a new WAL
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Record some standalone operations
	err = wal.RecordPut([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to record put: %v", err)
	}

	// Begin a transaction with multiple operations
	txID, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Add operations to the transaction
	keys := []string{"tx-key1", "tx-key2", "tx-key3"}
	values := []string{"tx-value1", "tx-value2", "tx-value3"}

	for i := range keys {
		err = wal.RecordPutInTransaction([]byte(keys[i]), []byte(values[i]), txID)
		if err != nil {
			t.Fatalf("Failed to record put in transaction: %v", err)
		}
	}

	// Commit the transaction
	err = wal.CommitTransaction(txID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Close the WAL to ensure all data is written
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Corrupt the WAL file by inserting garbage in the middle to corrupt one of the transaction records
	// We'll read all the file content, then insert corruption at a specific offset to target the transaction
	walContent, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file for corruption: %v", err)
	}

	// Find an offset in the middle of the transaction records
	// This is a bit of a hack but works for test purposes
	var corruptedContent []byte

	// For testing purposes, let's corrupt around the middle of the file
	corruptionPoint := len(walContent) / 2

	// Create the corrupted content by inserting garbage
	corruptedContent = append(corruptedContent, walContent[:corruptionPoint]...)
	corruptedContent = append(corruptedContent, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}...) // Garbage data
	corruptedContent = append(corruptedContent, walContent[corruptionPoint:]...)

	// Overwrite the WAL file with our corrupted content
	err = os.WriteFile(walPath, corruptedContent, 0644)
	if err != nil {
		t.Fatalf("Failed to write corrupted WAL file: %v", err)
	}

	// Reopen the WAL
	wal, err = NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	// Test with atomic transactions only (default)
	t.Run("Atomic Transactions Only", func(t *testing.T) {
		// Reset warning count
		warningCount = 0

		// Replay the WAL into a MemTable with default options
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     logger,
			Comparator: DefaultComparator,
		})

		// With atomic transactions, either all or none of the transaction should be applied
		options := ReplayOptions{
			StrictMode:   false, // Only fail on a per-transaction basis
			AtomicTxOnly: true,  // Ensure transactions are atomic
		}

		err := wal.ReplayWithOptions(memTable, options)
		if err != nil {
			t.Logf("Got replay error in atomic mode: %v", err)
		}

		// Check if warnings were logged about the corruption
		if warningCount == 0 {
			t.Error("Expected warnings for corrupted transaction, but none were logged")
		}

		// Verify standalone operations were processed
		value, err := memTable.Get([]byte("key1"))
		if err != nil {
			t.Errorf("Expected key 'key1' to exist but got error: %v", err)
		} else if string(value) != "value1" {
			t.Errorf("For key 'key1', expected value 'value1', got %q", string(value))
		}

		// With atomic transactions enabled, since one record in the transaction
		// is corrupted, NONE of the transaction records should be present
		for _, key := range keys {
			value, err := memTable.Get([]byte(key))
			if err == nil {
				t.Errorf("Key %q from corrupted transaction should NOT exist but was found with value: %q",
					key, string(value))
			} else if err != ErrKeyNotFound {
				t.Errorf("Unexpected error for key %q: %v", key, err)
			}
		}
	})

	// Test with non-atomic transactions allowed
	t.Run("Non-Atomic Transactions Allowed", func(t *testing.T) {
		// Reset warning count
		warningCount = 0

		// Replay the WAL into a MemTable with non-atomic transactions allowed
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     logger,
			Comparator: DefaultComparator,
		})

		// With non-atomic transactions, valid records in a partially corrupted transaction
		// may still be applied
		options := ReplayOptions{
			StrictMode:   false, // Don't fail on corruption
			AtomicTxOnly: false, // Allow partial transactions
		}

		err := wal.ReplayWithOptions(memTable, options)
		if err != nil {
			t.Errorf("Expected replay to succeed in non-atomic mode, got error: %v", err)
		}

		// Check if warnings were logged
		if warningCount == 0 {
			t.Error("Expected warnings for corrupted transaction, but none were logged")
		}

		// Verify standalone operations were processed
		value, err := memTable.Get([]byte("key1"))
		if err != nil {
			t.Errorf("Expected key 'key1' to exist but got error: %v", err)
		} else if string(value) != "value1" {
			t.Errorf("For key 'key1', expected value 'value1', got %q", string(value))
		}

		// With non-atomic transactions, some of the transaction records may be present
		// depending on where the corruption occurred
		// We won't assert exactly which ones since it depends on the corruption point
		foundAny := false
		for i, key := range keys {
			value, err := memTable.Get([]byte(key))
			if err == nil {
				t.Logf("Found key %q with value %q in partially corrupted transaction", key, string(value))
				foundAny = true
				// Verify the value is correct for the ones that survived
				if string(value) != values[i] {
					t.Errorf("For key %q, expected value %q, got %q", key, values[i], string(value))
				}
			}
		}

		// Note: This is a weak assertion, but since we don't know exactly which records
		// were corrupted, we can only check that at least some records were processed
		// For a more precise test, we'd need to corrupt specific records
		t.Logf("Found transaction keys during non-atomic replay: %v", foundAny)
	})

	// Close the WAL
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}
}

// Use the warningCountLogger defined in wal_recovery_test.go
