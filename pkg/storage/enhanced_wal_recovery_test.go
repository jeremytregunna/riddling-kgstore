package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// This is a test-only file that contains functions for testing transaction recovery

// ApplyIncompleteTransaction is a test-only function that applies operations from incomplete transactions
// if options.AtomicTxOnly is false
func ApplyIncompleteTransaction(w *WAL, memTable *MemTable, txID uint64, operations []WALRecord, options ReplayOptions) int {
	if !options.AtomicTxOnly {
		// If non-atomic transactions are allowed, apply operations from incomplete transactions
		w.logger.Debug("Applying incomplete transaction %d with %d operations (non-atomic mode)",
			txID, len(operations))

		txApplyCount := 0
		for _, op := range operations {
			if op.Type == RecordPut || op.Type == RecordDelete {
				var err error
				switch op.Type {
				case RecordPut:
					// Use the transaction ID as the version
					version := txID * 1000 // Multiply by 1000 to ensure higher txIDs get higher versions
					err = memTable.PutWithVersion(op.Key, op.Value, version)
				case RecordDelete:
					// Use the transaction ID as the version
					version := txID
					err = memTable.DeleteWithVersion(op.Key, version)
				}

				if err != nil {
					w.logger.Warn("Failed to apply operation from incomplete transaction %d: %v", txID, err)
				} else {
					txApplyCount++
				}
			}
		}

		w.logger.Debug("Applied %d operations from incomplete transaction %d (non-atomic mode)",
			txApplyCount, txID)
		return txApplyCount
	}
	return 0
}

// CheckTransactionCorruption is a test-only function that checks a transaction for corrupted records
func CheckTransactionCorruption(w *WAL, txID uint64, operations []WALRecord, options ReplayOptions) bool {
	// Check if this transaction has any corrupted records
	hasBadRecords := false
	for _, op := range operations {
		if op.Type == RecordPut || op.Type == RecordDelete {
			// Check for invalid key or value
			if op.Key == nil || (op.Type == RecordPut && op.Value == nil) {
				hasBadRecords = true
				msg := "partial application may occur"
				if options.AtomicTxOnly {
					msg = "skipping entire transaction"
				}
				w.logger.Warn("Transaction %d contains corrupted records, %s", txID, msg)
				break
			}
		}
	}

	// In atomic mode, skip the entire transaction if any corrupted records
	return hasBadRecords && options.AtomicTxOnly
}

// TestComplexWALRecovery tests WAL recovery in a variety of complex scenarios
// This is a test function that will be run by Go test
func TestComplexWALRecovery(t *testing.T) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "complex_wal_recovery_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Setup a warning logger to count the warnings during replay
	warningCount := 0
	logger := &warningCountLogger{
		t:            t,
		warningCount: &warningCount,
	}

	// Create a WAL with a mix of:
	// - Standalone operations
	// - Committed transactions
	// - Incomplete transactions
	// - Rolled back transactions
	walPath := filepath.Join(tempDir, "complex.wal")

	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Create some standalone operations
	standaloneKeys := []string{"standalone-1", "standalone-2", "standalone-3"}
	for i, key := range standaloneKeys {
		value := "value-" + key
		err = wal.RecordPut([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to record standalone put #%d: %v", i, err)
		}
	}

	// Create a committed transaction
	completeTxID, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin complete transaction: %v", err)
	}

	completeTxKeys := []string{"complete-1", "complete-2", "complete-3"}
	for i, key := range completeTxKeys {
		value := "value-" + key
		err = wal.RecordPutInTransaction([]byte(key), []byte(value), completeTxID)
		if err != nil {
			t.Fatalf("Failed to record complete transaction put #%d: %v", i, err)
		}
	}

	// Commit the transaction
	err = wal.CommitTransaction(completeTxID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Create a rolled back transaction
	rollbackTxID, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin rollback transaction: %v", err)
	}

	rollbackTxKeys := []string{"rollback-1", "rollback-2"}
	for i, key := range rollbackTxKeys {
		value := "value-" + key
		err = wal.RecordPutInTransaction([]byte(key), []byte(value), rollbackTxID)
		if err != nil {
			t.Fatalf("Failed to record rollback transaction put #%d: %v", i, err)
		}
	}

	// Roll back the transaction
	err = wal.RollbackTransaction(rollbackTxID)
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Create an incomplete transaction
	incompleteTxID, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin incomplete transaction: %v", err)
	}

	incompleteTxKeys := []string{"incomplete-1", "incomplete-2"}
	for i, key := range incompleteTxKeys {
		value := "value-" + key
		err = wal.RecordPutInTransaction([]byte(key), []byte(value), incompleteTxID)
		if err != nil {
			t.Fatalf("Failed to record incomplete transaction put #%d: %v", i, err)
		}
	}
	// Deliberately do NOT commit or rollback this transaction

	// Close the WAL to finalize its state
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Corrupt the WAL by inserting garbage bytes at specific positions
	walContent, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file for corruption: %v", err)
	}

	// Find a suitable position to insert corruption (around 70% of the way through)
	corruptionPoint := int(float64(len(walContent)) * 0.7)
	var corruptedContent []byte

	corruptedContent = append(corruptedContent, walContent[:corruptionPoint]...)
	corruptedContent = append(corruptedContent, []byte{0xFF, 0xEE, 0xDD, 0xCC, 0xBB}...) // Garbage data
	corruptedContent = append(corruptedContent, walContent[corruptionPoint:]...)

	// Overwrite the WAL file with our corrupted content
	err = os.WriteFile(walPath, corruptedContent, 0644)
	if err != nil {
		t.Fatalf("Failed to write corrupted WAL content: %v", err)
	}

	// Now test recovery with different options

	// Test 1: Default options (atomic transactions only, lenient mode)
	t.Run("DefaultOptions", func(t *testing.T) {
		// Reset warning counter
		warningCount = 0

		// Reopen the WAL
		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      logger,
		})
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}

		// Create a new MemTable for replay
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     logger,
			Comparator: DefaultComparator,
		})

		// Replay with default options
		err = wal.Replay(memTable)
		if err != nil {
			t.Fatalf("Failed to replay WAL with default options: %v", err)
		}

		// Verify warnings were logged for corrupted records
		if warningCount == 0 {
			t.Error("Expected warnings for corrupted records, but none were logged")
		}

		// Verify standalone operations were processed
		for _, key := range standaloneKeys {
			value, err := memTable.Get([]byte(key))
			if err != nil {
				// Some standalone ops might be corrupted, so skip if not found
				continue
			}
			expectedValue := "value-" + key
			if !bytes.Equal(value, []byte(expectedValue)) {
				t.Errorf("For standalone key %q, expected value %q, got %q",
					key, expectedValue, string(value))
			}
		}

		// Verify committed transaction operations were processed (possibly atomically)
		// If one record in the transaction was corrupted, none should be applied
		var foundComplete bool
		for _, key := range completeTxKeys {
			value, err := memTable.Get([]byte(key))
			if err == nil {
				foundComplete = true
				expectedValue := "value-" + key
				if !bytes.Equal(value, []byte(expectedValue)) {
					t.Errorf("For complete transaction key %q, expected value %q, got %q",
						key, expectedValue, string(value))
				}
			}
		}
		t.Logf("Complete transaction was %s", map[bool]string{
			true:  "applied (not corrupted)",
			false: "skipped (possibly corrupted)",
		}[foundComplete])

		// Verify rolled back transaction operations were NOT processed
		for _, key := range rollbackTxKeys {
			_, err := memTable.Get([]byte(key))
			if err == nil {
				t.Errorf("Rolled back transaction key %q was incorrectly applied", key)
			}
		}

		// Verify incomplete transaction operations were NOT processed (AtomicTxOnly=true by default)
		for _, key := range incompleteTxKeys {
			_, err := memTable.Get([]byte(key))
			if err == nil {
				t.Errorf("Incomplete transaction key %q was incorrectly applied with AtomicTxOnly=true", key)
			}
		}

		// Close the WAL
		err = wal.Close()
		if err != nil {
			t.Fatalf("Failed to close WAL: %v", err)
		}
	})

	// Test 2: Non-atomic transactions allowed (apply operations from incomplete transactions)
	t.Run("NonAtomicTransactions", func(t *testing.T) {
		// Reset warning counter
		warningCount = 0

		// Reopen the WAL
		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      logger,
		})
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}

		// Create a new MemTable for replay
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     logger,
			Comparator: DefaultComparator,
		})

		// Replay with non-atomic transactions allowed
		options := ReplayOptions{
			StrictMode:   false,
			AtomicTxOnly: false, // Apply operations from incomplete transactions
		}
		err = wal.ReplayWithOptions(memTable, options)
		if err != nil {
			t.Fatalf("Failed to replay WAL with non-atomic options: %v", err)
		}

		// Verify warnings were logged
		if warningCount == 0 {
			t.Error("Expected warnings for corrupted records, but none were logged")
		}

		// Check if incomplete transaction operations were applied with AtomicTxOnly=false
		incompleteApplied := false
		for _, key := range incompleteTxKeys {
			value, err := memTable.Get([]byte(key))
			if err == nil {
				incompleteApplied = true
				expectedValue := "value-" + key
				if !bytes.Equal(value, []byte(expectedValue)) {
					t.Errorf("For incomplete transaction key %q, expected value %q, got %q",
						key, expectedValue, string(value))
				}
			}
		}

		// With non-atomic mode, incomplete transactions should be applied
		// But since we inserted corruption, they might be skipped if the
		// corruption affects the incomplete transaction
		t.Logf("Incomplete transaction was %s", map[bool]string{
			true:  "applied (as expected with AtomicTxOnly=false)",
			false: "not applied (possibly corrupted or after corruption point)",
		}[incompleteApplied])

		// Close the WAL
		err = wal.Close()
		if err != nil {
			t.Fatalf("Failed to close WAL: %v", err)
		}
	})

	// Test 3: Strict mode (should fail on corruption)
	t.Run("StrictMode", func(t *testing.T) {
		// Reset warning counter
		warningCount = 0

		// Reopen the WAL
		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      logger,
		})
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}

		// Create a new MemTable for replay
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     logger,
			Comparator: DefaultComparator,
		})

		// Replay with strict mode enabled
		options := ReplayOptions{
			StrictMode:   true, // Fail on corruption
			AtomicTxOnly: true,
		}
		err = wal.ReplayWithOptions(memTable, options)

		// Expect an error in strict mode
		if err == nil {
			t.Error("Expected replay to fail in strict mode, but it succeeded")
		} else {
			t.Logf("Replay correctly failed in strict mode with error: %v", err)
		}

		// Close the WAL
		err = wal.Close()
		if err != nil {
			t.Fatalf("Failed to close WAL: %v", err)
		}
	})
}
