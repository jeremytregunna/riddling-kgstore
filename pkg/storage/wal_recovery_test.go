package storage

import (
	"os"
	"path/filepath"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// warningCountLogger is a custom logger that counts warnings and implements model.Logger
type warningCountLogger struct {
	t            *testing.T
	warningCount *int
}

func (l *warningCountLogger) Debug(format string, args ...interface{}) {
	l.t.Logf("[DEBUG] "+format, args...)
}

func (l *warningCountLogger) Info(format string, args ...interface{}) {
	l.t.Logf("[INFO] "+format, args...)
}

func (l *warningCountLogger) Warn(format string, args ...interface{}) {
	l.t.Logf("[WARN] "+format, args...)
	*l.warningCount++
	// Satisfies model.Logger interface
}

func (l *warningCountLogger) Error(format string, args ...interface{}) {
	l.t.Logf("[ERROR] "+format, args...)
}

// IsLevelEnabled implements the model.Logger interface
func (l *warningCountLogger) IsLevelEnabled(level model.LogLevel) bool {
	return true // Always enabled for tests
}

// TestWALCorruptedRecovery tests how the WAL handles recovery when encountering corrupted records
func TestWALCorruptedRecovery(t *testing.T) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "wal_corrupted_recovery_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "corrupted.wal")

	// Create a custom logger that captures warnings
	warningCount := 0

	// Custom logger implementation that counts warnings
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

	// Record some valid operations
	err = wal.RecordPut([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to record put: %v", err)
	}

	err = wal.RecordPut([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Failed to record put: %v", err)
	}

	// Begin a transaction
	txID, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Add some operations to the transaction
	err = wal.RecordPutInTransaction([]byte("tx-key1"), []byte("tx-value1"), txID)
	if err != nil {
		t.Fatalf("Failed to record put in transaction: %v", err)
	}

	err = wal.RecordPutInTransaction([]byte("tx-key2"), []byte("tx-value2"), txID)
	if err != nil {
		t.Fatalf("Failed to record put in transaction: %v", err)
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

	// Corrupt the WAL file by appending invalid data
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open WAL file for corruption: %v", err)
	}

	// Append some garbage bytes to simulate corruption
	corruptData := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	_, err = file.Write(corruptData)
	if err != nil {
		t.Fatalf("Failed to write corrupt data: %v", err)
	}
	file.Close()

	// Now try to replay the corrupted WAL
	wal, err = NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	// Test Lenient Mode (default)
	t.Run("Lenient Mode", func(t *testing.T) {
		// Reset warning count for this test
		warningCount = 0

		// Replay the WAL into a MemTable
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     logger,
			Comparator: DefaultComparator,
		})

		// The default mode should log warnings but continue processing
		options := DefaultReplayOptions()
		err = wal.ReplayWithOptions(memTable, options)

		if err != nil {
			t.Errorf("Expected lenient WAL replay to succeed despite corruption, got error: %v", err)
		}

		// Verify warnings were generated for the corrupt records
		if warningCount == 0 {
			t.Error("Expected warnings during replay of corrupted WAL, but none were logged")
		}

		// Verify valid records were still processed
		validRecords := []struct {
			key   string
			value string
		}{
			{"key1", "value1"},
			{"key2", "value2"},
			{"tx-key1", "tx-value1"},
			{"tx-key2", "tx-value2"},
		}

		for _, record := range validRecords {
			value, err := memTable.Get([]byte(record.key))
			if err != nil {
				t.Errorf("Failed to get key %q after corrupted replay: %v", record.key, err)
			} else if string(value) != record.value {
				t.Errorf("For key %q, expected value %q, got %q", record.key, record.value, string(value))
			}
		}
	})

	// Test Strict Mode
	t.Run("Strict Mode", func(t *testing.T) {
		// Reset warning count for this test
		warningCount = 0

		// Replay the WAL into a fresh MemTable
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     logger,
			Comparator: DefaultComparator,
		})

		// In strict mode, we expect the replay to fail on corruption
		options := ReplayOptions{
			StrictMode:   true,
			AtomicTxOnly: true,
		}

		err = wal.ReplayWithOptions(memTable, options)

		// We expect an error in strict mode
		if err == nil {
			t.Error("Expected strict mode WAL replay to fail on corruption, but it succeeded")
		} else {
			t.Logf("Strict mode correctly failed with error: %v", err)
		}
	})

	// Close the WAL
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}
}

// TestWALIncompleteTransactionRecovery tests how the WAL handles recovery of incomplete transactions
func TestWALIncompleteTransactionRecovery(t *testing.T) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "wal_incomplete_tx_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "incomplete_tx.wal")

	// Create a new WAL
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.DefaultLoggerInstance,
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Record some standalone operations
	err = wal.RecordPut([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to record put: %v", err)
	}

	err = wal.RecordPut([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("Failed to record put: %v", err)
	}

	// Begin a complete transaction
	completeTxID, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Add operations to the complete transaction
	err = wal.RecordPutInTransaction([]byte("complete-tx-key1"), []byte("complete-tx-value1"), completeTxID)
	if err != nil {
		t.Fatalf("Failed to record put in transaction: %v", err)
	}

	err = wal.RecordDeleteInTransaction([]byte("complete-tx-key-delete"), completeTxID)
	if err != nil {
		t.Fatalf("Failed to record delete in transaction: %v", err)
	}

	// Commit the complete transaction
	err = wal.CommitTransaction(completeTxID)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Begin an incomplete transaction
	incompleteTxID, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Add operations to the incomplete transaction
	err = wal.RecordPutInTransaction([]byte("incomplete-tx-key1"), []byte("incomplete-tx-value1"), incompleteTxID)
	if err != nil {
		t.Fatalf("Failed to record put in transaction: %v", err)
	}

	err = wal.RecordPutInTransaction([]byte("incomplete-tx-key2"), []byte("incomplete-tx-value2"), incompleteTxID)
	if err != nil {
		t.Fatalf("Failed to record put in transaction: %v", err)
	}

	// Deliberately do NOT commit this transaction

	// Close the WAL to ensure all data is written
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Reopen the WAL
	wal, err = NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.DefaultLoggerInstance,
	})
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}

	// Test with atomic transactions only (default)
	t.Run("Atomic Transactions Only", func(t *testing.T) {
		// Replay the WAL into a MemTable with default options
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     model.DefaultLoggerInstance,
			Comparator: DefaultComparator,
		})

		options := DefaultReplayOptions() // Default is AtomicTxOnly = true
		err := wal.ReplayWithOptions(memTable, options)
		if err != nil {
			t.Errorf("Expected replay to succeed, got error: %v", err)
		}

		// Verify standalone and complete transaction records were applied
		validRecords := []struct {
			key         string
			value       string
			shouldExist bool
		}{
			// Standalone records
			{"key1", "value1", true},
			{"key2", "value2", true},
			// Complete transaction records
			{"complete-tx-key1", "complete-tx-value1", true},
			{"complete-tx-key-delete", "", false}, // This was deleted
			// Incomplete transaction records - should NOT be present
			{"incomplete-tx-key1", "incomplete-tx-value1", false},
			{"incomplete-tx-key2", "incomplete-tx-value2", false},
		}

		for _, record := range validRecords {
			value, err := memTable.Get([]byte(record.key))
			if record.shouldExist {
				if err != nil {
					t.Errorf("Expected key %q to exist but got error: %v", record.key, err)
				} else if string(value) != record.value {
					t.Errorf("For key %q, expected value %q, got %q", record.key, record.value, string(value))
				}
			} else {
				// Should NOT exist
				if err == nil {
					t.Errorf("Key %q should not exist but was found with value: %q", record.key, string(value))
				} else if err != ErrKeyNotFound {
					t.Errorf("Unexpected error for key %q: %v", record.key, err)
				}
			}
		}
	})

	// Test with non-atomic transactions allowed
	t.Run("Non-Atomic Transactions Allowed", func(t *testing.T) {
		// Replay the WAL into a MemTable with non-atomic transactions allowed
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     model.DefaultLoggerInstance,
			Comparator: DefaultComparator,
		})

		options := ReplayOptions{
			StrictMode:   false,
			AtomicTxOnly: false, // Allow non-atomic transactions
		}

		err := wal.ReplayWithOptions(memTable, options)
		if err != nil {
			t.Errorf("Expected replay to succeed, got error: %v", err)
		}

		// Verify standalone, complete transaction, and incomplete transaction records
		validRecords := []struct {
			key         string
			value       string
			shouldExist bool
		}{
			// Standalone records
			{"key1", "value1", true},
			{"key2", "value2", true},
			// Complete transaction records
			{"complete-tx-key1", "complete-tx-value1", true},
			{"complete-tx-key-delete", "", false}, // This was deleted
			// Incomplete transaction records - SHOULD be present when AtomicTxOnly=false
			{"incomplete-tx-key1", "incomplete-tx-value1", true},
			{"incomplete-tx-key2", "incomplete-tx-value2", true},
		}

		for _, record := range validRecords {
			value, err := memTable.Get([]byte(record.key))
			if record.shouldExist {
				if err != nil {
					t.Errorf("Expected key %q to exist but got error: %v", record.key, err)
				} else if string(value) != record.value {
					t.Errorf("For key %q, expected value %q, got %q", record.key, record.value, string(value))
				}
			} else {
				// Should NOT exist
				if err == nil {
					t.Errorf("Key %q should not exist but was found with value: %q", record.key, string(value))
				} else if err != ErrKeyNotFound {
					t.Errorf("Unexpected error for key %q: %v", record.key, err)
				}
			}
		}
	})

	// Close the WAL
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}
}
