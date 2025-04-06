package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TestWALRecoveryStatistics tests that the WAL recovery statistics are correctly tracked
func TestWALRecoveryStatistics(t *testing.T) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "wal_stats_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "stats.wal")

	// Create a new WAL
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true,
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Record various operations to test statistics tracking

	// Standalone operations
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("standalone-key-%d", i+1))
		value := []byte(fmt.Sprintf("standalone-value-%d", i+1))
		err = wal.RecordPut(key, value)
		if err != nil {
			t.Fatalf("Failed to record standalone put: %v", err)
		}
	}

	// Committed transaction
	txID1, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	for i := 0; i < 3; i++ {
		key := []byte(fmt.Sprintf("commit-key-%d", i+1))
		value := []byte(fmt.Sprintf("commit-value-%d", i+1))
		err = wal.RecordPutInTransaction(key, value, txID1)
		if err != nil {
			t.Fatalf("Failed to record put in transaction: %v", err)
		}
	}
	err = wal.CommitTransaction(txID1)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Rolled back transaction
	txID2, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	for i := 0; i < 2; i++ {
		key := []byte(fmt.Sprintf("rollback-key-%d", i+1))
		value := []byte(fmt.Sprintf("rollback-value-%d", i+1))
		err = wal.RecordPutInTransaction(key, value, txID2)
		if err != nil {
			t.Fatalf("Failed to record put in transaction: %v", err)
		}
	}
	err = wal.RollbackTransaction(txID2)
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Incomplete transaction
	txID3, err := wal.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	for i := 0; i < 2; i++ {
		key := []byte(fmt.Sprintf("incomplete-key-%d", i+1))
		value := []byte(fmt.Sprintf("incomplete-value-%d", i+1))
		err = wal.RecordPutInTransaction(key, value, txID3)
		if err != nil {
			t.Fatalf("Failed to record put in transaction: %v", err)
		}
	}
	// Deliberately don't commit or rollback

	// Close the WAL
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Test statistics with default options (AtomicTxOnly=true)
	t.Run("DefaultOptions", func(t *testing.T) {
		// Reopen the WAL
		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      model.NewNoOpLogger(),
		})
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}
		defer wal.Close()

		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     model.NewNoOpLogger(),
			Comparator: DefaultComparator,
		})

		// Use the direct EnhancedReplayWithOptions function to get statistics
		stats, err := EnhancedReplayWithOptions(wal, memTable, DefaultReplayOptions())
		if err != nil {
			t.Fatalf("Failed to replay WAL: %v", err)
		}

		// Verify statistics
		// Total record count: 5 standalone + 1 tx begin + 3 tx ops + 1 tx commit + 1 tx begin + 2 tx ops + 1 tx rollback + 1 tx begin + 2 tx ops = 17
		if stats.RecordCount != 17 {
			t.Errorf("Expected RecordCount=17, got %d", stats.RecordCount)
		}

		// Applied count: 5 standalone + 3 committed tx ops = 8
		if stats.AppliedCount != 8 {
			t.Errorf("Expected AppliedCount=8, got %d", stats.AppliedCount)
		}

		// Standalone operations: 5
		if stats.StandaloneOpCount != 5 {
			t.Errorf("Expected StandaloneOpCount=5, got %d", stats.StandaloneOpCount)
		}

		// Transaction operations: 3 from committed transaction
		if stats.TransactionOpCount != 3 {
			t.Errorf("Expected TransactionOpCount=3, got %d", stats.TransactionOpCount)
		}

		// Incomplete transactions: 1
		if stats.IncompleteTransactions != 1 {
			t.Errorf("Expected IncompleteTransactions=1, got %d", stats.IncompleteTransactions)
		}

		// Skipped transactions: 1 (incomplete transaction)
		if stats.SkippedTxCount != 1 {
			t.Errorf("Expected SkippedTxCount=1, got %d", stats.SkippedTxCount)
		}
	})

	// Test statistics with non-atomic options (AtomicTxOnly=false)
	t.Run("NonAtomicOptions", func(t *testing.T) {
		// Reopen the WAL
		wal, err = NewWAL(WALConfig{
			Path:        walPath,
			SyncOnWrite: true,
			Logger:      model.NewNoOpLogger(),
		})
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}
		defer wal.Close()

		memTable := NewMemTable(MemTableConfig{
			MaxSize:    1024 * 1024,
			Logger:     model.NewNoOpLogger(),
			Comparator: DefaultComparator,
		})

		// Use non-atomic options
		options := ReplayOptions{
			StrictMode:   false,
			AtomicTxOnly: false,
		}

		// Use the direct EnhancedReplayWithOptions function to get statistics
		stats, err := EnhancedReplayWithOptions(wal, memTable, options)
		if err != nil {
			t.Fatalf("Failed to replay WAL: %v", err)
		}

		// Verify statistics
		// Total record count: should be the same as before = 17
		if stats.RecordCount != 17 {
			t.Errorf("Expected RecordCount=17, got %d", stats.RecordCount)
		}

		// Applied count: 5 standalone + 3 committed tx ops + 2 incomplete tx ops = 10
		if stats.AppliedCount != 10 {
			t.Errorf("Expected AppliedCount=10, got %d", stats.AppliedCount)
		}

		// Standalone operations: 5
		if stats.StandaloneOpCount != 5 {
			t.Errorf("Expected StandaloneOpCount=5, got %d", stats.StandaloneOpCount)
		}

		// Transaction operations: 3 from committed + 2 from incomplete = 5
		if stats.TransactionOpCount != 5 {
			t.Errorf("Expected TransactionOpCount=5, got %d", stats.TransactionOpCount)
		}

		// Incomplete transactions: 1
		if stats.IncompleteTransactions != 1 {
			t.Errorf("Expected IncompleteTransactions=1, got %d", stats.IncompleteTransactions)
		}

		// Skipped transactions: 0 (we're applying incomplete transactions)
		if stats.SkippedTxCount != 0 {
			t.Errorf("Expected SkippedTxCount=0, got %d", stats.SkippedTxCount)
		}
	})
}
