package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TestWALRecoveryStatistics tests that the WAL recovery correctly applies transactions
// with different options. Since we no longer expose the internal statistics directly,
// we'll test the behavior by examining the memtable state after replay.
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

	// Test behavior with default options (AtomicTxOnly=true)
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

		// Replay the WAL with default options
		err = wal.Replay(memTable)
		if err != nil {
			t.Fatalf("Failed to replay WAL: %v", err)
		}

		// Verify that standalone operations were applied
		for i := 0; i < 5; i++ {
			key := []byte(fmt.Sprintf("standalone-key-%d", i+1))
			value, err := memTable.Get(key)
			if err != nil {
				t.Errorf("Failed to get standalone key %s: %v", key, err)
			} else if string(value) != fmt.Sprintf("standalone-value-%d", i+1) {
				t.Errorf("Wrong value for standalone key %s: %s", key, value)
			}
		}

		// Verify that committed transaction operations were applied
		for i := 0; i < 3; i++ {
			key := []byte(fmt.Sprintf("commit-key-%d", i+1))
			value, err := memTable.Get(key)
			if err != nil {
				t.Errorf("Failed to get committed tx key %s: %v", key, err)
			} else if string(value) != fmt.Sprintf("commit-value-%d", i+1) {
				t.Errorf("Wrong value for committed tx key %s: %s", key, value)
			}
		}

		// Verify that rolled back transaction operations were NOT applied
		for i := 0; i < 2; i++ {
			key := []byte(fmt.Sprintf("rollback-key-%d", i+1))
			_, err := memTable.Get(key)
			if err != ErrKeyNotFound {
				t.Errorf("Rolled back tx key %s should not exist, but got %v", key, err)
			}
		}

		// Verify that incomplete transaction operations were NOT applied (due to AtomicTxOnly=true)
		for i := 0; i < 2; i++ {
			key := []byte(fmt.Sprintf("incomplete-key-%d", i+1))
			_, err := memTable.Get(key)
			if err != ErrKeyNotFound {
				t.Errorf("Incomplete tx key %s should not exist with AtomicTxOnly=true, but got %v", key, err)
			}
		}
	})

	// Test behavior with non-atomic options (AtomicTxOnly=false)
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

		// Replay the WAL with non-atomic options
		err = wal.ReplayWithOptions(memTable, options)
		if err != nil {
			t.Fatalf("Failed to replay WAL: %v", err)
		}

		// Verify that standalone operations were applied
		for i := 0; i < 5; i++ {
			key := []byte(fmt.Sprintf("standalone-key-%d", i+1))
			value, err := memTable.Get(key)
			if err != nil {
				t.Errorf("Failed to get standalone key %s: %v", key, err)
			} else if string(value) != fmt.Sprintf("standalone-value-%d", i+1) {
				t.Errorf("Wrong value for standalone key %s: %s", key, value)
			}
		}

		// Verify that committed transaction operations were applied
		for i := 0; i < 3; i++ {
			key := []byte(fmt.Sprintf("commit-key-%d", i+1))
			value, err := memTable.Get(key)
			if err != nil {
				t.Errorf("Failed to get committed tx key %s: %v", key, err)
			} else if string(value) != fmt.Sprintf("commit-value-%d", i+1) {
				t.Errorf("Wrong value for committed tx key %s: %s", key, value)
			}
		}

		// Verify that rolled back transaction operations were NOT applied
		for i := 0; i < 2; i++ {
			key := []byte(fmt.Sprintf("rollback-key-%d", i+1))
			_, err := memTable.Get(key)
			if err != ErrKeyNotFound {
				t.Errorf("Rolled back tx key %s should not exist, but got %v", key, err)
			}
		}

		// Verify that incomplete transaction operations WERE applied (due to AtomicTxOnly=false)
		for i := 0; i < 2; i++ {
			key := []byte(fmt.Sprintf("incomplete-key-%d", i+1))
			value, err := memTable.Get(key)
			if err != nil {
				t.Errorf("Failed to get incomplete tx key %s with AtomicTxOnly=false: %v", key, err)
			} else if string(value) != fmt.Sprintf("incomplete-value-%d", i+1) {
				t.Errorf("Wrong value for incomplete tx key %s: %s", key, value)
			}
		}
	})
}
