package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

func setupTestEngineWithSnapshot(t *testing.T) (*StorageEngine, string) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "kgstore-snapshot-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create a storage engine
	engine, err := NewStorageEngine(EngineConfig{
		DataDir:              tempDir,
		MemTableSize:         1024 * 1024, // 1MB
		SyncWrites:           true,
		BackgroundCompaction: false,
	})
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create storage engine: %v", err)
	}

	return engine, tempDir
}

func TestSnapshotBasics(t *testing.T) {
	engine, tempDir := setupTestEngineWithSnapshot(t)
	defer os.RemoveAll(tempDir)
	defer engine.Close()

	// Insert some initial data
	initialData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	// Use a transaction to insert the data
	tx, err := engine.BeginTransaction(DefaultTxOptions())
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for k, v := range initialData {
		if err := engine.PutWithTx(tx, []byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Create a read-only transaction to take a snapshot
	readTx1, err := engine.BeginTransaction(ReadOnlyTxOptions())
	if err != nil {
		t.Fatalf("Failed to begin read-only transaction: %v", err)
	}

	// Verify the snapshot contains all initial data
	for k, v := range initialData {
		value, err := engine.GetWithTx(readTx1, []byte(k))
		if err != nil {
			t.Fatalf("Failed to read key %s from snapshot: %v", k, err)
		}
		if !bytes.Equal(value, []byte(v)) {
			t.Errorf("Expected value %s for key %s, got %s", v, k, value)
		}
	}

	// Modify the data with a new transaction
	updatedData := map[string]string{
		"key1": "new-value1",
		"key4": "value4",
	}

	updateTx, err := engine.BeginTransaction(DefaultTxOptions())
	if err != nil {
		t.Fatalf("Failed to begin update transaction: %v", err)
	}

	for k, v := range updatedData {
		if err := engine.PutWithTx(updateTx, []byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	if err := updateTx.Commit(); err != nil {
		t.Fatalf("Failed to commit update transaction: %v", err)
	}

	// Create a second read-only transaction after the updates
	readTx2, err := engine.BeginTransaction(ReadOnlyTxOptions())
	if err != nil {
		t.Fatalf("Failed to begin second read-only transaction: %v", err)
	}

	// Verify the first snapshot still has the original data
	for k, v := range initialData {
		value, err := engine.GetWithTx(readTx1, []byte(k))
		if err != nil {
			t.Fatalf("Failed to read key %s from first snapshot: %v", k, err)
		}
		if !bytes.Equal(value, []byte(v)) {
			t.Errorf("First snapshot: Expected original value %s for key %s, got %s", v, k, value)
		}
	}

	// Key4 should not exist in the first snapshot
	_, err = engine.GetWithTx(readTx1, []byte("key4"))
	if err == nil {
		t.Errorf("First snapshot: key4 should not be found")
	}

	// Verify the second snapshot has the updated data
	value, err := engine.GetWithTx(readTx2, []byte("key1"))
	if err != nil {
		t.Fatalf("Failed to read key1 from second snapshot: %v", err)
	}
	if !bytes.Equal(value, []byte("new-value1")) {
		t.Errorf("Second snapshot: Expected new value 'new-value1' for key1, got %s", value)
	}

	value, err = engine.GetWithTx(readTx2, []byte("key4"))
	if err != nil {
		t.Fatalf("Failed to read key4 from second snapshot: %v", err)
	}
	if !bytes.Equal(value, []byte("value4")) {
		t.Errorf("Second snapshot: Expected value 'value4' for key4, got %s", value)
	}

	// Clean up
	readTx1.Commit()
	readTx2.Commit()
}

func TestSnapshotAfterSSTableFlush(t *testing.T) {
	engine, tempDir := setupTestEngineWithSnapshot(t)
	defer os.RemoveAll(tempDir)
	defer engine.Close()

	// Insert enough data to force a MemTable flush
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := engine.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	// Force a flush to create an SSTable
	if err := engine.Flush(); err != nil {
		t.Fatalf("Failed to flush MemTable: %v", err)
	}

	// Create a read-only transaction to take a snapshot
	readTx, err := engine.BeginTransaction(ReadOnlyTxOptions())
	if err != nil {
		t.Fatalf("Failed to begin read-only transaction: %v", err)
	}

	// Verify the snapshot can read from the SSTable
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expectedValue := []byte(fmt.Sprintf("value-%d", i))
		value, err := engine.GetWithTx(readTx, key)
		if err != nil {
			t.Fatalf("Failed to read key %s from snapshot: %v", key, err)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Expected value %s for key %s, got %s", expectedValue, key, value)
		}
	}

	// Update some keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		newValue := []byte(fmt.Sprintf("new-value-%d", i))
		if err := engine.Put(key, newValue); err != nil {
			t.Fatalf("Failed to update key-value pair: %v", err)
		}
	}

	// Create a new read-only transaction after the updates
	readTx2, err := engine.BeginTransaction(ReadOnlyTxOptions())
	if err != nil {
		t.Fatalf("Failed to begin second read-only transaction: %v", err)
	}

	// First snapshot should still see original values
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expectedValue := []byte(fmt.Sprintf("value-%d", i))
		value, err := engine.GetWithTx(readTx, key)
		if err != nil {
			t.Fatalf("Failed to read key %s from first snapshot: %v", key, err)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("First snapshot: Expected original value %s for key %s, got %s", 
				expectedValue, key, value)
		}
	}

	// Second snapshot should see updated values
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expectedValue := []byte(fmt.Sprintf("new-value-%d", i))
		value, err := engine.GetWithTx(readTx2, key)
		if err != nil {
			t.Fatalf("Failed to read key %s from second snapshot: %v", key, err)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Second snapshot: Expected updated value %s for key %s, got %s", 
				expectedValue, key, value)
		}
	}

	// Clean up
	readTx.Commit()
	readTx2.Commit()
}

func TestSnapshotCleanup(t *testing.T) {
	engine, tempDir := setupTestEngineWithSnapshot(t)
	defer os.RemoveAll(tempDir)
	defer engine.Close()

	// Force initialize the snapshot manager
	if engine.snapshotManager == nil {
		engine.snapshotManager = NewSnapshotManager(engine)
	}

	// Create several read-only transactions to take snapshots
	var txs []*Transaction
	for i := 0; i < 5; i++ {
		tx, err := engine.BeginTransaction(ReadOnlyTxOptions())
		if err != nil {
			t.Fatalf("Failed to begin read-only transaction %d: %v", i, err)
		}
		
		// Manually verify that each transaction has a snapshot
		if tx.snapshot == nil {
			t.Logf("Warning: Transaction %d has no snapshot", i)
		} else {
			t.Logf("Transaction %d has snapshot ID %d with refCount %d", 
				i, tx.snapshot.ID(), tx.snapshot.ReferenceCount())
		}
		
		txs = append(txs, tx)
	}

	// Verify snapshots were created
	if engine.snapshotManager == nil {
		t.Fatal("Snapshot manager should be initialized")
	}

	// Count snapshots before any commits
	engine.snapshotManager.mu.RLock()
	beforeCommitCount := len(engine.snapshotManager.snapshots)
	engine.snapshotManager.mu.RUnlock()

	t.Logf("Before any commits: %d snapshots in manager", beforeCommitCount)
	if beforeCommitCount != 5 {
		t.Errorf("Expected 5 snapshots, got %d", beforeCommitCount)
	}

	// Commit a few transactions
	for i := 0; i < 3; i++ {
		t.Logf("Committing transaction %d", i)
		if err := txs[i].Commit(); err != nil {
			t.Fatalf("Failed to commit transaction %d: %v", i, err)
		}
	}

	// Force snapshot refcounts to update for test purposes
	// Due to the way snapshots are released, sometimes we need to explicitly wait
	// or check that the refcounts have been properly decremented
	time.Sleep(10 * time.Millisecond)

	// Verify the reference counts in snapshots
	engine.snapshotManager.mu.RLock()
	zeroRefCount := 0
	for id, s := range engine.snapshotManager.snapshots {
		s.mu.RLock()
		refCount := s.refCount
		s.mu.RUnlock()
		t.Logf("After commits: Snapshot %d has refCount %d", id, refCount)
		if refCount == 0 {
			zeroRefCount++
		}
	}
	engine.snapshotManager.mu.RUnlock()
	t.Logf("Found %d snapshots with refCount = 0", zeroRefCount)

	// Count snapshots after the first 3 commits - should be 2 since 3 were auto-removed
	engine.snapshotManager.mu.RLock()
	afterCommitCount := len(engine.snapshotManager.snapshots)
	t.Logf("After committing 3 txs: %d snapshots remain", afterCommitCount)
	engine.snapshotManager.mu.RUnlock()
	
	if afterCommitCount != 2 {
		t.Errorf("Expected 2 snapshots remaining after committing 3 transactions, got %d", afterCommitCount)
	}

	// Our implementation automatically removes snapshots when their refCount reaches 0
	// in the ReleaseSnapshot method. This means CleanupSnapshots should find 0.
	t.Logf("Running CleanupSnapshots(0) - should find 0 since snapshots are auto-cleaned")
	cleaned := engine.snapshotManager.CleanupSnapshots(0)
	t.Logf("CleanupSnapshots returned: %d snapshots cleaned", cleaned)
	
	// Since our implementation already removed them at commit time, we expect 0 here
	if cleaned != 0 {
		t.Errorf("Expected 0 snapshots cleaned by CleanupSnapshots since they were auto-cleaned, got %d", cleaned)
	}

	// Commit the remaining transactions
	for i := 3; i < 5; i++ {
		t.Logf("Committing transaction %d", i)
		if err := txs[i].Commit(); err != nil {
			t.Fatalf("Failed to commit transaction %d: %v", i, err)
		}
	}

	// Force snapshot refcounts to update for test purposes
	time.Sleep(10 * time.Millisecond)

	// Count snapshots after all commits - all should be auto-removed
	engine.snapshotManager.mu.RLock()
	finalCount := len(engine.snapshotManager.snapshots)
	t.Logf("After all commits: %d snapshots remain", finalCount)
	engine.snapshotManager.mu.RUnlock()

	if finalCount != 0 {
		t.Errorf("Expected 0 snapshots remaining after all commits, got %d", finalCount)
	}

	// Run cleanup again - should find 0 since all were auto-removed
	t.Logf("Running CleanupSnapshots(0) again")
	cleaned = engine.snapshotManager.CleanupSnapshots(0)
	t.Logf("Second CleanupSnapshots returned: %d snapshots cleaned", cleaned)
	
	// Since our implementation already removed them at commit time, we expect 0 here
	if cleaned != 0 {
		t.Errorf("Expected 0 snapshots cleaned by CleanupSnapshots since they were auto-cleaned, got %d", cleaned)
	}
}

func BenchmarkSnapshotReads(b *testing.B) {
	// Create a temporary directory for the benchmark
	tempDir, err := os.MkdirTemp("", "kgstore-snapshot-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a storage engine
	engine, err := NewStorageEngine(EngineConfig{
		DataDir:              tempDir,
		MemTableSize:         10 * 1024 * 1024, // 10MB
		SyncWrites:           false,            // Disable sync for benchmarking
		BackgroundCompaction: false,
	})
	if err != nil {
		b.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	// Insert some data
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("bench-key-%d", i))
		value := []byte(fmt.Sprintf("bench-value-%d", i))
		if err := engine.Put(key, value); err != nil {
			b.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	b.ResetTimer()

	b.Run("DirectRead", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx := i % 10000
			key := []byte(fmt.Sprintf("bench-key-%d", idx))
			_, err := engine.Get(key)
			if err != nil {
				b.Fatalf("Failed to get key: %v", err)
			}
		}
	})

	b.Run("SnapshotRead", func(b *testing.B) {
		// Create a read-only transaction to take a snapshot
		readTx, err := engine.BeginTransaction(ReadOnlyTxOptions())
		if err != nil {
			b.Fatalf("Failed to begin read-only transaction: %v", err)
		}
		defer readTx.Commit()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx := i % 10000
			key := []byte(fmt.Sprintf("bench-key-%d", idx))
			_, err := engine.GetWithTx(readTx, key)
			if err != nil {
				b.Fatalf("Failed to get key from snapshot: %v", err)
			}
		}
	})

	b.Run("ConcurrentSnapshotReads", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			// Each goroutine creates its own snapshot
			readTx, err := engine.BeginTransaction(ReadOnlyTxOptions())
			if err != nil {
				b.Fatalf("Failed to begin read-only transaction: %v", err)
			}
			defer readTx.Commit()

			i := 0
			for pb.Next() {
				idx := i % 10000
				key := []byte(fmt.Sprintf("bench-key-%d", idx))
				_, err := engine.GetWithTx(readTx, key)
				if err != nil {
					b.Fatalf("Failed to get key from snapshot: %v", err)
				}
				i++
			}
		})
	})
}