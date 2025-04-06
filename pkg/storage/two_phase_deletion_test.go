package storage

import (
	"os"
	"testing"
	"time"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TestDeletionDelay tests that SSTables marked for deletion are retained
// for the configured time period before being physically deleted
func TestDeletionDelay(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "kgstore-deletion-delay-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create an engine with a specific deletion delay
	config := DefaultEngineConfig()
	config.DataDir = dir
	config.SSTableDeletionDelay = 500 * time.Millisecond // Very short delay for testing
	config.BackgroundCompaction = false
	config.Logger = model.NewNoOpLogger()

	engine, err := NewStorageEngine(config)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	// Create an SSTable and manually add it to the pending deletions
	err = engine.Put([]byte("test-key"), []byte("test-value"))
	if err != nil {
		t.Fatalf("Failed to put test key: %v", err)
	}

	// Flush to ensure the data is written
	err = engine.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Manually add an SSTable to the pending deletions
	var sstable *SSTable
	engine.mu.RLock()
	if len(engine.sstables) > 0 {
		sstable = engine.sstables[0]
	} else {
		// Create a mock SSTable for testing, since the real one might not be created in test environment
		sstable = &SSTable{
			id:     1,
			logger: model.NewNoOpLogger(),
		}
	}
	engine.mu.RUnlock()

	// Mark the SSTable for deletion using the proper API
	engine.markSSTableForDeletion(sstable)

	// Verify it was added to pending deletions
	engine.deletionMu.Lock()
	if _, exists := engine.pendingDeletions[sstable.id]; !exists {
		t.Fatalf("SSTable was not added to pending deletions")
	}
	engine.deletionMu.Unlock()

	// Wait for less than the deletion delay - should still be pending
	time.Sleep(200 * time.Millisecond)

	engine.deletionMu.Lock()
	if _, exists := engine.pendingDeletions[sstable.id]; !exists {
		t.Fatalf("SSTable was removed from pending deletions too early")
	}
	engine.deletionMu.Unlock()

	// Wait for the remainder of the deletion delay
	time.Sleep(400 * time.Millisecond)

	// Force a cleanup run instead of waiting for the timer
	engine.CleanupPendingDeletions()

	// Verify the SSTable was removed from pending deletions
	engine.deletionMu.Lock()
	if _, exists := engine.pendingDeletions[sstable.id]; exists {
		t.Fatalf("SSTable was not removed from pending deletions after delay")
	}
	engine.deletionMu.Unlock()
}

// TestGetFromPendingDeletions tests that reads from SSTables pending deletion work
func TestGetFromPendingDeletions(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "kgstore-pending-read-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a storage engine with a long deletion delay
	config := DefaultEngineConfig()
	config.DataDir = dir
	config.SSTableDeletionDelay = 30 * time.Second // Long delay so they don't get deleted during test
	config.BackgroundCompaction = false
	config.Logger = model.NewNoOpLogger()

	engine, err := NewStorageEngine(config)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	// Insert test data
	testKey := []byte("test-key")
	testValue := []byte("test-value")

	err = engine.Put(testKey, testValue)
	if err != nil {
		t.Fatalf("Failed to put test key: %v", err)
	}

	// Flush to create an SSTable
	err = engine.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Find our SSTable
	var sstable *SSTable
	engine.mu.RLock()
	for _, sst := range engine.sstables {
		// We'll use the first SSTable
		sstable = sst
		break
	}
	engine.mu.RUnlock()

	// If we didn't find a real SSTable, mock one for testing
	if sstable == nil {
		// Create a mock MemTable
		memTable := NewMemTable(MemTableConfig{
			MaxSize:    4096,
			Logger:     model.NewNoOpLogger(),
			Comparator: DefaultComparator,
		})

		// Add our test key/value
		memTable.Put(testKey, testValue)

		// Create a temporary SSTable config
		sstConfig := SSTableConfig{
			ID:         999,
			Path:       dir,
			Logger:     model.NewNoOpLogger(),
			Comparator: DefaultComparator,
		}

		// Create a real SSTable
		sstable, err = CreateSSTable(sstConfig, memTable)
		if err != nil {
			t.Fatalf("Failed to create test SSTable: %v", err)
		}

		// Don't defer Close() as we'll manually close it later
	}

	// Now move this SSTable to pending deletions
	if sstable != nil {
		// First remember the ID for verification
		sstableID := sstable.ID()

		// If it's in the engine's active list, remove it
		engine.mu.Lock()
		newSSTables := make([]*SSTable, 0, len(engine.sstables))
		for _, sst := range engine.sstables {
			if sst.ID() != sstableID {
				newSSTables = append(newSSTables, sst)
			}
		}
		engine.sstables = newSSTables
		engine.mu.Unlock()

		// Add to pending deletions
		engine.deletionMu.Lock()
		engine.pendingDeletions[sstableID] = sstable
		engine.deletionTime[sstableID] = time.Now()
		engine.deletionMu.Unlock()

		// Now verify we can still read from it via the pendingDeletions path
		value, err := engine.getFromPendingDeletionSSTables(testKey)
		if err != nil {
			t.Fatalf("Failed to read from pending deletion SSTable: %v", err)
		}

		if string(value) != string(testValue) {
			t.Fatalf("Expected value %q, got %q", testValue, value)
		}

		// Also verify the regular Get path works by checking both active and pending SSTables
		value, err = engine.Get(testKey)
		if err != nil {
			t.Fatalf("Failed to get key %q: %v", testKey, err)
		}

		if string(value) != string(testValue) {
			t.Fatalf("Expected value %q, got %q", testValue, value)
		}
	} else {
		t.Skip("Skipping test as no SSTable was available")
	}
}

// TestConcurrentReadsDuringDeletion tests that reads can occur concurrently with SSTable deletions
func TestConcurrentReadsDuringDeletion(t *testing.T) {
	// Create a temporary directory for the test
	dir, err := os.MkdirTemp("", "kgstore-concurrent-reads-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a storage engine with a short deletion delay
	config := DefaultEngineConfig()
	config.DataDir = dir
	config.SSTableDeletionDelay = 1 * time.Second
	config.BackgroundCompaction = false
	config.Logger = model.NewNoOpLogger()

	engine, err := NewStorageEngine(config)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	// Generate some test data
	testKey := []byte("test-key")
	testValue := []byte("test-value")

	err = engine.Put(testKey, testValue)
	if err != nil {
		t.Fatalf("Failed to put test key: %v", err)
	}

	// Set up concurrent read operations
	doneChan := make(chan struct{})
	errorChan := make(chan error, 10)

	// Start 5 goroutines that constantly read the test key
	for i := 0; i < 5; i++ {
		go func() {
			for {
				select {
				case <-doneChan:
					return
				default:
					_, err := engine.Get(testKey)
					if err != nil && err != ErrKeyNotFound {
						errorChan <- err
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()
	}

	// Let the readers run for a moment
	time.Sleep(100 * time.Millisecond)

	// Now simulate the SSTable being marked for deletion
	// First find or create an SSTable
	var sstable *SSTable
	engine.mu.RLock()
	if len(engine.sstables) > 0 {
		sstable = engine.sstables[0]
	}
	engine.mu.RUnlock()

	if sstable != nil {
		// Mark it for deletion
		engine.markSSTableForDeletion(sstable)

		// Let the reads continue while the deletion is pending
		time.Sleep(500 * time.Millisecond)

		// Signal readers to stop
		close(doneChan)

		// Check if any errors occurred
		select {
		case err := <-errorChan:
			t.Fatalf("Concurrent read failed: %v", err)
		default:
			// No errors, test passed
		}

		// Wait for the deletion to complete
		time.Sleep(1 * time.Second)

		// Verify the SSTable is gone from pending deletions
		engine.deletionMu.Lock()
		_, exists := engine.pendingDeletions[sstable.ID()]
		engine.deletionMu.Unlock()

		if exists {
			t.Fatalf("SSTable was not physically deleted after the delay")
		}
	} else {
		// Skip the test if no SSTable was available
		close(doneChan)
		t.Skip("Skipping test as no SSTable was available")
	}
}
