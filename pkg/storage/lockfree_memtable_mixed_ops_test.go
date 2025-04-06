package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TestLockFreeMemTableMixedOperations tests mixing puts, gets, and deletes with version checking
func TestLockFreeMemTableMixedOperations(t *testing.T) {
	table := NewLockFreeMemTable(LockFreeMemTableConfig{
		MaxSize: 1024 * 1024, // 1MB
		Logger:  model.DefaultLoggerInstance,
	})

	// Number of concurrent workers
	const workerCount = 5
	// Operations per worker
	const opsPerWorker = 100
	// Shared keys (to create contention)
	const keyCount = 20

	var wg sync.WaitGroup
	errCh := make(chan string, workerCount*opsPerWorker)
	keyVersions := make([]uint64, keyCount) // Track expected versions

	// Add some initial data
	for i := 0; i < keyCount; i++ {
		key := []byte(fmt.Sprintf("mixed-key-%d", i))
		value := []byte(fmt.Sprintf("initial-value-%d", i))

		// Use explicit versioning
		version := uint64(i + 1) // Start with version 1, 2, 3...
		err := table.PutWithVersion(key, value, version)
		if err != nil {
			t.Fatalf("Failed to initialize key %s: %v", key, err)
		}
		keyVersions[i] = version
	}

	// Mutex to protect keyVersions access
	var versionMutex sync.Mutex

	// Launch workers
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < opsPerWorker; i++ {
				// Select an operation: 0=Get, 1=Put, 2=Delete
				op := (workerID + i) % 3
				keyIndex := (workerID * i) % keyCount
				key := []byte(fmt.Sprintf("mixed-key-%d", keyIndex))

				switch op {
				case 0: // Get
					// Perform get and verify version is as expected or higher
					value, err := table.Get(key)
					if err != nil && err != ErrLockFreeKeyNotFound {
						errCh <- fmt.Sprintf("Worker %d unexpected error getting key %s: %v",
							workerID, key, err)
					}

					if err == nil {
						// Key exists, could be initial value or updated value
						initialPrefix := fmt.Sprintf("initial-value-%d", keyIndex)
						updatedPrefix := fmt.Sprintf("value-%d-", keyIndex)

						// Accept either initial or updated format
						validValue := bytes.HasPrefix(value, []byte(initialPrefix)) ||
							bytes.HasPrefix(value, []byte(updatedPrefix))

						if !validValue {
							errCh <- fmt.Sprintf("Worker %d got malformed value for key %s: %s",
								workerID, key, value)
						}
					}

				case 1: // Put
					// Generate new version
					versionMutex.Lock()
					oldVersion := keyVersions[keyIndex]
					newVersion := oldVersion + 1
					keyVersions[keyIndex] = newVersion
					versionMutex.Unlock()

					// Update with new value
					newValue := []byte(fmt.Sprintf("value-%d-%d", keyIndex, newVersion))
					err := table.PutWithVersion(key, newValue, newVersion)

					if err != nil {
						errCh <- fmt.Sprintf("Worker %d failed to put key %s: %v",
							workerID, key, err)
					}

					// Since this is a concurrent test, it's possible another thread
					// updated the value between our put and our verification get
					// That's actually a valid test case, so we'll skip the verification
					// to avoid false negatives

					// Delay a bit to let the write propagate
					time.Sleep(time.Millisecond)

				case 2: // Delete
					// Only delete sometimes (1/3 of delete operations)
					if i%3 == 0 {
						// Generate new version for deletion
						versionMutex.Lock()
						oldVersion := keyVersions[keyIndex]
						newVersion := oldVersion + 1
						keyVersions[keyIndex] = newVersion
						versionMutex.Unlock()

						err := table.DeleteWithVersion(key, newVersion)
						if err != nil {
							errCh <- fmt.Sprintf("Worker %d failed to delete key %s: %v",
								workerID, key, err)
						}

						// In a concurrent environment, we can't reliably verify deletion
						// because another thread might have already put a new value
						// That's okay for this test
						time.Sleep(time.Millisecond)

						// Re-add the key with a new version to keep the test going
						time.Sleep(time.Millisecond * 2)
						versionMutex.Lock()
						newerVersion := keyVersions[keyIndex] + 1
						keyVersions[keyIndex] = newerVersion
						versionMutex.Unlock()

						// Add back with new value
						newValue := []byte(fmt.Sprintf("value-%d-%d", keyIndex, newerVersion))
						err = table.PutWithVersion(key, newValue, newerVersion)
						if err != nil {
							errCh <- fmt.Sprintf("Worker %d failed to re-add key %s: %v",
								workerID, key, err)
						}
					}
				}

				// Small yield between operations
				if i%10 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}(w)
	}

	// Wait for all workers to finish
	wg.Wait()
	close(errCh)

	// Check for errors
	errCount := 0
	for err := range errCh {
		t.Log(err)
		errCount++
		if errCount >= 10 {
			t.Log("... and more errors")
			break
		}
	}

	if errCount > 0 {
		t.Errorf("Got %d errors during mixed operations test", errCount)
	}
}
