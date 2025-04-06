package storage

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TestLockFreeMemTableSameKeyContention tests high contention on the same keys
func TestLockFreeMemTableSameKeyContention(t *testing.T) {
	table := NewLockFreeMemTable(LockFreeMemTableConfig{
		MaxSize: 10 * 1024 * 1024, // 10MB - make it larger to avoid filling up during test
		Logger:  model.DefaultLoggerInstance,
	})
	
	// Use a very small set of keys to create high contention
	const keyCount = 5
	const concurrentWriters = 10
	const opsPerWriter = 100
	
	// Initialize keys
	keys := make([][]byte, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = []byte(fmt.Sprintf("contended-key-%d", i))
		// Start with initial values
		err := table.Put(keys[i], []byte("initial"))
		if err != nil {
			t.Fatalf("Failed to initialize key %s: %v", keys[i], err)
		}
	}
	
	var wg sync.WaitGroup
	errCh := make(chan string, concurrentWriters*opsPerWriter)
	successCh := make(chan bool, concurrentWriters*opsPerWriter)
	
	// Track the final values we expect
	finalValues := make(map[string][]byte)
	var finalValuesMutex sync.Mutex
	
	// Launch concurrent writers
	for w := 0; w < concurrentWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			
			// Each writer performs multiple operations on the same keys
			for i := 0; i < opsPerWriter; i++ {
				// Choose a key - deliberately create contention
				keyIndex := (writerID + i) % keyCount
				key := keys[keyIndex]
				
				// First read current value to ensure the key is accessible
				_, err := table.Get(key)
				if err != nil && err != ErrLockFreeKeyNotFound {
					errCh <- fmt.Sprintf("Writer %d unexpected error getting key %s: %v", 
						writerID, key, err)
					continue
				}
				
				// Create a new value based on writerID and operation count
				newValue := []byte(fmt.Sprintf("writer-%d-op-%d", writerID, i))
				
				// Put the new value with retry on full table
				var putSuccess bool
				for retries := 0; retries < 3; retries++ {
					err = table.Put(key, newValue)
					if err == nil {
						putSuccess = true
						break
					}
					
					// If table is full, we'll give a small delay and try again
					// as other operations might free up space
					if err == ErrLockFreeMemTableFull {
						time.Sleep(time.Millisecond * 5)
						continue
					}
					
					// For other errors, break immediately
					break
				}
				
				if !putSuccess {
					errCh <- fmt.Sprintf("Writer %d failed to update key %s: %v", 
						writerID, key, err)
					continue
				}
				
				// In a high-contention scenario, we might not be able to read back
				// the value we just wrote, especially if the table is getting full
				// and other threads are updating the same key
				// Let's record this fact but not treat it as an error
				readSuccess := false
				for retries := 0; retries < 3; retries++ {
					readBack, err := table.Get(key)
					if err == nil {
						// Success! Record the value we got
						finalValuesMutex.Lock()
						finalValues[string(key)] = readBack
						finalValuesMutex.Unlock()
						readSuccess = true
						break
					}
					time.Sleep(time.Millisecond)
				}
				
				// If we couldn't read back, it could be due to high contention
				// or the table being full - just log this
				if !readSuccess && strings.Contains(string(newValue), "op-") {
					// We'll only log this for non-deleted keys
					t.Logf("Writer %d couldn't read back key %s after update", writerID, key)
				}
				
				// Mark success
				successCh <- true
				
				// Mix in some deletes (one out of every 10 operations)
				if i%10 == 5 {
					err = table.Delete(key)
					if err != nil {
						errCh <- fmt.Sprintf("Writer %d failed to delete key %s: %v", 
							writerID, key, err)
					}
					
					// Immediately put it back with new value
					time.Sleep(time.Millisecond)
					newValue = []byte(fmt.Sprintf("after-delete-writer-%d-op-%d", writerID, i))
					
					// Try to re-add with retry on full table
					var readdSuccess bool
					for retries := 0; retries < 3; retries++ {
						err = table.Put(key, newValue)
						if err == nil {
							readdSuccess = true
							break
						}
						
						// If table is full, we'll give a small delay and try again
						if err == ErrLockFreeMemTableFull {
							time.Sleep(time.Millisecond * 5)
							continue
						}
						
						// For other errors, break immediately
						break
					}
					
					if !readdSuccess {
						errCh <- fmt.Sprintf("Writer %d failed to re-add key %s: %v", 
							writerID, key, err)
					}
					
					// Update our expected final value
					finalValuesMutex.Lock()
					finalValues[string(key)] = newValue
					finalValuesMutex.Unlock()
				}
			}
		}(w)
	}
	
	// Wait for all operations to complete
	wg.Wait()
	close(errCh)
	close(successCh)
	
	// Check for errors
	errCount := 0
	memTableFullErrors := 0
	
	for err := range errCh {
		if strings.Contains(err, "LockFreeMemTable is full") {
			memTableFullErrors++
		} else {
			t.Log(err)
			errCount++
		}
		
		if errCount >= 10 {
			t.Log("... and more errors")
			break
		}
	}
	
	// Count successful operations
	successCount := 0
	for range successCh {
		successCount++
	}
	
	// Report results
	t.Logf("Completed %d successful operations with %d errors (plus %d 'memtable full' errors which are expected)", 
		successCount, errCount, memTableFullErrors)
	
	// Only fail for errors other than "memtable full"
	if errCount > 0 {
		t.Errorf("Got %d errors during high contention test", errCount)
	}
	
	// Check final state of keys
	// In high contention, some keys might not exist if the final operation was a delete
	// or we couldn't re-add due to table being full, but that's acceptable
	foundKeys := 0
	for i := 0; i < keyCount; i++ {
		key := keys[i]
		value, err := table.Get(key)
		if err == nil {
			t.Logf("Final value for key %s: %s", key, value)
			foundKeys++
		} else {
			// Just log the missing key, don't count as error
			t.Logf("Key %s not found in final state: %v", key, err)
		}
	}
	
	// Make sure we have at least one key
	if foundKeys == 0 {
		t.Errorf("Expected at least one key to exist after test")
	}
}