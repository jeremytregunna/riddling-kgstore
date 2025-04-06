package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TestLockFreeMemTableConcurrentReads tests high-volume concurrent read operations
// after data has been written to the memtable
func TestLockFreeMemTableConcurrentReads(t *testing.T) {
	table := NewLockFreeMemTable(LockFreeMemTableConfig{
		MaxSize: 1024 * 1024, // 1MB
		Logger:  model.DefaultLoggerInstance,
	})

	// Number of keys to write
	const keyCount = 100
	// Number of concurrent readers
	const readerCount = 10
	// Number of reads per reader
	const readsPerReader = 1000

	// Prepare data
	keys := make([][]byte, keyCount)
	values := make([][]byte, keyCount)
	
	// Insert data sequentially 
	for i := 0; i < keyCount; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		values[i] = []byte(fmt.Sprintf("value-%d", i))
		
		err := table.Put(keys[i], values[i])
		if err != nil {
			t.Fatalf("Failed to put key %s: %v", keys[i], err)
		}
	}
	
	// Now perform concurrent reads
	var wg sync.WaitGroup
	errCh := make(chan string, readerCount*readsPerReader)
	
	// Launch readers
	for r := 0; r < readerCount; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			
			// Each reader performs random reads
			for i := 0; i < readsPerReader; i++ {
				// Choose a random key (simple deterministic approach)
				keyIndex := (readerID * i) % keyCount
				key := keys[keyIndex]
				expectedValue := values[keyIndex]
				
				// Read with retry logic
				var result []byte
				var err error
				var found bool
				
				// Try a few times to handle transient issues
				for retries := 0; retries < 5; retries++ {
					result, err = table.Get(key)
					if err == nil {
						found = true
						break
					}
					// Small backoff before retry
					time.Sleep(time.Millisecond)
				}
				
				if !found {
					errCh <- fmt.Sprintf("Reader %d failed to get key %s after retries: %v", 
						readerID, key, err)
					continue
				}
				
				if !bytes.Equal(result, expectedValue) {
					errCh <- fmt.Sprintf("Reader %d got incorrect value for key %s: expected %s, got %s", 
						readerID, key, expectedValue, result)
				}
			}
		}(r)
	}
	
	// Wait for all readers to finish
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
		t.Errorf("Got %d errors during concurrent reads", errCount)
	}
}