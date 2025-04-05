package storage

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// generateKeyValuePairs generates n random key-value pairs
func generateKeyValuePairs(n int) ([][]byte, [][]byte) {
	keys := make([][]byte, n)
	values := make([][]byte, n)

	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		values[i] = []byte(fmt.Sprintf("value-%d", i))
	}

	return keys, values
}

// benchmarkMemTablePut benchmarks the Put operation for a specific MemTable implementation
func benchmarkMemTablePut(b *testing.B, isLockFree bool) {
	var mt MemTableInterface
	if isLockFree {
		mt = NewLockFreeMemTable(LockFreeMemTableConfig{
			MaxSize:    100 * 1024 * 1024, // 100MB
			Logger:     model.DefaultLoggerInstance,
			Comparator: DefaultComparator,
		})
	} else {
		mt = NewMemTable(MemTableConfig{
			MaxSize:    100 * 1024 * 1024, // 100MB
			Logger:     model.DefaultLoggerInstance,
			Comparator: DefaultComparator,
		})
	}

	keys, values := generateKeyValuePairs(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mt.Put(keys[i], values[i])
	}
}

// benchmarkMemTableGet benchmarks the Get operation for a specific MemTable implementation
func benchmarkMemTableGet(b *testing.B, isLockFree bool) {
	var mt MemTableInterface
	if isLockFree {
		mt = NewLockFreeMemTable(LockFreeMemTableConfig{
			MaxSize:    100 * 1024 * 1024, // 100MB
			Logger:     model.DefaultLoggerInstance,
			Comparator: DefaultComparator,
		})
	} else {
		mt = NewMemTable(MemTableConfig{
			MaxSize:    100 * 1024 * 1024, // 100MB
			Logger:     model.DefaultLoggerInstance,
			Comparator: DefaultComparator,
		})
	}

	// Prepare data
	keys, values := generateKeyValuePairs(b.N)
	for i := 0; i < b.N; i++ {
		mt.Put(keys[i], values[i])
	}

	// Shuffle indices to avoid cache locality effects
	indices := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		indices[i] = i
	}
	rand.Shuffle(b.N, func(i, j int) { indices[i], indices[j] = indices[j], indices[i] })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := indices[i]
		mt.Get(keys[idx])
	}
}

// benchmarkMemTableConcurrentPut benchmarks concurrent Put operations
func benchmarkMemTableConcurrentPut(b *testing.B, isLockFree bool, numGoroutines int) {
	var mt MemTableInterface
	if isLockFree {
		mt = NewLockFreeMemTable(LockFreeMemTableConfig{
			MaxSize:    100 * 1024 * 1024, // 100MB
			Logger:     model.DefaultLoggerInstance,
			Comparator: DefaultComparator,
		})
	} else {
		mt = NewMemTable(MemTableConfig{
			MaxSize:    100 * 1024 * 1024, // 100MB
			Logger:     model.DefaultLoggerInstance,
			Comparator: DefaultComparator,
		})
	}

	// For concurrent benchmark, we'll generate a fixed number of operations
	// This is more predictable than using b.N which can be very large
	const totalOps = 10000
	opsPerGoroutine := totalOps / numGoroutines
	
	// Generate keys and values - use more descriptive strings to avoid issues
	keys := make([][]byte, totalOps)
	values := make([][]byte, totalOps)
	for i := 0; i < totalOps; i++ {
		keys[i] = []byte(fmt.Sprintf("benchmark-key-%d", i))
		values[i] = []byte(fmt.Sprintf("benchmark-value-%d", i))
	}

	b.ResetTimer()
	
	// Run the benchmark b.N times
	for n := 0; n < b.N; n++ {
		// Reset the memtable for each iteration
		if isLockFree {
			mt = NewLockFreeMemTable(LockFreeMemTableConfig{
				MaxSize:    100 * 1024 * 1024,
				Logger:     model.DefaultLoggerInstance,
				Comparator: DefaultComparator,
			})
		} else {
			mt = NewMemTable(MemTableConfig{
				MaxSize:    100 * 1024 * 1024,
				Logger:     model.DefaultLoggerInstance,
				Comparator: DefaultComparator,
			})
		}
		
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				defer wg.Done()
				startIdx := goroutineID * opsPerGoroutine
				endIdx := startIdx + opsPerGoroutine

				for i := startIdx; i < endIdx; i++ {
					// Ignore errors - they're expected in benchmarks
					_ = mt.Put(keys[i], values[i])
				}
			}(g)
		}

		wg.Wait()
	}
}

// benchmarkMemTableConcurrentGetPut benchmarks concurrent Get and Put operations
func benchmarkMemTableConcurrentGetPut(b *testing.B, isLockFree bool, numGoroutines int, readPercentage int) {
	// For concurrent benchmark, we'll use a fixed number of operations
	const totalOps = 10000
	opsPerGoroutine := totalOps / numGoroutines
	
	// Prepare keys and values for consistent usage
	initialDataSize := 1000
	initialKeys := make([][]byte, initialDataSize)
	initialValues := make([][]byte, initialDataSize)
	for i := 0; i < initialDataSize; i++ {
		initialKeys[i] = []byte(fmt.Sprintf("initial-key-%d", i))
		initialValues[i] = []byte(fmt.Sprintf("initial-value-%d", i))
	}
	
	// Prepare benchmark operations
	ops := make([]struct {
		isRead bool
		key    []byte
		value  []byte
	}, totalOps)
	
	// Initialize random number generator with fixed seed for reproducibility
	rng := rand.New(rand.NewSource(42))
	
	// Generate operations - fixed pattern for all iterations
	for i := 0; i < len(ops); i++ {
		ops[i].isRead = rng.Intn(100) < readPercentage
		if ops[i].isRead {
			// Read operation - use an existing key
			keyIndex := rng.Intn(initialDataSize)
			ops[i].key = initialKeys[keyIndex]
		} else {
			// Write operation - fixed pattern
			ops[i].key = []byte(fmt.Sprintf("new-key-%d", i))
			ops[i].value = []byte(fmt.Sprintf("new-value-%d", i))
		}
	}
	
	b.ResetTimer()
	
	// Run the benchmark b.N times
	for n := 0; n < b.N; n++ {
		// Create and populate a fresh MemTable for each iteration
		var mt MemTableInterface
		if isLockFree {
			mt = NewLockFreeMemTable(LockFreeMemTableConfig{
				MaxSize:    100 * 1024 * 1024, // 100MB
				Logger:     model.DefaultLoggerInstance,
				Comparator: DefaultComparator,
			})
		} else {
			mt = NewMemTable(MemTableConfig{
				MaxSize:    100 * 1024 * 1024, // 100MB
				Logger:     model.DefaultLoggerInstance,
				Comparator: DefaultComparator,
			})
		}
		
		// Prepopulate with initial data
		for i := 0; i < initialDataSize; i++ {
			_ = mt.Put(initialKeys[i], initialValues[i])
		}
		
		var wg sync.WaitGroup
		wg.Add(numGoroutines)
		
		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				defer wg.Done()
				startIdx := goroutineID * opsPerGoroutine
				endIdx := startIdx + opsPerGoroutine
				
				for i := startIdx; i < endIdx; i++ {
					if i >= len(ops) {
						break // Safety check
					}
					if ops[i].isRead {
						_, _ = mt.Get(ops[i].key)
					} else {
						_ = mt.Put(ops[i].key, ops[i].value)
					}
				}
			}(g)
		}
		
		wg.Wait()
	}
}

// Standard MemTable Benchmarks
func BenchmarkMemTable_Put(b *testing.B) {
	benchmarkMemTablePut(b, false)
}

func BenchmarkMemTable_Get(b *testing.B) {
	benchmarkMemTableGet(b, false)
}

func BenchmarkMemTable_ConcurrentPut_2(b *testing.B) {
	benchmarkMemTableConcurrentPut(b, false, 2)
}

func BenchmarkMemTable_ConcurrentPut_4(b *testing.B) {
	benchmarkMemTableConcurrentPut(b, false, 4)
}

func BenchmarkMemTable_ConcurrentPut_8(b *testing.B) {
	benchmarkMemTableConcurrentPut(b, false, 8)
}

func BenchmarkMemTable_ConcurrentPut_16(b *testing.B) {
	benchmarkMemTableConcurrentPut(b, false, 16)
}

func BenchmarkMemTable_ConcurrentGetPut_2_Read50(b *testing.B) {
	benchmarkMemTableConcurrentGetPut(b, false, 2, 50)
}

func BenchmarkMemTable_ConcurrentGetPut_4_Read50(b *testing.B) {
	benchmarkMemTableConcurrentGetPut(b, false, 4, 50)
}

func BenchmarkMemTable_ConcurrentGetPut_8_Read50(b *testing.B) {
	benchmarkMemTableConcurrentGetPut(b, false, 8, 50)
}

func BenchmarkMemTable_ConcurrentGetPut_16_Read50(b *testing.B) {
	benchmarkMemTableConcurrentGetPut(b, false, 16, 50)
}

func BenchmarkMemTable_ConcurrentGetPut_8_Read80(b *testing.B) {
	benchmarkMemTableConcurrentGetPut(b, false, 8, 80)
}

func BenchmarkMemTable_ConcurrentGetPut_8_Read20(b *testing.B) {
	benchmarkMemTableConcurrentGetPut(b, false, 8, 20)
}

// Lock-Free MemTable Benchmarks
func BenchmarkLockFreeMemTable_Put(b *testing.B) {
	benchmarkMemTablePut(b, true)
}

func BenchmarkLockFreeMemTable_Get(b *testing.B) {
	benchmarkMemTableGet(b, true)
}

func BenchmarkLockFreeMemTable_ConcurrentPut_2(b *testing.B) {
	benchmarkMemTableConcurrentPut(b, true, 2)
}

func BenchmarkLockFreeMemTable_ConcurrentPut_4(b *testing.B) {
	benchmarkMemTableConcurrentPut(b, true, 4)
}

func BenchmarkLockFreeMemTable_ConcurrentPut_8(b *testing.B) {
	benchmarkMemTableConcurrentPut(b, true, 8)
}

func BenchmarkLockFreeMemTable_ConcurrentPut_16(b *testing.B) {
	benchmarkMemTableConcurrentPut(b, true, 16)
}

func BenchmarkLockFreeMemTable_ConcurrentGetPut_2_Read50(b *testing.B) {
	benchmarkMemTableConcurrentGetPut(b, true, 2, 50)
}

func BenchmarkLockFreeMemTable_ConcurrentGetPut_4_Read50(b *testing.B) {
	benchmarkMemTableConcurrentGetPut(b, true, 4, 50)
}

func BenchmarkLockFreeMemTable_ConcurrentGetPut_8_Read50(b *testing.B) {
	benchmarkMemTableConcurrentGetPut(b, true, 8, 50)
}

func BenchmarkLockFreeMemTable_ConcurrentGetPut_16_Read50(b *testing.B) {
	benchmarkMemTableConcurrentGetPut(b, true, 16, 50)
}

func BenchmarkLockFreeMemTable_ConcurrentGetPut_8_Read80(b *testing.B) {
	benchmarkMemTableConcurrentGetPut(b, true, 8, 80)
}

func BenchmarkLockFreeMemTable_ConcurrentGetPut_8_Read20(b *testing.B) {
	benchmarkMemTableConcurrentGetPut(b, true, 8, 20)
}