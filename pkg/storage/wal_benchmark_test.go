package storage

import (
	"os"
	"path/filepath"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// setupWALBenchmark creates a temporary WAL file for benchmarking
func setupWALBenchmark(b *testing.B) (*WAL, string, func()) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "wal_benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}

	walPath := filepath.Join(tempDir, "benchmark.wal")

	// Create a new WAL
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: false, // Turn off sync for performance benchmarking
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create WAL: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		wal.Close()
		os.RemoveAll(tempDir)
	}

	return wal, tempDir, cleanup
}

// BenchmarkWALAppend measures WAL append performance for different record sizes
func BenchmarkWALAppend(b *testing.B) {
	tests := []struct {
		name      string
		valueSize int
	}{
		{"SmallValue_100B", 100},
		{"MediumValue_1KB", 1 * 1024},
		{"LargeValue_10KB", 10 * 1024},
		{"VeryLargeValue_100KB", 100 * 1024},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			wal, _, cleanup := setupWALBenchmark(b)
			defer cleanup()

			// Prepare a fixed key and value of specified size
			key := []byte("benchmark-key")
			value := make([]byte, tt.valueSize)
			for i := range value {
				value[i] = byte(i % 256)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Use a dynamic key to avoid any caching effects
				dynamicKey := append([]byte(nil), key...)
				dynamicKey = append(dynamicKey, byte(i%256))

				if err := wal.RecordPut(dynamicKey, value); err != nil {
					b.Fatalf("Failed to record put operation: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

// BenchmarkWALAppendWithSync measures WAL append performance with sync enabled
func BenchmarkWALAppendWithSync(b *testing.B) {
	// Create a temporary directory for WAL files
	tempDir, err := os.MkdirTemp("", "wal_sync_benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "sync_benchmark.wal")

	// Create a new WAL with sync enabled
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true, // Enable sync for durability benchmarking
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		b.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Prepare a fixed key and value
	key := []byte("benchmark-key")
	value := make([]byte, 1024) // 1KB value
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dynamicKey := append([]byte(nil), key...)
		dynamicKey = append(dynamicKey, byte(i%256))

		if err := wal.RecordPut(dynamicKey, value); err != nil {
			b.Fatalf("Failed to record put operation: %v", err)
		}
	}
}

// BenchmarkWALReplay measures WAL recovery performance with varying numbers of records
func BenchmarkWALReplay(b *testing.B) {
	tests := []struct {
		name         string
		recordCount  int
		valueSize    int
		transactions bool
	}{
		{"SmallWAL_100Records", 100, 100, false},
		{"MediumWAL_1000Records", 1000, 100, false},
		{"LargeWAL_10000Records", 10000, 100, false},
		{"TransactionalWAL_1000Records", 1000, 100, true},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			// Create a temporary directory for WAL files
			tempDir, err := os.MkdirTemp("", "wal_replay_benchmark")
			if err != nil {
				b.Fatalf("Failed to create temp directory: %v", err)
			}
			defer os.RemoveAll(tempDir)

			walPath := filepath.Join(tempDir, "replay_benchmark.wal")

			// Create a new WAL
			wal, err := NewWAL(WALConfig{
				Path:        walPath,
				SyncOnWrite: false,
				Logger:      model.NewNoOpLogger(),
			})
			if err != nil {
				b.Fatalf("Failed to create WAL: %v", err)
			}

			// Prepare data for the WAL
			value := make([]byte, tt.valueSize)
			for i := range value {
				value[i] = byte(i % 256)
			}

			// Populate the WAL with records
			if tt.transactions {
				// Create records in transactions (10 records per transaction)
				recordsPerTx := 10
				txCount := tt.recordCount / recordsPerTx
				for t := 0; t < txCount; t++ {
					txID, err := wal.BeginTransaction()
					if err != nil {
						b.Fatalf("Failed to begin transaction: %v", err)
					}

					for r := 0; r < recordsPerTx; r++ {
						key := []byte(filepath.Join("tx", "key", string(rune('0'+t)), string(rune('0'+r))))
						err := wal.RecordPutInTransaction(key, value, txID)
						if err != nil {
							b.Fatalf("Failed to record put in transaction: %v", err)
						}
					}

					err = wal.CommitTransaction(txID)
					if err != nil {
						b.Fatalf("Failed to commit transaction: %v", err)
					}
				}
			} else {
				// Create standalone records
				for i := 0; i < tt.recordCount; i++ {
					key := []byte(filepath.Join("standalone", "key", string(rune(i%10)), string(rune(i/10%10))))
					err := wal.RecordPut(key, value)
					if err != nil {
						b.Fatalf("Failed to record put operation: %v", err)
					}
				}
			}

			// Close the WAL to ensure all data is flushed
			err = wal.Close()
			if err != nil {
				b.Fatalf("Failed to close WAL: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Reopen the WAL
				wal, err = NewWAL(WALConfig{
					Path:        walPath,
					SyncOnWrite: false,
					Logger:      model.NewNoOpLogger(),
				})
				if err != nil {
					b.Fatalf("Failed to reopen WAL: %v", err)
				}

				// Create a MemTable for replay
				memTable := NewMemTable(MemTableConfig{
					MaxSize:    1024 * 1024 * 10, // 10MB
					Logger:     model.NewNoOpLogger(),
					Comparator: DefaultComparator,
				})

				// Replay the WAL
				err = wal.Replay(memTable)
				if err != nil {
					b.Fatalf("Failed to replay WAL: %v", err)
				}

				// Close the WAL
				err = wal.Close()
				if err != nil {
					b.Fatalf("Failed to close WAL: %v", err)
				}
			}
		})
	}
}

// BenchmarkWALConcurrentAppends measures concurrent WAL append performance
func BenchmarkWALConcurrentAppends(b *testing.B) {
	tests := []struct {
		name          string
		concurrency   int
		transactional bool
	}{
		{"Concurrent_2Threads_NoTx", 2, false},
		{"Concurrent_4Threads_NoTx", 4, false},
		{"Concurrent_8Threads_NoTx", 8, false},
		{"Concurrent_2Threads_WithTx", 2, true},
		{"Concurrent_4Threads_WithTx", 4, true},
		{"Concurrent_8Threads_WithTx", 8, true},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			wal, _, cleanup := setupWALBenchmark(b)
			defer cleanup()

			// Fixed value for all operations
			value := make([]byte, 1024) // 1KB value
			for i := range value {
				value[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.SetParallelism(tt.concurrency)

			b.RunParallel(func(pb *testing.PB) {
				var txID uint64
				var err error

				// Start a transaction if needed
				if tt.transactional {
					txID, err = wal.BeginTransaction()
					if err != nil {
						b.Fatalf("Failed to begin transaction: %v", err)
					}
				}

				// Create a unique base key for this goroutine
				keyBase := []byte("concurrent-key")

				i := 0
				for pb.Next() {
					// Create a unique key for each operation
					key := append([]byte(nil), keyBase...)
					key = append(key, byte(i%256))
					i++

					if tt.transactional {
						err = wal.RecordPutInTransaction(key, value, txID)
					} else {
						err = wal.RecordPut(key, value)
					}

					if err != nil {
						b.Fatalf("Failed to record operation: %v", err)
					}
				}

				// Commit the transaction if needed
				if tt.transactional {
					err = wal.CommitTransaction(txID)
					if err != nil {
						b.Fatalf("Failed to commit transaction: %v", err)
					}
				}
			})
		})
	}
}
