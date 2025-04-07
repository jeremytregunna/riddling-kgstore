package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// setupTransactionBenchmark creates a temporary directory and initializes a TransactionManager for benchmarking
func setupTransactionBenchmark(b *testing.B) (*TransactionManager, string, func()) {
	// Create a temporary directory for benchmark data
	tempDir, err := os.MkdirTemp("", "tx_benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create WAL for the transaction manager
	walPath := filepath.Join(tempDir, "tx_benchmark.wal")
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: false, // Turn off sync for performance benchmarking
		Logger:      model.NewNoOpLogger(),
	})
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create WAL: %v", err)
	}

	// Create TransactionManager
	tm, err := NewTransactionManager(tempDir, model.NewNoOpLogger(), wal)
	if err != nil {
		wal.Close()
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create TransactionManager: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		tm.Close()
		wal.Close()
		os.RemoveAll(tempDir)
	}

	return tm, tempDir, cleanup
}

// BenchmarkTransactionCommit measures the performance of committing transactions with varying numbers of operations
func BenchmarkTransactionCommit(b *testing.B) {
	tests := []struct {
		name           string
		operationCount int
	}{
		{"SmallTx_10Ops", 10},
		{"MediumTx_100Ops", 100},
		{"LargeTx_1000Ops", 1000},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			tm, _, cleanup := setupTransactionBenchmark(b)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Start a new transaction
				tx := tm.Begin()

				// Add operations to the transaction
				for j := 0; j < tt.operationCount; j++ {
					op := TransactionOperation{
						Type:   "add",
						Target: "sstable",
						ID:     uint64(j + 1),
						Data:   nil,
					}
					tx.AddOperation(op)
				}

				// Commit the transaction
				if err := tx.Commit(); err != nil {
					b.Fatalf("Failed to commit transaction: %v", err)
				}
			}
		})
	}
}

// BenchmarkTransactionRollback measures the performance of rolling back transactions with varying numbers of operations
func BenchmarkTransactionRollback(b *testing.B) {
	tests := []struct {
		name           string
		operationCount int
	}{
		{"SmallTx_10Ops", 10},
		{"MediumTx_100Ops", 100},
		{"LargeTx_1000Ops", 1000},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			tm, _, cleanup := setupTransactionBenchmark(b)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Start a new transaction
				tx := tm.Begin()

				// Add operations to the transaction
				for j := 0; j < tt.operationCount; j++ {
					op := TransactionOperation{
						Type:   "add",
						Target: "sstable",
						ID:     uint64(j + 1),
						Data:   nil,
					}
					tx.AddOperation(op)
				}

				// Rollback the transaction
				if err := tx.Rollback(); err != nil {
					b.Fatalf("Failed to rollback transaction: %v", err)
				}
			}
		})
	}
}

// BenchmarkConcurrentTransactions measures the performance of concurrent transaction processing
func BenchmarkConcurrentTransactions(b *testing.B) {
	tests := []struct {
		name           string
		concurrency    int
		operationCount int
		rollbackPct    int // Percentage of transactions to rollback (0-100)
	}{
		{"Concurrent_2Threads_10Ops_AllCommit", 2, 10, 0},
		{"Concurrent_4Threads_10Ops_AllCommit", 4, 10, 0},
		{"Concurrent_8Threads_10Ops_AllCommit", 8, 10, 0},
		{"Concurrent_4Threads_10Ops_50pctRollback", 4, 10, 50},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			tm, _, cleanup := setupTransactionBenchmark(b)
			defer cleanup()

			b.ResetTimer()
			b.SetParallelism(tt.concurrency)

			b.RunParallel(func(pb *testing.PB) {
				counter := 0
				for pb.Next() {
					// Start a new transaction
					tx := tm.Begin()

					// Add operations to the transaction
					for j := 0; j < tt.operationCount; j++ {
						op := TransactionOperation{
							Type:   "add",
							Target: "sstable",
							ID:     uint64(j + 1),
							Data:   nil,
						}
						tx.AddOperation(op)
					}

					// Commit or rollback based on the rollback percentage
					counter++
					if counter%100 < tt.rollbackPct {
						// Rollback
						if err := tx.Rollback(); err != nil {
							b.Fatalf("Failed to rollback transaction: %v", err)
						}
					} else {
						// Commit
						if err := tx.Commit(); err != nil {
							b.Fatalf("Failed to commit transaction: %v", err)
						}
					}
				}
			})
		})
	}
}

// BenchmarkMixedTransactionWorkload measures performance with a mix of operations
func BenchmarkMixedTransactionWorkload(b *testing.B) {
	tm, tempDir, cleanup := setupTransactionBenchmark(b)
	defer cleanup()

	// Create a shared counter for transaction IDs
	var counter uint64 = 0
	var mu sync.Mutex

	// Create sstables directory
	if err := os.MkdirAll(filepath.Join(tempDir, "sstables"), 0755); err != nil {
		b.Fatalf("Failed to create sstables directory: %v", err)
	}

	// Create some dummy SSTable files for operations to work with
	for i := 1; i <= 100; i++ {
		filePath := filepath.Join(tempDir, "sstables", fmt.Sprintf("%d.data", i))
		if err := os.WriteFile(filePath, []byte("dummy data"), 0644); err != nil {
			b.Fatalf("Failed to create dummy SSTable file: %v", err)
		}
	}

	b.ResetTimer()
	b.SetParallelism(4)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Get a unique counter value
			mu.Lock()
			counter++
			myCounter := counter
			mu.Unlock()

			// Start a transaction
			tx := tm.Begin()

			// Add a mix of operations based on counter value
			if myCounter%3 == 0 {
				// Add an add operation
				op := TransactionOperation{
					Type:   "add",
					Target: "sstable",
					ID:     myCounter,
					Data:   nil,
				}
				tx.AddOperation(op)
			} else if myCounter%3 == 1 {
				// Add a remove operation
				op := TransactionOperation{
					Type:   "remove",
					Target: "sstable",
					ID:     myCounter%100 + 1, // Use existing SSTable ID
					Data:   nil,
				}
				tx.AddOperation(op)
			} else {
				// Add a rename operation
				op := TransactionOperation{
					Type:   "rename",
					Target: "sstable",
					ID:     myCounter%100 + 1,                      // Source ID
					Data:   []uint64{myCounter%100 + 1, myCounter}, // Source and target IDs
				}
				tx.AddOperation(op)
			}

			// Commit or rollback based on counter value
			if myCounter%10 == 0 {
				// Rollback 10% of transactions
				if err := tx.Rollback(); err != nil {
					b.Fatalf("Failed to rollback transaction: %v", err)
				}
			} else {
				// Commit 90% of transactions
				if err := tx.Commit(); err != nil {
					b.Fatalf("Failed to commit transaction: %v", err)
				}
			}
		}
	})
}
