package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestTransactionManager tests basic transaction manager functionality
func TestTransactionManager(t *testing.T) {
	// Create a temporary directory for transaction files
	tempDir, err := os.MkdirTemp("", "transaction_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a WAL for the transaction manager
	walPath := filepath.Join(tempDir, "transaction.wal")
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: true, // For testing
		Logger:      nil,
	})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Create a transaction manager
	txManager, err := NewTransactionManager(tempDir, nil, wal)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	// Test basic transaction lifecycle
	t.Run("TransactionLifecycle", func(t *testing.T) {
		// Begin a transaction
		tx, err := txManager.BeginTransaction(DefaultTxOptions())
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		if tx == nil {
			t.Fatal("Failed to begin transaction")
		}

		if tx.IsCommitted() {
			t.Error("New transaction should not be committed")
		}

		if !tx.IsActive() {
			t.Error("New transaction should be active")
		}

		// Add an operation
		tx.AddOperation(TransactionOperation{
			Type:   "add",
			Target: "sstable",
			ID:     1001,
		})

		// Add another operation
		tx.AddOperation(TransactionOperation{
			Type:   "rename",
			Target: "sstable",
			Data:   []uint64{2001, 3001},
		})

		// Commit transaction
		if err := tx.Commit(); err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}

		// Verify state
		if !tx.IsCommitted() {
			t.Error("Transaction should be committed")
		}

		if tx.IsActive() {
			t.Error("Committed transaction should not be active")
		}
	})

	// Test transaction rollback
	t.Run("TransactionRollback", func(t *testing.T) {
		// Begin a transaction
		tx, err := txManager.BeginTransaction(DefaultTxOptions())
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		if tx == nil {
			t.Fatal("Failed to begin transaction")
		}

		// Add an operation
		tx.AddOperation(TransactionOperation{
			Type:   "add",
			Target: "sstable",
			ID:     2002,
		})

		// First rollback this transaction before starting a new one
		// to avoid contention in the single-writer model
		tx.Rollback()

		// Start a new transaction to ensure we can still use the transaction manager after rollback
		newTx, err := txManager.BeginTransaction(DefaultTxOptions())
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		if newTx == nil {
			t.Fatal("Failed to begin new transaction after rollback")
		}

		// Clean up
		newTx.Rollback()
	})

	// Test transaction operation recording and retrieval
	t.Run("TransactionOperations", func(t *testing.T) {
		// Begin a transaction
		tx, err := txManager.BeginTransaction(DefaultTxOptions())
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Add a few different operation types
		operations := []TransactionOperation{
			{
				Type:   "add",
				Target: "sstable",
				ID:     1001,
			},
			{
				Type:   "remove",
				Target: "sstable",
				ID:     2001,
			},
			{
				Type:   "rename",
				Target: "sstable",
				Data:   []uint64{3001, 4001},
			},
		}

		for _, op := range operations {
			tx.AddOperation(op)
		}

		// Verify operations are recorded in the transaction
		if len(tx.operations) != len(operations) {
			t.Errorf("Expected %d operations, got %d", len(operations), len(tx.operations))
		}

		// Commit transaction to ensure it's processed in WAL
		if err := tx.Commit(); err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}
	})

	// Test concurrent transactions
	t.Run("ConcurrentTransactions", func(t *testing.T) {
		// Create several concurrent transactions
		const numTx = 10
		var wg sync.WaitGroup
		txIDs := make([]uint64, numTx)
		var mu sync.Mutex

		for i := 0; i < numTx; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				// Begin transaction
				tx, err := txManager.BeginTransaction(ReadOnlyTxOptions()) // Use read-only to avoid contention
				if err != nil {
					t.Errorf("Failed to begin transaction %d: %v", idx, err)
					return
				}

				// Add operation
				tx.AddOperation(TransactionOperation{
					Type:   "add",
					Target: "sstable",
					ID:     uint64(5000 + idx),
				})

				// Store ID
				mu.Lock()
				txIDs[idx] = tx.ID()
				mu.Unlock()

				// Commit
				if err := tx.Commit(); err != nil {
					t.Errorf("Failed to commit transaction %d: %v", idx, err)
				}
			}(i)
		}

		// Wait for all transactions to complete
		wg.Wait()

		// Verify all transaction IDs are unique
		uniqueIDs := make(map[uint64]bool)
		for _, id := range txIDs {
			if uniqueIDs[id] {
				t.Errorf("Duplicate transaction ID: %d", id)
			}
			uniqueIDs[id] = true
		}
	})

	// Test transaction creation with different options
	t.Run("TransactionOptions", func(t *testing.T) {
		// Start a read-only transaction
		roTx, err := txManager.BeginTransaction(ReadOnlyTxOptions())
		if err != nil {
			t.Fatalf("Failed to begin read-only transaction: %v", err)
		}
		if roTx.Type() != READ_ONLY {
			t.Errorf("Expected READ_ONLY transaction type, got %s", roTx.Type())
		}
		roTx.Commit()

		// Start a read-write transaction with custom timeout and priority
		rwTx, err := txManager.BeginTransaction(ReadWriteTxOptions(5*time.Second, HIGH))
		if err != nil {
			t.Fatalf("Failed to begin read-write transaction: %v", err)
		}
		if rwTx.Type() != READ_WRITE {
			t.Errorf("Expected READ_WRITE transaction type, got %s", rwTx.Type())
		}
		if rwTx.options.Priority != HIGH {
			t.Errorf("Expected HIGH priority, got %s", rwTx.options.Priority)
		}
		rwTx.Commit()
	})
}