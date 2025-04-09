package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// TransactionOperation represents an operation within a transaction
type TransactionOperation struct {
	Type   string      // Type of operation (e.g., "add", "remove")
	Target string      // Target of the operation (e.g., "sstable")
	ID     uint64      // ID of the target
	Data   interface{} // Additional data for the operation
}

// BufferedWrite represents a buffered write operation
// External implementation is in engine_transaction.go, this is just to avoid import cycles
type BufferedWrite struct {
	Key      []byte
	Value    []byte
	IsDelete bool
}

// Transaction represents a set of operations that should be executed atomically
type Transaction struct {
	id           uint64
	operations   []TransactionOperation
	state        TransactionState
	manager      *TransactionManager
	options      TxOptions
	creationTime time.Time
	snapshot     *Snapshot    // Database snapshot for read isolation
	mu           sync.Mutex
	
	// Batch processing support
	operationBatch []TransactionOperation  // Holds operations for current batch
	batchSize      int                     // Current number of operations in batch
	maxBatchSize   int                     // Maximum batch size before auto-flush
	bufferedWrites map[string]interface{}  // Buffered writes for atomic visibility
}

// AddOperation adds an operation to a transaction
func (tx *Transaction) AddOperation(op TransactionOperation) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != ACTIVE {
		// Transaction not active, cannot add more operations
		return
	}

	// If batching or write coalescing is enabled, process the operation accordingly
	if (tx.maxBatchSize > 0 || tx.options.CoalesceWrites) && tx.options.Type == READ_WRITE {
		// If coalescing is enabled and this is a data operation, check if we're operating on an existing key
		if tx.options.CoalesceWrites && (op.Type == "data-put" || op.Type == "data-delete") {
			// Check if we already have a pending operation for this key
			if dataOp, ok := op.Data.(DataOperation); ok {
				keyStr := string(dataOp.Key)
				
				// If buffered writes is enabled, check if the key is already in the buffer
				if tx.bufferedWrites != nil {
					_, exists := tx.bufferedWrites[keyStr]
					if exists {
						// We already have an operation for this key, replace it with the newer one
						// (this is automatically handled by the map when we add the operation)
						tx.manager.logger.Debug("Coalesced write operation for key in transaction %d", tx.id)
						
						// Update or add to buffered writes
						if op.Type == "data-put" {
							tx.bufferedWrites[keyStr] = BufferedWrite{
								Key:      dataOp.Key,
								Value:    dataOp.Value,
								IsDelete: false,
							}
						} else { // data-delete
							tx.bufferedWrites[keyStr] = BufferedWrite{
								Key:      dataOp.Key,
								IsDelete: true,
							}
						}
						return
					}
				}
				
				// If not in buffer, check the batch for existing operations on this key
				if !tx.options.BufferWrites && tx.batchSize > 0 {
					for i := tx.batchSize - 1; i >= 0; i-- {
						batchOp := tx.operationBatch[i]
						if (batchOp.Type == "data-put" || batchOp.Type == "data-delete") && 
						   batchOp.Target == op.Target {
							if batchDataOp, ok := batchOp.Data.(DataOperation); ok {
								if string(batchDataOp.Key) == keyStr {
									// Replace the earlier operation with this one
									tx.operationBatch[i] = op
									tx.manager.logger.Debug("Coalesced batch operation for key in transaction %d", tx.id)
									return
								}
							}
						}
					}
				}
			}
		}
		
		// If we reach here, add to batch as normal
		tx.operationBatch = append(tx.operationBatch, op)
		tx.batchSize++
		
		// If batch size threshold is reached, flush the batch
		if tx.maxBatchSize > 0 && tx.batchSize >= tx.maxBatchSize {
			tx.flushBatchUnlocked()
		}
		
		// For data operations that are buffered, don't add to WAL yet (will be done on flush/commit)
		if op.Type == "data-put" || op.Type == "data-delete" {
			return
		}
	}

	// Record data operations in WAL if it supports transactional operations
	if tx.manager.wal != nil && op.Type == "add" || op.Type == "remove" {
		// For SSTable operations, we can't directly map them to WAL operations,
		// but we can record them in the WAL in a way that preserves transaction boundaries
		switch op.Type {
		case "add":
			// For "add" operations, we can log a put with a special key pattern
			key := []byte(fmt.Sprintf("__tx_op_%s_%d", op.Target, op.ID))
			value := []byte(fmt.Sprintf("%s:%s:%d", op.Type, op.Target, op.ID))
			if err := tx.manager.wal.RecordPutInTransaction(key, value, tx.id); err != nil {
				tx.manager.logger.Error("Failed to record operation in WAL: %v", err)
				// Continue with file-based transaction
			}
		case "remove":
			// For "remove" operations, we can log a delete with a special key pattern
			key := []byte(fmt.Sprintf("__tx_op_%s_%d", op.Target, op.ID))
			if err := tx.manager.wal.RecordDeleteInTransaction(key, tx.id); err != nil {
				tx.manager.logger.Error("Failed to record operation in WAL: %v", err)
				// Continue with file-based transaction
			}
		}
	}

	tx.operations = append(tx.operations, op)
}

// SetBatchSize sets the maximum batch size for batched operations
func (tx *Transaction) SetBatchSize(size int) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	
	if size < 0 {
		// Disable batching
		tx.maxBatchSize = 0
	} else {
		tx.maxBatchSize = size
	}
}

// flushBatchUnlocked flushes the current batch of operations to the WAL
// NOTE: This method assumes tx.mu is already locked
func (tx *Transaction) flushBatchUnlocked() {
	if tx.batchSize == 0 {
		return // Nothing to flush
	}
	
	// Optimize WAL writes by batching operations together
	if tx.manager.wal != nil && tx.options.Type == READ_WRITE {
		// Group operations by type for more efficient WAL processing
		dataOps := make(map[string][]TransactionOperation)
		
		// First, categorize operations by type
		for _, op := range tx.operationBatch {
			if op.Type == "data-put" || op.Type == "data-delete" {
				dataOps[op.Type] = append(dataOps[op.Type], op)
			}
		}
		
		// Process put operations in a batch
		if puts, exists := dataOps["data-put"]; exists && len(puts) > 0 {
			for _, op := range puts {
				if dataOp, ok := op.Data.(DataOperation); ok {
					if err := tx.manager.wal.RecordPutInTransaction(dataOp.Key, dataOp.Value, tx.id); err != nil {
						tx.manager.logger.Error("Failed to record batched put in WAL: %v", err)
					}
				}
			}
			tx.manager.logger.Debug("Flushed batch of %d PUT operations to WAL for transaction %d", 
				len(puts), tx.id)
		}
		
		// Process delete operations in a batch
		if deletes, exists := dataOps["data-delete"]; exists && len(deletes) > 0 {
			for _, op := range deletes {
				if dataOp, ok := op.Data.(DataOperation); ok {
					if err := tx.manager.wal.RecordDeleteInTransaction(dataOp.Key, tx.id); err != nil {
						tx.manager.logger.Error("Failed to record batched delete in WAL: %v", err)
					}
				}
			}
			tx.manager.logger.Debug("Flushed batch of %d DELETE operations to WAL for transaction %d", 
				len(deletes), tx.id)
		}
	}
	
	// Add all operations to the main operations list for commit/rollback
	tx.operations = append(tx.operations, tx.operationBatch...)
	
	// Clear the batch
	tx.operationBatch = tx.operationBatch[:0]
	tx.batchSize = 0
}

// FlushBatch manually flushes the current batch of operations
func (tx *Transaction) FlushBatch() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	
	if tx.state != ACTIVE {
		// Transaction not active, cannot flush batch
		return
	}
	
	tx.flushBatchUnlocked()
	tx.manager.logger.Debug("Manually flushed operation batch for transaction %d", tx.id)
}

// applyBufferedWritesUnlocked applies all buffered writes to storage
// NOTE: This method assumes tx.mu is already locked
func (tx *Transaction) applyBufferedWritesUnlocked() error {
	if tx.bufferedWrites == nil || len(tx.bufferedWrites) == 0 {
		return nil // Nothing to apply
	}
	
	engine := tx.manager.engine
	if engine == nil {
		return fmt.Errorf("storage engine not available")
	}
	
	// Acquire engine lock to safely modify storage
	engine.mu.Lock()
	defer engine.mu.Unlock()
	
	if !engine.isOpen {
		return ErrEngineClosed
	}
	
	// Check if the MemTable is full before starting
	if engine.memTable.IsFull() {
		// Mark the current MemTable as immutable
		engine.memTable.MarkFlushed()
		engine.immMemTables = append(engine.immMemTables, engine.memTable)

		// Create a new MemTable
		engine.memTable = NewMemTable(MemTableConfig{
			MaxSize:    engine.config.MemTableSize,
			Logger:     engine.logger,
			Comparator: engine.config.Comparator,
		})

		// Signal the compaction thread
		engine.compactionCond.Signal()
	}
	
	// Process all buffered writes in a deterministic order (sort keys for reproducibility)
	keys := make([]string, 0, len(tx.bufferedWrites))
	for key := range tx.bufferedWrites {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	
	// Now apply operations in sorted order
	for _, keyStr := range keys {
		bufWrite, ok := tx.bufferedWrites[keyStr].(BufferedWrite)
		if !ok {
			tx.manager.logger.Warn("Invalid buffered write type for key: %s", keyStr)
			continue
		}
		
		if bufWrite.IsDelete {
			// Record the delete operation in the WAL
			if engine.wal != nil {
				if err := engine.wal.RecordDeleteInTransaction(bufWrite.Key, tx.id); err != nil {
					return fmt.Errorf("failed to record buffered delete in WAL: %w", err)
				}
			}
			
			// Delete the key from the MemTable
			if err := engine.memTable.Delete(bufWrite.Key); err != nil {
				return fmt.Errorf("failed to apply buffered delete to MemTable: %w", err)
			}
		} else {
			// Record the put operation in the WAL
			if engine.wal != nil {
				if err := engine.wal.RecordPutInTransaction(bufWrite.Key, bufWrite.Value, tx.id); err != nil {
					return fmt.Errorf("failed to record buffered put in WAL: %w", err)
				}
			}
			
			// Add the key-value pair to the MemTable
			if err := engine.memTable.Put(bufWrite.Key, bufWrite.Value); err != nil {
				return fmt.Errorf("failed to apply buffered put to MemTable: %w", err)
			}
		}
	}
	
	// Clear buffered writes after successful application
	tx.bufferedWrites = make(map[string]interface{})
	
	return nil
}

// Commit commits the transaction to disk and applies the operations
func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != ACTIVE {
		// Not in active state, cannot commit
		return fmt.Errorf("transaction not active")
	}

	// For read-only transactions, just mark as committed
	if tx.options.Type == READ_ONLY {
		tx.state = COMMITTED
		
		// Release the snapshot if one exists
		if tx.snapshot != nil && tx.manager.engine != nil && tx.manager.engine.snapshotManager != nil {
			tx.manager.engine.snapshotManager.ReleaseSnapshot(tx.snapshot.ID())
			tx.snapshot = nil
		}
		
		// Remove from active transactions
		tx.manager.mu.Lock()
		delete(tx.manager.activeTransactions, tx.id)
		tx.manager.mu.Unlock()
		
		tx.manager.logger.Debug("Committed read-only transaction %d", tx.id)
		return nil
	}

	// Flush any pending batch operations before committing
	if tx.batchSize > 0 {
		tx.flushBatchUnlocked()
		tx.manager.logger.Debug("Flushed pending batch operations before commit for transaction %d", tx.id)
	}
	
	// Apply any buffered writes to the actual storage
	if tx.bufferedWrites != nil && len(tx.bufferedWrites) > 0 {
		if err := tx.applyBufferedWritesUnlocked(); err != nil {
			return fmt.Errorf("failed to apply buffered writes: %w", err)
		}
		tx.manager.logger.Debug("Applied %d buffered writes during commit for transaction %d", 
			len(tx.bufferedWrites), tx.id)
	}

	// Record transaction commit in WAL first if available
	if tx.manager.wal != nil {
		if err := tx.manager.wal.CommitTransaction(tx.id); err != nil {
			tx.manager.logger.Error("Failed to record transaction commit in WAL: %v", err)
			// Continue with file-based transaction commit
		}
	}

	// Write transaction log file
	txPath := filepath.Join(tx.manager.transactionDir, fmt.Sprintf("tx_%d.log", tx.id))
	file, err := os.Create(txPath)
	if err != nil {
		return fmt.Errorf("failed to create transaction file: %w", err)
	}

	// Write operations to log file
	for _, op := range tx.operations {
		var line string
		switch {
		case op.Type == "add" && op.Target == "sstable":
			line = fmt.Sprintf("add:sstable:%d\n", op.ID)
		case op.Type == "remove" && op.Target == "sstable":
			line = fmt.Sprintf("remove:sstable:%d\n", op.ID)
		case op.Type == "rename" && op.Target == "sstable":
			if ids, ok := op.Data.([]uint64); ok && len(ids) == 2 {
				line = fmt.Sprintf("rename:sstable:%d:%d\n", ids[0], ids[1])
			}
		}

		if line != "" {
			if _, err := file.WriteString(line); err != nil {
				file.Close()
				return fmt.Errorf("failed to write to transaction file: %w", err)
			}
		}
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("failed to sync transaction file: %w", err)
	}
	file.Close()

	// Create commit marker file
	commitPath := filepath.Join(tx.manager.transactionDir, fmt.Sprintf("tx_%d.commit", tx.id))
	commitFile, err := os.Create(commitPath)
	if err != nil {
		return fmt.Errorf("failed to create commit marker: %w", err)
	}

	// Write commit timestamp
	fmt.Fprintf(commitFile, "%d", time.Now().UnixNano())

	if err := commitFile.Sync(); err != nil {
		commitFile.Close()
		return fmt.Errorf("failed to sync commit marker: %w", err)
	}
	commitFile.Close()

	// Mark as committed
	tx.state = COMMITTED

	// Release the snapshot if one exists
	if tx.snapshot != nil && tx.manager.engine != nil && tx.manager.engine.snapshotManager != nil {
		tx.manager.engine.snapshotManager.ReleaseSnapshot(tx.snapshot.ID())
		tx.snapshot = nil
	}

	// Apply the transaction
	if err := tx.manager.applyTransaction(tx.id); err != nil {
		return fmt.Errorf("failed to apply transaction: %w", err)
	}

	// Clean up transaction files
	os.Remove(txPath)
	os.Remove(commitPath)

	// Update manager state and signal next transaction
	tx.manager.mu.Lock()
	// Remove from active transactions
	delete(tx.manager.activeTransactions, tx.id)
	
	// Add debug log
	var activeWriterID uint64
	if tx.manager.activeWriter != nil {
		activeWriterID = tx.manager.activeWriter.id
	}
	tx.manager.logger.Debug("Write transaction %d committed, activeWriter: %v, queue empty: %v", 
		tx.id, 
		activeWriterID,
		tx.manager.writeTxQueue.IsEmpty())
	
	// If this was the active writer, clear it and try to activate the next one
	if tx.manager.activeWriter == tx {
		tx.manager.logger.Debug("Clearing active writer %d and activating next transaction", tx.id)
		tx.manager.activeWriter = nil
		tx.manager.tryActivateNextWriteTransaction()
	}
	
	// Always broadcast to wake up waiting transactions
	tx.manager.txStateChangedCond.Broadcast()
	tx.manager.mu.Unlock()

	tx.manager.logger.Debug("Committed write transaction %d", tx.id)
	return nil
}

// Rollback aborts the transaction
func (tx *Transaction) Rollback() error {
	// DEBUGGING: Log all operation details to help diagnose issues
	if tx.manager != nil && tx.manager.logger != nil {
		tx.manager.logger.Debug("Rolling back transaction %d with %d operations", 
			tx.id, len(tx.operations))
		
		// Log all operations
		for i, op := range tx.operations {
			if dataOp, ok := op.Data.(DataOperation); ok {
				tx.manager.logger.Debug("  Op %d: Type=%s, Target=%s, Key=%s", 
					i, op.Type, op.Target, string(dataOp.Key))
			} else {
				tx.manager.logger.Debug("  Op %d: Type=%s, Target=%s", 
					i, op.Type, op.Target)
			}
		}
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != ACTIVE && tx.state != PENDING {
		// Already committed or aborted, cannot rollback
		return fmt.Errorf("transaction already committed or aborted")
	}

	// Record transaction rollback in WAL if available
	if tx.manager.wal != nil {
		if err := tx.manager.wal.RollbackTransaction(tx.id); err != nil {
			tx.manager.logger.Error("Failed to record transaction rollback in WAL: %v", err)
			// Continue with file-based transaction rollback
		}
	}

	// Release the snapshot if one exists
	if tx.snapshot != nil && tx.manager.engine != nil && tx.manager.engine.snapshotManager != nil {
		tx.manager.engine.snapshotManager.ReleaseSnapshot(tx.snapshot.ID())
		tx.snapshot = nil
	}

	// Clear any pending batch operations since we're rolling back
	if tx.batchSize > 0 {
		tx.operationBatch = tx.operationBatch[:0]
		tx.batchSize = 0
		tx.manager.logger.Debug("Cleared pending batch operations during rollback for transaction %d", tx.id)
	}
	
	// Clear any buffered writes
	if tx.bufferedWrites != nil && len(tx.bufferedWrites) > 0 {
		tx.bufferedWrites = make(map[string]interface{})
		tx.manager.logger.Debug("Cleared buffered writes during rollback for transaction %d", tx.id)
	}

	// Roll back data operations if in a read-write transaction
	if tx.options.Type == READ_WRITE && tx.manager.engine != nil {
		// We need to lock the engine to safely modify the MemTable
		tx.manager.engine.mu.Lock()
		
		// CLEANER APPROACH: Focus on database versions for proper isolation
		
		// 1. First identify the keys modified in this transaction
		keysToRollback := make(map[string]bool)
		
		for i := 0; i < len(tx.operations); i++ {
			op := tx.operations[i]
			
			// Only track key-value pair operations
			if op.Target == "kv-pair" {
				if dataOp, ok := op.Data.(DataOperation); ok {
					keyStr := string(dataOp.Key)
					keysToRollback[keyStr] = true
				}
			}
		}
		
		// 2. Create a new clean MemTable with the pre-transaction state
		newMemTable := NewMemTable(MemTableConfig{
			MaxSize:    tx.manager.engine.config.MemTableSize,
			Logger:     tx.manager.engine.logger,
			Comparator: tx.manager.engine.config.Comparator,
		})
		
		// 3. Copy only the keys that were not modified in this transaction
		tx.manager.logger.Debug("Keys modified in this transaction: %d", len(keysToRollback))
		for k := range keysToRollback {
			tx.manager.logger.Debug("Key to rollback: %s", k)
		}
		
		// Debug current MemTable
		tx.manager.logger.Debug("Current MemTable keys before rollback:")
		current := tx.manager.engine.memTable.head.forward[0]
		var keyCount int
		for current != nil {
			keyCount++
			tx.manager.logger.Debug("Key in MemTable: %s, isDeleted: %v", string(current.key), current.isDeleted)
			current = current.forward[0]
		}
		tx.manager.logger.Debug("Total keys in current MemTable: %d", keyCount)
		
		// Now copy keys to the new MemTable, excluding rolled back ones
		current = tx.manager.engine.memTable.head.forward[0]
		for current != nil {
			keyStr := string(current.key)
			
			// If key was modified in this transaction, skip it (roll it back)
			if keysToRollback[keyStr] {
				tx.manager.logger.Debug("Excluding key %s from new MemTable (rolling back)", keyStr)
			} else {
				// Only include non-deleted keys
				if !current.isDeleted {
					newMemTable.Put(current.key, current.value)
					tx.manager.logger.Debug("Preserved key %s in new MemTable", keyStr)
				} else {
					tx.manager.logger.Debug("Skipping already-deleted key %s", keyStr)
				}
			}
			
			current = current.forward[0]
		}
		
		// Debug new MemTable
		tx.manager.logger.Debug("New MemTable keys after rollback:")
		current = newMemTable.head.forward[0]
		keyCount = 0
		for current != nil {
			keyCount++
			tx.manager.logger.Debug("Key in new MemTable: %s, isDeleted: %v", string(current.key), current.isDeleted)
			current = current.forward[0]
		}
		tx.manager.logger.Debug("Total keys in new MemTable: %d", keyCount)
		
		// 4. Replace the MemTable with our new clean one
		tx.manager.engine.memTable = newMemTable
		tx.manager.logger.Debug("Created new MemTable for transaction rollback, excluded %d modified keys", 
			len(keysToRollback))
		
		// 5. Create a new database version - THIS IS THE KEY TO THE SOLUTION
		// This ensures that all future snapshots will use this new version
		if tx.manager.engine.snapshotManager != nil {
			tx.manager.engine.versionMu.Lock()
			
			// Force a generation change to ensure version is truly unique
			tx.manager.engine.dbGeneration++
			
			// Create a new database version with the clean MemTable
			tx.manager.engine.currentVersion = NewDatabaseVersion(
				tx.manager.engine.memTable, 
				tx.manager.engine.sstables,
				tx.manager.engine.dbGeneration)
			
			tx.manager.engine.logger.Debug("Created new database version during transaction rollback: %s", 
				tx.manager.engine.currentVersion.String())
			
			tx.manager.engine.versionMu.Unlock()
		}
		
		// Release the engine lock
		tx.manager.engine.mu.Unlock()
	}
	
	// Mark as aborted
	tx.state = ABORTED

	// Update manager state and signal next transaction
	tx.manager.mu.Lock()
	// Log the current state
	var activeWriterID uint64
	if tx.manager.activeWriter != nil {
		activeWriterID = tx.manager.activeWriter.id
	}
	tx.manager.logger.Debug("Rolling back transaction %d (was %s, activeWriter: %v, queue empty: %v)", 
		tx.id, tx.state,
		activeWriterID,
		tx.manager.writeTxQueue.IsEmpty())
		
	// Remove from active transactions
	delete(tx.manager.activeTransactions, tx.id)
	
	// If this was the active writer, clear it and try to activate the next one
	if tx.manager.activeWriter == tx {
		tx.manager.logger.Debug("Clearing active writer %d and activating next transaction", tx.id)
		tx.manager.activeWriter = nil
		tx.manager.tryActivateNextWriteTransaction()
	}
	
	// Signal any waiting transactions
	tx.manager.txStateChangedCond.Broadcast()
	tx.manager.mu.Unlock()

	tx.manager.logger.Debug("Rolled back transaction %d", tx.id)
	return nil
}

// IsCommitted returns whether the transaction has been committed
func (tx *Transaction) IsCommitted() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.state == COMMITTED
}

// IsActive returns whether the transaction is currently active
func (tx *Transaction) IsActive() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.state == ACTIVE
}

// IsPending returns whether the transaction is waiting to become active
func (tx *Transaction) IsPending() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.state == PENDING
}

// IsAborted returns whether the transaction has been aborted
func (tx *Transaction) IsAborted() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.state == ABORTED
}

// State returns the current state of the transaction
func (tx *Transaction) State() TransactionState {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.state
}

// ID returns the transaction ID
func (tx *Transaction) ID() uint64 {
	return tx.id
}

// Type returns the transaction type (read-only or read-write)
func (tx *Transaction) Type() TransactionType {
	return tx.options.Type
}

// CreationTime returns when the transaction was created
func (tx *Transaction) CreationTime() time.Time {
	return tx.creationTime
}

// WaitTime returns how long the transaction waited to become active
// Returns 0 for transactions that didn't wait
func (tx *Transaction) WaitTime() time.Duration {
	if tx.state == PENDING {
		return time.Since(tx.creationTime)
	}
	return 0
}