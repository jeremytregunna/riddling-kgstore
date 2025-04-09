package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TransactionQueue is a priority queue for pending transactions
type TransactionQueue struct {
	highPriority   []*Transaction
	normalPriority []*Transaction
	lowPriority    []*Transaction
	mu             sync.Mutex
}

// NewTransactionQueue creates a new transaction queue
func NewTransactionQueue() *TransactionQueue {
	return &TransactionQueue{
		highPriority:   make([]*Transaction, 0),
		normalPriority: make([]*Transaction, 0),
		lowPriority:    make([]*Transaction, 0),
	}
}

// Enqueue adds a transaction to the queue based on its priority
func (q *TransactionQueue) Enqueue(tx *Transaction) {
	q.mu.Lock()
	defer q.mu.Unlock()

	tx.manager.logger.Debug("TransactionQueue.Enqueue - Adding tx %d with priority %s", 
		tx.id, tx.options.Priority)

	switch tx.options.Priority {
	case HIGH:
		q.highPriority = append(q.highPriority, tx)
	case NORMAL:
		q.normalPriority = append(q.normalPriority, tx)
	case LOW:
		q.lowPriority = append(q.lowPriority, tx)
	}

	tx.manager.logger.Debug("TransactionQueue.Enqueue - Queue after: high=%d, normal=%d, low=%d", 
		len(q.highPriority), len(q.normalPriority), len(q.lowPriority))
}

// Dequeue removes and returns the next transaction with the highest priority
func (q *TransactionQueue) Dequeue() *Transaction {
	q.mu.Lock()
	defer q.mu.Unlock()

	// We need a transaction to use its manager's logger
	// Check if there's at least one transaction in any queue
	var loggerSource *Transaction
	if len(q.highPriority) > 0 {
		loggerSource = q.highPriority[0]
	} else if len(q.normalPriority) > 0 {
		loggerSource = q.normalPriority[0]
	} else if len(q.lowPriority) > 0 {
		loggerSource = q.lowPriority[0]
	}

	// If we have a transaction to use for logging
	if loggerSource != nil {
		loggerSource.manager.logger.Debug("TransactionQueue.Dequeue - Queue before: high=%d, normal=%d, low=%d", 
			len(q.highPriority), len(q.normalPriority), len(q.lowPriority))
	}

	if len(q.highPriority) > 0 {
		tx := q.highPriority[0]
		q.highPriority = q.highPriority[1:]
		tx.manager.logger.Debug("TransactionQueue.Dequeue - Returning high priority tx %d", tx.id)
		return tx
	}

	if len(q.normalPriority) > 0 {
		tx := q.normalPriority[0]
		q.normalPriority = q.normalPriority[1:]
		tx.manager.logger.Debug("TransactionQueue.Dequeue - Returning normal priority tx %d", tx.id)
		return tx
	}

	if len(q.lowPriority) > 0 {
		tx := q.lowPriority[0]
		q.lowPriority = q.lowPriority[1:]
		tx.manager.logger.Debug("TransactionQueue.Dequeue - Returning low priority tx %d", tx.id)
		return tx
	}

	// If we have a logger source, use it
	if loggerSource != nil {
		loggerSource.manager.logger.Debug("TransactionQueue.Dequeue - Queue empty, returning nil")
	}
	return nil
}

// RemoveTransaction removes a specific transaction from the queue
func (q *TransactionQueue) RemoveTransaction(txID uint64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check in high priority queue
	for i, tx := range q.highPriority {
		if tx.id == txID {
			q.highPriority = append(q.highPriority[:i], q.highPriority[i+1:]...)
			return true
		}
	}

	// Check in normal priority queue
	for i, tx := range q.normalPriority {
		if tx.id == txID {
			q.normalPriority = append(q.normalPriority[:i], q.normalPriority[i+1:]...)
			return true
		}
	}

	// Check in low priority queue
	for i, tx := range q.lowPriority {
		if tx.id == txID {
			q.lowPriority = append(q.lowPriority[:i], q.lowPriority[i+1:]...)
			return true
		}
	}

	return false
}

// IsEmpty returns true if the queue is empty
func (q *TransactionQueue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	isEmpty := len(q.highPriority) == 0 && len(q.normalPriority) == 0 && len(q.lowPriority) == 0
	
	// Use TransactionManager's logger if possible
	// Since this is called in contexts where we don't always have a transaction
	// We use a default logger if we can't get one
	if len(q.highPriority) > 0 {
		q.highPriority[0].manager.logger.Debug("TransactionQueue.IsEmpty - Queue status: high=%d, normal=%d, low=%d, isEmpty=%v", 
			len(q.highPriority), len(q.normalPriority), len(q.lowPriority), isEmpty)
	} else if len(q.normalPriority) > 0 {
		q.normalPriority[0].manager.logger.Debug("TransactionQueue.IsEmpty - Queue status: high=%d, normal=%d, low=%d, isEmpty=%v", 
			len(q.highPriority), len(q.normalPriority), len(q.lowPriority), isEmpty)
	} else if len(q.lowPriority) > 0 {
		q.lowPriority[0].manager.logger.Debug("TransactionQueue.IsEmpty - Queue status: high=%d, normal=%d, low=%d, isEmpty=%v", 
			len(q.highPriority), len(q.normalPriority), len(q.lowPriority), isEmpty)
	}
	// If all queues are empty, we can't log this info
	
	return isEmpty
}

// Count returns the total number of transactions in the queue
func (q *TransactionQueue) Count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	count := len(q.highPriority) + len(q.normalPriority) + len(q.lowPriority)
	return count
}

// DebugInfo returns debug information about the queue
func (q *TransactionQueue) DebugInfo() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return fmt.Sprintf("Queue status: high=%d, normal=%d, low=%d", 
		len(q.highPriority), len(q.normalPriority), len(q.lowPriority))
}

// TransactionManager handles the creation, committing, and recovery of transactions
// In the single-writer model, it ensures only one write transaction is active at any time
type TransactionManager struct {
	dataDir              string
	transactionDir       string
	logger               model.Logger
	mu                   sync.RWMutex
	nextTxID             uint64
	activeTransactions   map[uint64]*Transaction
	isOpen               bool                // Whether the transaction manager is open
	wal                  *WAL                // Reference to the WAL for recording transaction boundaries
	engine               *StorageEngine      // Reference to the storage engine
	
	// Single-writer concurrency control
	writerLock           sync.Mutex          // The exclusive lock for the active writer
	activeWriter         *Transaction        // The currently active writer transaction, if any
	writeTxQueue         *TransactionQueue   // Queue for pending write transactions
	txStateChangedCond   *sync.Cond          // Condition variable for signaling transaction state changes
}

// TransactionRecoveryInfo tracks transaction status during recovery
type TransactionRecoveryInfo struct {
	ID          uint64
	FilePath    string
	CommitPath  string
	IsCommitted bool
	WalStatus   string // can be "committed", "rolledback", "active", or "" (unknown)
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(dataDir string, logger model.Logger, wal *WAL) (*TransactionManager, error) {
	if logger == nil {
		logger = model.DefaultLoggerInstance
	}

	// Create transaction directory
	txDir := filepath.Join(dataDir, "transactions")
	if err := os.MkdirAll(txDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create transaction directory: %w", err)
	}

	// Initial transaction ID
	nextTxID := uint64(1)

	// Try to recover the transaction ID from the state file
	nextTxID = recoverTransactionID(txDir, logger, nextTxID)

	// Also check if WAL has a higher transaction ID
	if wal != nil && wal.nextTxID > nextTxID {
		logger.Info("Using higher transaction ID from WAL: %d (was %d)", wal.nextTxID, nextTxID)
		nextTxID = wal.nextTxID
	}

	tm := &TransactionManager{
		dataDir:            dataDir,
		transactionDir:     txDir,
		logger:             logger,
		nextTxID:           nextTxID,
		activeTransactions: make(map[uint64]*Transaction),
		isOpen:             true,
		wal:                wal,
		writeTxQueue:       NewTransactionQueue(),
	}

	// Initialize the condition variable for transaction state changes
	tm.txStateChangedCond = sync.NewCond(&tm.mu)

	// Recover any incomplete transactions
	if err := tm.recoverTransactions(); err != nil {
		logger.Error("Failed to recover transactions: %v", err)
	}

	return tm, nil
}

// SetEngine sets the storage engine reference
// This is needed to break the circular dependency between Engine and TransactionManager
func (tm *TransactionManager) SetEngine(engine *StorageEngine) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.engine = engine
}

// recoverTransactionID attempts to recover the next transaction ID from the state file
func recoverTransactionID(txDir string, logger model.Logger, defaultNextTxID uint64) uint64 {
	// Check if we have a persisted transaction state file
	stateFilePath := filepath.Join(txDir, "tx_state")
	if stateData, err := os.ReadFile(stateFilePath); err == nil {
		// File exists, try to parse the transaction ID
		if id, err := strconv.ParseUint(string(stateData), 10, 64); err == nil && id > 0 {
			logger.Info("Restored transaction ID from state file: %d", id)
			return id
		}
	}
	return defaultNextTxID
}

// BeginTransaction starts a new transaction with the given options
func (tm *TransactionManager) BeginTransaction(options TxOptions) (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !tm.isOpen {
		return nil, fmt.Errorf("transaction manager is closed")
	}

	txID := tm.nextTxID
	tm.nextTxID++

	// Calculate batch size based on options
	batchSize := 100 // Default
	if options.BatchSize > 0 {
		batchSize = options.BatchSize
	}

	// Whether to use buffered writes
	var bufferedWrites map[string]interface{}
	if options.BufferWrites {
		bufferedWrites = make(map[string]interface{})
	}

	tx := &Transaction{
		id:             txID,
		operations:     make([]TransactionOperation, 0),
		state:          PENDING,
		manager:        tm,
		options:        options,
		creationTime:   time.Now(),
		
		// Initialize batch processing support
		operationBatch: make([]TransactionOperation, 0, batchSize),
		batchSize:      0,
		maxBatchSize:   batchSize,
		bufferedWrites: bufferedWrites,
	}

	// Add debug log
	tm.logger.Debug("BeginTransaction %d of type %s with priority %s and timeout %v", 
		txID, options.Type, options.Priority, options.Timeout)

	// For read-only transactions, allow them to proceed immediately
	if options.Type == READ_ONLY {
		return tm.startReadOnlyTransaction(tx)
	}

	// For write transactions, handle contention
	return tm.startWriteTransaction(tx)
}

// startReadOnlyTransaction activates a read-only transaction immediately
func (tm *TransactionManager) startReadOnlyTransaction(tx *Transaction) (*Transaction, error) {
	tx.state = ACTIVE
	tm.activeTransactions[tx.id] = tx
	
	// Create a snapshot for read isolation if the engine is available
	if tm.engine != nil && tm.engine.snapshotManager != nil {
		tx.snapshot = tm.engine.snapshotManager.CreateSnapshot()
		tm.logger.Debug("Started read-only transaction %d with snapshot %d", 
			tx.id, tx.snapshot.ID())
	} else {
		tm.logger.Debug("Started read-only transaction %d without snapshot (engine not available)", tx.id)
	}

	// Record transaction begin in WAL if available
	if tm.wal != nil {
		if err := tm.wal.BeginTransactionWithID(tx.id); err != nil {
			tm.logger.Error("Failed to record transaction begin in WAL: %v", err)
			// Continue with file-based transaction only
		}
	}
	
	return tx, nil
}

// startWriteTransaction handles starting a write transaction, dealing with contention if needed
func (tm *TransactionManager) startWriteTransaction(tx *Transaction) (*Transaction, error) {
	tm.logger.Debug("startWriteTransaction - tx %d, activeWriter: %v, queue size: %d", 
		tx.id, tm.activeWriter != nil, tm.writeTxQueue.Count())

	// Check if there's already an active writer
	if tm.activeWriter != nil {
		tm.logger.Debug("startWriteTransaction - active writer exists (ID: %d), enqueueing tx %d", 
			tm.activeWriter.id, tx.id)
		return tm.enqueueWriteTransaction(tx)
	}
	
	// No active writer, this transaction can proceed immediately
	tm.logger.Debug("startWriteTransaction - no active writer, activating tx %d immediately", tx.id)
	tx.state = ACTIVE
	tm.activeWriter = tx
	tm.activeTransactions[tx.id] = tx
	tm.logger.Debug("Started write transaction %d immediately (no contention)", tx.id)

	// Record transaction begin in WAL if available
	if tm.wal != nil {
		if err := tm.wal.BeginTransactionWithID(tx.id); err != nil {
			tm.logger.Error("Failed to record transaction begin in WAL: %v", err)
			// Continue with file-based transaction only
		}
	}

	tm.logger.Debug("startWriteTransaction - tx %d now active", tx.id)
	return tx, nil
}

// enqueueWriteTransaction adds a write transaction to the queue and waits for it to become active
func (tm *TransactionManager) enqueueWriteTransaction(tx *Transaction) (*Transaction, error) {
	options := tx.options
	txID := tx.id
	
	// If there's a timeout, set up a timer to abort waiting
	var timer *time.Timer
	var timerDone chan struct{}
	
	if options.Timeout > 0 {
		timerDone = make(chan struct{})
		timer = time.AfterFunc(options.Timeout, func() {
			tm.mu.Lock()
			defer tm.mu.Unlock()
			
			// If the transaction is still pending, remove it from the queue
			if tx.state == PENDING {
				tm.writeTxQueue.RemoveTransaction(txID)
				tx.state = ABORTED
				tm.logger.Debug("Transaction %d aborted due to timeout", txID)
				tm.txStateChangedCond.Broadcast()
			}
			close(timerDone)
		})
	}

	// Add the transaction to the waiting queue
	tm.writeTxQueue.Enqueue(tx)
	tm.activeTransactions[txID] = tx
	var activeWriterID uint64
	if tm.activeWriter != nil {
		activeWriterID = tm.activeWriter.id
	}
	tm.logger.Debug("Added write transaction %d to queue (priority: %s, timeout: %v, current active writer: %v)", 
		txID, options.Priority, options.Timeout, activeWriterID)
	
	// Wait until this transaction becomes active or gets aborted
	for tx.state == PENDING {
		// Exit the loop if we've timed out or the manager is closed
		if !tm.isOpen {
			// If we have a timer, stop it
			if timer != nil {
				timer.Stop()
			}
			return nil, fmt.Errorf("transaction manager closed while waiting")
		}
		
		// Wait for a state change
		tm.logger.Debug("Transaction %d waiting for state change...", txID)
		tm.txStateChangedCond.Wait()
		tm.logger.Debug("Transaction %d woke up, state: %v", txID, tx.state)

		// Check if we've been aborted (e.g., due to timeout)
		if tx.state == ABORTED {
			if timer != nil {
				timer.Stop()
			}
			return nil, fmt.Errorf("write transaction timed out after %v", options.Timeout)
		}
	}

	// If we have a timer, stop it since we've successfully acquired the lock
	if timer != nil {
		timer.Stop()
	}

	// Record transaction begin in WAL if available
	if tm.wal != nil {
		if err := tm.wal.BeginTransactionWithID(tx.id); err != nil {
			tm.logger.Error("Failed to record transaction begin in WAL: %v", err)
			// Continue with file-based transaction only
		}
	}

	tm.logger.Debug("Transaction %d is now active", txID)
	return tx, nil
}

// tryActivateNextWriteTransaction attempts to activate the next write transaction in the queue
func (tm *TransactionManager) tryActivateNextWriteTransaction() {
	// This should be called with tm.mu already locked
	
	if tm.activeWriter != nil {
		tm.logger.Debug("Can't activate next transaction - there's already an active writer: %d", tm.activeWriter.id)
		return
	}
	
	if tm.writeTxQueue.IsEmpty() {
		tm.logger.Debug("Can't activate next transaction - queue is empty")
		return
	}
	
	// Get the next pending write transaction from the queue
	nextTx := tm.writeTxQueue.Dequeue()
	if nextTx != nil {
		tm.logger.Debug("Found pending transaction %d in queue, activating it", nextTx.id)
		nextTx.state = ACTIVE
		tm.activeWriter = nextTx
		tm.logger.Debug("Activated next write transaction %d from queue", nextTx.id)
		// Make sure to broadcast to wake up all waiting transactions
		tm.txStateChangedCond.Broadcast()
	} else {
		tm.logger.Debug("Dequeue returned nil transaction despite queue not being empty!")
	}
}

// Close closes the transaction manager and cleans up resources
func (tm *TransactionManager) Close() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !tm.isOpen {
		return nil
	}

	tm.logger.Debug("Closing transaction manager, active writer: %v, queue status: %s", 
		tm.activeWriter != nil, tm.writeTxQueue.DebugInfo())

	tm.isOpen = false

	// Roll back any active transactions
	pendingRollbacks := 0
	for id, tx := range tm.activeTransactions {
		if tx.state != COMMITTED && tx.state != ABORTED {
			tm.logger.Debug("Rolling back active transaction %d (state: %s) during shutdown", id, tx.state)
			tx.Rollback()
			pendingRollbacks++
		}
	}
	tm.logger.Debug("Rolled back %d active transactions during shutdown", pendingRollbacks)

	// Clear active transactions
	tm.activeTransactions = make(map[uint64]*Transaction)
	
	// Clear active writer
	if tm.activeWriter != nil {
		tm.logger.Debug("Clearing active writer %d during shutdown", tm.activeWriter.id)
		tm.activeWriter = nil
	}

	// Signal any waiting transactions to abort
	tm.logger.Debug("Broadcasting state change to wake up all waiting transactions")
	tm.txStateChangedCond.Broadcast()

	// Persist the next transaction ID to a file for recovery
	tm.persistTransactionState()

	tm.logger.Debug("Transaction manager closed successfully")
	return nil
}

// persistTransactionState saves the transaction manager state to disk
func (tm *TransactionManager) persistTransactionState() {
	stateFilePath := filepath.Join(tm.transactionDir, "tx_state")
	if err := os.WriteFile(stateFilePath, []byte(fmt.Sprintf("%d", tm.nextTxID)), 0644); err != nil {
		tm.logger.Warn("Failed to persist transaction manager state: %v", err)
	} else {
		tm.logger.Debug("Persisted transaction manager state (nextTxID: %d)", tm.nextTxID)
	}
}

// recoverTransactions recovers any incomplete transactions from disk
func (tm *TransactionManager) recoverTransactions() error {
	// Get WAL transaction status first if available
	walTxs := make(map[uint64]string) // txID -> status (committed, rolledback, active)
	
	// If we have a WAL, we should load transaction statuses from it
	if tm.wal != nil {
		// This would be implemented in a real system to get transaction statuses from WAL
		// For now, we'll leave it as a placeholder
	}

	// Find all transaction files
	txInfos, err := tm.findTransactionFiles()
	if err != nil {
		return err
	}

	// Update WAL statuses in the transaction info
	for i, info := range txInfos {
		if status, exists := walTxs[info.ID]; exists {
			txInfos[i].WalStatus = status
		}
	}

	// Process each transaction based on its status
	for _, info := range txInfos {
		tm.processRecoveredTransaction(info, walTxs)
		
		// Update next transaction ID if needed
		if info.ID >= tm.nextTxID {
			tm.nextTxID = info.ID + 1
		}
	}

	return nil
}

// findTransactionFiles scans the transaction directory and returns info about transaction files
func (tm *TransactionManager) findTransactionFiles() ([]TransactionRecoveryInfo, error) {
	var transactions []TransactionRecoveryInfo

	entries, err := os.ReadDir(tm.transactionDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read transaction directory: %w", err)
	}

	// First pass: identify transaction log files
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "tx_") && strings.HasSuffix(entry.Name(), ".log") {
			// Extract transaction ID
			idStr := strings.TrimPrefix(entry.Name(), "tx_")
			idStr = strings.TrimSuffix(idStr, ".log")
			txID, err := strconv.ParseUint(idStr, 10, 64)
			if err != nil {
				tm.logger.Warn("Invalid transaction file name: %s", entry.Name())
				continue
			}

			// Check if there's a matching .commit file
			commitPath := filepath.Join(tm.transactionDir, fmt.Sprintf("tx_%d.commit", txID))
			_, commitErr := os.Stat(commitPath)
			
			transactions = append(transactions, TransactionRecoveryInfo{
				ID:          txID,
				FilePath:    filepath.Join(tm.transactionDir, entry.Name()),
				CommitPath:  commitPath,
				IsCommitted: commitErr == nil,
			})
		}
	}

	return transactions, nil
}

// processRecoveredTransaction handles an individual transaction during recovery
func (tm *TransactionManager) processRecoveredTransaction(info TransactionRecoveryInfo, walTxs map[uint64]string) {
	// Get the transaction status from file state
	fileStatus := "uncommitted"
	if info.IsCommitted {
		fileStatus = "committed"
	}

	// Handle status mismatches between WAL and files
	if info.WalStatus != "" && info.WalStatus != fileStatus {
		tm.handleStatusMismatch(info, fileStatus)
		return
	}

	// Process based on status
	if info.IsCommitted {
		tm.processCommittedTransaction(info)
	} else {
		tm.processUncommittedTransaction(info, walTxs)
	}
}

// handleStatusMismatch resolves inconsistencies between WAL and file transaction status
func (tm *TransactionManager) handleStatusMismatch(info TransactionRecoveryInfo, fileStatus string) {
	tm.logger.Warn("Transaction %d status mismatch: WAL says %s, file says %s",
		info.ID, info.WalStatus, fileStatus)

	// WAL is the source of truth for transaction boundaries, but files
	// are the source of truth for the actual operations
	if info.WalStatus == "committed" && fileStatus == "uncommitted" {
		// WAL says committed but file doesn't have commit marker
		// This could happen if we crashed after WAL commit but before file commit
		// Create the missing commit marker
		tm.logger.Info("Creating missing commit marker for transaction %d", info.ID)
		commitFile, err := os.Create(info.CommitPath)
		if err != nil {
			tm.logger.Error("Failed to create commit marker: %v", err)
		} else {
			fmt.Fprintf(commitFile, "%d", time.Now().UnixNano())
			commitFile.Close()
			
			// Now we can process it as committed
			tm.processCommittedTransaction(info)
		}
	} else if info.WalStatus == "rolledback" && fileStatus == "uncommitted" {
		// WAL says rolledback, we should clean up the file
		tm.logger.Info("Removing rolled back transaction %d file", info.ID)
		os.Remove(info.FilePath)
	}
}

// processCommittedTransaction applies a committed transaction
func (tm *TransactionManager) processCommittedTransaction(info TransactionRecoveryInfo) {
	if err := tm.applyTransaction(info.ID); err != nil {
		tm.logger.Error("Failed to apply committed transaction %d: %v", info.ID, err)
	} else {
		// Successfully applied, clean up files
		os.Remove(info.FilePath)
		os.Remove(info.CommitPath)
	}
}

// processUncommittedTransaction handles an uncommitted transaction
func (tm *TransactionManager) processUncommittedTransaction(info TransactionRecoveryInfo, walTxs map[uint64]string) {
	// Rollback uncommitted transaction
	if err := tm.rollbackTransaction(info.ID); err != nil {
		tm.logger.Error("Failed to rollback transaction %d: %v", info.ID, err)
	} else {
		// Successfully rolled back, clean up file
		os.Remove(info.FilePath)
	}

	// Also rollback in WAL if needed and not already rolled back
	if tm.wal != nil && walTxs[info.ID] != "rolledback" {
		if err := tm.wal.RollbackTransaction(info.ID); err != nil {
			tm.logger.Error("Failed to rollback transaction %d in WAL: %v", info.ID, err)
		}
	}
}

// applyTransaction applies a committed transaction from disk
func (tm *TransactionManager) applyTransaction(txID uint64) error {
	// Open transaction log file
	txPath := filepath.Join(tm.transactionDir, fmt.Sprintf("tx_%d.log", txID))
	file, err := os.Open(txPath)
	if err != nil {
		return fmt.Errorf("failed to open transaction file: %w", err)
	}
	defer file.Close()

	// Read and execute each operation
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if len(parts) < 3 {
			continue // Invalid line format
		}

		opType := parts[0]
		target := parts[1]
		idStr := parts[2]

		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			tm.logger.Warn("Invalid ID in transaction %d: %s", txID, idStr)
			continue
		}

		switch {
		case opType == "remove" && target == "sstable":
			// Remove SSTable files
			tm.removeSSTableFiles(id)
		case opType == "rename" && target == "sstable" && len(parts) >= 4:
			// Rename SSTable files from tempID to finalID
			tempID := id
			finalID, err := strconv.ParseUint(parts[3], 10, 64)
			if err != nil {
				tm.logger.Warn("Invalid final ID in transaction %d: %s", txID, parts[3])
				continue
			}
			tm.renameSSTableFiles(tempID, finalID)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading transaction file: %w", err)
	}

	return nil
}

// rollbackTransaction rolls back an uncommitted transaction
func (tm *TransactionManager) rollbackTransaction(txID uint64) error {
	// Open transaction log file
	txPath := filepath.Join(tm.transactionDir, fmt.Sprintf("tx_%d.log", txID))
	file, err := os.Open(txPath)
	if err != nil {
		return fmt.Errorf("failed to open transaction file: %w", err)
	}
	defer file.Close()

	// Read and reverse each operation
	lines := make([]string, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading transaction file: %w", err)
	}

	// Process lines in reverse order for proper rollback
	for i := len(lines) - 1; i >= 0; i-- {
		line := lines[i]
		parts := strings.Split(line, ":")
		if len(parts) < 3 {
			continue // Invalid line format
		}

		opType := parts[0]
		target := parts[1]
		idStr := parts[2]

		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			tm.logger.Warn("Invalid ID in transaction %d: %s", txID, idStr)
			continue
		}

		switch {
		case opType == "add" && target == "sstable":
			// Remove added SSTable files
			tm.removeSSTableFiles(id)
		case opType == "rename" && target == "sstable" && len(parts) >= 4:
			// Reverse the rename - not needed for rollback as temp files aren't referenced yet
			// Just remove the temporary files
			tempID := id
			tm.removeSSTableFiles(tempID)
		}
	}

	return nil
}

// removeSSTableFiles removes SSTable files for the given ID
func (tm *TransactionManager) removeSSTableFiles(id uint64) {
	sstableDir := filepath.Join(tm.dataDir, "sstables")
	dataFile := filepath.Join(sstableDir, fmt.Sprintf("%d.data", id))
	indexFile := filepath.Join(sstableDir, fmt.Sprintf("%d.index", id))
	filterFile := filepath.Join(sstableDir, fmt.Sprintf("%d.filter", id))

	// Remove files
	if err := os.Remove(dataFile); err != nil && !os.IsNotExist(err) {
		tm.logger.Warn("Failed to remove SSTable data file %d: %v", id, err)
	}
	if err := os.Remove(indexFile); err != nil && !os.IsNotExist(err) {
		tm.logger.Warn("Failed to remove SSTable index file %d: %v", id, err)
	}
	if err := os.Remove(filterFile); err != nil && !os.IsNotExist(err) {
		tm.logger.Warn("Failed to remove SSTable filter file %d: %v", id, err)
	}
}

// renameSSTableFiles renames SSTable files from tempID to finalID
func (tm *TransactionManager) renameSSTableFiles(tempID, finalID uint64) {
	sstableDir := filepath.Join(tm.dataDir, "sstables")

	// Source files
	tempDataFile := filepath.Join(sstableDir, fmt.Sprintf("%d.data", tempID))
	tempIndexFile := filepath.Join(sstableDir, fmt.Sprintf("%d.index", tempID))
	tempFilterFile := filepath.Join(sstableDir, fmt.Sprintf("%d.filter", tempID))

	// Destination files
	finalDataFile := filepath.Join(sstableDir, fmt.Sprintf("%d.data", finalID))
	finalIndexFile := filepath.Join(sstableDir, fmt.Sprintf("%d.index", finalID))
	finalFilterFile := filepath.Join(sstableDir, fmt.Sprintf("%d.filter", finalID))

	// Rename files
	if err := os.Rename(tempDataFile, finalDataFile); err != nil && !os.IsNotExist(err) {
		tm.logger.Warn("Failed to rename SSTable data file %d to %d: %v", tempID, finalID, err)
	}
	if err := os.Rename(tempIndexFile, finalIndexFile); err != nil && !os.IsNotExist(err) {
		tm.logger.Warn("Failed to rename SSTable index file %d to %d: %v", tempID, finalID, err)
	}
	if err := os.Rename(tempFilterFile, finalFilterFile); err != nil && !os.IsNotExist(err) {
		tm.logger.Warn("Failed to rename SSTable filter file %d to %d: %v", tempID, finalID, err)
	}
}

// GetTransactionCount returns the number of active transactions
func (tm *TransactionManager) GetTransactionCount() (readOnly, readWrite, pending int) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	for _, tx := range tm.activeTransactions {
		if tx.options.Type == READ_ONLY {
			readOnly++
		} else if tx.state == ACTIVE {
			readWrite++
		} else if tx.state == PENDING {
			pending++
		}
	}

	return readOnly, readWrite, pending
}

// GetActiveWriter returns the currently active write transaction, if any
func (tm *TransactionManager) GetActiveWriter() *Transaction {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.activeWriter
}

// GetTransactionQueueLength returns the number of transactions waiting in the queue
func (tm *TransactionManager) GetTransactionQueueLength() int {
	return tm.writeTxQueue.Count()
}