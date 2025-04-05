package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TransactionOperation represents an operation within a transaction
type TransactionOperation struct {
	Type   string      // Type of operation (e.g., "add", "remove")
	Target string      // Target of the operation (e.g., "sstable")
	ID     uint64      // ID of the target
	Data   interface{} // Additional data for the operation
}

// Transaction represents a set of operations that should be executed atomically
type Transaction struct {
	id           uint64
	operations   []TransactionOperation
	commitStatus *atomic.Bool
	manager      *TransactionManager
	mu           sync.Mutex
}

// TransactionManager handles the creation, committing, and recovery of transactions
type TransactionManager struct {
	dataDir            string
	transactionDir     string
	logger             model.Logger
	mu                 sync.RWMutex
	nextTxID           uint64
	activeTransactions map[uint64]*Transaction
	isOpen             bool // Whether the transaction manager is open
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(dataDir string, logger model.Logger) (*TransactionManager, error) {
	if logger == nil {
		logger = model.DefaultLoggerInstance
	}

	// Create transaction directory
	txDir := filepath.Join(dataDir, "transactions")
	if err := os.MkdirAll(txDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create transaction directory: %w", err)
	}

	tm := &TransactionManager{
		dataDir:            dataDir,
		transactionDir:     txDir,
		logger:             logger,
		nextTxID:           1,
		activeTransactions: make(map[uint64]*Transaction),
		isOpen:             true,
	}

	// Recover any incomplete transactions
	if err := tm.recoverTransactions(); err != nil {
		logger.Error("Failed to recover transactions: %v", err)
	}

	return tm, nil
}

// Begin starts a new transaction
func (tm *TransactionManager) Begin() *Transaction {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !tm.isOpen {
		// Return an uncommittable transaction if manager is closed
		status := atomic.Bool{}
		status.Store(true) // Mark as already committed to prevent operations
		return &Transaction{
			id:           0,
			operations:   make([]TransactionOperation, 0),
			commitStatus: &status,
			manager:      nil,
		}
	}

	txID := tm.nextTxID
	tm.nextTxID++

	status := atomic.Bool{}
	status.Store(false) // Not committed initially

	tx := &Transaction{
		id:           txID,
		operations:   make([]TransactionOperation, 0),
		commitStatus: &status,
		manager:      tm,
	}

	tm.activeTransactions[txID] = tx
	return tx
}

// Close closes the transaction manager and cleans up resources
func (tm *TransactionManager) Close() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if !tm.isOpen {
		return nil
	}

	tm.isOpen = false

	// Roll back any active transactions
	for _, tx := range tm.activeTransactions {
		if !tx.IsCommitted() {
			tx.Rollback()
		}
	}

	// Clear active transactions
	tm.activeTransactions = make(map[uint64]*Transaction)

	return nil
}

// recoverTransactions recovers any incomplete transactions from disk
func (tm *TransactionManager) recoverTransactions() error {
	// List all transaction files
	entries, err := os.ReadDir(tm.transactionDir)
	if err != nil {
		return fmt.Errorf("failed to read transaction directory: %w", err)
	}

	// Process each transaction file
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
			isCommitted := commitErr == nil

			if isCommitted {
				// Process committed transaction
				if err := tm.applyTransaction(txID); err != nil {
					tm.logger.Error("Failed to apply committed transaction %d: %v", txID, err)
				} else {
					// Successfully applied, clean up files
					os.Remove(filepath.Join(tm.transactionDir, entry.Name()))
					os.Remove(commitPath)
				}
			} else {
				// Rollback uncommitted transaction
				if err := tm.rollbackTransaction(txID); err != nil {
					tm.logger.Error("Failed to rollback transaction %d: %v", txID, err)
				} else {
					// Successfully rolled back, clean up file
					os.Remove(filepath.Join(tm.transactionDir, entry.Name()))
				}
			}

			// Update next transaction ID if needed
			if txID >= tm.nextTxID {
				tm.nextTxID = txID + 1
			}
		}
	}

	return nil
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

// AddOperation adds an operation to a transaction
func (tx *Transaction) AddOperation(op TransactionOperation) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.commitStatus.Load() {
		// Transaction already committed, cannot add more operations
		return
	}

	tx.operations = append(tx.operations, op)
}

// Commit commits the transaction to disk and applies the operations
func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.commitStatus.Load() {
		// Already committed
		return nil
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
	tx.commitStatus.Store(true)

	// Apply the transaction
	if err := tx.manager.applyTransaction(tx.id); err != nil {
		return fmt.Errorf("failed to apply transaction: %w", err)
	}

	// Clean up transaction files
	os.Remove(txPath)
	os.Remove(commitPath)

	// Remove from active transactions
	tx.manager.mu.Lock()
	delete(tx.manager.activeTransactions, tx.id)
	tx.manager.mu.Unlock()

	return nil
}

// Rollback aborts the transaction
func (tx *Transaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.commitStatus.Load() {
		// Already committed, cannot rollback
		return fmt.Errorf("transaction already committed")
	}

	// Remove from active transactions
	tx.manager.mu.Lock()
	delete(tx.manager.activeTransactions, tx.id)
	tx.manager.mu.Unlock()

	return nil
}

// IsCommitted returns whether the transaction has been committed
func (tx *Transaction) IsCommitted() bool {
	return tx.commitStatus.Load()
}
