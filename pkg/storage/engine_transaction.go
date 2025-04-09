package storage

import (
	"fmt"
)

// BeginTransaction starts a new transaction with the given options
func (e *StorageEngine) BeginTransaction(options TxOptions) (*Transaction, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.isOpen {
		return nil, ErrEngineClosed
	}

	// Forward to the transaction manager
	tx, err := e.txManager.BeginTransaction(options)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	
	e.logger.Debug("Started %s transaction %d", options.Type, tx.ID())
	return tx, nil
}

// DataOperation represents an operation on data (key-value) in a transaction
type DataOperation struct {
	Type  string // "put" or "delete"
	Key   []byte
	Value []byte
}

// DataOperation struct is defined earlier and is used for transaction operations

// Put adds or updates a key-value pair in the storage engine within a transaction
func (e *StorageEngine) PutWithTx(tx *Transaction, key, value []byte) error {
	if !tx.IsActive() {
		return fmt.Errorf("transaction is not active")
	}

	// Read-only transactions cannot modify data
	if tx.Type() == READ_ONLY {
		return fmt.Errorf("cannot modify data in a read-only transaction")
	}

	// Create a data operation for tracking
	dataOp := DataOperation{
		Type:  "put",
		Key:   key,
		Value: value,
	}

	// Create a transaction operation
	txOp := TransactionOperation{
		Type:   "data-put",
		Target: "kv-pair",
		Data:   dataOp,
	}

	// Add to transaction's operation batch for efficient processing
	tx.AddOperation(txOp)

	// If the transaction has buffering enabled, add to buffered writes instead of
	// writing directly to the MemTable. This ensures atomic visibility of changes.
	// We don't need to lock e.mu here since we're only modifying the transaction.
	if tx.bufferedWrites != nil {
		keyStr := string(key)
		tx.bufferedWrites[keyStr] = BufferedWrite{
			Key:      key,
			Value:    value,
			IsDelete: false,
		}
		return nil
	}

	// If not using buffering, proceed with immediate write
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isOpen {
		return ErrEngineClosed
	}

	// Check if the MemTable is full before processing the operation
	if e.memTable.IsFull() {
		// Mark the current MemTable as immutable
		e.memTable.MarkFlushed()
		e.immMemTables = append(e.immMemTables, e.memTable)

		// Create a new MemTable
		e.memTable = NewMemTable(MemTableConfig{
			MaxSize:    e.config.MemTableSize,
			Logger:     e.logger,
			Comparator: e.config.Comparator,
		})

		// Signal the compaction thread
		e.compactionCond.Signal()
	}

	// Record the operation in the WAL as part of the transaction
	if err := e.wal.RecordPutInTransaction(key, value, tx.ID()); err != nil {
		return fmt.Errorf("failed to record put in WAL: %w", err)
	}

	// Add the key-value pair to the MemTable
	if err := e.memTable.Put(key, value); err != nil {
		return fmt.Errorf("failed to add key-value pair to MemTable: %w", err)
	}

	return nil
}

// Get retrieves a value by key from the storage engine within a transaction
func (e *StorageEngine) GetWithTx(tx *Transaction, key []byte) ([]byte, error) {
	if !tx.IsActive() {
		return nil, fmt.Errorf("transaction is not active")
	}
	
	// For write transactions with buffered writes, check the buffer first
	// This ensures reads within the transaction see the transaction's own uncommitted changes
	if tx.options.Type == READ_WRITE && tx.bufferedWrites != nil {
		tx.mu.Lock() // Lock to safely access bufferedWrites
		keyStr := string(key)
		if bufferedWriteRaw, exists := tx.bufferedWrites[keyStr]; exists {
			tx.mu.Unlock()
			
			// Convert to BufferedWrite type
			if bufferedWrite, ok := bufferedWriteRaw.(BufferedWrite); ok {
				// If it's a delete, return not found
				if bufferedWrite.IsDelete {
					return nil, ErrKeyNotFound
				}
				
				// Otherwise return the buffered value
				return bufferedWrite.Value, nil
			}
		}
		tx.mu.Unlock()
	}

	// For read-only transactions with a snapshot, use the snapshot for reads
	if tx.options.Type == READ_ONLY && tx.snapshot != nil {
		return tx.snapshot.Get(key)
	}

	// For read-only transactions without a snapshot, lazily create one now
	// This ensures read-only transactions always have a snapshot and don't see uncommitted data
	if tx.options.Type == READ_ONLY && tx.snapshot == nil && tx.manager.engine != nil && 
	   tx.manager.engine.snapshotManager != nil {
		// Create a snapshot for this transaction to ensure read isolation
		tx.snapshot = tx.manager.engine.snapshotManager.CreateSnapshot()
		tx.manager.engine.logger.Debug("Lazily created snapshot %d for read-only transaction %d",
			tx.snapshot.ID(), tx.ID())
		
		// Now use the snapshot for this read
		return tx.snapshot.Get(key)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.isOpen {
		return nil, ErrEngineClosed
	}

	// For all transaction types, search for the key in the database
	value, err := e.memTable.Get(key)
	if err == nil {
		e.logger.Debug("Found key in current MemTable")
		return value, nil
	} else if err != ErrKeyNotFound {
		return nil, err
	}

	// Try to get the value from immutable MemTables (newest to oldest)
	for i := len(e.immMemTables) - 1; i >= 0; i-- {
		value, err := e.immMemTables[i].Get(key)
		if err == nil {
			e.logger.Debug("Found key in immutable MemTable %d", i)
			return value, nil
		} else if err != ErrKeyNotFound {
			return nil, err
		}
	}

	// Try to get the value from SSTables (newest to oldest)
	for i := len(e.sstables) - 1; i >= 0; i-- {
		value, err := e.sstables[i].Get(key)
		if err == nil {
			e.logger.Debug("Found key in SSTable %d", e.sstables[i].ID())
			return value, nil
		} else if err != ErrKeyNotFoundInSSTable {
			return nil, err
		}
	}

	// If not found in active SSTables, also check those pending deletion
	// This ensures reads can still succeed during the deletion delay period
	value, err = e.getFromPendingDeletionSSTables(key)
	if err == nil {
		// Found in an SSTable that's pending deletion
		e.logger.Debug("Found key in SSTable pending deletion")
		return value, nil
	} else if err != ErrKeyNotFound {
		return nil, err
	}

	e.logger.Debug("Key not found in any storage tier")
	return nil, ErrKeyNotFound
}

// Delete removes a key from the storage engine within a transaction
func (e *StorageEngine) DeleteWithTx(tx *Transaction, key []byte) error {
	if !tx.IsActive() {
		return fmt.Errorf("transaction is not active")
	}

	// Read-only transactions cannot modify data
	if tx.Type() == READ_ONLY {
		return fmt.Errorf("cannot modify data in a read-only transaction")
	}

	// Create a data operation for tracking
	dataOp := DataOperation{
		Type:  "delete",
		Key:   key,
	}

	// Create a transaction operation
	txOp := TransactionOperation{
		Type:   "data-delete",
		Target: "kv-pair",
		Data:   dataOp,
	}

	// Add to transaction's operation batch for efficient processing
	tx.AddOperation(txOp)

	// If the transaction has buffering enabled, add to buffered writes instead of
	// writing directly to the MemTable. This ensures atomic visibility of changes.
	if tx.bufferedWrites != nil {
		keyStr := string(key)
		tx.bufferedWrites[keyStr] = BufferedWrite{
			Key:      key,
			IsDelete: true,
		}
		return nil
	}

	// If not using buffering, proceed with immediate write
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isOpen {
		return ErrEngineClosed
	}

	// Check if the MemTable is full before processing the operation
	if e.memTable.IsFull() {
		// Mark the current MemTable as immutable
		e.memTable.MarkFlushed()
		e.immMemTables = append(e.immMemTables, e.memTable)

		// Create a new MemTable
		e.memTable = NewMemTable(MemTableConfig{
			MaxSize:    e.config.MemTableSize,
			Logger:     e.logger,
			Comparator: e.config.Comparator,
		})

		// Signal the compaction thread
		e.compactionCond.Signal()
	}

	// Record the delete operation in the WAL
	if err := e.wal.RecordDeleteInTransaction(key, tx.ID()); err != nil {
		return fmt.Errorf("failed to record delete in WAL: %w", err)
	}

	// Delete the key from the MemTable
	if err := e.memTable.Delete(key); err != nil {
		return fmt.Errorf("failed to delete key from MemTable: %w", err)
	}

	return nil
}

// Contains checks if a key exists in the storage engine within a transaction
func (e *StorageEngine) ContainsWithTx(tx *Transaction, key []byte) (bool, error) {
	if !tx.IsActive() {
		return false, fmt.Errorf("transaction is not active")
	}

	// For read-only transactions with a snapshot, use the snapshot for reads
	if tx.options.Type == READ_ONLY && tx.snapshot != nil {
		return tx.snapshot.Contains(key)
	}

	// For read-only transactions without a snapshot, lazily create one now
	// This ensures read-only transactions always have a snapshot and don't see uncommitted data
	if tx.options.Type == READ_ONLY && tx.snapshot == nil && tx.manager.engine != nil && 
	   tx.manager.engine.snapshotManager != nil {
		// Create a snapshot for this transaction to ensure read isolation
		tx.snapshot = tx.manager.engine.snapshotManager.CreateSnapshot()
		tx.manager.engine.logger.Debug("Lazily created snapshot %d for read-only transaction %d",
			tx.snapshot.ID(), tx.ID())
		
		// Now use the snapshot for this check
		return tx.snapshot.Contains(key)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.isOpen {
		return false, ErrEngineClosed
	}

	// For write transactions, use the current database state directly
	// This allows the transaction to see its own uncommitted changes
	
	// Check the current MemTable
	if e.memTable.Contains(key) {
		return true, nil
	}

	// Check immutable MemTables (newest to oldest)
	for i := len(e.immMemTables) - 1; i >= 0; i-- {
		if e.immMemTables[i].Contains(key) {
			return true, nil
		}
	}

	// Check SSTables (newest to oldest)
	for i := len(e.sstables) - 1; i >= 0; i-- {
		contains, err := e.sstables[i].Contains(key)
		if err != nil {
			return false, fmt.Errorf("error checking SSTable: %w", err)
		}
		if contains {
			return true, nil
		}
	}

	// Also check SSTables pending deletion
	contains, err := e.containsInPendingDeletionSSTables(key)
	if err != nil {
		return false, fmt.Errorf("error checking pending deletion SSTables: %w", err)
	}
	if contains {
		return true, nil
	}

	return false, nil
}

// ScanWithTx returns a list of keys that start with the given prefix within a transaction
func (e *StorageEngine) ScanWithTx(tx *Transaction, prefix []byte, limit int) ([][]byte, error) {
	if !tx.IsActive() {
		return nil, fmt.Errorf("transaction is not active")
	}

	// For read-only transactions with a snapshot, use the snapshot for reads
	if tx.options.Type == READ_ONLY && tx.snapshot != nil {
		return tx.snapshot.Scan(prefix, limit)
	}

	// For read-only transactions without a snapshot, lazily create one now
	// This ensures read-only transactions always have a snapshot and don't see uncommitted data
	if tx.options.Type == READ_ONLY && tx.snapshot == nil && tx.manager.engine != nil && 
	   tx.manager.engine.snapshotManager != nil {
		// Create a snapshot for this transaction to ensure read isolation
		tx.snapshot = tx.manager.engine.snapshotManager.CreateSnapshot()
		tx.manager.engine.logger.Debug("Lazily created snapshot %d for read-only transaction %d",
			tx.snapshot.ID(), tx.ID())
		
		// Now use the snapshot for this scan
		return tx.snapshot.Scan(prefix, limit)
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.isOpen {
		return nil, ErrEngineClosed
	}

	// For write transactions, use the current database state directly
	// This allows the transaction to see its own uncommitted changes

	// Initialize result slice
	result := make([][]byte, 0, limit)
	prefixStr := string(prefix)
	e.logger.Debug("Scanning for keys with prefix: %s (limit: %d)", prefixStr, limit)
	fmt.Printf("SCAN DEBUG: Starting scan with prefix: %s (hex: %x, len: %d)\n", prefixStr, prefix, len(prefix))

	// Get keys from the current MemTable
	memKeys := e.memTable.GetKeysWithPrefix(prefix)
	e.logger.Debug("Found %d keys with prefix in current MemTable", len(memKeys))
	
	// DEBUG: Dump all keys in memtable to see what's there
	allKeys := e.memTable.GetAllKeys()
	fmt.Printf("SCAN DEBUG: Current MemTable has %d total keys\n", len(allKeys))
	for i, k := range allKeys {
		if i < 20 { // Limit to first 20 keys to avoid overwhelming output
			fmt.Printf("SCAN DEBUG: MemTable key %d: %s (hex: %x)\n", i, string(k), k)
		}
	}

	// Add keys from the current MemTable to the result, up to the limit
	for _, key := range memKeys {
		result = append(result, key)
		if len(result) >= limit {
			return result, nil
		}
	}

	// Check immutable MemTables (newest to oldest)
	for i := len(e.immMemTables) - 1; i >= 0; i-- {
		immKeys := e.immMemTables[i].GetKeysWithPrefix(prefix)
		e.logger.Debug("Found %d keys with prefix in immutable MemTable %d", len(immKeys), i)

		// Add keys to the result, up to the limit
		for _, key := range immKeys {
			// Skip duplicates
			isDuplicate := false
			for _, existingKey := range result {
				if string(existingKey) == string(key) {
					isDuplicate = true
					break
				}
			}

			if !isDuplicate {
				result = append(result, key)
				if len(result) >= limit {
					return result, nil
				}
			}
		}
	}

	// Check SSTables (newest to oldest)
	for i := len(e.sstables) - 1; i >= 0; i-- {
		iter, err := e.sstables[i].Iterator()
		if err != nil {
			e.logger.Warn("Failed to create iterator for SSTable %d: %v", e.sstables[i].ID(), err)
			continue
		}

		// Seek to the prefix
		iter.Seek(prefix)

		// Collect keys with the prefix
		for iter.Valid() {
			key := iter.Key()

			// Check if the key starts with the prefix
			if !hasPrefix(key, prefix) {
				break // We've moved past keys with this prefix
			}

			// Skip duplicates
			isDuplicate := false
			for _, existingKey := range result {
				if string(existingKey) == string(key) {
					isDuplicate = true
					break
				}
			}

			if !isDuplicate {
				result = append(result, key)
				if len(result) >= limit {
					iter.Close()
					return result, nil
				}
			}

			// Move to the next key
			if err := iter.Next(); err != nil {
				e.logger.Warn("Error advancing iterator in SSTable %d: %v", e.sstables[i].ID(), err)
				break
			}
		}

		iter.Close()
	}

	// Also check SSTables pending deletion
	pendingKeys, err := e.scanPendingDeletionSSTables(prefix, limit-len(result))
	if err != nil {
		e.logger.Warn("Error scanning pending deletion SSTables: %v", err)
	} else {
		// Add pending keys, skipping duplicates
		for _, pendingKey := range pendingKeys {
			isDuplicate := false
			for _, existingKey := range result {
				if string(existingKey) == string(pendingKey) {
					isDuplicate = true
					break
				}
			}

			if !isDuplicate {
				result = append(result, pendingKey)
				if len(result) >= limit {
					break
				}
			}
		}
	}

	e.logger.Debug("Scan completed, found %d keys with prefix %s", len(result), prefixStr)
	return result, nil
}

// Deprecated: Use BeginTransaction and the WithTx methods instead
func (e *StorageEngine) Put(key, value []byte) error {
	// Create a default transaction
	tx, err := e.BeginTransaction(DefaultTxOptions())
	if err != nil {
		return err
	}
	
	// Execute the operation in the transaction
	if err := e.PutWithTx(tx, key, value); err != nil {
		tx.Rollback()
		return err
	}
	
	// Commit the transaction
	return tx.Commit()
}

// Deprecated: Use BeginTransaction and the WithTx methods instead
func (e *StorageEngine) Get(key []byte) ([]byte, error) {
	// Create a read-only transaction
	tx, err := e.BeginTransaction(ReadOnlyTxOptions())
	if err != nil {
		return nil, err
	}
	defer tx.Commit() // Safe to commit right away for read-only
	
	// Execute the operation in the transaction
	return e.GetWithTx(tx, key)
}

// Deprecated: Use BeginTransaction and the WithTx methods instead
func (e *StorageEngine) Delete(key []byte) error {
	// Create a default transaction
	tx, err := e.BeginTransaction(DefaultTxOptions())
	if err != nil {
		return err
	}
	
	// Execute the operation in the transaction
	if err := e.DeleteWithTx(tx, key); err != nil {
		tx.Rollback()
		return err
	}
	
	// Commit the transaction
	return tx.Commit()
}

// Deprecated: Use BeginTransaction and the WithTx methods instead
func (e *StorageEngine) Contains(key []byte) (bool, error) {
	// Create a read-only transaction
	tx, err := e.BeginTransaction(ReadOnlyTxOptions())
	if err != nil {
		return false, err
	}
	defer tx.Commit() // Safe to commit right away for read-only
	
	// Execute the operation in the transaction
	return e.ContainsWithTx(tx, key)
}

// Deprecated: Use BeginTransaction and the WithTx methods instead
func (e *StorageEngine) Scan(prefix []byte, limit int) ([][]byte, error) {
	// Create a read-only transaction
	tx, err := e.BeginTransaction(ReadOnlyTxOptions())
	if err != nil {
		return nil, err
	}
	defer tx.Commit() // Safe to commit right away for read-only
	
	// Execute the operation in the transaction
	return e.ScanWithTx(tx, prefix, limit)
}