package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// Storage engine errors
var (
	ErrEngineClosed = errors.New("storage engine is closed")
)

// StorageEngine is the main interface to the storage system
// It manages the MemTable, SSTables, and WAL to provide a durable key-value store
type StorageEngine struct {
	mu                   sync.RWMutex
	config               EngineConfig
	memTable             MemTableInterface   // Current MemTable for writes - can be standard or lock-free
	immMemTables         []MemTableInterface // Immutable MemTables waiting to be flushed
	sstables             []*SSTable          // Sorted String Tables (persistent storage)
	wal                  *WAL                // Write-Ahead Log
	logger               model.Logger        // Logger for storage engine operations
	compactionCond       *sync.Cond          // Condition variable for signaling compaction
	compactionDone       chan struct{}       // Channel for signaling compaction is done
	isOpen               bool                // Whether the storage engine is open
	nextSSTableID        uint64              // Next SSTable ID to use
	leveledCompactor     *LeveledCompaction  // Manages the leveled compaction strategy
	txManager            *TransactionManager // Transaction manager for atomic operations
	useLSMNodeLabelIndex bool                // Whether to use LSM-tree based node label index
	usePropertyIndex     bool                // Whether to use specialized property index

	// Two-phase deletion mechanism for safe SSTable removal
	deletionMu       sync.Mutex           // Mutex for deletion operations (separate from main engine lock)
	pendingDeletions map[uint64]*SSTable  // SSTables pending deletion (phase 1: logical deletion)
	deletionTime     map[uint64]time.Time // When each SSTable was marked for deletion
	deletionExit     chan struct{}        // Channel to signal deletion worker to exit
}

// EngineConfig holds configuration options for the storage engine
type EngineConfig struct {
	// Directory where all storage files will be stored
	DataDir string

	// Maximum size of the MemTable before it's flushed to disk
	MemTableSize uint64

	// Whether to sync WAL writes immediately to disk
	SyncWrites bool

	// Logger for storage engine operations
	Logger model.Logger

	// Comparator for key comparison
	Comparator Comparator

	// Whether to start compaction in the background
	BackgroundCompaction bool

	// Bloom filter false positive rate
	BloomFilterFPR float64

	// Use lock-free MemTable implementation for better concurrent performance.
	// Defaults to true as of v1.x. Set to false for backward compatibility
	// with older code that may rely on mutex-based MemTable implementation.
	// See docs/MEMTABLE_IMPLEMENTATIONS.md for details.
	UseLockFreeMemTable bool

	// Use LSM-tree based node label index for better performance
	UseLSMNodeLabelIndex bool

	// Use specialized SSTable format for property indexing
	UsePropertyIndex bool

	// Time to wait before physically deleting SSTables after they're removed from the index
	// This allows ongoing read operations to complete when SSTables are being removed during compaction
	SSTableDeletionDelay time.Duration
}

// DefaultEngineConfig returns a default configuration for the storage engine
func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		DataDir:              "data",
		MemTableSize:         32 * 1024 * 1024, // 32MB
		SyncWrites:           true,
		Logger:               model.DefaultLoggerInstance,
		Comparator:           DefaultComparator,
		BackgroundCompaction: true,
		BloomFilterFPR:       0.01,             // 1% false positive rate
		UseLockFreeMemTable:  true,             // Default to lock-free MemTable for better concurrency
		UseLSMNodeLabelIndex: true,             // Default to using LSM-tree based node label index
		UsePropertyIndex:     true,             // Default to using specialized property index
		SSTableDeletionDelay: 30 * time.Second, // Default 30-second delay for SSTable deletion
	}
}

// NewStorageEngine creates a new storage engine
func NewStorageEngine(config EngineConfig) (*StorageEngine, error) {
	if config.Logger == nil {
		config.Logger = model.DefaultLoggerInstance
	}

	if config.Comparator == nil {
		config.Comparator = DefaultComparator
	}

	if config.MemTableSize == 0 {
		config.MemTableSize = DefaultEngineConfig().MemTableSize
	}

	if config.BloomFilterFPR <= 0 || config.BloomFilterFPR >= 1 {
		config.BloomFilterFPR = DefaultEngineConfig().BloomFilterFPR
	}

	// Create directory structure
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	sstableDir := filepath.Join(config.DataDir, "sstables")
	if err := os.MkdirAll(sstableDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create sstable directory: %w", err)
	}

	walDir := filepath.Join(config.DataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Create a new WAL
	walPath := filepath.Join(walDir, "kgstore.wal")
	wal, err := NewWAL(WALConfig{
		Path:        walPath,
		SyncOnWrite: config.SyncWrites,
		Logger:      config.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	// Create a new MemTable - either lock-free or standard based on config
	var memTable MemTableInterface
	if config.UseLockFreeMemTable {
		memTable = NewLockFreeMemTable(LockFreeMemTableConfig{
			MaxSize:    config.MemTableSize,
			Logger:     config.Logger,
			Comparator: config.Comparator,
		})
		config.Logger.Info("Using lock-free MemTable implementation")
	} else {
		memTable = NewMemTable(MemTableConfig{
			MaxSize:    config.MemTableSize,
			Logger:     config.Logger,
			Comparator: config.Comparator,
		})
	}

	// Create the transaction manager with WAL reference
	txManager, err := NewTransactionManager(config.DataDir, config.Logger, wal)
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to create transaction manager: %w", err)
	}

	// Create the storage engine
	engine := &StorageEngine{
		config:               config,
		memTable:             memTable,
		immMemTables:         make([]MemTableInterface, 0),
		sstables:             make([]*SSTable, 0),
		wal:                  wal,
		logger:               config.Logger,
		compactionDone:       make(chan struct{}),
		isOpen:               true,
		nextSSTableID:        1,
		leveledCompactor:     NewLeveledCompaction(),
		txManager:            txManager,
		pendingDeletions:     make(map[uint64]*SSTable),
		deletionTime:         make(map[uint64]time.Time),
		deletionExit:         make(chan struct{}),
		useLSMNodeLabelIndex: config.UseLSMNodeLabelIndex,
		usePropertyIndex:     config.UsePropertyIndex,
	}

	// Set up the condition variable for compaction
	engine.compactionCond = sync.NewCond(&engine.mu)

	// Start the deletion worker that handles the second phase of deletion
	go engine.deletionWorker()

	// Load existing SSTables
	if err := engine.loadSSTables(); err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to load SSTables: %w", err)
	}

	// Replay the WAL to recover the in-memory state
	if memTableImpl, ok := memTable.(*MemTable); ok {
		// Original implementation
		if err := wal.Replay(memTableImpl); err != nil {
			wal.Close()
			return nil, fmt.Errorf("failed to replay WAL: %w", err)
		}
	} else {
		// New lock-free implementation - we need to convert each operation
		// Currently this is limited, but would need proper implementation
		config.Logger.Warn("WAL replay for lock-free MemTable is limited - consider using standard MemTable for recovery")

		// Simple implementation to populate the MemTable
		opts := DefaultReplayOptions()
		stats, err := wal.ReplayToInterface(memTable, opts)
		if err != nil {
			wal.Close()
			return nil, fmt.Errorf("failed to replay WAL to lock-free MemTable: %w", err)
		}

		config.Logger.Info("Replayed %d of %d records from WAL to lock-free MemTable",
			stats.AppliedCount, stats.RecordCount)
	}

	// Start background compaction if enabled
	if config.BackgroundCompaction {
		go engine.compactionLoop()
	}

	engine.logger.Info("Opened storage engine in %s", config.DataDir)
	return engine, nil
}

// Close closes the storage engine and all its resources
func (e *StorageEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isOpen {
		return nil
	}

	e.isOpen = false

	// Signal compaction to stop
	close(e.compactionDone)
	e.compactionCond.Broadcast()

	// Signal deletion worker to stop
	close(e.deletionExit)

	// Flush the current MemTable to disk if it's not empty
	if e.memTable.EntryCount() > 0 {
		if err := e.flushMemTableLocked(e.memTable); err != nil {
			e.logger.Error("Failed to flush MemTable during close: %v", err)
		}
	}

	// Flush any immutable MemTables
	for _, immMemTable := range e.immMemTables {
		if err := e.flushMemTableLocked(immMemTable); err != nil {
			e.logger.Error("Failed to flush immutable MemTable during close: %v", err)
		}
	}

	// Close the WAL
	if err := e.wal.Close(); err != nil {
		e.logger.Error("Failed to close WAL: %v", err)
	}

	// Close all SSTables
	for _, sstable := range e.sstables {
		if err := sstable.Close(); err != nil {
			e.logger.Error("Failed to close SSTable: %v", err)
		}
	}

	// Immediately close any SSTables pending deletion
	e.deletionMu.Lock()
	for id, sstable := range e.pendingDeletions {
		if err := sstable.Close(); err != nil {
			e.logger.Error("Failed to close pending deletion SSTable %d: %v", id, err)
		}

		// Also delete the files since we're closing
		sstableDir := filepath.Join(e.config.DataDir, "sstables")
		dataFile := filepath.Join(sstableDir, fmt.Sprintf("%d.data", id))
		indexFile := filepath.Join(sstableDir, fmt.Sprintf("%d.index", id))
		filterFile := filepath.Join(sstableDir, fmt.Sprintf("%d.filter", id))

		for _, file := range []string{dataFile, indexFile, filterFile} {
			if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
				e.logger.Warn("Failed to delete file %s: %v", file, err)
			}
		}
	}
	e.pendingDeletions = nil
	e.deletionTime = nil
	e.deletionMu.Unlock()

	// Close the transaction manager
	if e.txManager != nil {
		if err := e.txManager.Close(); err != nil {
			e.logger.Error("Failed to close transaction manager: %v", err)
		}
	}

	e.logger.Info("Closed storage engine")
	return nil
}

// Put adds or updates a key-value pair in the storage engine
func (e *StorageEngine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isOpen {
		return ErrEngineClosed
	}

	// Create a transaction for this operation to ensure atomicity
	tx := e.txManager.Begin()
	txID := tx.id

	// Check if the MemTable is full before processing the operation
	if e.memTable.IsFull() {
		// Mark the current MemTable as immutable
		e.memTable.MarkFlushed()
		e.immMemTables = append(e.immMemTables, e.memTable)

		// Create a new MemTable - either lock-free or standard based on config
		if e.config.UseLockFreeMemTable {
			e.memTable = NewLockFreeMemTable(LockFreeMemTableConfig{
				MaxSize:    e.config.MemTableSize,
				Logger:     e.logger,
				Comparator: e.config.Comparator,
			})
		} else {
			e.memTable = NewMemTable(MemTableConfig{
				MaxSize:    e.config.MemTableSize,
				Logger:     e.logger,
				Comparator: e.config.Comparator,
			})
		}

		// Signal the compaction thread
		e.compactionCond.Signal()
	}

	// Record the operation in the WAL as part of the transaction
	if err := e.wal.RecordPutInTransaction(key, value, txID); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to record put in WAL: %w", err)
	}

	// Add the key-value pair to the MemTable
	if err := e.memTable.Put(key, value); err != nil {
		// If MemTable update fails, roll back the transaction
		tx.Rollback()
		return fmt.Errorf("failed to add key-value pair to MemTable: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Get retrieves a value by key from the storage engine
func (e *StorageEngine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.isOpen {
		return nil, ErrEngineClosed
	}

	// Try to get the value from the current MemTable
	value, err := e.memTable.Get(key)
	if err == nil {
		return value, nil
	} else if err != ErrKeyNotFound {
		return nil, err
	}

	// Try to get the value from immutable MemTables (newest to oldest)
	for i := len(e.immMemTables) - 1; i >= 0; i-- {
		value, err := e.immMemTables[i].Get(key)
		if err == nil {
			return value, nil
		} else if err != ErrKeyNotFound {
			return nil, err
		}
	}

	// Try to get the value from active SSTables (newest to oldest)
	for i := len(e.sstables) - 1; i >= 0; i-- {
		value, err := e.sstables[i].Get(key)
		if err == nil {
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
		return value, nil
	} else if err != ErrKeyNotFound {
		return nil, err
	}

	return nil, ErrKeyNotFound
}

// getFromPendingDeletionSSTables checks SSTables pending deletion for a key
// This is part of the two-phase deletion approach that ensures reads can
// still succeed during the deletion delay period
func (e *StorageEngine) getFromPendingDeletionSSTables(key []byte) ([]byte, error) {
	// Acquire deletion mutex to access pending deletion SSTables
	e.deletionMu.Lock()

	// Create a snapshot of the pending deletions to avoid long lock hold
	pendingSSTables := make([]*SSTable, 0, len(e.pendingDeletions))
	for _, sstable := range e.pendingDeletions {
		pendingSSTables = append(pendingSSTables, sstable)
	}
	e.deletionMu.Unlock()

	// Sort by ID in descending order to check newest first
	sort.Slice(pendingSSTables, func(i, j int) bool {
		return pendingSSTables[i].ID() > pendingSSTables[j].ID()
	})

	// Check each pending SSTable (newest to oldest)
	for _, sstable := range pendingSSTables {
		value, err := sstable.Get(key)
		if err == nil {
			return value, nil
		} else if err != ErrKeyNotFoundInSSTable {
			return nil, err
		}
	}

	return nil, ErrKeyNotFound
}

// Delete removes a key from the storage engine
func (e *StorageEngine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isOpen {
		return ErrEngineClosed
	}

	// Create a transaction for this operation to ensure atomicity
	tx := e.txManager.Begin()
	txID := tx.id

	// Check if the MemTable is full before processing the operation
	if e.memTable.IsFull() {
		// Mark the current MemTable as immutable
		e.memTable.MarkFlushed()
		e.immMemTables = append(e.immMemTables, e.memTable)

		// Create a new MemTable - either lock-free or standard based on config
		if e.config.UseLockFreeMemTable {
			e.memTable = NewLockFreeMemTable(LockFreeMemTableConfig{
				MaxSize:    e.config.MemTableSize,
				Logger:     e.logger,
				Comparator: e.config.Comparator,
			})
		} else {
			e.memTable = NewMemTable(MemTableConfig{
				MaxSize:    e.config.MemTableSize,
				Logger:     e.logger,
				Comparator: e.config.Comparator,
			})
		}

		// Signal the compaction thread
		e.compactionCond.Signal()
	}

	// Record the delete operation in the WAL as part of the transaction
	if err := e.wal.RecordDeleteInTransaction(key, txID); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to record delete in WAL: %w", err)
	}

	// Delete the key from the MemTable
	if err := e.memTable.Delete(key); err != nil {
		// If MemTable update fails, roll back the transaction
		tx.Rollback()
		return fmt.Errorf("failed to delete key from MemTable: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Contains checks if a key exists in the storage engine
func (e *StorageEngine) Contains(key []byte) (bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.isOpen {
		return false, ErrEngineClosed
	}

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

	// Check active SSTables (newest to oldest)
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

// containsInPendingDeletionSSTables checks if a key exists in any SSTable pending deletion
func (e *StorageEngine) containsInPendingDeletionSSTables(key []byte) (bool, error) {
	// Acquire deletion mutex to access pending deletion SSTables
	e.deletionMu.Lock()

	// Create a snapshot of the pending deletions to avoid long lock hold
	pendingSSTables := make([]*SSTable, 0, len(e.pendingDeletions))
	for _, sstable := range e.pendingDeletions {
		pendingSSTables = append(pendingSSTables, sstable)
	}
	e.deletionMu.Unlock()

	// Sort by ID in descending order to check newest first
	sort.Slice(pendingSSTables, func(i, j int) bool {
		return pendingSSTables[i].ID() > pendingSSTables[j].ID()
	})

	// Check each pending SSTable (newest to oldest)
	for _, sstable := range pendingSSTables {
		contains, err := sstable.Contains(key)
		if err != nil {
			return false, err
		}
		if contains {
			return true, nil
		}
	}

	return false, nil
}

// Flush flushes the current MemTable to disk, creating a new SSTable
func (e *StorageEngine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isOpen {
		return ErrEngineClosed
	}

	// Only flush if there are entries in the MemTable
	if e.memTable.EntryCount() > 0 {
		// Mark the current MemTable as immutable
		e.memTable.MarkFlushed()
		e.immMemTables = append(e.immMemTables, e.memTable)

		// Create a new MemTable - either lock-free or standard based on config
		if e.config.UseLockFreeMemTable {
			e.memTable = NewLockFreeMemTable(LockFreeMemTableConfig{
				MaxSize:    e.config.MemTableSize,
				Logger:     e.logger,
				Comparator: e.config.Comparator,
			})
		} else {
			e.memTable = NewMemTable(MemTableConfig{
				MaxSize:    e.config.MemTableSize,
				Logger:     e.logger,
				Comparator: e.config.Comparator,
			})
		}

		// Signal the compaction thread
		e.compactionCond.Signal()
	}

	return nil
}

// Compact triggers a manual compaction of SSTables
func (e *StorageEngine) Compact() error {
	e.logger.Debug("Starting compaction process")
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isOpen {
		e.logger.Debug("Engine is closed, cannot compact")
		return ErrEngineClosed
	}

	// First, flush all immutable MemTables
	e.logger.Debug("Flushing %d immutable MemTables", len(e.immMemTables))
	for i, immMemTable := range e.immMemTables {
		e.logger.Debug("Flushing immutable MemTable %d/%d with %d entries", 
			i+1, len(e.immMemTables), immMemTable.EntryCount())
		if err := e.flushMemTableLocked(immMemTable); err != nil {
			e.logger.Debug("Failed to flush immutable MemTable: %v", err)
			return fmt.Errorf("failed to flush immutable MemTable: %w", err)
		}
	}
	e.immMemTables = e.immMemTables[:0]

	// Update the levels in our leveledCompactor
	e.logger.Debug("Allocating SSTable levels")
	e.allocateSSTableLevels(e.leveledCompactor)

	// Print level allocation
	for i := 0; i < e.leveledCompactor.MaxLevels; i++ {
		e.logger.Debug("Level %d has %d SSTables", i, len(e.leveledCompactor.Levels[i]))
	}

	// Run full compaction on all levels
	// First compact level 0 into level 1
	if len(e.leveledCompactor.Levels[0]) > 0 {
		e.logger.Debug("Compacting level 0 with %d SSTables", len(e.leveledCompactor.Levels[0]))
		if err := e.compactLevel0(e.leveledCompactor); err != nil {
			e.logger.Debug("Failed to compact level 0: %v", err)
			return fmt.Errorf("failed to compact level 0: %w", err)
		}

		// After level 0 compaction, reallocate levels as level 0 files are now in level 1
		e.logger.Debug("Reallocating SSTable levels after level 0 compaction")
		e.allocateSSTableLevels(e.leveledCompactor)
	}

	// Compact all remaining levels sequentially
	for level := 1; level < e.leveledCompactor.MaxLevels-1; level++ {
		if len(e.leveledCompactor.Levels[level]) > 0 {
			e.logger.Debug("Compacting level %d with %d SSTables", level, len(e.leveledCompactor.Levels[level]))
			if err := e.compactLevel(e.leveledCompactor, level); err != nil {
				e.logger.Debug("Failed to compact level %d: %v", level, err)
				return fmt.Errorf("failed to compact level %d: %w", level, err)
			}

			// Reallocate levels after each level compaction
			e.logger.Debug("Reallocating SSTable levels after level %d compaction", level)
			e.allocateSSTableLevels(e.leveledCompactor)
		}
	}

	e.logger.Debug("Completed full manual compaction")
	e.logger.Info("Completed full manual compaction")
	return nil
}

// Sync ensures all pending writes are flushed to disk
func (e *StorageEngine) Sync() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isOpen {
		return ErrEngineClosed
	}

	// Sync the WAL
	if err := e.wal.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	return nil
}

// Stats returns statistics about the storage engine
func (e *StorageEngine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Reallocate SSTable levels to get current statistics
	if e.isOpen && len(e.sstables) > 0 {
		e.allocateSSTableLevels(e.leveledCompactor)
	}

	// Initialize level counts and sizes
	levelCounts := make([]int, e.leveledCompactor.MaxLevels)
	levelSizes := make([]uint64, e.leveledCompactor.MaxLevels)

	// Calculate level counts and sizes
	for level := 0; level < e.leveledCompactor.MaxLevels; level++ {
		levelCounts[level] = len(e.leveledCompactor.Levels[level])

		var levelSize uint64
		for _, sstable := range e.leveledCompactor.Levels[level] {
			levelSize += sstable.Size()
		}
		levelSizes[level] = levelSize
	}

	// Get count of pending deletions
	e.deletionMu.Lock()
	pendingDeletions := len(e.pendingDeletions)
	e.deletionMu.Unlock()

	stats := EngineStats{
		IsOpen:            e.isOpen,
		MemTableSize:      e.memTable.Size(),
		MemTableCount:     e.memTable.EntryCount(),
		ImmMemTables:      len(e.immMemTables),
		SSTables:          len(e.sstables),
		DataDirectorySize: e.getDataDirectorySizeLocked(),
		LevelCounts:       levelCounts,
		LevelSizes:        levelSizes,
		PendingDeletions:  pendingDeletions,
	}

	return stats
}

// flushMemTableLocked flushes a MemTable to disk, creating a new SSTable
// Caller must hold the lock
func (e *StorageEngine) flushMemTableLocked(memTable MemTableInterface) error {
	if memTable.EntryCount() == 0 {
		e.logger.Debug("Skipping flush of empty MemTable")
		return nil
	}

	// Begin a transaction for the flush operation
	tx := e.txManager.Begin()

	// Create a new SSTable from the MemTable
	sstableDir := filepath.Join(e.config.DataDir, "sstables")
	sstConfig := SSTableConfig{
		ID:         e.nextSSTableID,
		Path:       sstableDir,
		Logger:     e.logger,
		Comparator: e.config.Comparator,
	}

	sstable, err := CreateSSTable(sstConfig, memTable)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to create SSTable: %w", err)
	}

	// Add the 'add' operation to the transaction
	tx.AddOperation(TransactionOperation{
		Type:   "add",
		Target: "sstable",
		ID:     sstable.ID(),
	})

	// Prepare in-memory update
	newSSTables := make([]*SSTable, len(e.sstables)+1)
	copy(newSSTables, e.sstables)
	newSSTables[len(newSSTables)-1] = sstable

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		// On commit failure, we should close the new SSTable to release resources
		sstable.Close()
		return fmt.Errorf("failed to commit flush transaction: %w", err)
	}

	// After successful commit, update in-memory state
	e.sstables = newSSTables
	e.nextSSTableID++

	e.logger.Info("Flushed MemTable to SSTable %d with %d keys", sstable.ID(), sstable.KeyCount())
	return nil
}

// loadSSTables loads existing SSTables from disk
// Caller must hold the lock
func (e *StorageEngine) loadSSTables() error {
	sstableDir := filepath.Join(e.config.DataDir, "sstables")

	// Check if the directory exists
	if _, err := os.Stat(sstableDir); os.IsNotExist(err) {
		return nil // No SSTables yet
	}

	// List all SSTable data files
	entries, err := os.ReadDir(sstableDir)
	if err != nil {
		return fmt.Errorf("failed to read sstable directory: %w", err)
	}

	// Track the max ID to set nextSSTableID
	maxID := uint64(0)

	// Open each SSTable
	for _, entry := range entries {
		// Only consider data files
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".data" {
			// Extract the ID from the filename (e.g., "1.data" -> 1)
			var id uint64
			_, err := fmt.Sscanf(entry.Name(), "%d.data", &id)
			if err != nil {
				e.logger.Warn("Ignoring invalid SSTable filename: %s", entry.Name())
				continue
			}

			// Open the SSTable
			sstConfig := SSTableConfig{
				ID:         id,
				Path:       sstableDir,
				Logger:     e.logger,
				Comparator: e.config.Comparator,
			}

			sstable, err := OpenSSTable(sstConfig)
			if err != nil {
				e.logger.Warn("Failed to open SSTable %d: %v", id, err)
				continue
			}

			// Add the SSTable to the list
			e.sstables = append(e.sstables, sstable)

			// Update the max ID
			if id > maxID {
				maxID = id
			}
		}
	}

	// Sort SSTables by ID (oldest to newest)
	sort.Slice(e.sstables, func(i, j int) bool {
		return e.sstables[i].ID() < e.sstables[j].ID()
	})

	// Set the next SSTable ID
	e.nextSSTableID = maxID + 1

	e.logger.Info("Loaded %d SSTables", len(e.sstables))
	return nil
}

// compactionLoop runs in the background to periodically compact SSTables
func (e *StorageEngine) compactionLoop() {
	for {
		e.mu.Lock()

		// Wait for a signal to compact or for the engine to close
		for e.isOpen && len(e.immMemTables) == 0 && len(e.sstables) < 2 {
			// Use timed wait to periodically check for compaction
			waitChan := make(chan struct{})
			timer := time.NewTimer(10 * time.Minute)

			go func() {
				select {
				case <-timer.C:
					e.compactionCond.Signal()
				case <-e.compactionDone:
					return
				}
				close(waitChan)
			}()

			e.compactionCond.Wait()

			timer.Stop()
			select {
			case <-waitChan:
				// Timer fired or we were signaled
			default:
				// We were woken up by something else
			}
		}

		// Check if we should exit
		if !e.isOpen {
			e.mu.Unlock()
			return
		}

		// Flush immutable MemTables to Level 0 SSTables
		for _, immMemTable := range e.immMemTables {
			err := e.flushMemTableLocked(immMemTable)
			if err != nil {
				e.logger.Error("Failed to flush immutable MemTable: %v", err)
			}
		}
		e.immMemTables = e.immMemTables[:0]

		// Run compaction on all levels using the leveled compaction strategy
		if len(e.sstables) >= 2 {
			// Update the levels in our leveledCompactor
			e.allocateSSTableLevels(e.leveledCompactor)

			// Check if level 0 needs compaction (too many files)
			if len(e.leveledCompactor.Levels[0]) >= e.leveledCompactor.Level0Size {
				err := e.compactLevel0(e.leveledCompactor)
				if err != nil {
					e.logger.Error("Failed to compact level 0: %v", err)
				}
			}

			// Check other levels for compaction based on size
			for level := 1; level < e.leveledCompactor.MaxLevels-1; level++ {
				// Only compact if there are files in this level
				if len(e.leveledCompactor.Levels[level]) > 0 {
					// Calculate the target size for this level
					baseLevelSize := uint64(10 * 1024 * 1024) // 10MB
					targetSize := baseLevelSize
					for i := 0; i < level-1; i++ {
						targetSize *= uint64(e.leveledCompactor.SizeRatio)
					}

					// Calculate the current level size
					var currentLevelSize uint64
					for _, sstable := range e.leveledCompactor.Levels[level] {
						currentLevelSize += sstable.Size()
					}

					// Compact if the level is too large
					if currentLevelSize > targetSize {
						err := e.compactLevel(e.leveledCompactor, level)
						if err != nil {
							e.logger.Error("Failed to compact level %d: %v", level, err)
						}
					}
				}
			}
		}

		e.mu.Unlock()

		// Sleep for a short while to prevent excessive CPU usage
		time.Sleep(1 * time.Second)
	}
}

// LeveledCompaction represents a leveled compaction strategy to reduce write amplification
// by organizing SSTables into multiple levels with different characteristics
type LeveledCompaction struct {
	// Levels is a slice of SSTable slices, where each slice represents a level
	Levels [][]*SSTable

	// MaxLevels is the maximum number of levels (typically 7)
	MaxLevels int

	// Level0Size is the maximum number of files in level 0
	Level0Size int

	// SizeRatio is the size ratio between adjacent levels (typically 10)
	// Level L+1 is SizeRatio times larger than level L
	SizeRatio int
}

// NewLeveledCompaction creates a new leveled compaction strategy
func NewLeveledCompaction() *LeveledCompaction {
	maxLevels := 7 // Common default
	levels := make([][]*SSTable, maxLevels)
	for i := 0; i < maxLevels; i++ {
		levels[i] = make([]*SSTable, 0)
	}

	return &LeveledCompaction{
		Levels:     levels,
		MaxLevels:  maxLevels,
		Level0Size: 4,  // Max number of SSTables in level 0 before compaction
		SizeRatio:  10, // Size ratio between adjacent levels
	}
}

// compactLocked performs compaction of SSTables using a leveled approach
// Caller must hold the lock
func (e *StorageEngine) compactLocked() error {
	if len(e.sstables) < 2 {
		return nil // Nothing to compact
	}

	// Initialize the leveled compaction if we don't have one yet
	leveledCompaction := NewLeveledCompaction()

	// Allocate existing SSTables into levels
	e.allocateSSTableLevels(leveledCompaction)

	// Check if level 0 needs compaction (has too many files)
	level0Files := leveledCompaction.Levels[0]
	if len(level0Files) >= leveledCompaction.Level0Size {
		// Compact level 0 into level 1
		if err := e.compactLevel0(leveledCompaction); err != nil {
			return fmt.Errorf("failed to compact level 0: %w", err)
		}
	}

	// Check and compact other levels as needed
	for level := 1; level < leveledCompaction.MaxLevels-1; level++ {
		// Calculate the target size for this level
		// Level 1 target size is typically ~10MB * SizeRatio^(level-1)
		baseLevelSize := uint64(10 * 1024 * 1024) // 10MB
		targetSize := baseLevelSize
		for i := 0; i < level-1; i++ {
			targetSize *= uint64(leveledCompaction.SizeRatio)
		}

		// Calculate the current level size
		var currentLevelSize uint64
		for _, sstable := range leveledCompaction.Levels[level] {
			currentLevelSize += sstable.Size()
		}

		// Compact if the level is too large
		if currentLevelSize > targetSize {
			if err := e.compactLevel(leveledCompaction, level); err != nil {
				return fmt.Errorf("failed to compact level %d: %w", level, err)
			}
		}
	}

	return nil
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// allocateSSTableLevels allocates existing SSTables into levels
func (e *StorageEngine) allocateSSTableLevels(lc *LeveledCompaction) {
	e.logger.Debug("Allocating %d SSTables into levels", len(e.sstables))
	
	// Clear current levels
	for i := 0; i < lc.MaxLevels; i++ {
		lc.Levels[i] = make([]*SSTable, 0)
	}

	// Sort SSTables by ID (creation time)
	sort.Slice(e.sstables, func(i, j int) bool {
		return e.sstables[i].ID() < e.sstables[j].ID()
	})

	if e.logger.IsLevelEnabled(model.LogLevelDebug) {
		e.logger.Debug("Sorted SSTable IDs:")
		for i, sst := range e.sstables {
			e.logger.Debug("  SSTable[%d]: ID=%d", i, sst.ID())
		}
	}

	// Simple allocation based on creation time
	// Newer SSTables go to level 0, older ones to higher levels
	// In a real implementation, this would be more sophisticated based on key ranges

	// First 4 tables (or all if less than 4) go to level 0
	level0Count := min(len(e.sstables), lc.Level0Size)
	startIdx := len(e.sstables) - level0Count
	lc.Levels[0] = append(lc.Levels[0], e.sstables[startIdx:]...)
	e.logger.Debug("Allocated %d newest SSTables to level 0", level0Count)

	// Remainder get distributed to higher levels
	// Initially we'll put all older tables in level 1 for simplicity
	if startIdx > 0 {
		lc.Levels[1] = append(lc.Levels[1], e.sstables[:startIdx]...)
		e.logger.Debug("Allocated %d older SSTables to level 1", startIdx)
	}
	
	if e.logger.IsLevelEnabled(model.LogLevelDebug) {
		e.logger.Debug("Final level allocation:")
		for i := 0; i < lc.MaxLevels; i++ {
			if len(lc.Levels[i]) > 0 {
				// Build a string of SSTable IDs
				var idStr string
				for j, sst := range lc.Levels[i] {
					if j > 0 {
						idStr += ", "
					}
					idStr += fmt.Sprintf("%d", sst.ID())
				}
				e.logger.Debug("  Level %d: %d SSTables (IDs: %s)", i, len(lc.Levels[i]), idStr)
			} else {
				e.logger.Debug("  Level %d: empty", i)
			}
		}
	}
}

// compactLevel0 compacts level 0 SSTables into level 1
func (e *StorageEngine) compactLevel0(lc *LeveledCompaction) error {
	// Log the compaction operation
	e.logger.Debug("Starting compaction of level 0")
	e.logger.Info("Starting compaction of level 0 with %d files", len(lc.Levels[0]))

	// Create a new MemTable to merge level 0 SSTables - either lock-free or standard based on config
	var mergedMemTable MemTableInterface
	if e.config.UseLockFreeMemTable {
		e.logger.Debug("Creating lock-free MemTable for compaction")
		mergedMemTable = NewLockFreeMemTable(LockFreeMemTableConfig{
			MaxSize:    e.config.MemTableSize * 100, // Much larger for compaction
			Logger:     e.logger,
			Comparator: e.config.Comparator,
		})
	} else {
		e.logger.Debug("Creating standard MemTable for compaction")
		mergedMemTable = NewMemTable(MemTableConfig{
			MaxSize:    e.config.MemTableSize * 100, // Much larger for compaction
			Logger:     e.logger,
			Comparator: e.config.Comparator,
		})
	}

	e.logger.Debug("Reading key-value pairs from level 0 SSTables")
	// Read all key-value pairs from level 0 SSTables (newest to oldest for correct overwrite semantics)
	// Level 0 has overlapping key ranges, so we need to process newer files first
	for i := len(lc.Levels[0]) - 1; i >= 0; i-- {
		sstable := lc.Levels[0][i]
		e.logger.Debug("Processing level 0 SSTable %d (ID: %d)", i, sstable.ID())
		iter, err := sstable.Iterator()
		if err != nil {
			e.logger.Debug("Failed to create iterator for SSTable %d: %v", sstable.ID(), err)
			e.logger.Error("Failed to create iterator for SSTable %d: %v", sstable.ID(), err)
			continue
		}

		var keyCount int
		// Iterate through all key-value pairs
		for iter.Valid() {
			key := iter.Key()
			value := iter.Value()
			keyCount++

			// Add to merged MemTable (newer values will overwrite older ones)
			if err := mergedMemTable.Put(key, value); err != nil {
				e.logger.Debug("Failed to add key-value pair to merged MemTable: %v", err)
				e.logger.Error("Failed to add key-value pair to merged MemTable: %v", err)
			}

			if err := iter.Next(); err != nil {
				e.logger.Debug("Failed to advance iterator: %v", err)
				e.logger.Error("Failed to advance iterator: %v", err)
				break
			}
			
			// Log progress occasionally
			if keyCount%1000 == 0 && e.logger.IsLevelEnabled(model.LogLevelDebug) {
				e.logger.Debug("Processed %d keys from SSTable %d", keyCount, sstable.ID())
			}
		}

		e.logger.Debug("Finished processing %d keys from level 0 SSTable %d", keyCount, sstable.ID())
		iter.Close()
	}

	e.logger.Debug("Merged MemTable now contains %d entries", mergedMemTable.EntryCount())
	e.logger.Debug("Processing level 1 SSTables")
	// Now merge with any overlapping files from level 1
	// In level 1, files should have non-overlapping key ranges, but during the initial
	// implementation we'll just merge all level 1 files for simplicity
	for i, sstable := range lc.Levels[1] {
		e.logger.Debug("Processing level 1 SSTable %d (ID: %d)", i, sstable.ID())
		iter, err := sstable.Iterator()
		if err != nil {
			e.logger.Debug("Failed to create iterator for SSTable %d: %v", sstable.ID(), err)
			e.logger.Error("Failed to create iterator for SSTable %d: %v", sstable.ID(), err)
			continue
		}

		var keyCount int
		// Iterate through all key-value pairs
		for iter.Valid() {
			key := iter.Key()
			// For level 1, we need to consider version information
			// Read the value and potentially its version from the SSTable
			value := iter.Value()
			keyCount++

			// In current implementation, we can't easily get version from the iterator
			// So we'll add the key only if it doesn't exist in the merged MemTable (implying level 0 takes precedence)
			// This works for now because level 0 files are always newer than level 1 files
			// In a full implementation with versioned records, we would compare versions
			// and take the record with the higher version
			if !mergedMemTable.Contains(key) {
				if err := mergedMemTable.Put(key, value); err != nil {
					e.logger.Debug("Failed to add key-value pair from level 1 to merged MemTable: %v", err)
					e.logger.Error("Failed to add key-value pair from level 1 to merged MemTable: %v", err)
				}
			}

			if err := iter.Next(); err != nil {
				e.logger.Debug("Failed to advance iterator: %v", err)
				e.logger.Error("Failed to advance iterator: %v", err)
				break
			}
			
			// Log progress occasionally
			if keyCount%1000 == 0 && e.logger.IsLevelEnabled(model.LogLevelDebug) {
				e.logger.Debug("Processed %d keys from level 1 SSTable %d", keyCount, sstable.ID())
			}
		}

		e.logger.Debug("Finished processing %d keys from level 1 SSTable %d", keyCount, sstable.ID())
		iter.Close()
	}

	e.logger.Debug("Merged MemTable now contains %d entries total", mergedMemTable.EntryCount())
	e.logger.Debug("Beginning transaction for compaction")
	// Begin a transaction for the compaction operation
	tx := e.txManager.Begin()
	e.logger.Debug("Started transaction %d", tx.id)

	// Create a new SSTable from the merged MemTable
	sstableDir := filepath.Join(e.config.DataDir, "sstables")
	sstConfig := SSTableConfig{
		ID:         e.nextSSTableID,
		Path:       sstableDir,
		Logger:     e.logger,
		Comparator: e.config.Comparator,
	}

	e.logger.Debug("Creating new SSTable with ID %d", e.nextSSTableID)
	mergedSSTable, err := CreateSSTable(sstConfig, mergedMemTable)
	if err != nil {
		e.logger.Debug("Failed to create merged SSTable: %v", err)
		tx.Rollback()
		return fmt.Errorf("failed to create merged SSTable: %w", err)
	}

	// Add the 'add' operation to the transaction
	e.logger.Debug("Adding 'add' operation for SSTable %d to transaction", mergedSSTable.ID())
	tx.AddOperation(TransactionOperation{
		Type:   "add",
		Target: "sstable",
		ID:     mergedSSTable.ID(),
	})

	// Add 'remove' operations for all SSTables that will be removed
	e.logger.Debug("Adding 'remove' operations for %d SSTables to transaction", 
		len(lc.Levels[0])+len(lc.Levels[1]))
	allSSTablesToRemove := append(lc.Levels[0], lc.Levels[1]...)
	oldIDs := make([]uint64, 0, len(allSSTablesToRemove))
	for _, sstable := range allSSTablesToRemove {
		oldIDs = append(oldIDs, sstable.ID())
		tx.AddOperation(TransactionOperation{
			Type:   "remove",
			Target: "sstable",
			ID:     sstable.ID(),
		})
	}

	e.logger.Debug("Updating in-memory state")
	// Update the in-memory state (this happens before commit to ensure consistency)
	newSSTables := make([]*SSTable, 0, len(e.sstables)-len(allSSTablesToRemove)+1)
	for _, sstable := range e.sstables {
		isInCompactedLevels := false
		for _, oldSSTable := range allSSTablesToRemove {
			if sstable.ID() == oldSSTable.ID() {
				isInCompactedLevels = true
				break
			}
		}
		if !isInCompactedLevels {
			newSSTables = append(newSSTables, sstable)
		}
	}

	// Add the new SSTable to our in-memory list
	newSSTables = append(newSSTables, mergedSSTable)

	e.logger.Debug("Committing transaction")
	// Commit the transaction
	if err := tx.Commit(); err != nil {
		// On commit failure, we should close the new SSTable to release resources
		e.logger.Debug("Failed to commit compaction transaction: %v", err)
		mergedSSTable.Close()
		return fmt.Errorf("failed to commit compaction transaction: %w", err)
	}

	e.logger.Debug("Transaction committed successfully")
	// After successful commit, update in-memory state
	e.sstables = newSSTables
	e.nextSSTableID++

	e.logger.Debug("Marking old SSTables for deletion")
	// Mark old SSTables for deletion instead of immediately closing them
	// This allows ongoing reads to complete even after compaction finishes
	for _, sstable := range allSSTablesToRemove {
		e.markSSTableForDeletion(sstable)
	}

	e.logger.Debug("Level 0 compaction complete, created SSTable %d with %d keys", 
		mergedSSTable.ID(), mergedSSTable.KeyCount())
	e.logger.Info("Compacted %d SSTables (IDs: %v) into new SSTable %d with %d keys",
		len(oldIDs), oldIDs, mergedSSTable.ID(), mergedSSTable.KeyCount())
	return nil
}

// compactLevel compacts a specific level (other than level 0) into the next level
func (e *StorageEngine) compactLevel(lc *LeveledCompaction, level int) error {
	if level <= 0 || level >= lc.MaxLevels-1 {
		return fmt.Errorf("invalid level for compaction: %d", level)
	}

	// In a full implementation, we would select a subset of SSTables in this level
	// that have overlapping key ranges with the next level
	// For simplicity, we'll just compact all files in this level with all files in the next level

	e.logger.Info("Starting compaction of level %d with %d files", level, len(lc.Levels[level]))

	// Create a new MemTable to merge SSTables - either lock-free or standard based on config
	var mergedMemTable MemTableInterface
	if e.config.UseLockFreeMemTable {
		mergedMemTable = NewLockFreeMemTable(LockFreeMemTableConfig{
			MaxSize:    e.config.MemTableSize * 100, // Much larger for compaction
			Logger:     e.logger,
			Comparator: e.config.Comparator,
		})
	} else {
		mergedMemTable = NewMemTable(MemTableConfig{
			MaxSize:    e.config.MemTableSize * 100, // Much larger for compaction
			Logger:     e.logger,
			Comparator: e.config.Comparator,
		})
	}

	// First read all key-value pairs from the current level
	for _, sstable := range lc.Levels[level] {
		iter, err := sstable.Iterator()
		if err != nil {
			e.logger.Error("Failed to create iterator for SSTable %d: %v", sstable.ID(), err)
			continue
		}

		// Iterate through all key-value pairs
		for iter.Valid() {
			key := iter.Key()
			value := iter.Value()

			// Add to merged MemTable
			if err := mergedMemTable.Put(key, value); err != nil {
				e.logger.Error("Failed to add key-value pair to merged MemTable: %v", err)
			}

			if err := iter.Next(); err != nil {
				e.logger.Error("Failed to advance iterator: %v", err)
				break
			}
		}

		iter.Close()
	}

	// Now merge with any overlapping files from the next level
	for _, sstable := range lc.Levels[level+1] {
		iter, err := sstable.Iterator()
		if err != nil {
			e.logger.Error("Failed to create iterator for SSTable %d: %v", sstable.ID(), err)
			continue
		}

		// Iterate through all key-value pairs
		for iter.Valid() {
			key := iter.Key()
			// Read the value from the SSTable in the next level
			value := iter.Value()

			// With versioned records, we would compare versions and take the one with the higher version
			// Currently, we just check existence since level N files are guaranteed to be older than level N-1
			// In a future implementation, we'll use explicit version comparisons from each SSTable
			// This would prevent newer records from being overwritten during compaction
			if !mergedMemTable.Contains(key) {
				if err := mergedMemTable.Put(key, value); err != nil {
					e.logger.Error("Failed to add key-value pair from level %d to merged MemTable: %v", level+1, err)
				}
			}

			if err := iter.Next(); err != nil {
				e.logger.Error("Failed to advance iterator: %v", err)
				break
			}
		}

		iter.Close()
	}

	// Begin a transaction for the compaction operation
	tx := e.txManager.Begin()

	// Create a new SSTable from the merged MemTable
	sstableDir := filepath.Join(e.config.DataDir, "sstables")
	sstConfig := SSTableConfig{
		ID:         e.nextSSTableID,
		Path:       sstableDir,
		Logger:     e.logger,
		Comparator: e.config.Comparator,
	}

	mergedSSTable, err := CreateSSTable(sstConfig, mergedMemTable)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to create merged SSTable: %w", err)
	}

	// Add the 'add' operation to the transaction
	tx.AddOperation(TransactionOperation{
		Type:   "add",
		Target: "sstable",
		ID:     mergedSSTable.ID(),
	})

	// Add 'remove' operations for all SSTables that will be removed
	allSSTablesToRemove := append(lc.Levels[level], lc.Levels[level+1]...)
	oldIDs := make([]uint64, 0, len(allSSTablesToRemove))
	for _, sstable := range allSSTablesToRemove {
		oldIDs = append(oldIDs, sstable.ID())
		tx.AddOperation(TransactionOperation{
			Type:   "remove",
			Target: "sstable",
			ID:     sstable.ID(),
		})
	}

	// Update the in-memory state (prepare the new state)
	newSSTables := make([]*SSTable, 0, len(e.sstables)-len(allSSTablesToRemove)+1)
	for _, sstable := range e.sstables {
		isInCompactedLevels := false
		for _, oldSSTable := range allSSTablesToRemove {
			if sstable.ID() == oldSSTable.ID() {
				isInCompactedLevels = true
				break
			}
		}
		if !isInCompactedLevels {
			newSSTables = append(newSSTables, sstable)
		}
	}

	// Add the new SSTable to our in-memory list
	newSSTables = append(newSSTables, mergedSSTable)

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		// On commit failure, we should close the new SSTable to release resources
		mergedSSTable.Close()
		return fmt.Errorf("failed to commit compaction transaction: %w", err)
	}

	// After successful commit, update in-memory state
	e.sstables = newSSTables
	e.nextSSTableID++

	// Mark old SSTables for deletion instead of immediately closing them
	// This allows ongoing reads to complete even after compaction finishes
	for _, sstable := range allSSTablesToRemove {
		e.markSSTableForDeletion(sstable)
	}

	// In a full implementation, we might want to check if the resulting compaction
	// makes the next level too large, and trigger another compaction if needed

	e.logger.Info("Compacted %d SSTables from levels %d and %d (IDs: %v) into new SSTable %d with %d keys",
		len(oldIDs), level, level+1, oldIDs, mergedSSTable.ID(), mergedSSTable.KeyCount())
	return nil
}

// getDataDirectorySizeLocked calculates the total size of the data directory
// Caller must hold the lock
func (e *StorageEngine) getDataDirectorySizeLocked() int64 {
	var totalSize int64

	// Walk through all files in the data directory
	filepath.Walk(e.config.DataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})

	return totalSize
}

// EngineStats contains statistics about the storage engine
type EngineStats struct {
	IsOpen            bool     // Whether the engine is open
	MemTableSize      uint64   // Size of the current MemTable in bytes
	MemTableCount     int      // Number of entries in the current MemTable
	ImmMemTables      int      // Number of immutable MemTables
	SSTables          int      // Number of SSTables
	DataDirectorySize int64    // Total size of the data directory in bytes
	LevelCounts       []int    // Number of SSTables at each level
	LevelSizes        []uint64 // Size of each level in bytes
	PendingDeletions  int      // Number of SSTables pending deletion
}

// deletionWorker runs in the background to handle the second phase of SSTable deletion
// It periodically checks for SSTables that have been pending deletion for longer than
// the configured delay and physically deletes them
func (e *StorageEngine) deletionWorker() {
	// For fast detection, use an interval that's 1/4 of the deletion delay or 5 seconds, whichever is smaller
	checkInterval := e.config.SSTableDeletionDelay / 4
	if checkInterval > 5*time.Second || checkInterval <= 0 {
		checkInterval = 5 * time.Second // Default: check every 5 seconds
	}

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	e.logger.Debug("Starting deletion worker with interval: %v", checkInterval)

	for {
		select {
		case <-ticker.C:
			e.CleanupPendingDeletions()
		case <-e.deletionExit:
			return // Exit signal received
		}
	}
}

// CleanupPendingDeletions checks for SSTables that have been marked for deletion
// and removes them if they've been pending for longer than the configured delay
// This is exported for testing purposes.
func (e *StorageEngine) CleanupPendingDeletions() {
	e.deletionMu.Lock()
	defer e.deletionMu.Unlock()

	// Skip if no pending deletions
	if len(e.pendingDeletions) == 0 {
		return
	}

	now := time.Now()
	var toDelete []uint64

	// Find SSTables that have been pending deletion long enough
	for id, timestamp := range e.deletionTime {
		elapsed := now.Sub(timestamp)
		if elapsed >= e.config.SSTableDeletionDelay {
			toDelete = append(toDelete, id)
			e.logger.Debug("SSTable %d eligible for deletion (waited %v of %v delay)",
				id, elapsed, e.config.SSTableDeletionDelay)
		} else {
			e.logger.Debug("SSTable %d not yet eligible for deletion (waited %v of %v delay)",
				id, elapsed, e.config.SSTableDeletionDelay)
		}
	}

	if len(toDelete) == 0 {
		e.logger.Debug("No SSTables eligible for physical deletion yet")
		return
	}

	e.logger.Debug("Found %d SSTables eligible for physical deletion", len(toDelete))

	// Delete them one by one
	for _, id := range toDelete {
		sstable, exists := e.pendingDeletions[id]
		if !exists {
			e.logger.Warn("SSTable %d was marked for deletion but not in pendingDeletions map", id)
			continue
		}

		// Close the SSTable
		if err := sstable.Close(); err != nil {
			e.logger.Error("Failed to close SSTable %d during cleanup: %v", id, err)
			continue
		}

		// Delete the SSTable files
		sstableDir := filepath.Join(e.config.DataDir, "sstables")
		dataFile := filepath.Join(sstableDir, fmt.Sprintf("%d.data", id))
		indexFile := filepath.Join(sstableDir, fmt.Sprintf("%d.index", id))
		filterFile := filepath.Join(sstableDir, fmt.Sprintf("%d.filter", id))

		for _, file := range []string{dataFile, indexFile, filterFile} {
			if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
				e.logger.Warn("Failed to delete file %s: %v", file, err)
			}
		}

		// Remove from our tracking maps
		delete(e.pendingDeletions, id)
		delete(e.deletionTime, id)

		e.logger.Info("Physically deleted SSTable %d after deletion delay", id)
	}
}

// markSSTableForDeletion adds an SSTable to the pending deletion queue (phase 1)
// This logically deletes the SSTable while allowing ongoing reads to complete
func (e *StorageEngine) markSSTableForDeletion(sstable *SSTable) {
	id := sstable.ID()
	e.logger.Debug("Marking SSTable %d for deletion", id)
	
	// Set a timeout to make sure we don't hang on mutex acquisition
	acquired := make(chan bool, 1)
	go func() {
		e.deletionMu.Lock()
		acquired <- true
	}()
	
	select {
	case <-acquired:
		// Continue with normal operation now that we have the lock
		defer e.deletionMu.Unlock()
		
		// Add to pending deletions if not already there
		if _, exists := e.pendingDeletions[id]; !exists {
			e.pendingDeletions[id] = sstable
			e.deletionTime[id] = time.Now()
			e.logger.Debug("SSTable %d marked for deletion with delay %v", id, e.config.SSTableDeletionDelay)
		} else {
			e.logger.Debug("SSTable %d already marked for deletion", id)
		}
	case <-time.After(5 * time.Second):
		e.logger.Warn("Timeout waiting to acquire deletionMu for SSTable %d deletion", id)
		// Don't take any action, just log the warning
	}
}
