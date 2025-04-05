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
	mu               sync.RWMutex
	config           EngineConfig
	memTable         *MemTable        // Current MemTable for writes
	immMemTables     []*MemTable      // Immutable MemTables waiting to be flushed
	sstables         []*SSTable       // Sorted String Tables (persistent storage)
	wal              *WAL             // Write-Ahead Log
	logger           model.Logger     // Logger for storage engine operations
	compactionCond   *sync.Cond       // Condition variable for signaling compaction
	compactionDone   chan struct{}    // Channel for signaling compaction is done
	isOpen           bool             // Whether the storage engine is open
	nextSSTableID    uint64           // Next SSTable ID to use
	leveledCompactor *LeveledCompaction // Manages the leveled compaction strategy
	txManager        *TransactionManager // Transaction manager for atomic operations
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
}

// DefaultEngineConfig returns a default configuration for the storage engine
func DefaultEngineConfig() EngineConfig {
	return EngineConfig{
		DataDir:             "data",
		MemTableSize:        32 * 1024 * 1024, // 32MB
		SyncWrites:          true,
		Logger:              model.DefaultLoggerInstance,
		Comparator:          DefaultComparator,
		BackgroundCompaction: true,
		BloomFilterFPR:      0.01, // 1% false positive rate
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

	// Create a new MemTable
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    config.MemTableSize,
		Logger:     config.Logger,
		Comparator: config.Comparator,
	})

	// Create the transaction manager
	txManager, err := NewTransactionManager(config.DataDir, config.Logger)
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to create transaction manager: %w", err)
	}

	// Create the storage engine
	engine := &StorageEngine{
		config:           config,
		memTable:         memTable,
		immMemTables:     make([]*MemTable, 0),
		sstables:         make([]*SSTable, 0),
		wal:              wal,
		logger:           config.Logger,
		compactionDone:   make(chan struct{}),
		isOpen:           true,
		nextSSTableID:    1,
		leveledCompactor: NewLeveledCompaction(),
		txManager:        txManager,
	}

	// Set up the condition variable for compaction
	engine.compactionCond = sync.NewCond(&engine.mu)

	// Load existing SSTables
	if err := engine.loadSSTables(); err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to load SSTables: %w", err)
	}

	// Replay the WAL to recover the in-memory state
	if err := wal.Replay(memTable); err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
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

	// Record the operation in the WAL
	if err := e.wal.RecordPut(key, value); err != nil {
		return fmt.Errorf("failed to record put in WAL: %w", err)
	}

	// Check if the MemTable is full
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

	// Add the key-value pair to the MemTable
	if err := e.memTable.Put(key, value); err != nil {
		return fmt.Errorf("failed to add key-value pair to MemTable: %w", err)
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

	// Try to get the value from SSTables (newest to oldest)
	for i := len(e.sstables) - 1; i >= 0; i-- {
		value, err := e.sstables[i].Get(key)
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

	// Record the delete operation in the WAL
	if err := e.wal.RecordDelete(key); err != nil {
		return fmt.Errorf("failed to record delete in WAL: %w", err)
	}

	// Check if the MemTable is full
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

	// Delete the key from the MemTable
	if err := e.memTable.Delete(key); err != nil {
		return fmt.Errorf("failed to delete key from MemTable: %w", err)
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

		// Create a new MemTable
		e.memTable = NewMemTable(MemTableConfig{
			MaxSize:    e.config.MemTableSize,
			Logger:     e.logger,
			Comparator: e.config.Comparator,
		})

		// Signal the compaction thread
		e.compactionCond.Signal()
	}

	return nil
}

// Compact triggers a manual compaction of SSTables
func (e *StorageEngine) Compact() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isOpen {
		return ErrEngineClosed
	}

	// First, flush all immutable MemTables
	for _, immMemTable := range e.immMemTables {
		if err := e.flushMemTableLocked(immMemTable); err != nil {
			return fmt.Errorf("failed to flush immutable MemTable: %w", err)
		}
	}
	e.immMemTables = e.immMemTables[:0]

	// Update the levels in our leveledCompactor
	e.allocateSSTableLevels(e.leveledCompactor)

	// Run full compaction on all levels
	// First compact level 0 into level 1
	if len(e.leveledCompactor.Levels[0]) > 0 {
		if err := e.compactLevel0(e.leveledCompactor); err != nil {
			return fmt.Errorf("failed to compact level 0: %w", err)
		}
		
		// After level 0 compaction, reallocate levels as level 0 files are now in level 1
		e.allocateSSTableLevels(e.leveledCompactor)
	}

	// Compact all remaining levels sequentially
	for level := 1; level < e.leveledCompactor.MaxLevels-1; level++ {
		if len(e.leveledCompactor.Levels[level]) > 0 {
			if err := e.compactLevel(e.leveledCompactor, level); err != nil {
				return fmt.Errorf("failed to compact level %d: %w", level, err)
			}
			
			// Reallocate levels after each level compaction
			e.allocateSSTableLevels(e.leveledCompactor)
		}
	}

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

	stats := EngineStats{
		IsOpen:            e.isOpen,
		MemTableSize:      e.memTable.Size(),
		MemTableCount:     e.memTable.EntryCount(),
		ImmMemTables:      len(e.immMemTables),
		SSTables:          len(e.sstables),
		DataDirectorySize: e.getDataDirectorySizeLocked(),
		LevelCounts:       levelCounts,
		LevelSizes:        levelSizes,
	}

	return stats
}

// flushMemTableLocked flushes a MemTable to disk, creating a new SSTable
// Caller must hold the lock
func (e *StorageEngine) flushMemTableLocked(memTable *MemTable) error {
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
		Level0Size: 4,      // Max number of SSTables in level 0 before compaction
		SizeRatio:  10,     // Size ratio between adjacent levels
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
	// Clear current levels
	for i := 0; i < lc.MaxLevels; i++ {
		lc.Levels[i] = make([]*SSTable, 0)
	}
	
	// Sort SSTables by ID (creation time)
	sort.Slice(e.sstables, func(i, j int) bool {
		return e.sstables[i].ID() < e.sstables[j].ID()
	})
	
	// Simple allocation based on creation time
	// Newer SSTables go to level 0, older ones to higher levels
	// In a real implementation, this would be more sophisticated based on key ranges
	
	// First 4 tables (or all if less than 4) go to level 0
	level0Count := min(len(e.sstables), lc.Level0Size)
	startIdx := len(e.sstables) - level0Count
	lc.Levels[0] = append(lc.Levels[0], e.sstables[startIdx:]...)
	
	// Remainder get distributed to higher levels
	// Initially we'll put all older tables in level 1 for simplicity
	if startIdx > 0 {
		lc.Levels[1] = append(lc.Levels[1], e.sstables[:startIdx]...)
	}
}

// compactLevel0 compacts level 0 SSTables into level 1
func (e *StorageEngine) compactLevel0(lc *LeveledCompaction) error {
	// Log the compaction operation
	e.logger.Info("Starting compaction of level 0 with %d files", len(lc.Levels[0]))
	
	// Create a new MemTable to merge level 0 SSTables
	mergedMemTable := NewMemTable(MemTableConfig{
		MaxSize:    e.config.MemTableSize * 100, // Much larger for compaction
		Logger:     e.logger,
		Comparator: e.config.Comparator,
	})
	
	// Read all key-value pairs from level 0 SSTables (newest to oldest for correct overwrite semantics)
	// Level 0 has overlapping key ranges, so we need to process newer files first
	for i := len(lc.Levels[0]) - 1; i >= 0; i-- {
		sstable := lc.Levels[0][i]
		iter, err := sstable.Iterator()
		if err != nil {
			e.logger.Error("Failed to create iterator for SSTable %d: %v", sstable.ID(), err)
			continue
		}
		
		// Iterate through all key-value pairs
		for iter.Valid() {
			key := iter.Key()
			value := iter.Value()
			
			// Add to merged MemTable (newer values will overwrite older ones)
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
	
	// Now merge with any overlapping files from level 1
	// In level 1, files should have non-overlapping key ranges, but during the initial
	// implementation we'll just merge all level 1 files for simplicity
	for _, sstable := range lc.Levels[1] {
		iter, err := sstable.Iterator()
		if err != nil {
			e.logger.Error("Failed to create iterator for SSTable %d: %v", sstable.ID(), err)
			continue
		}
		
		// Iterate through all key-value pairs
		for iter.Valid() {
			key := iter.Key()
			// Check if the key from level 1 doesn't exist in the merged MemTable
			// If it doesn't exist, we add it; otherwise, level 0 value takes precedence
			if !mergedMemTable.Contains(key) {
				value := iter.Value()
				if err := mergedMemTable.Put(key, value); err != nil {
					e.logger.Error("Failed to add key-value pair from level 1 to merged MemTable: %v", err)
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
	
	// Update the in-memory state (this happens before commit to ensure consistency)
	newSSTables := make([]*SSTable, 0, len(e.sstables) - len(allSSTablesToRemove) + 1)
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
	
	// Close old SSTables since they've been removed
	for _, sstable := range allSSTablesToRemove {
		sstable.Close()
	}
	
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
	
	// Create a new MemTable to merge SSTables
	mergedMemTable := NewMemTable(MemTableConfig{
		MaxSize:    e.config.MemTableSize * 100, // Much larger for compaction
		Logger:     e.logger,
		Comparator: e.config.Comparator,
	})
	
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
			// Check if the key from next level doesn't exist in the merged MemTable
			// If it doesn't exist, we add it; otherwise, current level value takes precedence
			if !mergedMemTable.Contains(key) {
				value := iter.Value()
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
	newSSTables := make([]*SSTable, 0, len(e.sstables) - len(allSSTablesToRemove) + 1)
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
	
	// Close old SSTables since they've been removed
	for _, sstable := range allSSTablesToRemove {
		sstable.Close()
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
}