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
	mu             sync.RWMutex
	config         EngineConfig
	memTable       *MemTable        // Current MemTable for writes
	immMemTables   []*MemTable      // Immutable MemTables waiting to be flushed
	sstables       []*SSTable       // Sorted String Tables (persistent storage)
	wal            *WAL             // Write-Ahead Log
	logger         model.Logger     // Logger for storage engine operations
	compactionCond *sync.Cond       // Condition variable for signaling compaction
	compactionDone chan struct{}    // Channel for signaling compaction is done
	isOpen         bool             // Whether the storage engine is open
	nextSSTableID  uint64           // Next SSTable ID to use
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

	// Create the storage engine
	engine := &StorageEngine{
		config:         config,
		memTable:       memTable,
		immMemTables:   make([]*MemTable, 0),
		sstables:       make([]*SSTable, 0),
		wal:            wal,
		logger:         config.Logger,
		compactionDone: make(chan struct{}),
		isOpen:         true,
		nextSSTableID:  1,
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

	// Perform compaction on all SSTables
	if err := e.compactLocked(); err != nil {
		return fmt.Errorf("failed to compact SSTables: %w", err)
	}

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

	stats := EngineStats{
		IsOpen:         e.isOpen,
		MemTableSize:   e.memTable.Size(),
		MemTableCount:  e.memTable.EntryCount(),
		ImmMemTables:   len(e.immMemTables),
		SSTables:       len(e.sstables),
		DataDirectorySize: e.getDataDirectorySizeLocked(),
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
		return fmt.Errorf("failed to create SSTable: %w", err)
	}

	// Add the SSTable to the list
	e.sstables = append(e.sstables, sstable)
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
		for !e.isOpen && len(e.immMemTables) == 0 && len(e.sstables) < 2 {
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
		
		// Flush immutable MemTables
		for _, immMemTable := range e.immMemTables {
			err := e.flushMemTableLocked(immMemTable)
			if err != nil {
				e.logger.Error("Failed to flush immutable MemTable: %v", err)
			}
		}
		e.immMemTables = e.immMemTables[:0]
		
		// Compact SSTables if there are at least 2
		if len(e.sstables) >= 2 {
			err := e.compactLocked()
			if err != nil {
				e.logger.Error("Failed to compact SSTables: %v", err)
			}
		}
		
		e.mu.Unlock()
		
		// Sleep for a short while to prevent excessive CPU usage
		time.Sleep(1 * time.Second)
	}
}

// compactLocked performs compaction of SSTables
// Caller must hold the lock
func (e *StorageEngine) compactLocked() error {
	if len(e.sstables) < 2 {
		return nil // Nothing to compact
	}
	
	// Simple strategy: compact all SSTables into a single one
	// In a real implementation, we would use a level-based approach
	
	// Create a new MemTable to merge all SSTables
	mergedMemTable := NewMemTable(MemTableConfig{
		MaxSize:    e.config.MemTableSize * 100, // Much larger for compaction
		Logger:     e.logger,
		Comparator: e.config.Comparator,
	})
	
	// Read all key-value pairs from all SSTables (oldest to newest)
	for _, sstable := range e.sstables {
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
		return fmt.Errorf("failed to create merged SSTable: %w", err)
	}
	
	// Close and remove the old SSTables
	oldIDs := make([]uint64, 0, len(e.sstables))
	for _, sstable := range e.sstables {
		oldIDs = append(oldIDs, sstable.ID())
		sstable.Close()
		
		// Remove the files
		sstableDir := filepath.Join(e.config.DataDir, "sstables")
		dataFile := filepath.Join(sstableDir, fmt.Sprintf("%d.data", sstable.ID()))
		indexFile := filepath.Join(sstableDir, fmt.Sprintf("%d.index", sstable.ID()))
		filterFile := filepath.Join(sstableDir, fmt.Sprintf("%d.filter", sstable.ID()))
		
		os.Remove(dataFile)
		os.Remove(indexFile)
		os.Remove(filterFile)
	}
	
	// Update the SSTable list
	e.sstables = []*SSTable{mergedSSTable}
	e.nextSSTableID++
	
	// Truncate the WAL since all data is now in SSTables
	if err := e.wal.Truncate(); err != nil {
		e.logger.Error("Failed to truncate WAL after compaction: %v", err)
	}
	
	e.logger.Info("Compacted %d SSTables (IDs: %v) into new SSTable %d with %d keys", 
		len(oldIDs), oldIDs, mergedSSTable.ID(), mergedSSTable.KeyCount())
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
	IsOpen            bool  // Whether the engine is open
	MemTableSize      uint64 // Size of the current MemTable in bytes
	MemTableCount     int    // Number of entries in the current MemTable
	ImmMemTables      int    // Number of immutable MemTables
	SSTables          int    // Number of SSTables
	DataDirectorySize int64  // Total size of the data directory in bytes
}