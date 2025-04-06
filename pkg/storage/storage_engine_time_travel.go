package storage

import (
	"errors"
	"fmt"
	"time"
)

// TimeTravelError defines errors specific to time-travel operations
var (
	ErrInvalidTimestamp = errors.New("invalid timestamp for time-travel operation")
	ErrTimestampTooOld  = errors.New("timestamp is beyond retention period")
)

// GetAsOf retrieves a value as it existed at the specified timestamp
func (e *StorageEngine) GetAsOf(key []byte, timestamp int64) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}

	if timestamp <= 0 {
		return nil, ErrInvalidTimestamp
	}

	if timestamp > time.Now().UnixNano() {
		// Future timestamp doesn't make sense, adjust to current time
		timestamp = time.Now().UnixNano()
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// Check memtable first (only for current records, not historical)
	// We only check the current memtable if the timestamp is current (within a small window)
	currentTime := time.Now().UnixNano()
	if currentTime-timestamp < int64(time.Second*1) {
		value, err := e.memTable.Get(key)
		if err == nil {
			return value, nil
		} else if err != ErrKeyNotFound {
			return nil, err
		}
	}

	// Check immutable memtables in reverse order (newest to oldest)
	// Only check memtables created before or at the requested timestamp
	for i := len(e.immMemTables) - 1; i >= 0; i-- {
		imm := e.immMemTables[i]
		
		// Skip memtables created after the requested timestamp
		// Note: This assumes memtables have a CreationTime() method, which would need to be added
		// For now, we'll check all immutable memtables without timestamp filtering
		
		value, err := imm.Get(key)
		if err == nil {
			return value, nil
		} else if err != ErrKeyNotFound {
			return nil, err
		}
	}

	// Check active SSTables
	// Only consider SSTables created before or at the target timestamp
	for i := len(e.sstables) - 1; i >= 0; i-- {
		sst := e.sstables[i]
		
		// Skip SSTables created after the target timestamp
		if sst.timestamp > timestamp {
			continue
		}
		
		// Check if the key is potentially in this SSTable's range
		if sst.minKey != nil && e.config.Comparator(key, sst.minKey) < 0 {
			continue
		}
		if sst.maxKey != nil && e.config.Comparator(key, sst.maxKey) > 0 {
			continue
		}

		// Look up the key in this SSTable
		value, err := sst.Get(key)
		if err == nil {
			return value, nil
		} else if err != ErrKeyNotFoundInSSTable {
			return nil, err
		}
	}

	// Finally, check pending deletion SSTables that still exist
	// This is important for time-travel queries during the deletion delay window
	e.deletionMu.Lock()
	pendingSSTables := make([]*SSTable, 0, len(e.pendingDeletions))
	for _, sst := range e.pendingDeletions {
		if sst.timestamp <= timestamp {
			pendingSSTables = append(pendingSSTables, sst)
		}
	}
	e.deletionMu.Unlock()

	// Check each pending deletion SSTable
	for _, sst := range pendingSSTables {
		// Check if key is in range
		if sst.minKey != nil && e.config.Comparator(key, sst.minKey) < 0 {
			continue
		}
		if sst.maxKey != nil && e.config.Comparator(key, sst.maxKey) > 0 {
			continue
		}

		// Try to get the value
		value, err := sst.Get(key)
		if err == nil {
			return value, nil
		} else if err != ErrKeyNotFoundInSSTable {
			e.logger.Warn("Error reading from pending deletion SSTable: %v", err)
		}
	}

	return nil, ErrKeyNotFound
}

// PointInTimeIterator defines an iterator that represents data at a specific point in time
type PointInTimeIterator struct {
	engine    *StorageEngine
	timestamp int64
	// Implementation details would include iterators for active and pending SSTables
	// filtered by the target timestamp
}

// ScanAsOf returns an iterator for scanning data as it existed at the specified timestamp
func (e *StorageEngine) ScanAsOf(startKey, endKey []byte, timestamp int64) (*PointInTimeIterator, error) {
	if timestamp <= 0 {
		return nil, ErrInvalidTimestamp
	}

	if timestamp > time.Now().UnixNano() {
		// Future timestamp doesn't make sense, adjust to current time
		timestamp = time.Now().UnixNano()
	}

	// Create a point-in-time iterator that will only access appropriate SSTables
	// Implementation would be similar to MergeIterator but with timestamp filtering
	// For a complete implementation, we'd include iterators for all data sources:
	// 1. Current memtable (if timestamp is very recent)
	// 2. Immutable memtables created before timestamp
	// 3. SSTables created before timestamp
	// 4. Pending deletion SSTables created before timestamp
	
	// This is a placeholder for the full implementation
	iterator := &PointInTimeIterator{
		engine:    e,
		timestamp: timestamp,
	}
	
	return iterator, nil
}

// RecoverToPointInTime restores the database to its state at the specified timestamp
// This would create a new StorageEngine instance representing the state at that time
func (e *StorageEngine) RecoverToPointInTime(timestamp int64, targetDir string) (*StorageEngine, error) {
	if timestamp <= 0 {
		return nil, ErrInvalidTimestamp
	}

	if timestamp > time.Now().UnixNano() {
		// Future timestamp doesn't make sense, adjust to current time
		timestamp = time.Now().UnixNano()
	}
	
	// A real implementation would:
	// 1. Create a new storage engine at the target directory
	// 2. Copy relevant SSTable files (those created before the timestamp)
	// 3. Replay the WAL up to the target timestamp
	// 4. Open the new storage engine
	
	// This placeholder returns an error as the full implementation 
	// would be complex and require careful handling of files
	return nil, fmt.Errorf("recovery to point in time not fully implemented")
}