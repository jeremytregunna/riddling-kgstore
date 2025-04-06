# SSTable Implementation in KGStore

This document provides a comprehensive overview of the Sorted String Table (SSTable) implementation in KGStore, including isolation mechanisms and time-travel capabilities.

## Core SSTable Architecture

### Overview

SSTables are immutable, persistent data structures that store sorted key-value pairs on disk. In KGStore, they are the primary persistent storage mechanism and form the backbone of the LSM-tree architecture.

### Implementation Status

The SSTable implementation in KGStore is mature and includes:

✅ **Fully Implemented**:
- Immutable on-disk data structure with data, index, and bloom filter files
- Two-phase deletion mechanism for read isolation
- Creation timestamp tracking for point-in-time queries
- Bloom filters for efficient negative lookups
- Atomic creation process for durability
- Key range metadata for optimization
- Leveled compaction for reducing write amplification

⚠️ **Partially Implemented**:
- Time-travel queries (basic implementation exists)
- Point-in-time recovery (API defined but not fully implemented)

### Key Components

1. **Data Structure**:
   - `SSTable`: Main struct representing an on-disk sorted table
   - `SSTableHeader`: Metadata stored at the beginning of each data file
   - `SSTableConfig`: Configuration for creating new SSTables
   - `CreateSSTOptions`: Options controlling SSTable creation behavior

2. **File Organization**:
   - **Data file** (*.data): Contains the serialized key-value pairs and header
   - **Index file** (*.index): Contains key offsets for efficient binary search
   - **Bloom filter file** (*.filter): Contains a bloom filter for rapid existence checks

3. **Key Operations**:
   - `CreateSSTable`: Creates a new SSTable from a MemTable
   - `Get`: Retrieves a value by key
   - `Contains`: Checks if a key exists (optimized with bloom filter)
   - `IteratorWithOptions`: Creates an iterator for range scans

## Two-Phase Deletion Mechanism

### Problem Addressed

In a concurrent environment, immediately deleting SSTables after compaction can lead to read failures when:
1. A read operation acquires a reference to an SSTable
2. The SSTable is compacted and deleted before the read completes
3. The read operation fails when trying to access the now-deleted file

### Implementation

The two-phase deletion approach provides read isolation:

#### Phase 1: Logical Deletion
- SSTable is removed from the active in-memory list (`e.sstables`)
- SSTable is added to a pending deletion queue (`e.pendingDeletions`)
- A timestamp is recorded for when the SSTable was marked for deletion
- The SSTable remains fully operational for ongoing reads

#### Phase 2: Physical Deletion
- After a configurable delay period (default: 30 seconds)
- The SSTable is physically closed and its files are deleted from disk
- This occurs only after enough time has passed for existing reads to complete

### Key Components

```go
// In StorageEngine struct
deletionMu       sync.Mutex            // Mutex for deletion operations
pendingDeletions map[uint64]*SSTable   // SSTables pending deletion (phase 1)
deletionTime     map[uint64]time.Time  // When each SSTable was marked for deletion
deletionExit     chan struct{}         // Signal deletion worker to exit

// Configuration
SSTableDeletionDelay time.Duration     // Time to wait before physical deletion
```

### Key Functions

1. `markSSTableForDeletion`: Initiates phase 1 (logical deletion)
2. `deletionWorker`: Background goroutine that manages the deletion process
3. `CleanupPendingDeletions`: Implements phase 2 (physical deletion) after the delay period
4. `getFromPendingDeletionSSTables`: Checks pending deletion SSTables during reads

## Time-Travel Capabilities

### Overview

KGStore implements time-travel capabilities that allow querying data as it existed at a specific point in time, building on the SSTable implementation.

### Key Features

1. **SSTable Timestamps**:
   - Each SSTable records its creation timestamp
   - Used to filter SSTables based on the target timestamp for time-travel queries

   ```go
   // In SSTable struct
   timestamp int64 // Creation timestamp for this SSTable 

   // In SSTableHeader
   Timestamp int64 // Creation timestamp of the SSTable
   ```

2. **Time-Travel Query API**:
   - `GetAsOf(key []byte, timestamp int64)`: Gets a value as it existed at the specified time
   - `ScanAsOf(startKey, endKey []byte, timestamp int64)`: Range scan as of a specific time
   - `RecoverToPointInTime(timestamp int64, targetDir string)`: API for recovery (not fully implemented)

3. **Implementation Strategy**:
   - Queries filter SSTables based on their creation timestamp
   - Only SSTables created before or at the target timestamp are considered
   - Leverages two-phase deletion to keep historical SSTables accessible
   - Uses versioning within SSTables for optimal results

## Compaction Strategy

KGStore uses a leveled compaction strategy to organize SSTables, similar to LevelDB and RocksDB:

1. **Level Structure**:
   - Level 0: Contains newest SSTables with potential key overlaps
   - Level 1-N: Contain progressively older data with no key overlaps within a level

2. **Compaction Triggers**:
   - Level 0: When it contains too many files (typically 4+)
   - Other levels: When their size exceeds a target threshold

3. **Implementation**:
   - `LeveledCompaction` struct manages the level organization
   - `compactLevel0` and `compactLevel` perform the actual compaction
   - `allocateSSTableLevels` assigns SSTables to appropriate levels

## Implementation Details

### SSTable Creation Process

```go
// Create an SSTable from a MemTable
func CreateSSTableWithOptions(config SSTableConfig, memTable MemTableInterface, options CreateSSTOptions) (*SSTable, error) {
    // 1. Create temporary files
    // 2. Write header (with timestamp)
    // 3. Write sorted key-value pairs
    // 4. Build index and bloom filter
    // 5. Atomically move to final location
    // ...
}
```

### Atomic Creation for Durability

1. Files are first created in a temporary directory
2. All data is written and flushed to disk
3. Files are atomically moved to their final locations
4. This ensures that crashes during SSTable creation don't leave corrupted files

### Key Range Optimization

Each SSTable tracks its minimum and maximum keys, allowing quick filtering during queries:

```go
// Check if a key could be in this SSTable
if sst.minKey != nil && e.config.Comparator(key, sst.minKey) < 0 {
    // Key is before SSTable's range, skip it
    continue
}
if sst.maxKey != nil && e.config.Comparator(key, sst.maxKey) > 0 {
    // Key is after SSTable's range, skip it
    continue
}
```

## Future Enhancements

1. **Complete Point-in-Time Recovery**:
   - Implement the `RecoverToPointInTime` function fully
   - Add ability to clone a database at a specific point in time

2. **Advanced Time-Travel Functionality**:
   - Historical data retention policies
   - Auto-archiving of older SSTables
   - Differential backups based on timestamps

3. **Enhanced Iterator Support**:
   - Implement `PointInTimeIterator` fully for efficient range scans
   - Support timestamp-based filters at all levels

4. **Optimization Opportunities**:
   - SSTable partitioning for very large datasets
   - Prefetching for sequential workloads
   - Column-family support for multi-dimensional data

## Conclusion

The SSTable implementation in KGStore provides a robust foundation for persistent storage with isolation guarantees. The two-phase deletion mechanism ensures read consistency even during compaction, while the timestamp-based architecture enables powerful time-travel capabilities for both queries and recovery.

This design balances performance, durability, and advanced functionality while maintaining the simplicity ethos of the KGStore project as a SQLite-like graph database.