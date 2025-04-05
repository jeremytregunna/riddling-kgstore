# KGStore Architecture

KGStore is a specialized database designed for storing and querying knowledge graphs. This document outlines the architecture of KGStore, focusing on its key components and their interactions.

## Overview

KGStore is a single-file database for knowledge graphs that uses:
- Skip list-based in-memory tables and sorted string tables for data organization
- Page-based storage for efficient I/O
- Log-structured merge (LSM) tree for high write throughput
- Write-Ahead Logging (WAL) for durability and crash recovery
- Bloom filters for efficient key lookups

The database is built with a focus on:
- Fast graph traversals
- Efficient property lookups
- ACID (Atomicity, Consistency, Isolation, Durability) compliance
- High write throughput
- Low read latency

## Core Components

### 1. Data Model

#### Nodes

Nodes represent entities in the knowledge graph:
- Each node has a unique ID
- Nodes have a label (type/category)
- Nodes can have key-value properties

```go
type Node struct {
    ID         uint64            // Unique identifier for the node
    Label      string            // Type or category of the node
    Properties map[string]string // Key-value pairs of node properties
}
```

#### Edges

Edges represent relationships between nodes:
- Each edge connects two nodes (source and target)
- Edges have a label (relationship type)
- Edges can have key-value properties

```go
type Edge struct {
    SourceID   uint64            // ID of the source node
    TargetID   uint64            // ID of the target node
    Label      string            // Type or category of the relationship
    Properties map[string]string // Key-value pairs of edge properties
}
```

#### Pages

The database uses a page-based storage system:
- Pages are fixed-size blocks of data (the fundamental unit of I/O)
- Each page has a unique ID
- Pages can be flagged as dirty, pinned, or indexed
- Pages are used for both data and index structures

```go
type Page struct {
    ID    uint64   // Unique identifier for the page
    Data  []byte   // Raw data stored in the page
    Flags PageFlag // Status flags for the page
}
```

### 2. Storage Engine

#### MemTable

The MemTable is an in-memory data structure that buffers recent writes:
- Implemented as a skip list (O(log n) operations)
- Sorted key-value store
- Thread-safe with mutex protection
- Size-bounded (configurable max size)
- When full, becomes immutable and is flushed to disk as an SSTable

```go
type MemTable struct {
    head           *skipNode     // Pointer to the head node
    maxHeight      int           // Maximum height of skip list
    currentHeight  int           // Current height of the skip list
    size           uint64        // Total size in bytes
    count          int           // Number of entries
    maxSize        uint64        // Maximum size in bytes
    isFlushed      bool          // Whether the MemTable has been flushed
    comparator     Comparator    // Function for comparing keys
    currentVersion uint64        // Version counter for tracking changes
}
```

#### SSTables (Sorted String Tables)

SSTables are immutable on-disk files containing sorted key-value pairs:
- Each SSTable consists of:
  - Data file: contains actual key-value pairs
  - Index file: enables binary search lookup
  - Bloom filter file: enables quick non-membership tests
- SSTables are organized in a level-based structure for efficient compaction
- Format includes a header with metadata (magic number, version, key count, min/max keys)

```go
type SSTable struct {
    id         uint64           // Unique identifier for this SSTable
    path       string           // Path to the SSTable directory
    dataFile   string           // Path to the data file
    indexFile  string           // Path to the index file
    filterFile string           // Path to the bloom filter file
    keyCount   uint32           // Number of key-value pairs
    dataSize   uint64           // Size of the data file in bytes
    minKey     []byte           // Minimum key in the SSTable
    maxKey     []byte           // Maximum key in the SSTable
    indexCache map[string]uint64 // Cache for index lookups
}
```

#### WAL (Write-Ahead Log)

The WAL provides durability and crash recovery:
- Records all write operations before they are applied to the MemTable
- Each record includes:
  - Record type (PUT or DELETE)
  - Key and value
  - Timestamp
  - CRC32 checksum for integrity
- During recovery, the WAL is replayed to restore the state of the MemTable

```go
type WALRecord struct {
    Type      RecordType
    Key       []byte
    Value     []byte
    Timestamp int64
}
```

#### Bloom Filter

Bloom filters provide efficient membership tests for SSTables:
- Probabilistic data structure that allows for fast non-membership checks
- False positives are possible but false negatives are not
- Configurable false positive rate
- Used to quickly skip SSTables that don't contain a key

```go
type BloomFilter struct {
    bits       []byte   // The bit array
    size       uint64   // The size of the bit array in bits
    hashFuncs  uint64   // The number of hash functions
    expectedN  uint64   // The expected number of elements
    insertions uint64   // The number of elements inserted
}
```

### 3. Compaction Strategy

KGStore uses a leveled compaction strategy to manage SSTables:
- SSTables are organized into multiple levels (typically 7 levels)
- Level 0 contains newest SSTables, which may have overlapping key ranges
- Higher levels contain older SSTables with non-overlapping key ranges
- When level N gets too large, some SSTables are merged with level N+1
- Each level is exponentially larger than the previous (typically 10x)

This approach:
- Reduces write amplification
- Spreads out I/O load
- Improves read performance by reducing the number of SSTables to search

### 4. Query Engine

The query engine provides operations for traversing the knowledge graph:

#### Executor

Executes parsed queries against the storage engine:
- Handles various query types (find nodes/edges by label, find neighbors, find paths)
- Utilizes indexes for efficient lookups
- Returns structured results

#### Optimizer

Optimizes query plans for better performance:
- Reorders operations to minimize intermediate results
- Chooses the most efficient indexes
- Applies query-specific optimizations

#### Traversal

Handles graph traversal operations:
- Breadth-first and depth-first traversals
- Path finding between nodes
- Neighborhood exploration

## Data Flow

### Read Path

1. When a key is requested, the system checks:
   - Current MemTable
   - Immutable MemTables (newest to oldest)
   - SSTables (newest to oldest at each level)
2. For SSTable lookups:
   - Bloom filters are checked first to quickly skip SSTables
   - Binary search is used on the index file to find the key's position
   - The data file is accessed at the specific offset to retrieve the value

### Write Path

1. Each write operation (put/delete) is:
   - Recorded in the WAL
   - Applied to the current MemTable
2. When the MemTable reaches its size limit:
   - It is marked as immutable
   - A new MemTable is created for future writes
   - The immutable MemTable is scheduled for flushing
3. Flushing converts an immutable MemTable to an SSTable:
   - Key-value pairs are written to a new SSTable
   - The SSTable is added to Level 0
4. Compaction periodically merges SSTables:
   - Level 0 SSTables are merged into Level 1
   - If a level exceeds its size threshold, SSTables are merged into the next level
   - Merging resolves duplicate keys (newer values override older ones)

### Recovery Path

1. During database startup:
   - Existing SSTables are loaded
   - The WAL is replayed to recover recent operations
   - Any immutable MemTables are flushed to disk

## Performance Considerations

### Write Optimization

- Log-structured approach provides high write throughput
- In-memory MemTable for fast writes
- Batched disk I/O through SSTable creation
- Write amplification is minimized through careful compaction

### Read Optimization

- Skip list MemTable for O(log n) lookups
- Bloom filters to quickly skip irrelevant SSTables
- Binary search on SSTable indexes
- Level-based organization to reduce the number of SSTables to search
- MemTable and index caching for frequently accessed data

### Space Optimization

- Compaction removes duplicate and deleted keys
- Leveled structure balances space usage and compaction frequency
- Configurable bloom filter false positive rates to balance space and performance

## Conclusion

KGStore's architecture combines several proven database techniques with specialized optimizations for knowledge graph operations. The LSM tree structure provides excellent write performance, while the various indexing strategies ensure efficient reads and traversals. The combination of WAL and carefully structured SSTables provides durability and reliability.