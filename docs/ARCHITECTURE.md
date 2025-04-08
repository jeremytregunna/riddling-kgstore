# KGStore Architecture

This document provides a comprehensive overview of KGStore's architecture for developers who are building or extending the system. It describes the core components, design principles, data flows, and implementation details.

## Core Design Principles

KGStore is a high-performance, embedded knowledge graph database with a focus on durability and consistency. The database is built on a Log-Structured Merge (LSM) tree architecture optimized for knowledge graphs with these key design principles:

1. **Performance**: Optimized for both high write throughput and low-latency reads
2. **Durability**: Write-Ahead Logging ensures no data loss even during crashes
3. **Consistency**: ACID transaction support maintains data integrity
4. **Flexibility**: Support for various graph data models and query patterns
5. **Scalability**: Designed to handle large graphs with efficient memory usage
6. **Extensibility**: Modular architecture allows for easy component replacement

## Core Components

### 1. Data Model

The foundation of KGStore is its graph data model consisting of:

#### Nodes
```go
type Node struct {
    ID         uint64            // Unique identifier
    Label      string            // Type or category
    Properties map[string]string // Key-value attributes
}
```

#### Edges
```go
type Edge struct {
    SourceID   uint64            // Origin node
    TargetID   uint64            // Destination node
    Label      string            // Relationship type
    Properties map[string]string // Key-value attributes
}
```

#### Properties
```go
type Property struct {
    Key   string // Property name
    Value string // Property value (type detection handled internally)
}
```

#### Pages
```go
type Page struct {
    ID    uint64 // Unique identifier
    Data  []byte // Raw binary data
    Flags uint32 // Status flags (dirty, pinned, etc.)
}
```

### 2. Storage Layer

#### MemTable
The in-memory buffer for recent writes:
- Implemented as a skip list (O(log n) operations)
- Thread-safe with proper concurrency control
- Configurable size limits
- Becomes immutable after reaching size threshold

Two implementations are available:
1. **Standard MemTable**: Uses mutex-based concurrency control
2. **Lock-Free MemTable**: Uses atomic operations for higher concurrency

#### SSTables (Sorted String Tables)
Immutable on-disk files for persistent storage:
- Format: `{header}{index entries}{data blocks}`
- Each SSTable includes bloom filter for fast negatives
- Organized in levels (L0, L1, ..., L6) with increasing size
- Key format: `{prefix}:{entityID}`

#### Write-Ahead Log (WAL)
Ensures durability by recording changes before they're applied:
- Record format: `{checksum}{length}{type}{timestamp}{txID}{key}{value}`
- Transaction support (begin, commit, rollback)
- Recovery modes for handling corrupted or incomplete transactions
- xxHash64 for integrity verification

### 3. Indexing System

#### Primary Indexes
- Node ID → Node data
- Edge source+target → Edge data

#### Secondary Indexes
- Node label → List of node IDs
- Edge label → List of edge IDs
- Property name+value → List of entity IDs (specialized property index)

#### Property Index
Specialized index for efficient property lookups:
- Composite key format: `{prefix}:{propertyName}:{valueType}:{propertyValue}:{entityID}`
- Automatic type detection (string, numeric, boolean)
- Support for prefix scans and exact lookups
- Integrated with the LSM tree structure

### 4. Query Engine

#### Parser
Converts textual queries into structured query objects:
```go
type Query struct {
    Type       QueryType              // Operation type
    Parameters map[string]interface{} // Query parameters
}
```

#### Optimizer
Improves query execution plans:
- Reorders operations to minimize intermediate results
- Selects optimal indexes based on query predicates
- Applies query-specific optimizations

#### Executor
Runs queries against the storage engine:
- Traverses the graph based on query requirements
- Builds result sets from matched patterns
- Handles transaction context

#### Traversal
Implements graph traversal algorithms:
- Breadth-first search
- Depth-first search
- Pathfinding with distance limits

### 5. Transaction Management

#### Transaction Registry
Maintains active transactions:
- Assigns unique transaction IDs
- Tracks operation sequence
- Ensures proper isolation

#### Recovery Manager
Handles database recovery after crashes:
- Replays WAL records
- Applies only valid committed transactions
- Ignores or rolls back incomplete transactions

## Data Flow

### Write Path
1. Client issues write operation
2. Operation is assigned to a transaction (auto or explicit)
3. Transaction writes record to the WAL
4. On WAL success, operation is applied to the MemTable
5. Periodically, full MemTables are flushed to new SSTables
6. Background process periodically compacts SSTables

### Read Path
1. Client issues read operation
2. Query is parsed and optimized
3. Executor searches for matches in this order:
   - Current MemTable
   - Immutable MemTables
   - SSTables (using bloom filters to skip irrelevant files)
4. Results are collected, filtered, and returned

### Transaction Commit Path
1. Client issues commit operation
2. WAL records commit marker
3. Transaction is marked as committed
4. Changes become visible to other transactions

## Implementation Details

### Concurrency Control

KGStore provides two MemTable implementations:

#### Standard MemTable
- Uses mutex for thread safety
- Simple and predictable behavior
- Lower contention in small-scale deployments

#### Lock-Free MemTable
- Uses atomic operations for concurrent access
- Significantly higher throughput for multi-threaded workloads
- Up to 4x performance improvement for read-heavy scenarios

### Compaction Strategy

KGStore implements a leveled compaction strategy:
1. Level 0: Contains newest SSTables, may have overlapping keys
2. Levels 1-6: Each level is ~10x larger than previous level
3. Compaction merges overlapping SSTables from level N to N+1
4. Uses two-phase deletion to avoid disrupting ongoing operations

### Page Management

Page-based storage approach:
1. Fixed-size units of disk I/O (typically 4KB)
2. Page allocator tracks page usage
3. Page cache improves read performance
4. Dirty page tracking for efficient flushing

### Versioning System

Version control for concurrent operations:
1. Each operation gets a monotonically increasing version
2. Transactions get version ranges
3. Read operations see consistent snapshot based on version

## Performance Considerations

### Memory Management
- Configurable MemTable size limits
- Bloom filter size/accuracy tradeoffs
- Index caching policies
- Page buffer pool size

### Disk I/O Optimization
- Batched writes through SSTable creation
- Sequential writes for WAL and SSTables
- Configurable fsync frequency (durability vs. performance)
- Compression options for SSTables

### CPU Efficiency
- Lock-free data structures where appropriate
- Batch processing for compaction
- Optimized serialization/deserialization
- Proper use of Go's concurrency primitives

## Extension Points

KGStore is designed with modularity in mind, offering several extension points:

1. **Custom Indexes**: The index interface allows for specialized indexes
2. **Alternative MemTable Implementations**: The MemTable interface supports different implementations
3. **Compaction Strategies**: The compaction process can be customized
4. **Query Language Extensions**: The parser and executor can be extended for new query types
5. **Custom Serialization**: The serialization framework supports different formats

## Testing Strategy

KGStore employs a comprehensive testing approach:

1. **Unit Tests**: For individual components
2. **Integration Tests**: For component interactions
3. **End-to-End Tests**: For complete workflows
4. **Performance Benchmarks**: For optimization verification
5. **Durability Tests**: For crash recovery validation
6. **Consistency Tests**: For transaction behavior validation

## Roadmap and Future Enhancements

See the TODO.md file for a detailed roadmap, but key planned improvements include:

1. **Advanced Property Indexing**: Range queries, full-text search
2. **Enhanced ACID Compliance**: Stronger isolation guarantees
3. **Performance Optimizations**: Query caching, parallel execution
4. **Data Compression**: Reduced storage requirements
5. **Columnar Storage**: For property-focused queries

## Directory Structure

KGStore follows a standard Go project layout:

- `/pkg`: Core packages
  - `/model`: Data structures and serialization
  - `/storage`: Storage engine (MemTable, SSTable, WAL)
  - `/query`: Query processing and execution
  - `/server`: Network interfaces
- `/cmd`: Command-line applications
  - `/server`: gRPC server
  - `/client`: Command-line client
- `/docs`: Detailed documentation
- `/scripts`: Utility scripts for testing and deployment

## Conclusion

KGStore combines established database techniques with specialized graph optimizations to create a high-performance knowledge graph storage system. The LSM tree architecture provides excellent write throughput, while the various indexing strategies ensure efficient reads and traversals. The combination of WAL and carefully structured SSTables delivers durability and reliability.

This architecture document serves as a guide for developers working on or extending KGStore. For user-focused documentation, please refer to the README.md file.