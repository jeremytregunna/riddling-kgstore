# KGStore

KGStore is a high-performance, embedded knowledge graph database with a focus on durability and consistency. It provides a flexible foundation for applications requiring graph-based data storage with strong transactional guarantees.

## Overview

KGStore is designed as a single-file database format optimized for knowledge graphs, featuring:

- **Node and Edge Storage**: First-class support for graph elements
- **Property Storage**: Flexible property attachment to nodes and edges
- **ACID Transactions**: Strong consistency guarantees
- **Write-Ahead Logging**: Durability even in case of crashes
- **SSTable Storage**: Efficient, immutable data files with compaction
- **Query Support**: Graph traversal and querying capabilities
- **Property Indexing**: Specialized indexing for efficient property lookups
- **Lock-Free Data Structures**: High-performance concurrent operations

## Storage Architecture

KGStore uses a Log-Structured Merge (LSM) tree architecture for high write throughput:
- In-memory MemTable buffers recent writes
- Immutable SSTables for persistent storage
- Background compaction to merge SSTables
- Bloom filters for efficient key lookups
- Page-based storage for efficient I/O

## Durability and Performance

KGStore provides configurable durability options to balance write performance with data safety:

### `syncOnWrite` Configuration

The `syncOnWrite` option (configured as `SyncWrites` in the engine) determines whether data is immediately flushed to disk after write operations:

- **Default: `true`** - Ensures maximum durability by calling fsync after writes
- **Performance mode: `false`** - Improves write performance but risks data loss in case of system crash

#### Durability Guarantees

| Configuration | Guarantee | Use Case |
|---------------|-----------|----------|
| `syncOnWrite: true` | Data is durable after each write operation | Critical data where loss is unacceptable |
| `syncOnWrite: false` | Data is durable only after explicit Sync() or Close() | High-volume writes where performance is critical |

## Concurrency and Lock-Free Data Structures

KGStore provides high-concurrency options for workloads that need to maximize parallel operations.

### Lock-Free MemTable

The `UseLockFreeMemTable` option enables a lock-free implementation of the MemTable, which can significantly improve performance for concurrent workloads:

- Uses atomic operations instead of mutex locks
- Allows simultaneous reads and writes to different keys
- Particularly beneficial for read-heavy workloads or many cores
- Maintains the same ACID guarantees as the standard implementation

**Performance Comparison:**

| Scenario | Standard MemTable | Lock-Free MemTable | Improvement |
|----------|------------------|-------------------|-------------|
| Single-threaded writes | Baseline | Similar to baseline | - |
| Single-threaded reads | Baseline | Similar to baseline | - |
| 4 threads (50% read/50% write) | Baseline | Up to 2.5x faster | High |
| 8 threads (80% read/20% write) | Baseline | Up to 3.5x faster | Very High |
| 16 threads (mixed operations) | Baseline | Up to 4x faster | Very High |

The lock-free MemTable shows the most significant improvements when:
1. The workload is highly concurrent (many threads)
2. There are more reads than writes
3. The system has multiple CPU cores

## Property Indexing

KGStore includes a specialized property index for efficient lookups based on property values:

- **Value Type Support**: String, numeric (integer/float), and boolean values
- **Automatic Type Detection**: Property values are automatically typed and indexed appropriately
- **Composite Key Format**: Enables efficient lookups using `{prefix}:{propertyName}:{valueType}:{propertyValue}:{entityID}`
- **Separate Indexes**: Dedicated indexes for node and edge properties
- **Query Capabilities**:
  - Find entities by exact property value
  - Find all entities with a specific property (regardless of value)
  - Check property existence
- **Performance Optimizations**:
  - Result caching for frequent queries
  - Integrated with the LSM-tree structure
  - Bloom filter support for fast negative lookups

## Query Language

KGStore provides a simple but powerful query language for accessing graph data:

```
FIND_NODES_BY_LABEL(label: "Person")
FIND_EDGES_BY_LABEL(label: "KNOWS")
FIND_NODES_BY_PROPERTY(propertyName: "name", propertyValue: "John")
FIND_EDGES_BY_PROPERTY(propertyName: "since", propertyValue: "2023-01-01")
FIND_NEIGHBORS(nodeId: "1", direction: "outgoing")
FIND_PATH(sourceId: "1", targetId: "2", maxHops: "3")
```

The query language supports:
- Label-based lookups for nodes and edges
- Property-based lookups with various value types
- Neighborhood exploration with directional control
- Path finding between nodes with configurable hop limits

## Usage Example

```go
package main

import (
    "fmt"
    "log"

    "git.canoozie.net/riddling/kgstore/pkg/storage"
    "git.canoozie.net/riddling/kgstore/pkg/model"
    "git.canoozie.net/riddling/kgstore/pkg/query"
)

func main() {
    // Configure the storage engine
    config := storage.DefaultEngineConfig()
    config.DataDir = "/path/to/database"
    config.SyncWrites = true // Default, but shown for clarity
    config.UseLockFreeMemTable = true // High-performance concurrent operations
    config.UsePropertyIndex = true // Enable specialized property indexing

    // Initialize the storage engine
    engine, err := storage.NewStorageEngine(config)
    if err != nil {
        log.Fatalf("Failed to initialize storage engine: %v", err)
    }
    defer engine.Close()

    // Create a transaction
    tx, err := engine.Begin()
    if err != nil {
        log.Fatalf("Failed to create transaction: %v", err)
    }

    // Create nodes
    person1, err := tx.CreateNode("Person")
    if err != nil {
        tx.Rollback()
        log.Fatalf("Failed to create node: %v", err)
    }

    person2, err := tx.CreateNode("Person")
    if err != nil {
        tx.Rollback()
        log.Fatalf("Failed to create node: %v", err)
    }

    // Add properties to nodes
    err = person1.SetProperty("name", "John Doe")
    if err != nil {
        tx.Rollback()
        log.Fatalf("Failed to set property: %v", err)
    }

    err = person2.SetProperty("name", "Jane Smith")
    if err != nil {
        tx.Rollback()
        log.Fatalf("Failed to set property: %v", err)
    }

    // Create an edge between nodes
    knows, err := tx.CreateEdge(person1, person2, "KNOWS")
    if err != nil {
        tx.Rollback()
        log.Fatalf("Failed to create edge: %v", err)
    }

    // Add property to the edge
    err = knows.SetProperty("since", "2025-04-01")
    if err != nil {
        tx.Rollback()
        log.Fatalf("Failed to set edge property: %v", err)
    }

    // Commit the transaction
    if err := tx.Commit(); err != nil {
        log.Fatalf("Failed to commit transaction: %v", err)
    }

    // Read data in a new transaction
    readTx, err := engine.Begin()
    if err != nil {
        log.Fatalf("Failed to create read transaction: %v", err)
    }
    defer readTx.Rollback()

    // Query for outgoing edges
    edges, err := readTx.GetNodeOutgoingEdges(person1.ID(), "KNOWS")
    if err != nil {
        log.Fatalf("Failed to query edges: %v", err)
    }

    for _, edge := range edges {
        target, err := readTx.GetNodeByID(edge.TargetID())
        if err != nil {
            log.Fatalf("Failed to get target node: %v", err)
        }

        targetName, _ := target.GetProperty("name")
        since, _ := edge.GetProperty("since")

        fmt.Printf("%s KNOWS %s since %s\n",
            person1.GetProperty("name"),
            targetName,
            since)
    }

    // Property-based query example
    queryStr := "FIND_NODES_BY_PROPERTY(propertyName: \"name\", propertyValue: \"John Doe\")"
    parsedQuery, err := query.Parse(queryStr)
    if err != nil {
        log.Fatalf("Failed to parse query: %v", err)
    }

    executor := query.NewExecutor(readTx)
    results, err := executor.Execute(parsedQuery)
    if err != nil {
        log.Fatalf("Failed to execute query: %v", err)
    }

    fmt.Printf("Found %d nodes with name 'John Doe'\n", len(results.Nodes))
}
```

## Configuration Options

The storage engine can be configured with various options:

```go
config := storage.DefaultEngineConfig()
config.DataDir = "data"                // Directory to store database files
config.SyncWrites = true               // Ensure durability (default)
config.MemTableSize = 32 * 1024 * 1024 // 32MB threshold before flushing to SSTable
config.BackgroundCompaction = true     // Enable automatic background compaction
config.BloomFilterFPR = 0.01           // 1% false positive rate for bloom filters
config.UseLockFreeMemTable = true      // Use lock-free MemTable for higher concurrency
config.UseLSMNodeLabelIndex = true     // Use LSM-tree based node label index
config.UsePropertyIndex = true         // Enable specialized property indexing
config.SSTableDeletionDelay = 30 * time.Second // Time to wait before physically deleting SSTables
```

## ACID Compliance

KGStore implements full ACID (Atomicity, Consistency, Isolation, Durability) compliance:

- **Atomicity**: Transactions are all-or-nothing, with proper rollback handling
- **Consistency**: The database moves from one valid state to another valid state
- **Isolation**: Transactions are isolated from each other's changes
- **Durability**: Write-Ahead Logging ensures data safety even during crashes

Enhanced ACID features include:
- Transaction boundary support in the WAL
- Configurable WAL replay options for recovery scenarios
- Two-phase deletion mechanism for safe SSTable removal

## Performance Optimizations

KGStore includes several optimizations for high performance:

- **Bloom Filters**: Fast negative lookups to skip irrelevant SSTables
- **LSM Tree Structure**: Optimized for write-heavy workloads
- **Level-based Compaction**: Reduces write amplification and improves read performance
- **Skip Lists**: Efficient O(log n) in-memory data structure
- **Two-Phase Deletion**: Allows ongoing reads to complete during compaction
- **Result Caching**: For frequently accessed data
- **Configurable Memory Budget**: Adjustable MemTable size for different workloads

## Development

### Building and Testing

```bash
# Run tests
go test ./...

# Run tests with coverage
./scripts/run_tests_with_coverage.sh

# Run benchmarks
./scripts/run_benchmarks.sh
```

## License

   Copyright 2025 Jeremy Tregunna

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
