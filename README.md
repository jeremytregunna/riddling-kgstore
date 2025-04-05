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

## Usage Example

```go
package main

import (
    "fmt"
    "log"
    
    "git.canoozie.net/riddling/kgstore/pkg/storage"
    "git.canoozie.net/riddling/kgstore/pkg/model"
)

func main() {
    // Configure the storage engine
    config := storage.DefaultEngineConfig()
    config.DatabasePath = "/path/to/database"
    config.SyncWrites = true // Default, but shown for clarity
    
    // Initialize the storage engine
    engine, err := storage.NewEngine(config)
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
    err = knows.SetProperty("since", "2023-04-01")
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
}
```

## Configuration Options

The storage engine can be configured with various options:

```go
config := storage.DefaultEngineConfig()
config.SyncWrites = true               // Ensure durability (default)
config.MemtableSizeThreshold = 4 << 20 // 4MB threshold before flushing to SSTable
config.CompactionThreshold = 3         // Number of SSTables before triggering compaction
```

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

[License information here]