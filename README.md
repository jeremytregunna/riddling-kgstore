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
- **gRPC Service**: Language-agnostic network interface with efficient Protocol Buffers encoding

For detailed information about KGStore's architecture and implementation, please refer to the [Architecture Documentation](docs/ARCHITECTURE.md).

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

KGStore provides a simple but powerful query language for both querying and manipulating graph data:

### Read Operations
```
FIND_NODES_BY_LABEL(label: "Person")
FIND_EDGES_BY_LABEL(label: "KNOWS")
FIND_NODES_BY_PROPERTY(propertyName: "name", propertyValue: "John")
FIND_EDGES_BY_PROPERTY(propertyName: "since", propertyValue: "2023-01-01")
FIND_NEIGHBORS(nodeId: "1", direction: "outgoing")
FIND_PATH(sourceId: "1", targetId: "2", maxHops: "3")
```

### Data Manipulation Operations
```
CREATE_NODE(label: "Person")
CREATE_EDGE(sourceId: "1", targetId: "2", label: "KNOWS")
DELETE_NODE(nodeId: "1")
DELETE_EDGE(edgeId: "e1")
SET_PROPERTY(target: "node", id: "1", name: "name", value: "John Doe")
SET_PROPERTY(target: "edge", id: "e1", name: "since", value: "2025-04-01")
REMOVE_PROPERTY(target: "node", id: "1", name: "age")
```

### Transaction Management
```
BEGIN_TRANSACTION()
COMMIT_TRANSACTION()
ROLLBACK_TRANSACTION()
```

The query language supports:
- Label-based lookups for nodes and edges
- Property-based lookups with various value types
- Neighborhood exploration with directional control
- Path finding between nodes with configurable hop limits
- Full CRUD operations (Create, Read, Update, Delete)
- Explicit transaction control with commit and rollback capabilities
- Auto-transaction support for single operations when not in an explicit transaction

## gRPC Service

KGStore includes a high-performance gRPC service that provides a language-agnostic interface to the database:

- **Efficient Protocol Buffers Encoding**: Compact binary representation of queries and results
- **Full Query Language Support**: All operations available via the query language are supported over gRPC
- **Transaction Management**: Begin, commit, and rollback transactions over the network
- **Streaming Support**: Efficiently stream large result sets
- **Command-line Client**: Simple client for interacting with the service from the command line

See [GRPC_SERVICE.md](docs/GRPC_SERVICE.md) for detailed documentation on using the gRPC service.

### gRPC Service Example

```bash
# Start the server
./bin/server

# In another terminal, begin a transaction
./bin/client -type=BEGIN_TRANSACTION
# Returns: {"transaction_id": "tx-1", "success": true, "operation": "BEGIN_TRANSACTION"}

# Create a node within the transaction and name it with ref "person1"
./bin/client -type=CREATE_NODE -params='{"label":"Person","ref":"person1"}' -txid=tx-1
# Returns: {"node_id": "1", "success": true, "operation": "CREATE_NODE", "txId": "tx-1", "entityIds": ["1"]}

# Create another node with ref "person2"
./bin/client -type=CREATE_NODE -params='{"label":"Person","ref":"person2"}' -txid=tx-1
# Returns: {"node_id": "2", "success": true, "operation": "CREATE_NODE", "txId": "tx-1", "entityIds": ["2"]}

# Create an edge between the two nodes using references
./bin/client -type=CREATE_EDGE -params='{"sourceId":"$person1","targetId":"$person2","label":"KNOWS"}' -txid=tx-1
# Returns: {"edge_id": "1-2", "success": true, "operation": "CREATE_EDGE", "txId": "tx-1", "entityIds": ["1-2"]}

# Commit the transaction
./bin/client -type=COMMIT_TRANSACTION -txid=tx-1
# Returns: {"transaction_id": "tx-1", "success": true, "operation": "COMMIT_TRANSACTION"}

# Query the database
./bin/client -type=FIND_NODES_BY_LABEL -params='{"label":"Person"}'
# Returns: {"nodes": [{"id": "1", "label": "Person", "properties": {}}, {"id": "2", "label": "Person", "properties": {}}]}
```

## Usage Example

```go
package main

import (
    "fmt"
    "log"

    "git.canoozie.net/riddling/kgstore/pkg/storage"
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

    // Create an executor for running queries
    executor := query.NewExecutor(engine)

    // Using query language with transactions
    queries := []string{
        // Start a transaction
        "BEGIN_TRANSACTION()",
        
        // Create nodes with references
        "CREATE_NODE(label: \"Person\", ref: \"person1\")", // Store node with reference "person1"
        "CREATE_NODE(label: \"Person\", ref: \"person2\")", // Store node with reference "person2"
        
        // Set properties on nodes using references
        "SET_PROPERTY(target: \"node\", id: \"$person1\", name: \"name\", value: \"John Doe\")",
        "SET_PROPERTY(target: \"node\", id: \"$person2\", name: \"name\", value: \"Jane Smith\")",
        
        // Create an edge between nodes using references
        "CREATE_EDGE(sourceId: \"$person1\", targetId: \"$person2\", label: \"KNOWS\", ref: \"knows_edge\")",
        
        // Set property on edge using reference
        "SET_PROPERTY(target: \"edge\", id: \"$knows_edge\", name: \"since\", value: \"2025-04-01\")",
        
        // Commit the transaction
        "COMMIT_TRANSACTION()"
    }

    // Execute the transaction
    var txID string
    var nodeID1, nodeID2, edgeID string
    
    for i, queryStr := range queries {
        parsedQuery, err := query.Parse(queryStr)
        if err != nil {
            log.Fatalf("Failed to parse query: %v", err)
        }
        
        // Add transaction ID to subsequent queries
        if i > 0 && txID != "" {
            parsedQuery.Parameters[query.ParamTransactionID] = txID
        }
        
        results, err := executor.Execute(parsedQuery)
        if err != nil {
            log.Fatalf("Failed to execute query: %v", err)
        }
        
        // Store transaction ID from BEGIN_TRANSACTION
        if i == 0 {
            txID = results.TxID
        }
        
        // Store IDs from creation operations for later use (outside transaction)
        if i == 1 && len(results.EntityIDs) > 0 {
            nodeID1 = results.EntityIDs[0] // First node ID
        } else if i == 2 && len(results.EntityIDs) > 0 {
            nodeID2 = results.EntityIDs[0] // Second node ID
        } else if i == 5 && len(results.EntityIDs) > 0 {
            edgeID = results.EntityIDs[0] // Edge ID
        }
    }

    // Querying data using the query language
    
    // Find outgoing edges from person1
    findEdgesQuery := fmt.Sprintf("FIND_NEIGHBORS(nodeId: \"%s\", direction: \"outgoing\")", nodeID1)
    parsedQuery, err := query.Parse(findEdgesQuery)
    if err != nil {
        log.Fatalf("Failed to parse query: %v", err)
    }
    
    results, err := executor.Execute(parsedQuery)
    if err != nil {
        log.Fatalf("Failed to execute query: %v", err)
    }
    
    // Process query results
    if len(results.Edges) > 0 {
        // For each edge found, get the target node details
        for _, edge := range results.Edges {
            // Get target node name using a property query
            targetNodeQuery := fmt.Sprintf(
                "FIND_NODES_BY_PROPERTY(propertyName: \"name\", propertyValue: \"Jane Smith\")")
            parsedTargetQuery, _ := query.Parse(targetNodeQuery)
            targetResults, _ := executor.Execute(parsedTargetQuery)
            
            if len(targetResults.Nodes) > 0 {
                // Get edge property using a property query
                edgePropQuery := fmt.Sprintf(
                    "FIND_EDGES_BY_PROPERTY(propertyName: \"since\", propertyValue: \"2025-04-01\")")
                parsedEdgePropQuery, _ := query.Parse(edgePropQuery)
                edgePropResults, _ := executor.Execute(parsedEdgePropQuery)
                
                if len(edgePropResults.Edges) > 0 {
                    fmt.Printf("John Doe KNOWS Jane Smith since 2025-04-01\n")
                }
            }
        }
    }
    
    // Property-based query example using the query language
    queryStr := "FIND_NODES_BY_PROPERTY(propertyName: \"name\", propertyValue: \"John Doe\")"
    parsedQuery, err = query.Parse(queryStr)
    if err != nil {
        log.Fatalf("Failed to parse query: %v", err)
    }
    
    results, err = executor.Execute(parsedQuery)
    if err != nil {
        log.Fatalf("Failed to execute query: %v", err)
    }
    
    fmt.Printf("Found %d nodes with name 'John Doe'\n", len(results.Nodes))
    
    // Demonstration of rollback
    rollbackQueries := []string{
        "BEGIN_TRANSACTION()",
        "CREATE_NODE(label: \"Person\")",
        "SET_PROPERTY(target: \"node\", id: \"3\", name: \"name\", value: \"To Be Rolled Back\")",
        "ROLLBACK_TRANSACTION()"
    }
    
    for _, queryStr := range rollbackQueries {
        parsedQuery, _ := query.Parse(queryStr)
        _, err := executor.Execute(parsedQuery)
        if err != nil {
            log.Printf("Error executing query: %v", err)
        }
    }
    
    // Verify the rolled back data doesn't exist
    verifyQuery := "FIND_NODES_BY_PROPERTY(propertyName: \"name\", propertyValue: \"To Be Rolled Back\")"
    parsedVerifyQuery, _ := query.Parse(verifyQuery)
    verifyResults, _ := executor.Execute(parsedVerifyQuery)
    
    fmt.Printf("Found %d nodes after rollback (should be 0)\n", len(verifyResults.Nodes))
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

### Transaction Management

KGStore provides both programmatic and query-language-based transaction support:

- **Explicit Transactions**: Begin, commit, and rollback operations through the query language:
  ```
  BEGIN_TRANSACTION()
  CREATE_NODE(label: "Person")
  // ... more operations
  COMMIT_TRANSACTION()  // or ROLLBACK_TRANSACTION() to undo all changes
  ```

- **Auto-Transactions**: Single operations automatically occur in their own transaction when not in an explicit transaction context

- **Error Handling**: Automatic rollback when errors occur during transaction execution

- **Transaction Registry**: Maintains active transactions and associates them with their unique IDs

Enhanced ACID features include:
- Transaction boundary support in the WAL
- Configurable WAL replay options for recovery scenarios
- Two-phase deletion mechanism for safe SSTable removal
- Transaction lifecycle tracking for atomicity guarantees

## Performance Optimizations

KGStore includes several optimizations for high performance:

- **Bloom Filters**: Fast negative lookups to skip irrelevant SSTables
- **LSM Tree Structure**: Optimized for write-heavy workloads
- **Level-based Compaction**: Reduces write amplification and improves read performance
- **Skip Lists**: Efficient O(log n) in-memory data structure
- **Two-Phase Deletion**: Allows ongoing reads to complete during compaction
- **Result Caching**: For frequently accessed data
- **Configurable Memory Budget**: Adjustable MemTable size for different workloads
- **Protocol Buffers**: Efficient binary encoding for network communication

## Development

### Building and Testing

```bash
# Run tests
go test ./...

# Run tests with coverage
./scripts/run_tests_with_coverage.sh

# Run benchmarks
./scripts/run_benchmarks.sh

# Run the gRPC service demo
./scripts/demo_grpc.sh
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