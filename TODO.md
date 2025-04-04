## KGStore Action Plan - Detailed TODO Lists Per Phase

Here's a detailed breakdown of TODO lists for each phase of the KGStore development, aiming for comprehensiveness without unnecessary additions.  Assumes Go as the implementation language.

**Phase 1: Core Data Structures & Serialization (2-3 months)**

* **Data Structure Definitions:**
    * [x] Define `Node` struct: ID (uint64), Label (string), Properties (map[string]string)
    * [ ] Define `Edge` struct: Source Node ID (uint64), Target Node ID (uint64), Label (string), Properties (map[string]string)
    * [ ] Define `Property` struct: Key (string), Value (string) - Consider different value types later.
    * [ ] Define `Page` struct: Page ID (uint64), Data ([]byte),  Flags (e.g., allocated, dirty)
    * [ ] Implement basic error handling for data structure operations.
    * [ ] Implement basic logging for data structure operations.
* **Serialization/Deserialization Library:**
    * [ ] Implement functions to serialize `Node`, `Edge`, `Property` to custom encoding/binary format.
    * [ ] Implement functions to deserialize from custom encoding/binary format to `Node`, `Edge`, `Property`.
    * [ ] Implement functions to serialize/deserialize byte arrays directly to/from `Page` data.
    * [ ] Implement error handling for serialization/deserialization.
* **Basic Page Management:**
    * [ ] Implement a `PageAllocator` struct.
    * [ ] Implement `AllocatePage()` function.
    * [ ] Implement `DeallocatePage()` function.
    * [ ] Implement a simple page table (e.g., vector of booleans indicating allocation status).
    * [ ] Implement basic error handling for page allocation/deallocation (out of memory, invalid page ID).
* **Unit Tests:**
    * [ ] Unit tests for `Node` struct/class (creation, access, modification).
    * [ ] Unit tests for `Edge` struct/class (creation, access, modification).
    * [ ] Unit tests for `Property` struct/class (creation, access, modification).
    * [ ] Unit tests for `Page` struct/class (creation, access, modification).
    * [ ] Unit tests for serialization/deserialization of `Node`, `Edge`, `Property`.
    * [ ] Unit tests for `PageAllocator` (allocation, deallocation, error handling).
    * [ ] Implement test coverage reporting.
    * [ ] Implement memory leak detection during testing.
    * [ ] Performance benchmarks for read/write operations.

**Phase 2: Storage Engine & Basic Indexing (2-3 months)**

* **Storage Engine:**
    * [ ] Implement `MemTable` with sorted map and write operations as an in-memory sorted map. Writes are always appended to the MemTable.
    * [ ] Implement SSTable writing (serialization from Phase 1) as a Sorted String Table, immuatble, sorted files on disk.
    * [ ] Implement reading from an SSTable.
    * [ ] Implement Bloom Filter creation and lookup.
    * [ ] When the MemTable reaches a certain size, it's data flushes to a new SSTable.
    * [ ] Implement a background process that merges multiple SSTables into larger ones, reducing the number of files nad improving read performance. Start with a simple level-based compaction strategy.
    * [ ] Implement basic file management system for SSTables.
    * [ ] Implement a write-ahead log (WAL) for durability. This is *critical*
* **Basic Indexing:**
    * [ ] Implement primary index (Node ID -> Node data)
    * [ ] Implement Secondary index for Node Labels (Label -> List of Node IDs)
    * [ ] Implement Secondary index for Edge Labels (Label -> List of Edge IDs)
    * [ ] Implement index update logic during write operations
* **Integration Tests:**
    * [ ] Integration tests to verify that Nodes and Edges can be written to and read from storage.
    * [ ] Integration tests to verify that the index is working correctly.
    * [ ] Integration tests to verify that data consistency is maintained.
    * [ ] Implement performance benchmarks for read/write operations.
    * [ ] Performance tests to measure read/write throughput and latency.
    * [ ] Durability tests to verify the WAL is working correctly.
    * [ ] Stress tests to verify the database's stability under heavy load.

**Phase 3: Query Processing & Basic Query Language (2-3 months)**

* **Query Processing:**
    * [ ] Define a basic query language (e.g., a simple graph pattern matching language, or a subset of Cypher).
    * [ ] Implement a query parser to parse the query language (e.g., text/template or a dedicated parser generator).
    * [ ] Implement a query optimizer to optimize the query execution plan (can be simple rule-based optimizer).
    * [ ] Implement a query executor to execute the query plan.
    * [ ] Implement a basic graph traversal algorithm (e.g., depth-first search, breadth-first search).
* **Query Language Implementation:**
    * [ ] Implement `FindNodesByLabel()` query.
    * [ ] Implement `FindEdgesByLabel()` query.
    * [ ] Implement `FindNeighbors()` query.
    * [ ] Implement `FindPath()` query (limited to a small number of hops).
* **Testing:**
    * [ ] Unit tests for query parser.
    * [ ] Unit tests for query optimizer.
    * [ ] Integration tests for query execution.
    * [ ] Performance benchmarks for different query types.
    * [ ] Implement a test suite with a variety of graph data.

**Phase 4: Advanced Indexing & Optimization (2-3 months)**

* **Advanced Indexing:**
    * [ ] Implement a B-tree index on Node labels.
    * [ ] Implement a full-text index on Node properties.
    * [ ] Implement a spatial index (if applicable).
* **Optimization:**
    * [ ] Implement query caching (using sync.Map).
    * [ ] Implement query rewriting (e.g., constant folding).
    * [ ] Implement parallel query execution (using goroutines and channels or workgroups, whatever works).
    * [ ] Implement data compression (could be compress/gzip or similar, but has to be fast and good performance).
* **Testing:**
    * [ ] Performance benchmarks for different indexing strategies.
    * [ ] Performance benchmarks for different optimization techniques.
    * [ ] Stress tests to verify the database’s stability under heavy load.

**Phase 5: Refinement, Documentation & Deployment (1-2 months)**

* **Code Refactoring:**
    * [ ] Review and refactor the codebase to improve readability and maintainability.
    * [ ] Implement code style guidelines (using go fmt and go lint).
* **Documentation:**
    * [ ] Create API documentation.
    * [ ] Create user documentation.
    * [ ] Create developer documentation.
* **Deployment:**
    * [ ] Create deployment scripts (using Docker or similar).
    * [ ] Create monitoring and alerting tools.
    * [ ] Create a CI/CD pipeline.
    * [ ] Perform performance testing in a production-like environment.
    * [ ] Implement logging and auditing.
    * [ ] Implement security measures.
