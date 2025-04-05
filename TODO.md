## KGStore Action Plan - Detailed TODO Lists Per Phase

Here's a detailed breakdown of TODO lists for each phase of the KGStore development, aiming for comprehensiveness without unnecessary additions.  Assumes Go as the implementation language.

**Phase 1: Core Data Structures & Serialization (2-3 months)**

* **Data Structure Definitions:**
    * [x] Define `Node` struct: ID (uint64), Label (string), Properties (map[string]string)
    * [x] Define `Edge` struct: Source Node ID (uint64), Target Node ID (uint64), Label (string), Properties (map[string]string)
    * [x] Define `Property` struct: Key (string), Value (string) - Consider different value types later.
    * [x] Define `Page` struct: Page ID (uint64), Data ([]byte),  Flags (e.g., allocated, dirty)
    * [x] Implement basic error handling for data structure operations.
    * [x] Implement basic logging for data structure operations.
* **Serialization/Deserialization Library:**
    * [x] Implement functions to serialize `Node`, `Edge`, `Property` to custom encoding/binary format.
    * [x] Implement functions to deserialize from custom encoding/binary format to `Node`, `Edge`, `Property`.
    * [x] Implement functions to serialize/deserialize byte arrays directly to/from `Page` data.
    * [x] Implement error handling for serialization/deserialization.
* **Basic Page Management:**
    * [x] Implement a `PageAllocator` struct.
    * [x] Implement `AllocatePage()` function.
    * [x] Implement `DeallocatePage()` function.
    * [x] Implement a simple page table (e.g., vector of booleans indicating allocation status).
    * [x] Implement basic error handling for page allocation/deallocation (out of memory, invalid page ID).
* **Unit Tests:**
    * [x] Unit tests for `Node` struct/class (creation, access, modification).
    * [x] Unit tests for `Edge` struct/class (creation, access, modification).
    * [x] Unit tests for `Property` struct/class (creation, access, modification).
    * [x] Unit tests for `Page` struct/class (creation, access, modification).
    * [x] Unit tests for serialization/deserialization of `Node`, `Edge`, `Property`.
    * [x] Unit tests for `PageAllocator` (allocation, deallocation, error handling).
    * [x] Implement test coverage reporting.
    * [x] Implement memory leak detection during testing.
    * [x] Performance benchmarks for read/write operations.

**Phase 2: Storage Engine & Basic Indexing (2-3 months)**

* **Storage Engine:**
    * [x] Implement `MemTable` with sorted map and write operations as an in-memory sorted map. Writes are always appended to the MemTable.
    * [x] Implement SSTable writing (serialization from Phase 1) as a Sorted String Table, immuatble, sorted files on disk.
    * [x] Implement reading from an SSTable.
    * [x] Implement Bloom Filter creation and lookup.
    * [x] When the MemTable reaches a certain size, it's data flushes to a new SSTable.
    * [x] Implement a background process that merges multiple SSTables into larger ones, reducing the number of files nad improving read performance. Start with a simple level-based compaction strategy.
    * [x] Implement basic file management system for SSTables.
    * [x] Implement a write-ahead log (WAL) for durability. This is *critical*
* **Basic Indexing:**
    * [x] Implement primary index (Node ID -> Node data)
    * [x] Implement Secondary index for Node Labels (Label -> List of Node IDs)
    * [x] Implement Secondary index for Edge Labels (Label -> List of Edge IDs)
    * [x] Implement index update logic during write operations
* **Integration Tests:**
    * [x] Integration tests to verify that Nodes and Edges can be written to and read from storage.
    * [x] Integration tests to verify that the index is working correctly.
    * [x] Integration tests to verify that data consistency is maintained.
    * [x] Implement performance benchmarks for read/write operations.
    * [x] Performance tests to measure read/write throughput and latency.
    * [x] Durability tests to verify the WAL is working correctly.
    * [x] Stress tests to verify the database's stability under heavy load.

**Phase 3: Query Processing & Basic Query Language (2-3 months)**

* **Query Processing:**
    * [x] Define a basic query language (e.g., a simple graph pattern matching language, or a subset of Cypher).
    * [x] Implement a query parser to parse the query language (e.g., text/template or a dedicated parser generator).
    * [x] Implement a query optimizer to optimize the query execution plan (can be simple rule-based optimizer).
    * [x] Implement a query executor to execute the query plan.
    * [x] Implement a basic graph traversal algorithm (e.g., depth-first search, breadth-first search).
* **Query Language Implementation:**
    * [x] Implement `FindNodesByLabel()` query.
    * [x] Implement `FindEdgesByLabel()` query.
    * [x] Implement `FindNeighbors()` query.
    * [x] Implement `FindPath()` query (limited to a small number of hops).
* **Testing:**
    * [x] Unit tests for query parser.
    * [x] Unit tests for query optimizer.
    * [x] Integration tests for query execution.
    * [x] Performance benchmarks for different query types.
    * [x] Implement a test suite with a variety of graph data.

**Phase 4: Advanced Indexing & Optimization (2-3 months)**

* **Advanced Indexing:**
    * [ ] Implement LSM-tree based index on Node labels (consistent with storage engine architecture).
    * [ ] Implement full-text index as specialized SSTable format for Node properties.
    * [ ] Implement spatial index as specialized SSTable format (if applicable).
* **Optimization:**
    * [ ] Implement LRU query cache with configurable size limits to prevent memory explosion.
    * [ ] Implement query rewriting (e.g., constant folding).
    * [ ] Implement parallel query execution (using goroutines and channels or workgroups, whatever works).
    * [ ] Implement data compression (could be compress/gzip or similar, but has to be fast and good performance).
    * [ ] Implement property value statistics for better query optimization.
    * [ ] Implement columnar storage for property indexes to improve compression and query performance.
    * [ ] Implement transaction buffer with size limits to trigger background flushes when reaching thresholds.
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
