# Single-Writer Transaction Implementation

This document outlines the plan for implementing a single-writer transaction model in KGStore.

## Overview

Replace the planned MVCC approach with a simpler single-writer model that allows only one active write transaction at a time, while supporting multiple concurrent read transactions.

## Implementation Tasks

### Phase 1: Core Transaction Manager (1 week)

- [x] Create a global transaction manager
  - [x] Implement writer transaction lock acquisition/release
  - [x] Add writer transaction queue for pending transactions
  - [x] Create transaction state tracking (PENDING, ACTIVE, COMMITTED, ABORTED)

- [x] Modify `BeginTransaction()` to handle writer contention
  - [x] Add transaction type parameter (READ_ONLY, READ_WRITE)
  - [x] Implement blocking behavior for write transactions
  - [x] Add optional timeout for write transaction acquisition

- [x] Implement transaction priority system
  - [x] Add priority levels for transactions in the queue
  - [x] Create configurable fairness policies

### Phase 2: Read Transaction Support (1 week)

- [x] Implement read transaction snapshot system
  - [x] Create read view of database at transaction start
  - [x] Track read transaction start timestamps
  - [x] Ensure read transactions see consistent state

- [x] Add read transaction optimizations
  - [x] Allow multiple concurrent read transactions
  - [x] Implement no-wait read transaction starts

- [x] Modify `Get()` operations to use transaction state
  - [x] Ensure reads use appropriate snapshot based on transaction
  - [x] Implement read stability guarantees

### Phase 3: MemTable Simplification (1 week)

- [x] Simplify MemTable implementation
  - [x] Remove concurrent write-specific optimizations
  - [x] Optimize for single-writer scenario
  - [x] Reduce memory overhead from concurrency controls
  - [x] Remove LockFreeMemTable and ConcurrentSkipList implementations

- [x] Modify MemTable operations for transaction awareness
  - [x] Add transaction context to operations
  - [x] Simplify version tracking with single writer

- [x] Enhance read performance
  - [x] Optimize read-only access paths
  - [x] Add read caching optimizations

### Phase 4: Write Transaction Enhancements (1 week)

- [x] Implement batch processing for write transactions
  - [x] Group operations for better throughput
  - [x] Optimize WAL writes for batched operations

- [x] Add transaction operation buffering
  - [x] Buffer writes until commit
  - [x] Implement atomic visibility of changes

- [x] Create write transaction scheduling optimizations
  - [x] Prioritize small/fast transactions
  - [x] Add write coalescing for related operations

### Phase 5: WAL & Storage Integration (1 week)

- [ ] Simplify WAL for single-writer model
  - [ ] Remove concurrent transaction complexities
  - [ ] Optimize WAL append operations

- [ ] Enhance transaction durability
  - [ ] Simplify fsync patterns with single writer
  - [ ] Add group commit capabilities for batched operations

- [ ] Improve recovery process
  - [x] Simplify crash recovery with single-writer
  - [ ] Reduce recovery time with cleaner transaction boundaries

### Phase 6: Query System Integration (1 week)

- [x] Update query language for transaction model
  - [x] Add READ_ONLY / READ_WRITE transaction hints
  - [x] Implement automatic transaction type detection

- [x] Enhance query executor for transaction awareness
  - [x] Add transaction context to query execution
  - [x] Optimize read-only query paths

- [x] Add transaction timeout and retry mechanisms
  - [x] Implement configurable timeout for waiting writers
  - [x] Add automatic retry policies for applications

### Phase 7: Testing & Benchmarking (1-2 weeks)

- [x] Develop specific single-writer test suite
  - [x] Test writer contention scenarios
  - [x] Validate read transaction isolation
  - [x] Test recovery from interrupted transactions

- [x] Create performance benchmarks
  - [x] Measure throughput with varying read/write ratios
  - [ ] Compare to previous multi-writer implementation
  - [x] Test transaction queuing under load

- [x] Implement stress tests
  - [x] Test system under high contention
  - [x] Validate fairness of transaction scheduling
  - [x] Test recovery from crashes during transactions

### Phase 8: Documentation & API Refinement (1 week)

- [x] Update API documentation for single-writer model
  - [x] Document transaction types and behavior
  - [x] Add best practices for transaction usage
  - [x] Create examples for common patterns

- [x] Add configuration options
  - [x] Make transaction queue behavior configurable
  - [x] Add transaction timeout settings
  - [x] Create performance tuning guidelines

- [x] Document performance characteristics
  - [x] Document expected throughput under different workloads
  - [x] Provide guidance on read/write ratio considerations
  - [x] Add recommendations for workload optimization

## Usage Example

```go
// Read-only transaction example
txOpts := TxOptions{Type: READ_ONLY}
tx, err := engine.BeginTransaction(txOpts)
if err != nil {
    return err
}

// Multiple read operations
value1, err := tx.Get(key1)
value2, err := tx.Get(key2)

err = tx.Commit()

// Write transaction example
txOpts := TxOptions{
    Type: READ_WRITE,
    Timeout: 5 * time.Second,
    Priority: NORMAL,
}
tx, err := engine.BeginTransaction(txOpts)
if err != nil {
    return err
}

// Only one write transaction can be active at a time
err = tx.Put(key1, value1)
err = tx.Put(key2, value2)

err = tx.Commit()
```

## Query Language Example

```
// Read-only transaction
BEGIN READ ONLY TRANSACTION;
MATCH (p:Person) WHERE p.age > 30 RETURN p;
COMMIT;

// Write transaction
BEGIN WRITE TRANSACTION;
CREATE (p:Person {name: "John"});
CREATE (c:City {name: "New York"});
CREATE (p)-[:LIVES_IN {since: 2010}]->(c);
COMMIT;
```

## Expected Benefits

1. Simplified implementation compared to MVCC
2. Reduced memory overhead from concurrency control
3. Easier transaction recovery after crashes
4. Simpler reasoning about transaction semantics
5. Better optimization potential for read-heavy workloads
6. More predictable performance characteristics

## Expected Limitations

1. Reduced write throughput under contention
2. Potential for writer starvation under heavy read loads
3. Limited scalability for write-heavy workloads
4. Possible increased latency for write transactions
5. Need for application-level retry logic for timeouts
