# MemTable Implementations in KGStore

## Overview

KGStore currently provides two MemTable implementations:

1. **Standard MemTable** - Uses a traditional mutex-based skiplist implementation
2. **LockFreeMemTable** - Uses a lock-free concurrent skiplist implementation

Both implementations provide the same functionality through the common `MemTableInterface`, but with different concurrency characteristics and performance profiles.

## Current Status and Future Direction

As of our latest refactoring, the two implementations have been standardized with:
- Common error types (in `pkg/model/errors.go`)
- Shared helper functions
- A comprehensive shared interface
- Consistent behavior across implementations

**Future Direction:** The lock-free implementation will become the default and preferred option going forward. The traditional mutex-based implementation will remain available for backward compatibility and specific use cases where its simpler design may be advantageous.

## Choosing Between Implementations

### Use Lock-Free MemTable When:

- You need high concurrency with multiple writer threads
- Performance under contention is important
- You're building new components and want to leverage the latest implementation
- Memory usage is not a primary concern

### Use Standard MemTable When:

- Simple, predictable behavior is needed
- Single-writer scenarios are common
- Memory usage is a concern
- You're maintaining older code that relies on this implementation
- Debugging and tracing through the code is important

## Performance Characteristics

| Characteristic | Standard MemTable | LockFreeMemTable |
|----------------|------------------|------------------|
| Concurrency    | Limited by mutex | High concurrency |
| Single-thread  | Slightly faster  | Slight overhead  |
| Memory usage   | Lower           | Higher           |
| Code complexity| Simpler         | More complex     |
| Read-heavy     | Good            | Excellent        |
| Write-heavy    | Bottlenecks     | Better scaling   |

## Implementation Notes

### Standard MemTable

The standard MemTable implementation uses a traditional skiplist with mutex-based concurrency control. This means:

- Only one writer can modify the structure at a time
- Readers must acquire a read lock
- Simpler implementation and debugging
- Lower memory overhead (no atomic operation overhead)
- Efficient key scanning with prefix matching

### LockFreeMemTable

The lock-free implementation uses a concurrent skiplist with atomic operations:

- Multiple writers can operate concurrently
- No locks for read operations
- More complex implementation with careful handling of atomicity
- May use more memory for atomic operation support
- Prefix-based key scanning with atomic safety guarantees

### Key Scanning

Both implementations support key scanning via the `GetKeysWithPrefix` method, which returns all keys that start with a given prefix. This is particularly useful for:

- Finding all outgoing or incoming edges for a node
- Discovering all keys with a specific pattern
- Implementing range queries
- Supporting neighbor search functionality in graph operations

## WAL Considerations

Both implementations support Write-Ahead Logging (WAL), but with some differences:

- Standard MemTable has more direct support for WAL recovery
- LockFreeMemTable recovery may be more complex due to potential concurrent operations

It is recommended to use the standard MemTable if WAL recovery is a critical feature of your application.