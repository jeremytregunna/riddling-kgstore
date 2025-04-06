# Write-Ahead Log (WAL) Implementation in KGStore

This document provides a comprehensive overview of the Write-Ahead Log (WAL) implementation in KGStore, including its architecture, features, recovery mechanisms, and transaction support.

## Overview

The Write-Ahead Log (WAL) is a critical component of KGStore that ensures durability by recording all changes before they are applied to the in-memory structures. It follows the principle of "write ahead," meaning that no changes are considered committed until they've been successfully written to the log.

## Implementation Status

The WAL implementation in KGStore is mature and includes:

✅ **Fully Implemented**:
- Durable write-ahead logging with xxHash64 integrity checks
- Transaction support (begin, commit, rollback)
- Comprehensive recovery with multiple recovery modes
- Atomic transaction application
- Configurable synchronization options
- Version tracking for point-in-time recovery
- Detailed statistics for recovery operations
- Corrupted record detection and handling

💡 **Future ideas for improvements**:
- Enhanced compression for records to reduce storage requirements

## Core Architecture

### Data Structures

1. **WALRecord**: Represents a single record in the WAL
   ```go
   type WALRecord struct {
       Type      RecordType // Type of the record (put, delete, transaction begin/commit/rollback)
       Key       []byte     // Key for put/delete operations
       Value     []byte     // Value for put operations
       Timestamp int64      // Timestamp when the record was created
       TxID      uint64     // Transaction ID for transaction-related records
       Version   uint64     // Version for versioned records
   }
   ```

2. **WAL**: Main structure managing the write-ahead log
   ```go
   type WAL struct {
       mu          sync.Mutex       // For thread safety
       file        *os.File         // Underlying file
       writer      *bufio.Writer    // Buffered writer for better performance
       path        string           // Path to the WAL file
       isOpen      bool             // Whether the WAL is open
       syncOnWrite bool             // Whether to sync to disk after each write
       logger      model.Logger     // For logging operations
       activeTxs   map[uint64]bool  // Track active transactions
       nextTxID    uint64           // Next transaction ID to assign
   }
   ```

3. **ReplayOptions**: Configuration for WAL replay behavior
   ```go
   type ReplayOptions struct {
       // StrictMode causes replay to fail completely if any record is corrupted
       StrictMode bool

       // AtomicTxOnly ensures that transactions are applied atomically or not at all
       AtomicTxOnly bool
   }
   ```

4. **ReplayStats**: Detailed statistics about WAL replay operations
   ```go
   type ReplayStats struct {
       RecordCount            int           // Total records processed
       AppliedCount           int           // Records successfully applied
       CorruptedCount         int           // Corrupted records encountered
       SkippedTxCount         int           // Transactions skipped
       StandaloneOpCount      int           // Standalone operations processed
       TransactionOpCount     int           // Transaction operations processed
       IncompleteTransactions int           // Incomplete transactions found
       CorruptedTransactions  int           // Corrupted transactions found
       StartTime              time.Time     // When replay started
       EndTime                time.Time     // When replay finished
       Duration               time.Duration // Total replay time
   }
   ```

### Key Components

1. **WAL Header**:
   - Magic number (`0x57414C4C`, "WALL") for file identification
   - Version number (currently at version 2)
   - Next transaction ID (added in version 2)

2. **Record Types**:
   - `RecordPut` (1): Records a key-value pair addition
   - `RecordDelete` (2): Records a key deletion
   - `RecordTxBegin` (3): Marks the beginning of a transaction
   - `RecordTxCommit` (4): Marks the successful completion of a transaction
   - `RecordTxRollback` (5): Marks the rollback of a transaction

3. **Record Format**:
   - xxHash64 checksum (8 bytes)
   - Record size (4 bytes)
   - Record type (1 byte)
   - Timestamp (8 bytes)
   - Transaction ID (8 bytes)
   - For Put/Delete: Key length (4 bytes) + key (variable)
   - For Put only: Value length (4 bytes) + value (variable)

## Key Operations

### Writing Operations

1. **Put**: Records a key-value addition
   ```go
   func (w *WAL) Put(key, value []byte) error
   ```

2. **Delete**: Records a key deletion
   ```go
   func (w *WAL) Delete(key []byte) error
   ```

3. **Transaction Management**:
   ```go
   func (w *WAL) BeginTransaction() (uint64, error)
   func (w *WAL) CommitTransaction(txID uint64) error
   func (w *WAL) RollbackTransaction(txID uint64) error
   ```

### Recovery Operations

1. **Replay**: Applies all valid WAL records to a MemTable
   ```go
   func (w *WAL) ReplayToInterface(memTable MemTableInterface, options ReplayOptions) (ReplayStats, error)
   ```

2. **Truncate**: Clears the WAL file and creates a new one
   ```go
   func (w *WAL) Truncate() error
   ```

## Recovery Mechanism

The WAL recovery process is sophisticated and consists of multiple phases:

### Phase 1: Transaction Status Identification

During the first pass through the WAL, the system:
1. Identifies complete transactions (those with matching begin/commit records)
2. Flags incomplete transactions (begin without commit/rollback)
3. Marks corrupted records and transactions
4. Builds a mapping of transaction IDs to their operations

### Phase 2: Standalone Operation Application

The second pass applies all standalone operations (those not part of any transaction):
1. Each operation is applied with a monotonically increasing version number
2. Failed operations are logged and skipped unless StrictMode is enabled

### Phase 3: Transaction Application

The third pass applies transactions based on the ReplayOptions:

1. **For AtomicTxOnly = true (default)**:
   - Only committed transactions are applied
   - Corrupted transactions are skipped entirely
   - Incomplete transactions are skipped
   - Before applying, all operations are validated in a temporary MemTable

2. **For AtomicTxOnly = false**:
   - Committed transactions are applied normally
   - For incomplete or corrupted transactions, valid operations are applied
   - Each operation gets a version number based on its transaction ID

### Special Error Handling

- In StrictMode, any corruption causes the entire replay to fail
- Detailed statistics are collected on each type of error or edge case
- The recovery process updates the next transaction ID based on all records seen

## Transaction Support

The WAL provides full ACID transaction support:

1. **Atomicity**: Ensured by the replay mechanism - transactions are either applied in full or not at all
2. **Consistency**: Maintained through proper version assignment
3. **Isolation**: Supported through version numbers for concurrent operations
4. **Durability**: Guaranteed by writing records to disk before confirming operations

### Transaction Versioning

KGStore implements a versioning system during replay to handle transaction precedence:

```go
// Version assignment for committed transactions
version = (txID + 1) * 1000000

// Version assignment for non-committed transactions in non-atomic mode
version = txID * 1000
```

This ensures that committed transactions always have higher versions than incomplete ones, and later transactions override earlier ones.

## Error Handling and Data Integrity

### Data Integrity Features

1. **xxHash64 Checksums**: Every record includes a xxHash64 checksum that is verified during reading
2. **Record Type Validation**: Record types are validated against known types
3. **Size Verification**: Record sizes are included in the header
4. **Sync Options**: Configuration option for immediate disk syncing after writes

### Error Recovery Mechanisms

1. **Corrupted Record Detection**: Records with invalid checksums or structure are detected
2. **Partial Transaction Handling**: Options for dealing with incomplete transactions
3. **Recovery Statistics**: Detailed statistics for auditing and debugging

## Performance Considerations

The WAL implementation balances durability with performance:

1. **Buffered Writing**: Uses a buffered writer for better I/O performance
2. **Configurable Sync**: Optional immediate disk synchronization
3. **Multi-pass Recovery**: Optimized for different recovery scenarios
4. **Pre-validation**: Transactions are validated before application to avoid partial state

## Usage Example

```go
// Creating a WAL
walConfig := WALConfig{
    Path:        "/path/to/data/wal.log",
    SyncOnWrite: true,
    Logger:      logger,
}
wal, err := NewWAL(walConfig)

// Starting a transaction
txID, err := wal.BeginTransaction()

// Recording operations
wal.PutWithTransaction([]byte("key1"), []byte("value1"), txID)
wal.DeleteWithTransaction([]byte("key2"), txID)

// Committing the transaction
wal.CommitTransaction(txID)

// Recovery
memTable := NewMemTable(config)
options := DefaultReplayOptions()
stats, err := wal.ReplayToInterface(memTable, options)
```

## Future Enhancements

1. **Compression**: Add support for compressed WAL records to reduce disk space
2. **Record Checkpointing**: Implement checkpointing to limit recovery time
3. **File Rotation**: Support for WAL file rotation to manage size
4. **Enhanced Recovery Parallelism**: Optimize recovery for multi-core systems
5. **Advanced Transaction Isolation**: Add support for more isolation levels

## Conclusion

The WAL implementation in KGStore provides a robust foundation for durability and transaction support. The multi-phase recovery process ensures data integrity while providing flexibility through configuration options. This implementation balances performance with safety, making it suitable for a wide range of use cases, from rapid development to critical production environments.
