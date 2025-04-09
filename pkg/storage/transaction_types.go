package storage

import "time"

// TransactionType defines the type of transaction (read-only or read-write)
type TransactionType string

const (
	// READ_ONLY transactions can only read data, never modify it
	READ_ONLY TransactionType = "READ_ONLY"

	// READ_WRITE transactions can both read and modify data
	READ_WRITE TransactionType = "READ_WRITE"
)

// TransactionPriority defines the priority of a transaction in the queue
type TransactionPriority string

const (
	// LOW priority transactions are processed after normal and high priority ones
	LOW TransactionPriority = "LOW"

	// NORMAL priority transactions are processed after high priority ones
	NORMAL TransactionPriority = "NORMAL"

	// HIGH priority transactions are processed first
	HIGH TransactionPriority = "HIGH"
)

// TransactionState represents the current state of a transaction
type TransactionState string

const (
	// PENDING transaction is waiting to be activated
	PENDING TransactionState = "PENDING"

	// ACTIVE transaction is currently running
	ACTIVE TransactionState = "ACTIVE"

	// COMMITTED transaction has been successfully committed
	COMMITTED TransactionState = "COMMITTED"

	// ABORTED transaction has been rolled back
	ABORTED TransactionState = "ABORTED"
)

// TxOptions contains the parameters for a new transaction
type TxOptions struct {
	// Type specifies whether this is a read-only or read-write transaction
	Type TransactionType

	// Timeout is the maximum time to wait for a write lock acquisition
	// Only applicable for READ_WRITE transactions
	// Zero means no timeout (will wait indefinitely)
	Timeout time.Duration

	// Priority determines the order in which waiting transactions are processed
	// Higher priority transactions are activated before lower priority ones
	Priority TransactionPriority
	
	// BatchSize sets the maximum number of operations to buffer before auto-flushing
	// Zero means no batching (operations are processed individually)
	// Negative means unlimited batch size (manual flush required)
	BatchSize int
	
	// BufferWrites determines whether writes should be buffered until commit
	// This provides atomic visibility of all changes within the transaction
	BufferWrites bool
	
	// CoalesceWrites enables write coalescing for related operations
	// When enabled, multiple writes to the same key are combined
	CoalesceWrites bool
}

// DefaultTxOptions returns the default transaction options
func DefaultTxOptions() TxOptions {
	return TxOptions{
		Type:          READ_WRITE, // Default to read-write for backward compatibility
		Timeout:       0,          // No timeout
		Priority:      NORMAL,     // Normal priority
		BatchSize:     100,        // Default batch size
		BufferWrites:  false,      // No write buffering by default
		CoalesceWrites: false,     // No write coalescing by default
	}
}

// ReadOnlyTxOptions returns options for a read-only transaction
func ReadOnlyTxOptions() TxOptions {
	return TxOptions{
		Type:          READ_ONLY,
		Timeout:       0,
		Priority:      NORMAL,
		BatchSize:     0,
		BufferWrites:  false,
		CoalesceWrites: false,
	}
}

// ReadWriteTxOptions returns options for a read-write transaction
func ReadWriteTxOptions(timeout time.Duration, priority TransactionPriority) TxOptions {
	return TxOptions{
		Type:          READ_WRITE,
		Timeout:       timeout,
		Priority:      priority,
		BatchSize:     100,       // Default batch size
		BufferWrites:  false,     // No write buffering by default
		CoalesceWrites: false,    // No write coalescing by default
	}
}

// BatchedWriteTxOptions returns options for a batched write transaction
func BatchedWriteTxOptions(batchSize int, timeout time.Duration, priority TransactionPriority) TxOptions {
	return TxOptions{
		Type:          READ_WRITE,
		Timeout:       timeout,
		Priority:      priority,
		BatchSize:     batchSize,
		BufferWrites:  false,
		CoalesceWrites: false,
	}
}

// BufferedWriteTxOptions returns options for a transaction with buffered writes
func BufferedWriteTxOptions(timeout time.Duration, priority TransactionPriority) TxOptions {
	return TxOptions{
		Type:          READ_WRITE,
		Timeout:       timeout,
		Priority:      priority,
		BatchSize:     100,
		BufferWrites:  true,
		CoalesceWrites: true, // Enable coalescing when buffering
	}
}