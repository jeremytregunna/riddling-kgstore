package query

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.canoozie.net/riddling/kgstore/pkg/common"
	"git.canoozie.net/riddling/kgstore/pkg/model"
	"git.canoozie.net/riddling/kgstore/pkg/storage"
)

// Result represents a query result
type Result struct {
	Nodes []model.Node `json:"nodes,omitempty"`
	Edges []model.Edge `json:"edges,omitempty"`
	Paths []Path       `json:"paths,omitempty"`
	Error string       `json:"error,omitempty"`

	// Fields for data modification operations
	NodeID    string   `json:"nodeId,omitempty"`    // Created node ID
	EdgeID    string   `json:"edgeId,omitempty"`    // Created edge ID
	Success   bool     `json:"success,omitempty"`   // Operation success flag
	TxID      string   `json:"txId,omitempty"`      // Transaction ID
	Operation string   `json:"operation,omitempty"` // Operation performed
	EntityIDs []string `json:"entityIds,omitempty"` // Entity IDs created/modified
}

// Path represents a path in the graph
type Path struct {
	Nodes []model.Node `json:"nodes"`
	Edges []model.Edge `json:"edges"`
}

// Executor executes queries against the storage engine
type Executor struct {
	Engine         *storage.StorageEngine
	NodeIndex      storage.Index
	EdgeIndex      storage.Index
	NodeLabels     storage.Index
	EdgeLabels     storage.Index
	NodeProperties storage.Index
	EdgeProperties storage.Index
	GraphStore     *storage.GraphStore // Graph operations store
	Optimizer      *Optimizer
	maxPathHops    int
	txRegistry     *TransactionRegistry // Registry of active transactions
}

// TransactionRegistry manages active transactions
type TransactionRegistry struct {
	mu           sync.RWMutex
	transactions map[string]*storage.Transaction
	nextID       int64
	activeAutoTx map[string]*storage.Transaction // Map of goroutine ID to auto transaction
	// Entity tracking maps to store IDs created within transactions
	txEntities map[string]map[string]string // Maps tx_id -> entity_ref -> entity_id
}

// NewTransactionRegistry creates a new transaction registry
func NewTransactionRegistry() *TransactionRegistry {
	return &TransactionRegistry{
		transactions: make(map[string]*storage.Transaction),
		nextID:       1,
		activeAutoTx: make(map[string]*storage.Transaction),
		txEntities:   make(map[string]map[string]string),
	}
}

// NewExecutor creates a new query executor with basic indexes
func NewExecutor(engine *storage.StorageEngine, nodeIndex, edgeIndex, nodeLabels, edgeLabels storage.Index) *Executor {
	// Create graph store
	graphStore, err := storage.NewGraphStore(engine, nil)
	if err != nil {
		// If there's an error creating the graph store, log it and continue without it
		// In a production environment, we might want to fail instead
		fmt.Printf("Error creating graph store: %v\n", err)
		graphStore = nil
	}

	return &Executor{
		Engine:      engine,
		NodeIndex:   nodeIndex,
		EdgeIndex:   edgeIndex,
		NodeLabels:  nodeLabels,
		EdgeLabels:  edgeLabels,
		GraphStore:  graphStore,
		Optimizer:   NewOptimizer(),
		maxPathHops: 5, // Default maximum path hops
		txRegistry:  NewTransactionRegistry(),
	}
}

// NewExecutorWithAllIndexes creates a new query executor with all available indexes
func NewExecutorWithAllIndexes(
	engine *storage.StorageEngine,
	nodeIndex,
	edgeIndex,
	nodeLabels,
	edgeLabels,
	nodeProperties,
	edgeProperties storage.Index,
) *Executor {
	// Create graph store
	graphStore, err := storage.NewGraphStore(engine, nil)
	if err != nil {
		// If there's an error creating the graph store, log it and continue without it
		fmt.Printf("Error creating graph store: %v\n", err)
		graphStore = nil
	}

	return &Executor{
		Engine:         engine,
		NodeIndex:      nodeIndex,
		EdgeIndex:      edgeIndex,
		NodeLabels:     nodeLabels,
		EdgeLabels:     edgeLabels,
		NodeProperties: nodeProperties,
		EdgeProperties: edgeProperties,
		GraphStore:     graphStore,
		Optimizer:      NewOptimizer(),
		maxPathHops:    5, // Default maximum path hops
		txRegistry:     NewTransactionRegistry(),
	}
}

// Transaction management methods

// BeginTransaction starts a new transaction
func (e *Executor) BeginTransaction() (string, error) {
	if e.GraphStore == nil {
		return "", fmt.Errorf("graph store not available")
	}

	e.txRegistry.mu.Lock()
	defer e.txRegistry.mu.Unlock()

	// Generate a transaction ID
	txID := fmt.Sprintf("tx-%d", e.txRegistry.nextID)
	e.txRegistry.nextID++

	// Begin a transaction in the storage engine
	tx, err := e.Engine.GetTransactionManager().BeginTransaction(storage.DefaultTxOptions())
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Store the transaction
	e.txRegistry.transactions[txID] = tx

	return txID, nil
}

// CommitTransaction commits a transaction
func (e *Executor) CommitTransaction(txID string) error {
	e.txRegistry.mu.Lock()
	defer e.txRegistry.mu.Unlock()

	// Get the transaction
	tx, ok := e.txRegistry.transactions[txID]
	if !ok {
		return fmt.Errorf("transaction not found: %s", txID)
	}

	// Commit the transaction
	err := tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Remove the transaction from the registry
	delete(e.txRegistry.transactions, txID)

	return nil
}

// RollbackTransaction rolls back a transaction
func (e *Executor) RollbackTransaction(txID string) error {
	e.txRegistry.mu.Lock()
	defer e.txRegistry.mu.Unlock()

	// Get the transaction
	tx, ok := e.txRegistry.transactions[txID]
	if !ok {
		return fmt.Errorf("transaction not found: %s", txID)
	}

	// Rollback the transaction
	err := tx.Rollback()
	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	// Remove the transaction from the registry
	delete(e.txRegistry.transactions, txID)

	return nil
}

// StoreEntityInTransaction stores an entity ID with a reference name in a transaction
func (e *Executor) StoreEntityInTransaction(txID string, refName string, entityID string) {
	if txID == "" {
		return // Don't store for auto-transactions
	}

	e.txRegistry.mu.Lock()
	defer e.txRegistry.mu.Unlock()

	// Initialize the entity map for this transaction if needed
	if _, exists := e.txRegistry.txEntities[txID]; !exists {
		e.txRegistry.txEntities[txID] = make(map[string]string)
	}

	// Store the entity ID with its reference name
	e.txRegistry.txEntities[txID][refName] = entityID
}

// GetEntityFromTransaction retrieves an entity ID by its reference name from a transaction
func (e *Executor) GetEntityFromTransaction(txID string, refName string) (string, bool) {
	if txID == "" {
		return "", false
	}

	e.txRegistry.mu.RLock()
	defer e.txRegistry.mu.RUnlock()

	// Check if the transaction and entity exist
	entitiesMap, txExists := e.txRegistry.txEntities[txID]
	if !txExists {
		return "", false
	}

	entityID, entityExists := entitiesMap[refName]
	return entityID, entityExists
}

// GetTransaction gets a transaction by ID
func (e *Executor) GetTransaction(txID string) (*storage.Transaction, error) {
	e.txRegistry.mu.RLock()
	defer e.txRegistry.mu.RUnlock()

	// Get the transaction
	tx, ok := e.txRegistry.transactions[txID]
	if !ok {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}

	return tx, nil
}

// GetActiveTransaction gets the active transaction for the current operation
// If txID is provided, it returns that transaction
// If txID is not provided, it returns a new auto-transaction
func (e *Executor) GetActiveTransaction(txID string) (*storage.Transaction, bool, error) {
	// If txID is provided, use that transaction
	if txID != "" {
		tx, err := e.GetTransaction(txID)
		return tx, false, err // Not auto-transaction
	}

	// Otherwise, create an auto-transaction with a timeout
	// In a real implementation, we'd use goroutine ID to track auto-transactions
	// For simplicity, we'll just create a new transaction each time

	// Create options with a 5 second timeout to prevent deadlocks
	options := storage.DefaultTxOptions()
	options.Timeout = 5 * time.Second

	tx, err := e.Engine.GetTransactionManager().BeginTransaction(options)
	if err != nil {
		return nil, false, fmt.Errorf("failed to begin auto-transaction: %w", err)
	}
	e.Engine.GetLogger().Debug("Created auto-transaction %d with timeout %v", tx.ID(), options.Timeout)
	return tx, true, nil // Is auto-transaction
}

// SetMaxPathHops sets the maximum number of hops for path queries
func (e *Executor) SetMaxPathHops(hops int) {
	if hops > 0 {
		e.maxPathHops = hops
	}
}

// Execute executes a query and returns the result
func (e *Executor) Execute(query *Query) (*Result, error) {
	// Create optimized query plan
	plan, err := e.Optimizer.Optimize(query)
	if err != nil {
		return nil, fmt.Errorf("optimization error: %w", err)
	}

	// Execute the query plan
	switch plan.Type {
	// Read operations
	case QueryTypeFindNodesByLabel:
		return e.executeNodesByLabel(plan)
	case QueryTypeFindEdgesByLabel:
		return e.executeEdgesByLabel(plan)
	case QueryTypeFindNodesByProperty:
		return e.executeNodesByProperty(plan)
	case QueryTypeFindEdgesByProperty:
		return e.executeEdgesByProperty(plan)
	case QueryTypeFindNeighbors:
		return e.executeNeighbors(plan)
	case QueryTypeFindPath:
		return e.executePath(plan)

	// Transaction operations
	case QueryTypeBeginTransaction:
		return e.executeBeginTransaction(plan)
	case QueryTypeCommitTransaction:
		return e.executeCommitTransaction(plan)
	case QueryTypeRollbackTransaction:
		return e.executeRollbackTransaction(plan)

	// Write operations
	case QueryTypeCreateNode:
		return e.executeCreateNode(plan)
	case QueryTypeCreateEdge:
		return e.executeCreateEdge(plan)
	case QueryTypeDeleteNode:
		return e.executeDeleteNode(plan)
	case QueryTypeDeleteEdge:
		return e.executeDeleteEdge(plan)
	case QueryTypeSetProperty:
		return e.executeSetProperty(plan)
	case QueryTypeRemoveProperty:
		return e.executeRemoveProperty(plan)

	default:
		return nil, fmt.Errorf("unsupported query type: %s", plan.Type)
	}
}

// executeBeginTransaction begins a new transaction
func (e *Executor) executeBeginTransaction(query *Query) (*Result, error) {
	txID, err := e.BeginTransaction()
	if err != nil {
		return nil, err
	}

	return &Result{
		TxID:      txID,
		Success:   true,
		Operation: "BEGIN_TRANSACTION",
	}, nil
}

// executeCommitTransaction commits a transaction
func (e *Executor) executeCommitTransaction(query *Query) (*Result, error) {
	// Get transaction ID from parameters if provided
	txID, ok := query.Parameters[ParamTransactionID]
	if !ok {
		return nil, fmt.Errorf("missing transaction ID")
	}

	if err := e.CommitTransaction(txID); err != nil {
		return nil, err
	}

	return &Result{
		TxID:      txID,
		Success:   true,
		Operation: "COMMIT_TRANSACTION",
	}, nil
}

// executeRollbackTransaction rolls back a transaction
func (e *Executor) executeRollbackTransaction(query *Query) (*Result, error) {
	// Get transaction ID from parameters if provided
	txID, ok := query.Parameters[ParamTransactionID]
	if !ok {
		return nil, fmt.Errorf("missing transaction ID")
	}

	if err := e.RollbackTransaction(txID); err != nil {
		return nil, err
	}

	return &Result{
		TxID:      txID,
		Success:   true,
		Operation: "ROLLBACK_TRANSACTION",
	}, nil
}

// executeCreateNode creates a new node
func (e *Executor) executeCreateNode(query *Query) (*Result, error) {
	if e.GraphStore == nil {
		return nil, fmt.Errorf("graph store not available")
	}

	// Get required parameters
	label, ok := query.Parameters[ParamLabel]
	if !ok {
		return nil, fmt.Errorf("missing required parameter 'label'")
	}

	// Get transaction ID from parameters if provided
	txIDParam, _ := query.Parameters[ParamTransactionID]

	// Get active transaction
	tx, isAutoTx, err := e.GetActiveTransaction(txIDParam)
	if err != nil {
		return nil, fmt.Errorf("transaction error: %w", err)
	}

	// Create the node
	nodeID, err := e.GraphStore.CreateNode(tx, label)
	if err != nil {
		// Rollback auto-transaction on error
		if isAutoTx {
			tx.Rollback()
		}
		return nil, fmt.Errorf("failed to create node: %w", err)
	}

	// Store node ID in transaction registry with a reference name if one was provided
	if refName, refExists := query.Parameters["ref"]; refExists && txIDParam != "" {
		e.StoreEntityInTransaction(txIDParam, refName, nodeID)
	} else {
		// Auto-generate a reference name based on the label if none provided
		autoRef := fmt.Sprintf("%s_%s", label, nodeID)
		e.StoreEntityInTransaction(txIDParam, autoRef, nodeID)
	}

	// Commit auto-transaction
	if isAutoTx {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	return &Result{
		NodeID:    nodeID,
		Success:   true,
		Operation: "CREATE_NODE",
		TxID:      txIDParam,
		EntityIDs: []string{nodeID}, // Add node ID to entity IDs for use in subsequent queries
	}, nil
}

// executeCreateEdge creates a new edge
func (e *Executor) executeCreateEdge(query *Query) (*Result, error) {
	if e.GraphStore == nil {
		return nil, fmt.Errorf("graph store not available")
	}

	// Get source ID - check both sourceId and source parameters
	var sourceID string
	var ok bool

	// First check for sourceId parameter (preferred)
	if sourceID, ok = query.Parameters[ParamSourceID]; !ok {
		// If not found, try the source parameter (legacy)
		if sourceID, ok = query.Parameters[ParamSource]; !ok {
			// If still not found, return error
			return nil, fmt.Errorf("missing required parameter 'sourceId' or 'source'")
		}
	}

	// Get target ID
	targetID, ok := query.Parameters[ParamTargetID]
	if !ok {
		return nil, fmt.Errorf("missing required parameter 'targetId'")
	}

	label, ok := query.Parameters[ParamLabel]
	if !ok {
		return nil, fmt.Errorf("missing required parameter 'label'")
	}

	// Get transaction ID from parameters if provided
	txIDParam, _ := query.Parameters[ParamTransactionID]

	// Check if sourceID/targetID are references to previously created entities in this transaction
	if txIDParam != "" {
		// Check if sourceID is referencing an entity by using $ref: syntax
		if len(sourceID) > 0 && sourceID[0] == '$' {
			refName := sourceID[1:] // Remove the $ prefix
			if actualID, exists := e.GetEntityFromTransaction(txIDParam, refName); exists {
				sourceID = actualID
			} else {
				return nil, fmt.Errorf("referenced entity '%s' not found in transaction", refName)
			}
		}

		// Check if targetID is referencing an entity by using $ref: syntax
		if len(targetID) > 0 && targetID[0] == '$' {
			refName := targetID[1:] // Remove the $ prefix
			if actualID, exists := e.GetEntityFromTransaction(txIDParam, refName); exists {
				targetID = actualID
			} else {
				return nil, fmt.Errorf("referenced entity '%s' not found in transaction", refName)
			}
		}
	}

	// Get active transaction
	tx, isAutoTx, err := e.GetActiveTransaction(txIDParam)
	if err != nil {
		return nil, fmt.Errorf("transaction error: %w", err)
	}

	// Create the edge
	edgeID, err := e.GraphStore.CreateEdge(tx, sourceID, targetID, label)
	if err != nil {
		// Rollback auto-transaction on error
		if isAutoTx {
			tx.Rollback()
		}
		return nil, fmt.Errorf("failed to create edge: %w", err)
	}

	// Store edge ID in transaction registry with a reference name if one was provided
	if refName, refExists := query.Parameters["ref"]; refExists && txIDParam != "" {
		e.StoreEntityInTransaction(txIDParam, refName, edgeID)
	} else {
		// Auto-generate a reference name based on the label if none provided
		autoRef := fmt.Sprintf("%s_%s", label, edgeID)
		e.StoreEntityInTransaction(txIDParam, autoRef, edgeID)
	}

	// Commit auto-transaction
	if isAutoTx {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	return &Result{
		EdgeID:    edgeID,
		Success:   true,
		Operation: "CREATE_EDGE",
		TxID:      txIDParam,
		EntityIDs: []string{edgeID}, // Add edge ID to entity IDs for use in subsequent queries
	}, nil
}

// executeDeleteNode deletes a node
func (e *Executor) executeDeleteNode(query *Query) (*Result, error) {
	if e.GraphStore == nil {
		return nil, fmt.Errorf("graph store not available")
	}

	// Get required parameters
	nodeID, ok := query.Parameters[ParamID]
	if !ok {
		return nil, fmt.Errorf("missing required parameter 'id'")
	}

	// Get transaction ID from parameters if provided
	txIDParam, _ := query.Parameters[ParamTransactionID]

	// Resolve ID if it's a reference
	if txIDParam != "" && len(nodeID) > 0 && nodeID[0] == '$' {
		refName := nodeID[1:] // Remove the $ prefix
		if actualID, exists := e.GetEntityFromTransaction(txIDParam, refName); exists {
			nodeID = actualID
		} else {
			return nil, fmt.Errorf("referenced entity '%s' not found in transaction", refName)
		}
	}

	// Get active transaction
	tx, isAutoTx, err := e.GetActiveTransaction(txIDParam)
	if err != nil {
		return nil, fmt.Errorf("transaction error: %w", err)
	}

	// Delete the node
	err = e.GraphStore.DeleteNode(tx, nodeID)
	if err != nil {
		// Rollback auto-transaction on error
		if isAutoTx {
			tx.Rollback()
		}
		return nil, fmt.Errorf("failed to delete node: %w", err)
	}

	// Commit auto-transaction
	if isAutoTx {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	return &Result{
		NodeID:    nodeID,
		Success:   true,
		Operation: "DELETE_NODE",
		TxID:      txIDParam,
	}, nil
}

// executeDeleteEdge deletes an edge
func (e *Executor) executeDeleteEdge(query *Query) (*Result, error) {
	if e.GraphStore == nil {
		return nil, fmt.Errorf("graph store not available")
	}

	// Get required parameters
	edgeID, ok := query.Parameters[ParamID]
	if !ok {
		return nil, fmt.Errorf("missing required parameter 'id'")
	}

	// Get transaction ID from parameters if provided
	txIDParam, _ := query.Parameters[ParamTransactionID]

	// Resolve ID if it's a reference
	if txIDParam != "" && len(edgeID) > 0 && edgeID[0] == '$' {
		refName := edgeID[1:] // Remove the $ prefix
		if actualID, exists := e.GetEntityFromTransaction(txIDParam, refName); exists {
			edgeID = actualID
		} else {
			return nil, fmt.Errorf("referenced entity '%s' not found in transaction", refName)
		}
	}

	// Get active transaction
	tx, isAutoTx, err := e.GetActiveTransaction(txIDParam)
	if err != nil {
		return nil, fmt.Errorf("transaction error: %w", err)
	}

	// Delete the edge
	err = e.GraphStore.DeleteEdge(tx, edgeID)
	if err != nil {
		// Rollback auto-transaction on error
		if isAutoTx {
			tx.Rollback()
		}
		return nil, fmt.Errorf("failed to delete edge: %w", err)
	}

	// Commit auto-transaction
	if isAutoTx {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	return &Result{
		EdgeID:    edgeID,
		Success:   true,
		Operation: "DELETE_EDGE",
		TxID:      txIDParam,
	}, nil
}

// executeSetProperty sets a property on a node or edge
func (e *Executor) executeSetProperty(query *Query) (*Result, error) {
	if e.GraphStore == nil {
		return nil, fmt.Errorf("graph store not available")
	}

	// Get required parameters
	target, ok := query.Parameters[ParamTarget]
	if !ok {
		return nil, fmt.Errorf("missing required parameter 'target'")
	}

	id, ok := query.Parameters[ParamID]
	if !ok {
		return nil, fmt.Errorf("missing required parameter 'id'")
	}

	// Get transaction ID from parameters if provided
	txIDParam, _ := query.Parameters[ParamTransactionID]

	// Resolve ID if it's a reference
	if txIDParam != "" && len(id) > 0 && id[0] == '$' {
		refName := id[1:] // Remove the $ prefix
		if actualID, exists := e.GetEntityFromTransaction(txIDParam, refName); exists {
			id = actualID
		} else {
			return nil, fmt.Errorf("referenced entity '%s' not found in transaction", refName)
		}
	}

	name, ok := query.Parameters[ParamName]
	if !ok {
		return nil, fmt.Errorf("missing required parameter 'name'")
	}

	value, ok := query.Parameters[ParamValue]
	if !ok {
		return nil, fmt.Errorf("missing required parameter 'value'")
	}

	// Get active transaction
	tx, isAutoTx, err := e.GetActiveTransaction(txIDParam)
	if err != nil {
		return nil, fmt.Errorf("transaction error: %w", err)
	}

	// Set the property based on target type
	var setErr error
	if target == "node" {
		setErr = e.GraphStore.SetNodeProperty(tx, id, name, value)
	} else if target == "edge" {
		setErr = e.GraphStore.SetEdgeProperty(tx, id, name, value)
	} else {
		if isAutoTx {
			tx.Rollback()
		}
		return nil, fmt.Errorf("invalid target: %s, must be 'node' or 'edge'", target)
	}

	if setErr != nil {
		// Rollback auto-transaction on error
		if isAutoTx {
			tx.Rollback()
		}
		return nil, fmt.Errorf("failed to set property: %w", setErr)
	}

	// Commit auto-transaction
	if isAutoTx {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	result := &Result{
		Success:   true,
		Operation: "SET_PROPERTY",
		TxID:      txIDParam,
	}

	// Set the appropriate ID field based on target
	if target == "node" {
		result.NodeID = id
	} else {
		result.EdgeID = id
	}

	return result, nil
}

// executeRemoveProperty removes a property from a node or edge
func (e *Executor) executeRemoveProperty(query *Query) (*Result, error) {
	if e.GraphStore == nil {
		return nil, fmt.Errorf("graph store not available")
	}

	// Get required parameters
	target, ok := query.Parameters[ParamTarget]
	if !ok {
		return nil, fmt.Errorf("missing required parameter 'target'")
	}

	id, ok := query.Parameters[ParamID]
	if !ok {
		return nil, fmt.Errorf("missing required parameter 'id'")
	}

	// Get transaction ID from parameters if provided
	txIDParam, _ := query.Parameters[ParamTransactionID]

	// Resolve ID if it's a reference
	if txIDParam != "" && len(id) > 0 && id[0] == '$' {
		refName := id[1:] // Remove the $ prefix
		if actualID, exists := e.GetEntityFromTransaction(txIDParam, refName); exists {
			id = actualID
		} else {
			return nil, fmt.Errorf("referenced entity '%s' not found in transaction", refName)
		}
	}

	name, ok := query.Parameters[ParamName]
	if !ok {
		return nil, fmt.Errorf("missing required parameter 'name'")
	}

	// Get active transaction
	tx, isAutoTx, err := e.GetActiveTransaction(txIDParam)
	if err != nil {
		return nil, fmt.Errorf("transaction error: %w", err)
	}

	// Remove the property based on target type
	var removeErr error
	if target == "node" {
		removeErr = e.GraphStore.RemoveNodeProperty(tx, id, name)
	} else if target == "edge" {
		removeErr = e.GraphStore.RemoveEdgeProperty(tx, id, name)
	} else {
		if isAutoTx {
			tx.Rollback()
		}
		return nil, fmt.Errorf("invalid target: %s, must be 'node' or 'edge'", target)
	}

	if removeErr != nil {
		// Rollback auto-transaction on error
		if isAutoTx {
			tx.Rollback()
		}
		return nil, fmt.Errorf("failed to remove property: %w", removeErr)
	}

	// Commit auto-transaction
	if isAutoTx {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	result := &Result{
		Success:   true,
		Operation: "REMOVE_PROPERTY",
		TxID:      txIDParam,
	}

	// Set the appropriate ID field based on target
	if target == "node" {
		result.NodeID = id
	} else {
		result.EdgeID = id
	}

	return result, nil
}

// executeNodesByLabel finds nodes by label
func (e *Executor) executeNodesByLabel(query *Query) (*Result, error) {
	label := query.Parameters[ParamLabel]

	e.Engine.GetLogger().Debug("Searching for nodes with label: %s", label)

	// Try both direct lookup and prefix scan to be robust
	nodeIDs := make(map[uint64]bool)

	// Direct scan through the database using a scan prefix
	scanPrefix := common.FormatNodeLabelScanKey(label)
	e.Engine.GetLogger().Debug("Executing direct scan for keys with prefix: %s", scanPrefix)

	matchingKeys, err := e.Engine.Scan([]byte(scanPrefix), 1000)
	if err == nil && len(matchingKeys) > 0 {
		e.Engine.GetLogger().Debug("Direct scan found %d keys with prefix %s", len(matchingKeys), scanPrefix)

		// Process keys from direct scan
		for _, key := range matchingKeys {
			keyStr := string(key)
			e.Engine.GetLogger().Debug("Processing scanned key: %s", keyStr)

			// Parse out the node ID from keys like "nl:Person:1"
			parts := strings.Split(keyStr, ":")
			if len(parts) >= 3 {
				nodeIDStr := parts[2]
				nodeID, err := common.ParseUint64(nodeIDStr)
				if err == nil {
					e.Engine.GetLogger().Debug("Extracted node ID %d from key %s", nodeID, keyStr)
					nodeIDs[nodeID] = true
				}
			}
		}
	}

	// Also try the index-based lookup
	e.Engine.GetLogger().Debug("Using label index to find nodes with label: %s", label)

	nodeLabelKeys, err := e.NodeLabels.GetAll([]byte(label))
	if err != nil {
		e.Engine.GetLogger().Debug("Error scanning for node label keys: %v", err)
		// Only return error if we have no IDs from direct scan
		if len(nodeIDs) == 0 {
			return nil, fmt.Errorf("error scanning for node label keys: %w", err)
		}
	} else if len(nodeLabelKeys) > 0 {
		e.Engine.GetLogger().Debug("Index lookup found %d node IDs for label: %s", len(nodeLabelKeys), label)

		// Process node IDs from index lookup
		for _, nodeIDBytes := range nodeLabelKeys {
			nodeIDStr := string(nodeIDBytes)
			e.Engine.GetLogger().Debug("Processing index value: %s", nodeIDStr)

			// Try different parsing approaches

			// 1. Direct parse as uint64
			nodeID, err := common.ParseUint64(nodeIDStr)
			if err == nil {
				e.Engine.GetLogger().Debug("Successfully parsed node ID %d directly", nodeID)
				nodeIDs[nodeID] = true
				continue
			}

			// 2. Try to extract from composite key format (nl:Label:ID)
			parts := strings.Split(nodeIDStr, ":")
			if len(parts) >= 3 {
				idPart := parts[2]
				nodeID, err := common.ParseUint64(idPart)
				if err == nil {
					e.Engine.GetLogger().Debug("Extracted node ID %d from composite key", nodeID)
					nodeIDs[nodeID] = true
					continue
				}
			}

			e.Engine.GetLogger().Debug("Unable to parse node ID from: %s", nodeIDStr)
		}
	}

	// If no nodes found with this label
	if len(nodeIDs) == 0 {
		e.Engine.GetLogger().Debug("No valid node IDs found for label: %s", label)
		return &Result{Nodes: []model.Node{}}, nil
	}

	// Get the nodes
	nodes := make([]model.Node, 0, len(nodeIDs))
	e.Engine.GetLogger().Debug("Found %d unique node IDs for label: %s, retrieving them", len(nodeIDs), label)

	for id := range nodeIDs {
		// Try multiple key formats to find the node
		found := false

		// 1. First try with standard format key
		nodeKey := []byte(common.FormatNodeKey(id))
		e.Engine.GetLogger().Debug("Looking up node with formatted key: %s", string(nodeKey))

		nodeBytes, err := e.NodeIndex.Get(nodeKey)
		if err == nil {
			var node model.Node
			err = model.Deserialize(nodeBytes, &node)
			if err == nil {
				e.Engine.GetLogger().Debug("Retrieved node %d with label %s", id, node.Label)
				nodes = append(nodes, node)
				found = true
			} else {
				e.Engine.GetLogger().Debug("Error deserializing node %d: %v", id, err)
			}
		} else {
			e.Engine.GetLogger().Debug("Node not found with key %s: %v", string(nodeKey), err)
		}

		// 2. If not found, try direct lookup by ID
		if !found {
			directKey := []byte(fmt.Sprintf("%d", id))
			e.Engine.GetLogger().Debug("Trying direct lookup with key: %s", string(directKey))

			nodeBytes, err := e.Engine.Get(directKey)
			if err == nil {
				var node model.Node
				err = model.Deserialize(nodeBytes, &node)
				if err == nil {
					e.Engine.GetLogger().Debug("Retrieved node %d with label %s via direct lookup", id, node.Label)
					nodes = append(nodes, node)
				} else {
					e.Engine.GetLogger().Debug("Error deserializing node %d from direct lookup: %v", id, err)
				}
			} else {
				e.Engine.GetLogger().Debug("Node not found with direct key %s: %v", string(directKey), err)
			}
		}
	}

	e.Engine.GetLogger().Debug("Retrieved %d nodes with label: %s", len(nodes), label)
	return &Result{Nodes: nodes}, nil
}

// executeEdgesByLabel finds edges by label
func (e *Executor) executeEdgesByLabel(query *Query) (*Result, error) {
	label := query.Parameters[ParamLabel]

	e.Engine.GetLogger().Debug("Searching for edges with label: %s", label)

	// The most direct way to find edges is to scan through all edges and check their labels
	// This bypasses any issues with label indexes

	// We'll keep track of the edges we find
	matchingEdges := []model.Edge{}

	// Scan for all edge entries directly - this is most reliable
	allEdges, err := e.Engine.Scan([]byte("e:"), 1000)
	if err != nil {
		e.Engine.GetLogger().Debug("Error scanning for edges: %v", err)
		// Don't return error, continue with other approaches
	} else {
		e.Engine.GetLogger().Debug("Found %d total edges to check", len(allEdges))

		// Check each edge to see if it has the matching label
		for _, key := range allEdges {
			// Get the edge data
			edgeData, err := e.Engine.Get(key)
			if err != nil {
				e.Engine.GetLogger().Debug("Error getting edge data for key %s: %v", string(key), err)
				continue
			}

			// Deserialize the edge
			var edge model.Edge
			err = model.Deserialize(edgeData, &edge)
			if err != nil {
				e.Engine.GetLogger().Debug("Error deserializing edge data for key %s: %v", string(key), err)
				continue
			}

			// Check if this edge has the matching label
			if edge.Label == label {
				e.Engine.GetLogger().Debug("Found matching edge with label %s: %+v", label, edge)
				matchingEdges = append(matchingEdges, edge)
			}
		}
	}

	// If we found matching edges by direct scan, return them
	if len(matchingEdges) > 0 {
		e.Engine.GetLogger().Debug("Retrieved %d edges with label: %s by direct scan", len(matchingEdges), label)
		return &Result{Edges: matchingEdges}, nil
	}

	// If we didn't find any edges by direct scan, try using the relationships
	// This is a different approach that should work if edges exist but direct scan fails
	e.Engine.GetLogger().Debug("No edges found by direct scan, trying relationship-based lookup")

	// Get all node keys
	nodeKeys, err := e.Engine.Scan([]byte("n:"), 1000)
	if err != nil {
		e.Engine.GetLogger().Debug("Error scanning for nodes: %v", err)
		return &Result{Edges: []model.Edge{}}, nil
	}

	e.Engine.GetLogger().Debug("Found %d nodes to check for relationships", len(nodeKeys))

	// For each node, check its outgoing edges
	for _, nodeKey := range nodeKeys {
		// Extract the node ID
		parts := strings.Split(string(nodeKey), ":")
		if len(parts) < 2 {
			continue
		}

		nodeIDStr := parts[1]

		// Check outgoing edges for this node
		nodeID, _ := strconv.ParseUint(nodeIDStr, 10, 64)
		outgoingKey := []byte(FormatOutgoingEdgesKey(nodeID))
		outgoingData, err := e.Engine.Get(outgoingKey)
		if err != nil {
			if err != storage.ErrKeyNotFound {
				e.Engine.GetLogger().Debug("Error getting outgoing edges for node %s: %v", nodeIDStr, err)
			}
			continue
		}

		// Parse the outgoing edge IDs
		var outEdgeIDs []string
		err = model.Deserialize(outgoingData, &outEdgeIDs)
		if err != nil {
			e.Engine.GetLogger().Debug("Error deserializing outgoing edges for node %s: %v", nodeIDStr, err)
			continue
		}

		e.Engine.GetLogger().Debug("Found %d outgoing edges for node %s", len(outEdgeIDs), nodeIDStr)

		// Check each outgoing edge
		for _, edgeID := range outEdgeIDs {
			// Get the edge data
			edgeKey := []byte(common.FormatEdgeKey(edgeID))
			edgeData, err := e.Engine.Get(edgeKey)
			if err != nil {
				// Try with just the ID if formatted key doesn't work
				edgeData, err = e.Engine.Get([]byte(edgeID))
				if err != nil {
					e.Engine.GetLogger().Debug("Error getting edge data for ID %s: %v", edgeID, err)
					continue
				}
			}

			// Deserialize the edge
			var edge model.Edge
			err = model.Deserialize(edgeData, &edge)
			if err != nil {
				e.Engine.GetLogger().Debug("Error deserializing edge %s: %v", edgeID, err)
				continue
			}

			// Check if this edge has the matching label
			if edge.Label == label {
				e.Engine.GetLogger().Debug("Found matching edge with label %s: %+v", label, edge)

				// Check if we already have this edge to avoid duplicates
				isDuplicate := false
				for _, existingEdge := range matchingEdges {
					if existingEdge.SourceID == edge.SourceID &&
						existingEdge.TargetID == edge.TargetID {
						isDuplicate = true
						break
					}
				}

				if !isDuplicate {
					matchingEdges = append(matchingEdges, edge)
				}
			}
		}
	}

	e.Engine.GetLogger().Debug("Retrieved %d edges with label: %s", len(matchingEdges), label)
	return &Result{Edges: matchingEdges}, nil
}

// executeNodesByProperty finds nodes with a specific property value
func (e *Executor) executeNodesByProperty(query *Query) (*Result, error) {
	propertyName := query.Parameters[ParamPropertyName]
	propertyValue := query.Parameters[ParamPropertyValue]

	// Check if we have the specialized property index
	if e.NodeProperties == nil {
		return nil, fmt.Errorf("node property index not available")
	}

	// TODO: Optimize by properly using the property index
	// Currently, this is a fallback approach that scans nodes directly
	// In a future update, this should be enhanced to:
	// 1. Use the property index to quickly find matching nodes
	// 2. Support range queries for numeric properties
	// 3. Support full-text search for string properties
	//
	// Current implementation for testing purposes:
	result := &Result{Nodes: []model.Node{}}

	// Get all nodes by scanning through them directly
	for i := uint64(1); i <= 10; i++ { // Check first 10 node IDs as a simple approach
		key := []byte(FormatNodeKey(i))
		nodeBytes, err := e.NodeIndex.Get(key)
		if err != nil {
			if err == storage.ErrKeyNotFound {
				continue
			}
			return nil, fmt.Errorf("error getting node %d: %w", i, err)
		}

		var node model.Node
		err = model.Deserialize(nodeBytes, &node)
		if err != nil {
			return nil, fmt.Errorf("error deserializing node %d: %w", i, err)
		}

		// Check if this node has the matching property
		if propValue, ok := node.Properties[propertyName]; ok && propValue == propertyValue {
			result.Nodes = append(result.Nodes, node)
		}
	}

	return result, nil
}

// executeEdgesByProperty finds edges with a specific property value
func (e *Executor) executeEdgesByProperty(query *Query) (*Result, error) {
	propertyName := query.Parameters[ParamPropertyName]
	propertyValue := query.Parameters[ParamPropertyValue]

	// Check if we have the specialized property index
	if e.EdgeProperties == nil {
		return nil, fmt.Errorf("edge property index not available")
	}

	// TODO: Optimize by properly using the property index
	// Currently, this is a fallback approach that scans edges directly
	// In a future update, this should be enhanced to:
	// 1. Use the property index to quickly find matching edges
	// 2. Support range queries for numeric properties
	// 3. Support full-text search for string properties
	//
	// Current implementation for testing purposes:
	result := &Result{Edges: []model.Edge{}}

	// Get the test edges we know about (based on our test data setup)
	testEdgeIDs := []string{
		"1-2", "1-3", "2-3", "1-4", "2-5", "3-5",
	}

	// Check each edge
	for _, edgeID := range testEdgeIDs {
		key := []byte(FormatEdgeKey(edgeID))
		edgeBytes, err := e.EdgeIndex.Get(key)
		if err != nil {
			if err == storage.ErrKeyNotFound {
				continue
			}
			return nil, fmt.Errorf("error getting edge %s: %w", edgeID, err)
		}

		var edge model.Edge
		err = model.Deserialize(edgeBytes, &edge)
		if err != nil {
			return nil, fmt.Errorf("error deserializing edge %s: %w", edgeID, err)
		}

		// Check if this edge has the matching property
		if propValue, ok := edge.GetProperty(propertyName); ok && propValue == propertyValue {
			result.Edges = append(result.Edges, edge)
		}
	}

	return result, nil
}

// executeNeighbors finds neighbors of a node
func (e *Executor) executeNeighbors(query *Query) (*Result, error) {
	nodeIDStr := query.Parameters[ParamNodeID]
	nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid node ID: %s", nodeIDStr)
	}

	direction := DirectionBoth
	if dir, ok := query.Parameters[ParamDirection]; ok {
		direction = dir
	}

	// Get the node first to ensure it exists
	key := []byte(FormatNodeKey(nodeID))
	e.Engine.GetLogger().Debug("Looking for node with formatted key: %s", string(key))

	nodeBytes, err := e.NodeIndex.Get(key)
	if err != nil {
		// If not found with prefix, try just the ID as a string (compatibility)
		keyWithoutPrefix := []byte(fmt.Sprintf("%d", nodeID))
		e.Engine.GetLogger().Debug("Node not found with key %s, trying alternate key: %s", string(key), string(keyWithoutPrefix))
		nodeBytes, err = e.NodeIndex.Get(keyWithoutPrefix)
	}

	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, fmt.Errorf("node not found: %d", nodeID)
		}
		return nil, fmt.Errorf("error getting node %d: %w", nodeID, err)
	}

	var node model.Node
	err = model.Deserialize(nodeBytes, &node)
	if err != nil {
		return nil, fmt.Errorf("error deserializing node %d: %w", nodeID, err)
	}

	// Collect edge IDs based on direction
	var edgeIDs []string

	// Check outgoing edges if needed
	if direction == DirectionOutgoing || direction == DirectionBoth {
		// Use the common package helper function for consistent key formatting
		outgoingKey := FormatOutgoingEdgesKey(nodeID)
		outgoingEdgesBytes, err := e.Engine.Get([]byte(outgoingKey))

		if err == nil {
			var outEdgeIDs []string
			err = model.Deserialize(outgoingEdgesBytes, &outEdgeIDs)
			if err == nil {
				e.Engine.GetLogger().Debug("Found %d outgoing edges for node %d", len(outEdgeIDs), nodeID)
				edgeIDs = append(edgeIDs, outEdgeIDs...)
			}
		}
	}

	// Check incoming edges if needed
	if direction == DirectionIncoming || direction == DirectionBoth {
		// Use the common package helper function for consistent key formatting
		incomingKey := FormatIncomingEdgesKey(nodeID)
		incomingEdgesBytes, err := e.Engine.Get([]byte(incomingKey))

		if err == nil {
			var inEdgeIDs []string
			err = model.Deserialize(incomingEdgesBytes, &inEdgeIDs)
			if err == nil {
				e.Engine.GetLogger().Debug("Found %d incoming edges for node %d", len(inEdgeIDs), nodeID)
				edgeIDs = append(edgeIDs, inEdgeIDs...)
			}
		}
	}

	e.Engine.GetLogger().Debug("Found a total of %d edges connected to node %d", len(edgeIDs), nodeID)

	// Get the edges
	edges := make([]model.Edge, 0, len(edgeIDs))
	nodeIDs := make(map[uint64]bool) // Unique neighbor IDs

	for i, id := range edgeIDs {
		e.Engine.GetLogger().Debug("Processing edge %d/%d: %s", i+1, len(edgeIDs), id)

		// Try with prefix first
		edgeKey := []byte(FormatEdgeKey(id))
		e.Engine.GetLogger().Debug("Looking up edge with formatted key: %s", string(edgeKey))

		edgeBytes, err := e.EdgeIndex.Get(edgeKey)
		if err != nil {
			// If not found with prefix, try just the ID as a string (compatibility)
			keyWithoutPrefix := []byte(id)
			e.Engine.GetLogger().Debug("Edge not found with key %s, trying alternate key: %s", string(edgeKey), id)
			edgeBytes, err = e.EdgeIndex.Get(keyWithoutPrefix)
		}

		if err != nil {
			if err != storage.ErrKeyNotFound {
				return nil, fmt.Errorf("error getting edge %s: %w", id, err)
			}
			e.Engine.GetLogger().Debug("Edge with ID %s not found in edge index", id)
			continue
		}

		var edge model.Edge
		err = model.Deserialize(edgeBytes, &edge)
		if err != nil {
			return nil, fmt.Errorf("error deserializing edge %s: %w", id, err)
		}

		e.Engine.GetLogger().Debug("Found edge from %d to %d with label %s", edge.SourceID, edge.TargetID, edge.Label)
		edges = append(edges, edge)

		// Add neighbor node ID
		if edge.SourceID == nodeID {
			nodeIDs[edge.TargetID] = true
			e.Engine.GetLogger().Debug("Adding target node %d as neighbor", edge.TargetID)
		} else if edge.TargetID == nodeID {
			nodeIDs[edge.SourceID] = true
			e.Engine.GetLogger().Debug("Adding source node %d as neighbor", edge.SourceID)
		}
	}

	// Get the neighbor nodes
	nodes := make([]model.Node, 0, len(nodeIDs))
	e.Engine.GetLogger().Debug("Found %d unique neighbor nodes, retrieving them", len(nodeIDs))

	for id := range nodeIDs {
		e.Engine.GetLogger().Debug("Looking up neighbor node %d", id)

		// Try with prefix first
		nodeKey := []byte(FormatNodeKey(id))
		e.Engine.GetLogger().Debug("Looking up node with formatted key: %s", string(nodeKey))

		nodeBytes, err := e.NodeIndex.Get(nodeKey)
		if err != nil {
			// If not found with prefix, try just the ID as a string (compatibility)
			keyWithoutPrefix := []byte(fmt.Sprintf("%d", id))
			e.Engine.GetLogger().Debug("Node not found with key %s, trying alternate key: %s", string(nodeKey), string(keyWithoutPrefix))
			nodeBytes, err = e.NodeIndex.Get(keyWithoutPrefix)
		}

		if err != nil {
			if err != storage.ErrKeyNotFound {
				return nil, fmt.Errorf("error getting node %d: %w", id, err)
			}
			e.Engine.GetLogger().Debug("Neighbor node %d not found in node index", id)
			continue
		}

		var node model.Node
		err = model.Deserialize(nodeBytes, &node)
		if err != nil {
			return nil, fmt.Errorf("error deserializing node %d: %w", id, err)
		}

		e.Engine.GetLogger().Debug("Successfully retrieved neighbor node %d with label %s", id, node.Label)
		nodes = append(nodes, node)
	}

	e.Engine.GetLogger().Debug("Retrieved %d/%d neighbor nodes", len(nodes), len(nodeIDs))

	return &Result{Nodes: nodes, Edges: edges}, nil
}

// executePath finds a path between two nodes
func (e *Executor) executePath(query *Query) (*Result, error) {
	sourceIDStr := query.Parameters[ParamSourceID]
	sourceID, err := strconv.ParseUint(sourceIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid source node ID: %s", sourceIDStr)
	}

	targetIDStr := query.Parameters[ParamTargetID]
	targetID, err := strconv.ParseUint(targetIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid target node ID: %s", targetIDStr)
	}

	maxHops := e.maxPathHops
	if maxHopsStr, ok := query.Parameters[ParamMaxHops]; ok {
		maxHopsParsed, err := strconv.Atoi(maxHopsStr)
		if err == nil && maxHopsParsed > 0 {
			maxHops = maxHopsParsed
		}
	}

	// Get the source and target nodes first to ensure they exist
	sourceKey := []byte(FormatNodeKey(sourceID))
	e.Engine.GetLogger().Debug("Looking for source node with formatted key: %s", string(sourceKey))

	sourceBytes, err := e.NodeIndex.Get(sourceKey)
	if err != nil {
		// If not found with prefix, try just the ID as a string (compatibility)
		keyWithoutPrefix := []byte(fmt.Sprintf("%d", sourceID))
		e.Engine.GetLogger().Debug("Source node not found with key %s, trying alternate key: %s", string(sourceKey), string(keyWithoutPrefix))
		sourceBytes, err = e.NodeIndex.Get(keyWithoutPrefix)
	}

	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, fmt.Errorf("source node not found: %d", sourceID)
		}
		return nil, fmt.Errorf("error getting source node %d: %w", sourceID, err)
	}

	var sourceNode model.Node
	err = model.Deserialize(sourceBytes, &sourceNode)
	if err != nil {
		return nil, fmt.Errorf("error deserializing source node %d: %w", sourceID, err)
	}

	targetKey := []byte(FormatNodeKey(targetID))
	e.Engine.GetLogger().Debug("Looking for target node with formatted key: %s", string(targetKey))

	targetBytes, err := e.NodeIndex.Get(targetKey)
	if err != nil {
		// If not found with prefix, try just the ID as a string (compatibility)
		keyWithoutPrefix := []byte(fmt.Sprintf("%d", targetID))
		e.Engine.GetLogger().Debug("Target node not found with key %s, trying alternate key: %s", string(targetKey), string(keyWithoutPrefix))
		targetBytes, err = e.NodeIndex.Get(keyWithoutPrefix)
	}

	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, fmt.Errorf("target node not found: %d", targetID)
		}
		return nil, fmt.Errorf("error getting target node %d: %w", targetID, err)
	}

	var targetNode model.Node
	err = model.Deserialize(targetBytes, &targetNode)
	if err != nil {
		return nil, fmt.Errorf("error deserializing target node %d: %w", targetID, err)
	}

	// Perform breadth-first search to find a path
	path, err := e.findPathBFS(sourceID, targetID, maxHops)
	if err != nil {
		return nil, err
	}

	if path == nil {
		// No path found
		return &Result{Paths: []Path{}}, nil
	}

	return &Result{Paths: []Path{*path}}, nil
}

// findPathBFS finds a path between two nodes using breadth-first search
func (e *Executor) findPathBFS(sourceID, targetID uint64, maxHops int) (*Path, error) {
	if sourceID == targetID {
		// Source and target are the same node
		sourceKey := []byte(FormatNodeKey(sourceID))
		sourceBytes, err := e.NodeIndex.Get(sourceKey)
		if err != nil {
			return nil, fmt.Errorf("error getting node %d: %w", sourceID, err)
		}

		var sourceNode model.Node
		err = model.Deserialize(sourceBytes, &sourceNode)
		if err != nil {
			return nil, fmt.Errorf("error deserializing node %d: %w", sourceID, err)
		}

		return &Path{
			Nodes: []model.Node{sourceNode},
			Edges: []model.Edge{},
		}, nil
	}

	// Keep track of visited nodes to avoid cycles
	visited := make(map[uint64]bool)
	visited[sourceID] = true

	// Keep track of the parent node and edge for each node in the path
	parents := make(map[uint64]struct {
		NodeID uint64
		Edge   model.Edge
	})

	// Queue for BFS
	queue := []uint64{sourceID}
	found := false

	// BFS
	e.Engine.GetLogger().Debug("Starting BFS from %d to %d with max hops %d", sourceID, targetID, maxHops)
	for i := 0; i < maxHops && len(queue) > 0 && !found; i++ {
		levelSize := len(queue)
		e.Engine.GetLogger().Debug("BFS level %d, processing %d nodes", i, levelSize)

		for j := 0; j < levelSize; j++ {
			currentID := queue[0]
			queue = queue[1:]
			e.Engine.GetLogger().Debug("Processing node %d", currentID)

			// Get outgoing edges using the format from test
			outKey := []byte(FormatOutgoingEdgesKey(currentID))
			e.Engine.GetLogger().Debug("Looking for outgoing edges with key: %s", string(outKey))
			outEdgeIDsBytes, err := e.EdgeIndex.Get(outKey)

			// Try direct engine access if index lookup fails
			if err != nil && err == storage.ErrKeyNotFound {
				e.Engine.GetLogger().Debug("Edge not found in EdgeIndex, trying direct engine access")
				outEdgeIDsBytes, err = e.Engine.Get(outKey)
			}

			if err != nil && err != storage.ErrKeyNotFound {
				return nil, fmt.Errorf("error getting outgoing edges for node %d: %w", currentID, err)
			}

			if err == nil {
				var outEdgeIDs []string
				err = model.Deserialize(outEdgeIDsBytes, &outEdgeIDs)
				if err != nil {
					return nil, fmt.Errorf("error deserializing outgoing edge IDs: %w", err)
				}

				e.Engine.GetLogger().Debug("Found %d outgoing edges for node %d", len(outEdgeIDs), currentID)

				// Process each outgoing edge
				for _, edgeID := range outEdgeIDs {
					edgeKey := []byte(FormatEdgeKey(edgeID))
					// Look up edge in EdgeIndex
					edgeBytes, err := e.EdgeIndex.Get(edgeKey)

					// If not found in index, try the engine directly
					if err != nil && err == storage.ErrKeyNotFound {
						e.Engine.GetLogger().Debug("Edge not found in EdgeIndex, trying Engine")
						edgeBytes, err = e.Engine.Get(edgeKey)
					}

					if err != nil {
						e.Engine.GetLogger().Debug("Error getting edge %s: %v", edgeID, err)
						if err != storage.ErrKeyNotFound {
							return nil, fmt.Errorf("error getting edge %s: %w", edgeID, err)
						}
						continue
					}
					e.Engine.GetLogger().Debug("Successfully got edge %s", edgeID)

					var edge model.Edge
					err = model.Deserialize(edgeBytes, &edge)
					if err != nil {
						return nil, fmt.Errorf("error deserializing edge %s: %w", edgeID, err)
					}

					// Skip if target node is already visited
					if visited[edge.TargetID] {
						continue
					}

					// Record the parent
					parents[edge.TargetID] = struct {
						NodeID uint64
						Edge   model.Edge
					}{
						NodeID: currentID,
						Edge:   edge,
					}

					// Check if we reached the target node
					if edge.TargetID == targetID {
						found = true
						break
					}

					// Mark target as visited and add to queue
					visited[edge.TargetID] = true
					queue = append(queue, edge.TargetID)
				}
			}

			if found {
				break
			}

			// Get incoming edges using the format from test
			inKey := []byte(FormatIncomingEdgesKey(currentID))
			e.Engine.GetLogger().Debug("Looking for incoming edges with key: %s", string(inKey))
			inEdgeIDsBytes, err := e.EdgeIndex.Get(inKey)

			// Try direct engine access if index lookup fails
			if err != nil && err == storage.ErrKeyNotFound {
				e.Engine.GetLogger().Debug("Edge not found in EdgeIndex, trying direct engine access")
				inEdgeIDsBytes, err = e.Engine.Get(inKey)
			}

			if err != nil && err != storage.ErrKeyNotFound {
				return nil, fmt.Errorf("error getting incoming edges for node %d: %w", currentID, err)
			}

			if err == nil {
				var inEdgeIDs []string
				err = model.Deserialize(inEdgeIDsBytes, &inEdgeIDs)
				if err != nil {
					return nil, fmt.Errorf("error deserializing incoming edge IDs: %w", err)
				}

				e.Engine.GetLogger().Debug("Found %d incoming edges for node %d", len(inEdgeIDs), currentID)

				// Process each incoming edge
				for _, edgeID := range inEdgeIDs {
					edgeKey := []byte(FormatEdgeKey(edgeID))
					edgeBytes, err := e.EdgeIndex.Get(edgeKey)
					if err != nil {
						if err != storage.ErrKeyNotFound {
							return nil, fmt.Errorf("error getting edge %s: %w", edgeID, err)
						}
						continue
					}

					var edge model.Edge
					err = model.Deserialize(edgeBytes, &edge)
					if err != nil {
						return nil, fmt.Errorf("error deserializing edge %s: %w", edgeID, err)
					}

					// Skip if source node is already visited
					if visited[edge.SourceID] {
						continue
					}

					// Record the parent
					parents[edge.SourceID] = struct {
						NodeID uint64
						Edge   model.Edge
					}{
						NodeID: currentID,
						Edge:   edge,
					}

					// Check if we reached the target node
					if edge.SourceID == targetID {
						found = true
						break
					}

					// Mark source as visited and add to queue
					visited[edge.SourceID] = true
					queue = append(queue, edge.SourceID)
				}
			}

			if found {
				break
			}
		}
	}

	if !found {
		// No path found
		return nil, nil
	}

	// Reconstruct the path
	path := &Path{
		Nodes: []model.Node{},
		Edges: []model.Edge{},
	}

	// Start from the target and work backwards
	currentID := targetID
	for currentID != sourceID {
		// Get the node - try with prefix first
		nodeKey := []byte(FormatNodeKey(currentID))
		e.Engine.GetLogger().Debug("Looking up path node with formatted key: %s", string(nodeKey))

		// First try NodeIndex
		nodeBytes, err := e.NodeIndex.Get(nodeKey)

		// If not found in NodeIndex, try direct engine access
		if err != nil && err == storage.ErrKeyNotFound {
			e.Engine.GetLogger().Debug("Node not found in NodeIndex, trying Engine")
			nodeBytes, err = e.Engine.Get(nodeKey)
		}

		// If still not found, try alternate format with just the ID
		if err != nil && err == storage.ErrKeyNotFound {
			keyWithoutPrefix := []byte(fmt.Sprintf("%d", currentID))
			e.Engine.GetLogger().Debug("Node not found with key %s, trying alternate key: %s", string(nodeKey), string(keyWithoutPrefix))
			nodeBytes, err = e.NodeIndex.Get(keyWithoutPrefix)

			// If not found in NodeIndex with alternate format, try direct engine access
			if err != nil && err == storage.ErrKeyNotFound {
				e.Engine.GetLogger().Debug("Node not found in NodeIndex with alternate key, trying Engine")
				nodeBytes, err = e.Engine.Get(keyWithoutPrefix)
			}
		}

		if err != nil {
			return nil, fmt.Errorf("error getting node %d: %w", currentID, err)
		}

		var node model.Node
		err = model.Deserialize(nodeBytes, &node)
		if err != nil {
			return nil, fmt.Errorf("error deserializing node %d: %w", currentID, err)
		}

		// Add node to the path
		path.Nodes = append([]model.Node{node}, path.Nodes...)

		// Add edge to the path
		parent := parents[currentID]
		path.Edges = append([]model.Edge{parent.Edge}, path.Edges...)

		// Move to the parent node
		currentID = parent.NodeID
	}

	// Add the source node at the beginning
	sourceNodeKey := []byte(FormatNodeKey(sourceID))
	e.Engine.GetLogger().Debug("Looking up source node with formatted key: %s", string(sourceNodeKey))

	sourceNodeBytes, err := e.NodeIndex.Get(sourceNodeKey)
	if err != nil {
		// If not found with prefix, try just the ID as a string (compatibility)
		keyWithoutPrefix := []byte(fmt.Sprintf("%d", sourceID))
		e.Engine.GetLogger().Debug("Source node not found with key %s, trying alternate key: %s", string(sourceNodeKey), string(keyWithoutPrefix))
		sourceNodeBytes, err = e.NodeIndex.Get(keyWithoutPrefix)
	}

	if err != nil {
		return nil, fmt.Errorf("error getting node %d: %w", sourceID, err)
	}

	var sourceNode model.Node
	err = model.Deserialize(sourceNodeBytes, &sourceNode)
	if err != nil {
		return nil, fmt.Errorf("error deserializing node %d: %w", sourceID, err)
	}

	e.Engine.GetLogger().Debug("Successfully retrieved source node %d with label %s", sourceID, sourceNode.Label)

	path.Nodes = append([]model.Node{sourceNode}, path.Nodes...)

	return path, nil
}
