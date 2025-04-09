package query

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/storage"
)

// TestTransactionOperations tests transaction operations in the query language
func TestTransactionOperations(t *testing.T) {
	// Create a temporary directory for the storage engine
	tempDir, err := ioutil.TempDir("", "kgstore-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a storage engine with a unique data directory
	dataDir := filepath.Join(tempDir, "db")
	engineConfig := storage.DefaultEngineConfig()
	engineConfig.DataDir = dataDir
	engine, err := storage.NewStorageEngine(engineConfig)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	// Create an executor
	executor := NewExecutor(engine, nil, nil, nil, nil)

	// Test transaction operations
	t.Run("Begin_Commit_Transaction", func(t *testing.T) {
		// Begin a transaction
		beginQuery := &Query{
			Type:       QueryTypeBeginTransaction,
			Parameters: make(map[string]string),
		}

		result, err := executor.Execute(beginQuery)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		if !result.Success {
			t.Errorf("Expected success to be true, got false")
		}
		if result.Operation != "BEGIN_TRANSACTION" {
			t.Errorf("Expected operation to be BEGIN_TRANSACTION, got %s", result.Operation)
		}
		if result.TxID == "" {
			t.Errorf("Expected transaction ID to be non-empty")
		}

		txID := result.TxID

		// Commit the transaction
		commitQuery := &Query{
			Type: QueryTypeCommitTransaction,
			Parameters: map[string]string{
				ParamTransactionID: txID,
			},
		}

		result, err = executor.Execute(commitQuery)
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
		if !result.Success {
			t.Errorf("Expected success to be true, got false")
		}
		if result.Operation != "COMMIT_TRANSACTION" {
			t.Errorf("Expected operation to be COMMIT_TRANSACTION, got %s", result.Operation)
		}
		if result.TxID != txID {
			t.Errorf("Expected transaction ID to be %s, got %s", txID, result.TxID)
		}
	})

	t.Run("Begin_Rollback_Transaction", func(t *testing.T) {
		// Begin a transaction
		beginQuery := &Query{
			Type:       QueryTypeBeginTransaction,
			Parameters: make(map[string]string),
		}

		result, err := executor.Execute(beginQuery)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		if !result.Success {
			t.Errorf("Expected success to be true, got false")
		}
		if result.Operation != "BEGIN_TRANSACTION" {
			t.Errorf("Expected operation to be BEGIN_TRANSACTION, got %s", result.Operation)
		}
		if result.TxID == "" {
			t.Errorf("Expected transaction ID to be non-empty")
		}

		txID := result.TxID

		// Rollback the transaction
		rollbackQuery := &Query{
			Type: QueryTypeRollbackTransaction,
			Parameters: map[string]string{
				ParamTransactionID: txID,
			},
		}

		result, err = executor.Execute(rollbackQuery)
		if err != nil {
			t.Fatalf("Failed to rollback transaction: %v", err)
		}
		if !result.Success {
			t.Errorf("Expected success to be true, got false")
		}
		if result.Operation != "ROLLBACK_TRANSACTION" {
			t.Errorf("Expected operation to be ROLLBACK_TRANSACTION, got %s", result.Operation)
		}
		if result.TxID != txID {
			t.Errorf("Expected transaction ID to be %s, got %s", txID, result.TxID)
		}
	})

	t.Run("Transaction_Not_Found", func(t *testing.T) {
		// Try to commit a non-existent transaction
		commitQuery := &Query{
			Type: QueryTypeCommitTransaction,
			Parameters: map[string]string{
				ParamTransactionID: "non-existent-tx",
			},
		}

		_, err := executor.Execute(commitQuery)
		if err == nil {
			t.Errorf("Expected error for non-existent transaction, got nil")
		} else if err.Error() == "" || err.Error() == "transaction not found: non-existent-tx" {
			// Pass - we expect an error about transaction not found
		} else {
			t.Errorf("Expected error about transaction not found, got: %v", err)
		}
	})
}

// TestDataManipulationOperations tests data manipulation operations in the query language
func TestDataManipulationOperations(t *testing.T) {
	// Create a temporary directory for the storage engine
	tempDir, err := ioutil.TempDir("", "kgstore-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a storage engine with a unique data directory
	dataDir := filepath.Join(tempDir, "db")
	engineConfig := storage.DefaultEngineConfig()
	engineConfig.DataDir = dataDir
	engine, err := storage.NewStorageEngine(engineConfig)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	// Create an executor with GraphStore
	graphStore, err := storage.NewGraphStore(engine, nil)
	if err != nil {
		t.Fatalf("Failed to create graph store: %v", err)
	}
	executor := &Executor{
		Engine:      engine,
		Optimizer:   NewOptimizer(),
		GraphStore:  graphStore,
		txRegistry:  NewTransactionRegistry(),
		maxPathHops: 5,
	}

	// Test data manipulation operations
	t.Run("Create_Node", func(t *testing.T) {
		// Create a node
		createNodeQuery := &Query{
			Type: QueryTypeCreateNode,
			Parameters: map[string]string{
				ParamLabel: "Person",
			},
		}

		result, err := executor.Execute(createNodeQuery)
		if err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}
		if !result.Success {
			t.Errorf("Expected success to be true, got false")
		}
		if result.Operation != "CREATE_NODE" {
			t.Errorf("Expected operation to be CREATE_NODE, got %s", result.Operation)
		}
		if result.NodeID == "" {
			t.Errorf("Expected node ID to be non-empty")
		}
	})

	t.Run("Create_Edge", func(t *testing.T) {
		// Create two nodes first
		createNode1Query := &Query{
			Type: QueryTypeCreateNode,
			Parameters: map[string]string{
				ParamLabel: "Person",
			},
		}

		result1, err := executor.Execute(createNode1Query)
		if err != nil {
			t.Fatalf("Failed to create node 1: %v", err)
		}
		nodeID1 := result1.NodeID

		createNode2Query := &Query{
			Type: QueryTypeCreateNode,
			Parameters: map[string]string{
				ParamLabel: "Person",
			},
		}

		result2, err := executor.Execute(createNode2Query)
		if err != nil {
			t.Fatalf("Failed to create node 2: %v", err)
		}
		nodeID2 := result2.NodeID

		// Create an edge between them
		createEdgeQuery := &Query{
			Type: QueryTypeCreateEdge,
			Parameters: map[string]string{
				ParamSourceID: nodeID1,
				ParamTargetID: nodeID2,
				ParamLabel:    "KNOWS",
			},
		}

		result, err := executor.Execute(createEdgeQuery)
		if err != nil {
			t.Fatalf("Failed to create edge: %v", err)
		}
		if !result.Success {
			t.Errorf("Expected success to be true, got false")
		}
		if result.Operation != "CREATE_EDGE" {
			t.Errorf("Expected operation to be CREATE_EDGE, got %s", result.Operation)
		}
		if result.EdgeID == "" {
			t.Errorf("Expected edge ID to be non-empty")
		}
	})

	t.Run("Set_Node_Property", func(t *testing.T) {
		// Create a node first
		createNodeQuery := &Query{
			Type: QueryTypeCreateNode,
			Parameters: map[string]string{
				ParamLabel: "Person",
			},
		}

		result, err := executor.Execute(createNodeQuery)
		if err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}
		nodeID := result.NodeID

		// Set a property on the node
		setPropertyQuery := &Query{
			Type: QueryTypeSetProperty,
			Parameters: map[string]string{
				ParamTarget: "node",
				ParamID:     nodeID,
				ParamName:   "name",
				ParamValue:  "Alice",
			},
		}

		result, err = executor.Execute(setPropertyQuery)
		if err != nil {
			t.Fatalf("Failed to set property: %v", err)
		}
		if !result.Success {
			t.Errorf("Expected success to be true, got false")
		}
		if result.Operation != "SET_PROPERTY" {
			t.Errorf("Expected operation to be SET_PROPERTY, got %s", result.Operation)
		}
		if result.NodeID != nodeID {
			t.Errorf("Expected node ID to be %s, got %s", nodeID, result.NodeID)
		}
	})

	t.Run("Remove_Node_Property", func(t *testing.T) {
		// Create a node first
		createNodeQuery := &Query{
			Type: QueryTypeCreateNode,
			Parameters: map[string]string{
				ParamLabel: "Person",
			},
		}

		result, err := executor.Execute(createNodeQuery)
		if err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}
		nodeID := result.NodeID

		// Set a property on the node
		setPropertyQuery := &Query{
			Type: QueryTypeSetProperty,
			Parameters: map[string]string{
				ParamTarget: "node",
				ParamID:     nodeID,
				ParamName:   "name",
				ParamValue:  "Alice",
			},
		}

		_, err = executor.Execute(setPropertyQuery)
		if err != nil {
			t.Fatalf("Failed to set property: %v", err)
		}

		// Remove the property
		removePropertyQuery := &Query{
			Type: QueryTypeRemoveProperty,
			Parameters: map[string]string{
				ParamTarget: "node",
				ParamID:     nodeID,
				ParamName:   "name",
			},
		}

		result, err = executor.Execute(removePropertyQuery)
		if err != nil {
			t.Fatalf("Failed to remove property: %v", err)
		}
		if !result.Success {
			t.Errorf("Expected success to be true, got false")
		}
		if result.Operation != "REMOVE_PROPERTY" {
			t.Errorf("Expected operation to be REMOVE_PROPERTY, got %s", result.Operation)
		}
		if result.NodeID != nodeID {
			t.Errorf("Expected node ID to be %s, got %s", nodeID, result.NodeID)
		}
	})

	t.Run("Delete_Node", func(t *testing.T) {
		// Create a node first
		createNodeQuery := &Query{
			Type: QueryTypeCreateNode,
			Parameters: map[string]string{
				ParamLabel: "Person",
			},
		}

		result, err := executor.Execute(createNodeQuery)
		if err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}
		nodeID := result.NodeID

		// Delete the node
		deleteNodeQuery := &Query{
			Type: QueryTypeDeleteNode,
			Parameters: map[string]string{
				ParamID: nodeID,
			},
		}

		result, err = executor.Execute(deleteNodeQuery)
		if err != nil {
			t.Fatalf("Failed to delete node: %v", err)
		}
		if !result.Success {
			t.Errorf("Expected success to be true, got false")
		}
		if result.Operation != "DELETE_NODE" {
			t.Errorf("Expected operation to be DELETE_NODE, got %s", result.Operation)
		}
		if result.NodeID != nodeID {
			t.Errorf("Expected node ID to be %s, got %s", nodeID, result.NodeID)
		}
	})

	t.Run("Delete_Edge", func(t *testing.T) {
		// Instead of using a transaction, create nodes individually to ensure they're committed
		// before we try to create an edge between them
		createNode1Query := &Query{
			Type: QueryTypeCreateNode,
			Parameters: map[string]string{
				ParamLabel: "Person",
			},
		}

		result1, err := executor.Execute(createNode1Query)
		if err != nil {
			t.Fatalf("Failed to create node 1: %v", err)
		}
		nodeID1 := result1.NodeID
		t.Logf("Created node 1 with ID: %s", nodeID1)

		createNode2Query := &Query{
			Type: QueryTypeCreateNode,
			Parameters: map[string]string{
				ParamLabel: "Person",
			},
		}

		result2, err := executor.Execute(createNode2Query)
		if err != nil {
			t.Fatalf("Failed to create node 2: %v", err)
		}
		nodeID2 := result2.NodeID
		t.Logf("Created node 2 with ID: %s", nodeID2)

		// Create an edge between them - each operation is now in its own auto-transaction
		createEdgeQuery := &Query{
			Type: QueryTypeCreateEdge,
			Parameters: map[string]string{
				ParamSourceID: nodeID1,
				ParamTargetID: nodeID2,
				ParamLabel:    "KNOWS",
			},
		}

		result, err := executor.Execute(createEdgeQuery)
		if err != nil {
			t.Fatalf("Failed to create edge: %v", err)
		}
		edgeID := result.EdgeID
		t.Logf("Created edge with ID: %s", edgeID)

		// Delete the edge in a new transaction
		deleteEdgeQuery := &Query{
			Type: QueryTypeDeleteEdge,
			Parameters: map[string]string{
				ParamID: edgeID,
			},
		}

		result, err = executor.Execute(deleteEdgeQuery)
		if err != nil {
			t.Fatalf("Failed to delete edge: %v", err)
		}
		if !result.Success {
			t.Errorf("Expected success to be true, got false")
		}
		if result.Operation != "DELETE_EDGE" {
			t.Errorf("Expected operation to be DELETE_EDGE, got %s", result.Operation)
		}
		if result.EdgeID != edgeID {
			t.Errorf("Expected edge ID to be %s, got %s", edgeID, result.EdgeID)
		}
		t.Log("Edge deleted successfully")
	})
}

// TestTransactionalOperations tests operations within transactions
func TestTransactionalOperations(t *testing.T) {
	// Create a temporary directory for the storage engine
	tempDir, err := ioutil.TempDir("", "kgstore-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a storage engine with a unique data directory
	dataDir := filepath.Join(tempDir, "db")
	engineConfig := storage.DefaultEngineConfig()
	engineConfig.DataDir = dataDir
	engine, err := storage.NewStorageEngine(engineConfig)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	// Create an executor with GraphStore
	graphStore, err := storage.NewGraphStore(engine, nil)
	if err != nil {
		t.Fatalf("Failed to create graph store: %v", err)
	}
	executor := &Executor{
		Engine:      engine,
		Optimizer:   NewOptimizer(),
		GraphStore:  graphStore,
		txRegistry:  NewTransactionRegistry(),
		maxPathHops: 5,
	}

	t.Run("Create_Node_And_Set_Property_In_Transaction", func(t *testing.T) {
		// Begin a transaction
		beginQuery := &Query{
			Type:       QueryTypeBeginTransaction,
			Parameters: make(map[string]string),
		}

		result, err := executor.Execute(beginQuery)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		txID := result.TxID

		// Create a node in the transaction
		createNodeQuery := &Query{
			Type: QueryTypeCreateNode,
			Parameters: map[string]string{
				ParamLabel:         "Person",
				ParamTransactionID: txID,
			},
		}

		result, err = executor.Execute(createNodeQuery)
		if err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}
		nodeID := result.NodeID

		// Set a property on the node in the transaction
		setPropertyQuery := &Query{
			Type: QueryTypeSetProperty,
			Parameters: map[string]string{
				ParamTarget:        "node",
				ParamID:            nodeID,
				ParamName:          "name",
				ParamValue:         "Alice",
				ParamTransactionID: txID,
			},
		}

		_, err = executor.Execute(setPropertyQuery)
		if err != nil {
			t.Fatalf("Failed to set property: %v", err)
		}

		// Commit the transaction
		commitQuery := &Query{
			Type: QueryTypeCommitTransaction,
			Parameters: map[string]string{
				ParamTransactionID: txID,
			},
		}

		_, err = executor.Execute(commitQuery)
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	})
}
