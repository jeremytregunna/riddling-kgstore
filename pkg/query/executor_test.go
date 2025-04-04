package query

import (
	"os"
	"path/filepath"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
	"git.canoozie.net/riddling/kgstore/pkg/storage"
)

// setupTestDB sets up a test database with some nodes and edges
func setupTestDB(t *testing.T) (*storage.StorageEngine, storage.Index, storage.Index, storage.Index, storage.Index, string) {
	// Create a temporary directory for the test database
	tempDir, err := os.MkdirTemp("", "kgstore-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create the storage engine
	config := storage.EngineConfig{
		DataDir: filepath.Join(tempDir, "db"),
		Logger:  model.DefaultLoggerInstance,
	}
	
	engine, err := storage.NewStorageEngine(config)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create storage engine: %v", err)
	}

	// Create the indexes
	nodeIndex, err := storage.NewNodeIndex(engine, model.DefaultLoggerInstance)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create node index: %v", err)
	}

	edgeIndex, err := storage.NewEdgeIndex(engine, model.DefaultLoggerInstance)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create edge index: %v", err)
	}

	nodeLabels, err := storage.NewNodeLabelIndex(engine, model.DefaultLoggerInstance)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create node label index: %v", err)
	}

	edgeLabels, err := storage.NewEdgeLabelIndex(engine, model.DefaultLoggerInstance)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create edge label index: %v", err)
	}

	// Add some test data
	addTestData(t, nodeIndex, edgeIndex, nodeLabels, edgeLabels)

	return engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, tempDir
}

// addTestData adds some test nodes and edges to the indexes
func addTestData(t *testing.T, nodeIndex, edgeIndex, nodeLabels, edgeLabels storage.Index) {
	// Create some nodes
	nodes := []model.Node{
		{ID: 1, Label: "Person", Properties: map[string]string{"name": "Alice", "age": "30"}},
		{ID: 2, Label: "Person", Properties: map[string]string{"name": "Bob", "age": "25"}},
		{ID: 3, Label: "Person", Properties: map[string]string{"name": "Charlie", "age": "35"}},
		{ID: 4, Label: "Company", Properties: map[string]string{"name": "Acme Inc.", "founded": "2010"}},
		{ID: 5, Label: "Company", Properties: map[string]string{"name": "TechCorp", "founded": "2015"}},
	}

	// Add nodes to the node index and update node label index
	for _, node := range nodes {
		// Serialize the node
		nodeBytes, err := model.Serialize(node)
		if err != nil {
			t.Fatalf("Failed to serialize node: %v", err)
		}

		// Add to node index
		err = nodeIndex.Put([]byte(FormatNodeKey(node.ID)), nodeBytes)
		if err != nil {
			t.Fatalf("Failed to add node to index: %v", err)
		}

		// Update node label index
		key := []byte(node.Label)
		var nodeIDs []uint64

		// Check if the label already has nodes
		nodeIDsBytes, err := nodeLabels.Get(key)
		if err == nil {
			// Label exists, deserialize node IDs
			err = model.Deserialize(nodeIDsBytes, &nodeIDs)
			if err != nil {
				t.Fatalf("Failed to deserialize node IDs: %v", err)
			}
		}

		// Add the node ID
		nodeIDs = append(nodeIDs, node.ID)

		// Serialize and update
		nodeIDsBytes, err = model.Serialize(nodeIDs)
		if err != nil {
			t.Fatalf("Failed to serialize node IDs: %v", err)
		}

		err = nodeLabels.Put(key, nodeIDsBytes)
		if err != nil {
			t.Fatalf("Failed to update node label index: %v", err)
		}
	}

	// Create some edges
	edges := []model.Edge{
		{ID: "1-2", Source: 1, Target: 2, Label: "KNOWS", Properties: map[string]string{"since": "2018"}},
		{ID: "1-3", Source: 1, Target: 3, Label: "KNOWS", Properties: map[string]string{"since": "2019"}},
		{ID: "2-3", Source: 2, Target: 3, Label: "KNOWS", Properties: map[string]string{"since": "2020"}},
		{ID: "1-4", Source: 1, Target: 4, Label: "WORKS_AT", Properties: map[string]string{"role": "Developer", "since": "2015"}},
		{ID: "2-5", Source: 2, Target: 5, Label: "WORKS_AT", Properties: map[string]string{"role": "Manager", "since": "2018"}},
		{ID: "3-5", Source: 3, Target: 5, Label: "WORKS_AT", Properties: map[string]string{"role": "Developer", "since": "2016"}},
	}

	// Add edges to the edge index and update edge label index
	for _, edge := range edges {
		// Serialize the edge
		edgeBytes, err := model.Serialize(edge)
		if err != nil {
			t.Fatalf("Failed to serialize edge: %v", err)
		}

		// Add to edge index
		err = edgeIndex.Put([]byte(FormatEdgeKey(edge.ID)), edgeBytes)
		if err != nil {
			t.Fatalf("Failed to add edge to index: %v", err)
		}

		// Update edge label index
		key := []byte(edge.Label)
		var edgeIDs []string

		// Check if the label already has edges
		edgeIDsBytes, err := edgeLabels.Get(key)
		if err == nil {
			// Label exists, deserialize edge IDs
			err = model.Deserialize(edgeIDsBytes, &edgeIDs)
			if err != nil {
				t.Fatalf("Failed to deserialize edge IDs: %v", err)
			}
		}

		// Add the edge ID
		edgeIDs = append(edgeIDs, edge.ID)

		// Serialize and update
		edgeIDsBytes, err = model.Serialize(edgeIDs)
		if err != nil {
			t.Fatalf("Failed to serialize edge IDs: %v", err)
		}

		err = edgeLabels.Put(key, edgeIDsBytes)
		if err != nil {
			t.Fatalf("Failed to update edge label index: %v", err)
		}

		// Update outgoing edges index
		outKey := []byte(FormatOutgoingEdgesKey(edge.Source))
		var outEdgeIDs []string

		// Check if the node already has outgoing edges
		outEdgeIDsBytes, err := edgeIndex.Get(outKey)
		if err == nil {
			// Node has outgoing edges, deserialize edge IDs
			err = model.Deserialize(outEdgeIDsBytes, &outEdgeIDs)
			if err != nil {
				t.Fatalf("Failed to deserialize outgoing edge IDs: %v", err)
			}
		}

		// Add the edge ID
		outEdgeIDs = append(outEdgeIDs, edge.ID)

		// Serialize and update
		outEdgeIDsBytes, err = model.Serialize(outEdgeIDs)
		if err != nil {
			t.Fatalf("Failed to serialize outgoing edge IDs: %v", err)
		}

		err = edgeIndex.Put(outKey, outEdgeIDsBytes)
		if err != nil {
			t.Fatalf("Failed to update outgoing edges index: %v", err)
		}

		// Update incoming edges index
		inKey := []byte(FormatIncomingEdgesKey(edge.Target))
		var inEdgeIDs []string

		// Check if the node already has incoming edges
		inEdgeIDsBytes, err := edgeIndex.Get(inKey)
		if err == nil {
			// Node has incoming edges, deserialize edge IDs
			err = model.Deserialize(inEdgeIDsBytes, &inEdgeIDs)
			if err != nil {
				t.Fatalf("Failed to deserialize incoming edge IDs: %v", err)
			}
		}

		// Add the edge ID
		inEdgeIDs = append(inEdgeIDs, edge.ID)

		// Serialize and update
		inEdgeIDsBytes, err = model.Serialize(inEdgeIDs)
		if err != nil {
			t.Fatalf("Failed to serialize incoming edge IDs: %v", err)
		}

		err = edgeIndex.Put(inKey, inEdgeIDsBytes)
		if err != nil {
			t.Fatalf("Failed to update incoming edges index: %v", err)
		}
	}
}

// cleanupTestDB cleans up the test database
func cleanupTestDB(t *testing.T, tempDir string) {
	os.RemoveAll(tempDir)
}

func TestExecutor_Execute(t *testing.T) {
	// Skip during short tests
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	// Set up test database
	engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, tempDir := setupTestDB(t)
	defer cleanupTestDB(t, tempDir)

	// Create executor
	executor := NewExecutor(engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels)

	// Test cases
	tests := []struct {
		name    string
		query   *Query
		wantErr bool
		validate func(*Result, *testing.T)
	}{
		{
			name: "Find nodes by label - Person",
			query: &Query{
				Type: QueryTypeFindNodesByLabel,
				Parameters: map[string]string{
					ParamLabel: "Person",
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Nodes) != 3 {
					t.Errorf("Expected 3 nodes, got %d", len(result.Nodes))
				}
				for _, node := range result.Nodes {
					if node.Label != "Person" {
						t.Errorf("Expected node label 'Person', got '%s'", node.Label)
					}
				}
			},
		},
		{
			name: "Find nodes by label - Company",
			query: &Query{
				Type: QueryTypeFindNodesByLabel,
				Parameters: map[string]string{
					ParamLabel: "Company",
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Nodes) != 2 {
					t.Errorf("Expected 2 nodes, got %d", len(result.Nodes))
				}
				for _, node := range result.Nodes {
					if node.Label != "Company" {
						t.Errorf("Expected node label 'Company', got '%s'", node.Label)
					}
				}
			},
		},
		{
			name: "Find nodes by label - NonExistent",
			query: &Query{
				Type: QueryTypeFindNodesByLabel,
				Parameters: map[string]string{
					ParamLabel: "NonExistent",
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Nodes) != 0 {
					t.Errorf("Expected 0 nodes, got %d", len(result.Nodes))
				}
			},
		},
		{
			name: "Find edges by label - KNOWS",
			query: &Query{
				Type: QueryTypeFindEdgesByLabel,
				Parameters: map[string]string{
					ParamLabel: "KNOWS",
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Edges) != 3 {
					t.Errorf("Expected 3 edges, got %d", len(result.Edges))
				}
				for _, edge := range result.Edges {
					if edge.Label != "KNOWS" {
						t.Errorf("Expected edge label 'KNOWS', got '%s'", edge.Label)
					}
				}
			},
		},
		{
			name: "Find edges by label - WORKS_AT",
			query: &Query{
				Type: QueryTypeFindEdgesByLabel,
				Parameters: map[string]string{
					ParamLabel: "WORKS_AT",
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Edges) != 3 {
					t.Errorf("Expected 3 edges, got %d", len(result.Edges))
				}
				for _, edge := range result.Edges {
					if edge.Label != "WORKS_AT" {
						t.Errorf("Expected edge label 'WORKS_AT', got '%s'", edge.Label)
					}
				}
			},
		},
		{
			name: "Find edges by label - NonExistent",
			query: &Query{
				Type: QueryTypeFindEdgesByLabel,
				Parameters: map[string]string{
					ParamLabel: "NonExistent",
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Edges) != 0 {
					t.Errorf("Expected 0 edges, got %d", len(result.Edges))
				}
			},
		},
		{
			name: "Find neighbors - Alice's neighbors (outgoing)",
			query: &Query{
				Type: QueryTypeFindNeighbors,
				Parameters: map[string]string{
					ParamNodeID:    "1",
					ParamDirection: DirectionOutgoing,
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Nodes) != 3 {
					t.Errorf("Expected 3 neighbor nodes, got %d", len(result.Nodes))
				}
				if len(result.Edges) != 3 {
					t.Errorf("Expected 3 edges, got %d", len(result.Edges))
				}
			},
		},
		{
			name: "Find neighbors - Bob's neighbors (both directions)",
			query: &Query{
				Type: QueryTypeFindNeighbors,
				Parameters: map[string]string{
					ParamNodeID:    "2",
					ParamDirection: DirectionBoth,
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				// Bob (2) is connected to Alice (1), Charlie (3), and TechCorp (5)
				if len(result.Nodes) != 3 {
					t.Errorf("Expected 3 neighbor nodes, got %d", len(result.Nodes))
				}
				if len(result.Edges) != 3 {
					t.Errorf("Expected 3 edges, got %d", len(result.Edges))
				}
			},
		},
		{
			name: "Find path - Alice to Charlie",
			query: &Query{
				Type: QueryTypeFindPath,
				Parameters: map[string]string{
					ParamSourceID: "1",
					ParamTargetID: "3",
					ParamMaxHops:  "2",
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Paths) != 1 {
					t.Errorf("Expected 1 path, got %d", len(result.Paths))
				}
				if len(result.Paths[0].Nodes) < 2 {
					t.Errorf("Expected at least 2 nodes in path, got %d", len(result.Paths[0].Nodes))
				}
				if len(result.Paths[0].Edges) < 1 {
					t.Errorf("Expected at least 1 edge in path, got %d", len(result.Paths[0].Edges))
				}
				if result.Paths[0].Nodes[0].ID != 1 {
					t.Errorf("Expected first node to be Alice (1), got %d", result.Paths[0].Nodes[0].ID)
				}
				if result.Paths[0].Nodes[len(result.Paths[0].Nodes)-1].ID != 3 {
					t.Errorf("Expected last node to be Charlie (3), got %d", result.Paths[0].Nodes[len(result.Paths[0].Nodes)-1].ID)
				}
			},
		},
		{
			name: "Find path - Alice to TechCorp (via Bob)",
			query: &Query{
				Type: QueryTypeFindPath,
				Parameters: map[string]string{
					ParamSourceID: "1",
					ParamTargetID: "5",
					ParamMaxHops:  "3",
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Paths) != 1 {
					t.Errorf("Expected 1 path, got %d", len(result.Paths))
				}
				if len(result.Paths[0].Nodes) < 3 {
					t.Errorf("Expected at least 3 nodes in path, got %d", len(result.Paths[0].Nodes))
				}
				if len(result.Paths[0].Edges) < 2 {
					t.Errorf("Expected at least 2 edges in path, got %d", len(result.Paths[0].Edges))
				}
				if result.Paths[0].Nodes[0].ID != 1 {
					t.Errorf("Expected first node to be Alice (1), got %d", result.Paths[0].Nodes[0].ID)
				}
				if result.Paths[0].Nodes[len(result.Paths[0].Nodes)-1].ID != 5 {
					t.Errorf("Expected last node to be TechCorp (5), got %d", result.Paths[0].Nodes[len(result.Paths[0].Nodes)-1].ID)
				}
			},
		},
		{
			name: "Find path - NonExistent nodes",
			query: &Query{
				Type: QueryTypeFindPath,
				Parameters: map[string]string{
					ParamSourceID: "100",
					ParamTargetID: "200",
					ParamMaxHops:  "3",
				},
			},
			wantErr: true,
			validate: func(result *Result, t *testing.T) {
				// Should get an error, result will be nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Executor.Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			
			if !tt.wantErr && result != nil {
				tt.validate(result, t)
			}
		})
	}
}

func TestExecutor_ExecuteWithOptimizer(t *testing.T) {
	// Skip during short tests
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	// Set up test database
	engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, tempDir := setupTestDB(t)
	defer cleanupTestDB(t, tempDir)

	// Create query engine
	queryEngine := NewEngine(engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels)

	// Test cases
	tests := []struct {
		name    string
		queryStr string
		wantErr bool
		validate func(*Result, *testing.T)
	}{
		{
			name: "Find nodes by label - Person",
			queryStr: `FIND_NODES_BY_LABEL(label: "Person")`,
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Nodes) != 3 {
					t.Errorf("Expected 3 nodes, got %d", len(result.Nodes))
				}
			},
		},
		{
			name: "Find neighbors - Both directions",
			queryStr: `FIND_NEIGHBORS(nodeId: "1")`, // Test default direction
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Nodes) < 3 {
					t.Errorf("Expected at least 3 neighbor nodes, got %d", len(result.Nodes))
				}
			},
		},
		{
			name: "Find path - With default max hops",
			queryStr: `FIND_PATH(sourceId: "1", targetId: "5")`, // Test default max hops
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Paths) != 1 {
					t.Errorf("Expected 1 path, got %d", len(result.Paths))
				}
			},
		},
		{
			name: "Invalid query - Parse error",
			queryStr: `INVALID_QUERY(foo: "bar")`,
			wantErr: true,
			validate: func(result *Result, t *testing.T) {
				// Should get an error, result will be nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := queryEngine.Execute(tt.queryStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("QueryEngine.Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			
			if !tt.wantErr && result != nil {
				tt.validate(result, t)
			}
		})
	}
}