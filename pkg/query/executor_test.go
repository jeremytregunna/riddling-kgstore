package query

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
	"git.canoozie.net/riddling/kgstore/pkg/storage"
)

// setupTestDB sets up a test database with some nodes and edges
func setupTestDB(t *testing.T) (*storage.StorageEngine, storage.Index, storage.Index, storage.Index, storage.Index, storage.Index, storage.Index, string) {
	// Create a temporary directory for the test database
	tempDir, err := os.MkdirTemp("", "kgstore-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create the storage engine with LSM node label index enabled
	config := storage.EngineConfig{
		DataDir:              filepath.Join(tempDir, "db"),
		Logger:               model.DefaultLoggerInstance,
		UseLSMNodeLabelIndex: true, // Enable LSM-based node label index for better prefix scanning
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

	// Create the property indexes
	nodeProperties, err := storage.NewNodePropertyIndex(engine, model.DefaultLoggerInstance)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create node property index: %v", err)
	}

	edgeProperties, err := storage.NewEdgePropertyIndex(engine, model.DefaultLoggerInstance)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create edge property index: %v", err)
	}

	// Add some test data
	addTestData(t, engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties)

	return engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties, tempDir
}

// addTestData adds some test nodes and edges to the indexes
func addTestData(t *testing.T, engine *storage.StorageEngine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties storage.Index) {
	fmt.Println("TEST DEBUG: Adding test data to indexes")
	// Create some nodes
	nodes := []*model.Node{
		model.NewNode(1, "Person"),
		model.NewNode(2, "Person"),
		model.NewNode(3, "Person"),
		model.NewNode(4, "Company"),
		model.NewNode(5, "Company"),
	}

	// Add properties to nodes
	nodes[0].AddProperty("name", "Alice")
	nodes[0].AddProperty("age", "30")
	nodes[1].AddProperty("name", "Bob")
	nodes[1].AddProperty("age", "25")
	nodes[2].AddProperty("name", "Charlie")
	nodes[2].AddProperty("age", "35")
	nodes[3].AddProperty("name", "Acme Inc.")
	nodes[3].AddProperty("founded", "2010")
	nodes[4].AddProperty("name", "TechCorp")
	nodes[4].AddProperty("founded", "2015")

	// Create maps to collect all nodes by label
	nodesByLabel := make(map[string][]uint64)

	// First pass: collect all nodes by label
	for _, node := range nodes {
		nodesByLabel[node.Label] = append(nodesByLabel[node.Label], node.ID)
	}

	// Add nodes to the node index
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

		// Add node properties to the property index
		for propName, propValue := range node.GetProperties() {
			// Create key in the format propertyName|propertyValue
			propKey := []byte(fmt.Sprintf("%s|%s", propName, propValue))
			// The entityID (nodeID as string) becomes the value for the property index
			nodeIDStr := fmt.Sprintf("%d", node.ID)
			err = nodeProperties.Put(propKey, []byte(nodeIDStr))
			if err != nil {
				t.Fatalf("Failed to add node property to index: %v", err)
			}
		}
	}

	// Add node labels to the label index
	for label, nodeIDs := range nodesByLabel {
		// For each node ID, add it to the label index by directly using the Put method
		// with the ID as a value
		for _, id := range nodeIDs {
			// For NodeLabelIndex, we want to store the ID directly
			idStr := fmt.Sprintf("%d", id)
			err := nodeLabels.Put([]byte(label), []byte(idStr))
			if err != nil {
				t.Fatalf("Failed to add node %d to label %s index: %v", id, label, err)
			}
		}
	}

	// Create some edges
	edges := []*model.Edge{
		model.NewEdge(1, 2, "KNOWS"),
		model.NewEdge(1, 3, "KNOWS"),
		model.NewEdge(2, 3, "KNOWS"),
		model.NewEdge(1, 4, "WORKS_AT"),
		model.NewEdge(2, 5, "WORKS_AT"),
		model.NewEdge(3, 5, "WORKS_AT"),
	}

	// Add properties to edges
	edges[0].AddProperty("since", "2018")
	edges[1].AddProperty("since", "2019")
	edges[2].AddProperty("since", "2020")
	edges[3].AddProperty("role", "Developer")
	edges[3].AddProperty("since", "2015")
	edges[4].AddProperty("role", "Manager")
	edges[4].AddProperty("since", "2018")
	edges[5].AddProperty("role", "Developer")
	edges[5].AddProperty("since", "2016")

	// Create maps to collect edges by label, outgoing, and incoming
	edgesByLabel := make(map[string][]string)
	outgoingByNode := make(map[uint64][]string)
	incomingByNode := make(map[uint64][]string)

	// Add edges to the edge index
	for _, edge := range edges {
		// Create edge ID from source and target
		edgeID := fmt.Sprintf("%d-%d", edge.SourceID, edge.TargetID)

		// Collect by label
		edgesByLabel[edge.Label] = append(edgesByLabel[edge.Label], edgeID)

		// Collect by source (outgoing)
		outgoingByNode[edge.SourceID] = append(outgoingByNode[edge.SourceID], edgeID)

		// Collect by target (incoming)
		incomingByNode[edge.TargetID] = append(incomingByNode[edge.TargetID], edgeID)

		// Serialize the edge
		edgeBytes, err := model.Serialize(edge)
		if err != nil {
			t.Fatalf("Failed to serialize edge: %v", err)
		}

		// Add to edge index
		err = edgeIndex.Put([]byte(FormatEdgeKey(edgeID)), edgeBytes)
		if err != nil {
			t.Fatalf("Failed to add edge to index: %v", err)
		}

		// Add edge properties to the property index
		for propName, propValue := range edge.GetProperties() {
			// Create key in the format propertyName|propertyValue
			propKey := []byte(fmt.Sprintf("%s|%s", propName, propValue))
			// The entityID (edgeID) becomes the value for the property index
			err = edgeProperties.Put(propKey, []byte(edgeID))
			if err != nil {
				t.Fatalf("Failed to add edge property to index: %v", err)
			}
		}
	}

	// Add edge labels to the label index
	for label, edgeIDs := range edgesByLabel {
		// Add each edge ID directly to the label index
		for _, id := range edgeIDs {
			// Just store the edge ID directly
			err := edgeLabels.Put([]byte(label), []byte(id))
			if err != nil {
				t.Fatalf("Failed to add edge %s to label %s index: %v", id, label, err)
			}
		}
	}

	// Add outgoing edges to the outgoing index
	for nodeID, edgeIDs := range outgoingByNode {
		// Serialize the edge IDs
		edgeIDsBytes, err := model.Serialize(edgeIDs)
		if err != nil {
			t.Fatalf("Failed to serialize outgoing edge IDs for node %d: %v", nodeID, err)
		}

		// Add to outgoing index - store in both edgeIndex and engine with same key
		outKey := []byte(FormatOutgoingEdgesKey(nodeID))
		fmt.Printf("DEBUG: Storing outgoing edges to key: %s\n", string(outKey))
		err = edgeIndex.Put(outKey, edgeIDsBytes)
		if err != nil {
			t.Fatalf("Failed to add outgoing edges index entry for node %d: %v", nodeID, err)
		}
		// Also store in engine directly for consistency
		err = engine.Put(outKey, edgeIDsBytes)
		if err != nil {
			t.Fatalf("Failed to add outgoing edges index entry for node %d: %v", nodeID, err)
		}
	}

	// Add incoming edges to the incoming index
	for nodeID, edgeIDs := range incomingByNode {
		// Serialize the edge IDs
		edgeIDsBytes, err := model.Serialize(edgeIDs)
		if err != nil {
			t.Fatalf("Failed to serialize incoming edge IDs for node %d: %v", nodeID, err)
		}

		// Add to incoming index - store in both edgeIndex and engine with same key
		inKey := []byte(FormatIncomingEdgesKey(nodeID))
		fmt.Printf("DEBUG: Storing incoming edges to key: %s\n", string(inKey))
		err = edgeIndex.Put(inKey, edgeIDsBytes)
		if err != nil {
			t.Fatalf("Failed to add incoming edges index entry for node %d: %v", nodeID, err)
		}
		// Also store in engine directly for consistency
		err = engine.Put(inKey, edgeIDsBytes)
		if err != nil {
			t.Fatalf("Failed to add incoming edges index entry for node %d: %v", nodeID, err)
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
	engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties, tempDir := setupTestDB(t)
	defer cleanupTestDB(t, tempDir)

	// Create executor with all indexes
	executor := NewExecutorWithAllIndexes(engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties)

	// Test cases
	tests := []struct {
		name     string
		query    *Query
		wantErr  bool
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
					return // Skip further checks to avoid panic
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
					return // Skip further checks to avoid panic
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

func TestExecutor_PropertyIndexingDebug(t *testing.T) {
	// Skip during short tests
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	// Set up test database
	_, _, _, _, _, nodeProperties, _, tempDir := setupTestDB(t)
	defer cleanupTestDB(t, tempDir)

	// Add a test node with a property directly to the property index
	nodeProperties.Put([]byte("name|Alice"), []byte("1"))

	// Verify the property index contains the data
	results, err := nodeProperties.GetAll([]byte("name|Alice"))
	if err != nil {
		t.Fatalf("Error retrieving property from index: %v", err)
	}

	t.Logf("Property index has %d results for name|Alice", len(results))
	for i, res := range results {
		t.Logf("Result %d: %s", i, string(res))
	}
}

func TestExecutor_PropertyQueries(t *testing.T) {
	// Skip during short tests
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	// Set up test database
	engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties, tempDir := setupTestDB(t)
	defer cleanupTestDB(t, tempDir)

	// Create executor with all indexes
	executor := NewExecutorWithAllIndexes(engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties)

	// Test cases
	tests := []struct {
		name     string
		query    *Query
		wantErr  bool
		validate func(*Result, *testing.T)
	}{
		{
			name: "Find nodes by property - name:Alice",
			query: &Query{
				Type: QueryTypeFindNodesByProperty,
				Parameters: map[string]string{
					ParamPropertyName:  "name",
					ParamPropertyValue: "Alice",
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Nodes) != 1 {
					t.Errorf("Expected 1 node, got %d", len(result.Nodes))
				}
				if len(result.Nodes) > 0 && result.Nodes[0].ID != 1 {
					t.Errorf("Expected node ID 1, got %d", result.Nodes[0].ID)
				}
			},
		},
		{
			name: "Find nodes by property - age:25",
			query: &Query{
				Type: QueryTypeFindNodesByProperty,
				Parameters: map[string]string{
					ParamPropertyName:  "age",
					ParamPropertyValue: "25",
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Nodes) != 1 {
					t.Errorf("Expected 1 node, got %d", len(result.Nodes))
				}
				if len(result.Nodes) > 0 && result.Nodes[0].ID != 2 {
					t.Errorf("Expected node ID 2, got %d", result.Nodes[0].ID)
				}
			},
		},
		{
			name: "Find edges by property - role:Developer",
			query: &Query{
				Type: QueryTypeFindEdgesByProperty,
				Parameters: map[string]string{
					ParamPropertyName:  "role",
					ParamPropertyValue: "Developer",
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Edges) != 2 {
					t.Errorf("Expected 2 edges, got %d", len(result.Edges))
				}
				for _, edge := range result.Edges {
					value, exists := edge.GetProperty("role")
					if !exists || value != "Developer" {
						t.Errorf("Expected edge property 'role'='Developer', got '%s'", value)
					}
				}
			},
		},
		{
			name: "Find edges by property - since:2018",
			query: &Query{
				Type: QueryTypeFindEdgesByProperty,
				Parameters: map[string]string{
					ParamPropertyName:  "since",
					ParamPropertyValue: "2018",
				},
			},
			wantErr: false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Edges) != 2 {
					t.Errorf("Expected 2 edges, got %d", len(result.Edges))
				}
				for _, edge := range result.Edges {
					value, exists := edge.GetProperty("since")
					if !exists || value != "2018" {
						t.Errorf("Expected edge property 'since'='2018', got '%s'", value)
					}
				}
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
	engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties, tempDir := setupTestDB(t)
	defer cleanupTestDB(t, tempDir)

	// Create query engine with all indexes
	queryEngine := NewEngineWithAllIndexes(engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties)

	// Test cases
	tests := []struct {
		name     string
		queryStr string
		wantErr  bool
		validate func(*Result, *testing.T)
	}{
		{
			name:     "Find nodes by label - Person",
			queryStr: `FIND_NODES_BY_LABEL(label: "Person")`,
			wantErr:  false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Nodes) != 3 {
					t.Errorf("Expected 3 nodes, got %d", len(result.Nodes))
				}
			},
		},
		{
			name:     "Find neighbors - Both directions",
			queryStr: `FIND_NEIGHBORS(nodeId: "1")`, // Test default direction
			wantErr:  false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Nodes) < 3 {
					t.Errorf("Expected at least 3 neighbor nodes, got %d", len(result.Nodes))
				}
			},
		},
		{
			name:     "Find path - With default max hops",
			queryStr: `FIND_PATH(sourceId: "1", targetId: "5")`, // Test default max hops
			wantErr:  false,
			validate: func(result *Result, t *testing.T) {
				if len(result.Paths) != 1 {
					t.Errorf("Expected 1 path, got %d", len(result.Paths))
					return // Skip further checks to avoid panic
				}
			},
		},
		{
			name:     "Invalid query - Parse error",
			queryStr: `INVALID_QUERY(foo: "bar")`,
			wantErr:  true,
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
