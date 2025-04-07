package query

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"git.canoozie.net/riddling/kgstore/pkg/model"
	"git.canoozie.net/riddling/kgstore/pkg/storage"
)

// setupBenchmarkDB sets up a test database with benchmark data
func setupBenchmarkDB(b *testing.B) (*storage.StorageEngine, storage.Index, storage.Index, storage.Index, storage.Index, storage.Index, storage.Index, string) {
	// Create a temporary directory for the test database
	tempDir, err := os.MkdirTemp("", "kgstore-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create the storage engine
	config := storage.EngineConfig{
		DataDir: filepath.Join(tempDir, "db"),
		Logger:  model.NewNoOpLogger(), // Use NoOp logger for benchmarks
	}

	engine, err := storage.NewStorageEngine(config)
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create storage engine: %v", err)
	}

	// Create the indexes
	nodeIndex, err := storage.NewNodeIndex(engine, model.NewNoOpLogger())
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create node index: %v", err)
	}

	edgeIndex, err := storage.NewNodeIndex(engine, model.NewNoOpLogger())
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create edge index: %v", err)
	}

	nodeLabels, err := storage.NewNodeLabelIndex(engine, model.NewNoOpLogger())
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create node label index: %v", err)
	}

	edgeLabels, err := storage.NewEdgeLabelIndex(engine, model.NewNoOpLogger())
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create edge label index: %v", err)
	}

	// Create the property indexes
	nodeProperties, err := storage.NewNodeIndex(engine, model.NewNoOpLogger())
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create node property index: %v", err)
	}

	edgeProperties, err := storage.NewNodeIndex(engine, model.NewNoOpLogger())
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create edge property index: %v", err)
	}

	// Add benchmark data
	addBenchmarkData(b, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties)

	return engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties, tempDir
}

// addBenchmarkData adds benchmark nodes and edges to the indexes
func addBenchmarkData(b *testing.B, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties storage.Index) {
	// Create nodes with different labels for testing label queries
	labels := []string{"Person", "Company", "Product", "Location", "Event"}
	nodesPerLabel := 20 // 100 nodes total (to keep tests fast)

	// Create nodes for each label
	nodeID := uint64(1)
	for _, label := range labels {
		for i := 0; i < nodesPerLabel; i++ {
			// Create node
			node := model.NewNode(nodeID, label)
			node.AddProperty("name", fmt.Sprintf("%s-%d", label, i))
			node.AddProperty("id", fmt.Sprintf("%d", nodeID))

			// Serialize and store the node
			nodeBytes, err := model.Serialize(node)
			if err != nil {
				b.Fatalf("Failed to serialize node: %v", err)
			}

			err = nodeIndex.Put([]byte(fmt.Sprintf("node:%d", nodeID)), nodeBytes)
			if err != nil {
				b.Fatalf("Failed to add node to index: %v", err)
			}

			// Add node label to index
			err = nodeLabels.Put([]byte(label), []byte(fmt.Sprintf("%d", nodeID)))
			if err != nil {
				b.Fatalf("Failed to add node to label index: %v", err)
			}

			// Add node properties to index
			for propName, propValue := range node.GetProperties() {
				propKey := []byte(fmt.Sprintf("%s|%s", propName, propValue))
				nodeIDStr := fmt.Sprintf("%d", nodeID)
				err = nodeProperties.Put(propKey, []byte(nodeIDStr))
				if err != nil {
					b.Fatalf("Failed to add node property to index: %v", err)
				}
			}

			nodeID++
		}
	}

	// Create edges between nodes with different relationships
	edgeLabelVals := []string{"KNOWS", "WORKS_AT", "OWNS", "LOCATED_IN", "PARTICIPATES_IN"}

	// Create a moderate connection density between nodes
	edgesPerNode := 5
	rand.Seed(time.Now().UnixNano())

	// Track outgoing and incoming edges by node
	outgoingEdges := make(map[uint64][]string)
	incomingEdges := make(map[uint64][]string)

	// Create edges for nodes
	totalNodes := uint64(len(labels) * nodesPerLabel)
	for sourceID := uint64(1); sourceID <= totalNodes; sourceID++ {
		for e := 0; e < edgesPerNode; e++ {
			// Select a random target (that isn't the source)
			targetID := sourceID
			for targetID == sourceID {
				targetID = uint64(rand.Intn(int(totalNodes)) + 1)
			}

			// Select a random edge label
			edgeLabel := edgeLabelVals[rand.Intn(len(edgeLabelVals))]

			// Create edge
			edge := model.NewEdge(sourceID, targetID, edgeLabel)
			edge.AddProperty("weight", fmt.Sprintf("%d", rand.Intn(10)+1))
			if rand.Intn(2) == 0 {
				edge.AddProperty("timestamp", fmt.Sprintf("%d", time.Now().Unix()-int64(rand.Intn(86400*365))))
			}

			// Create edge ID
			edgeID := fmt.Sprintf("%d-%d", sourceID, targetID)

			// Track outgoing/incoming edges
			if outgoingEdges[sourceID] == nil {
				outgoingEdges[sourceID] = []string{}
			}
			outgoingEdges[sourceID] = append(outgoingEdges[sourceID], edgeID)

			if incomingEdges[targetID] == nil {
				incomingEdges[targetID] = []string{}
			}
			incomingEdges[targetID] = append(incomingEdges[targetID], edgeID)

			// Serialize and store the edge
			edgeBytes, err := model.Serialize(edge)
			if err != nil {
				b.Fatalf("Failed to serialize edge: %v", err)
			}

			err = edgeIndex.Put([]byte(fmt.Sprintf("edge:%s", edgeID)), edgeBytes)
			if err != nil {
				b.Fatalf("Failed to add edge to index: %v", err)
			}

			// Add edge label to index
			err = edgeLabels.Put([]byte(edgeLabel), []byte(edgeID))
			if err != nil {
				b.Fatalf("Failed to add edge to label index: %v", err)
			}

			// Add edge properties to index
			for propName, propValue := range edge.GetProperties() {
				propKey := []byte(fmt.Sprintf("%s|%s", propName, propValue))
				err = edgeProperties.Put(propKey, []byte(edgeID))
				if err != nil {
					b.Fatalf("Failed to add edge property to index: %v", err)
				}
			}
		}
	}

	// Add outgoing edges to index
	for nodeID, edgeIDs := range outgoingEdges {
		// Properly serialize the edge IDs using the model.Serialize function
		edgeIDsBytes, err := model.Serialize(edgeIDs)
		if err != nil {
			b.Fatalf("Failed to serialize outgoing edge IDs: %v", err)
		}

		outKey := []byte(fmt.Sprintf("outgoing:%d", nodeID))
		err = edgeIndex.Put(outKey, edgeIDsBytes)
		if err != nil {
			b.Fatalf("Failed to add outgoing edges index: %v", err)
		}
	}

	// Add incoming edges to index
	for nodeID, edgeIDs := range incomingEdges {
		// Properly serialize the edge IDs using the model.Serialize function
		edgeIDsBytes, err := model.Serialize(edgeIDs)
		if err != nil {
			b.Fatalf("Failed to serialize incoming edge IDs: %v", err)
		}

		inKey := []byte(fmt.Sprintf("incoming:%d", nodeID))
		err = edgeIndex.Put(inKey, edgeIDsBytes)
		if err != nil {
			b.Fatalf("Failed to add incoming edges index: %v", err)
		}
	}
}

// cleanupBenchmarkDB cleans up the test database
func cleanupBenchmarkDB(b *testing.B, tempDir string) {
	os.RemoveAll(tempDir)
}

// BenchmarkQueryExecution_FindNodesByLabel benchmarks finding nodes by label
func BenchmarkQueryExecution_FindNodesByLabel(b *testing.B) {
	engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties, tempDir := setupBenchmarkDB(b)
	defer cleanupBenchmarkDB(b, tempDir)

	// Create query executor
	executor := NewExecutorWithAllIndexes(
		engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties)

	// Reset timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Use different labels in each iteration to prevent caching effects
		label := []string{"Person", "Company", "Product", "Location", "Event"}[i%5]
		query := &Query{
			Type: QueryTypeFindNodesByLabel,
			Parameters: map[string]string{
				ParamLabel: label,
			},
		}

		_, err := executor.Execute(query)
		if err != nil {
			b.Fatalf("Error executing query: %v", err)
		}
	}
}

// BenchmarkQueryExecution_FindEdgesByLabel benchmarks finding edges by label
func BenchmarkQueryExecution_FindEdgesByLabel(b *testing.B) {
	engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties, tempDir := setupBenchmarkDB(b)
	defer cleanupBenchmarkDB(b, tempDir)

	// Create query executor
	executor := NewExecutorWithAllIndexes(
		engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties)

	// Reset timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Use different labels in each iteration
		label := []string{"KNOWS", "WORKS_AT", "OWNS", "LOCATED_IN", "PARTICIPATES_IN"}[i%5]
		query := &Query{
			Type: QueryTypeFindEdgesByLabel,
			Parameters: map[string]string{
				ParamLabel: label,
			},
		}

		_, err := executor.Execute(query)
		if err != nil {
			b.Fatalf("Error executing query: %v", err)
		}
	}
}

// BenchmarkQueryExecution_FindNeighbors benchmarks finding node neighbors
func BenchmarkQueryExecution_FindNeighbors(b *testing.B) {
	engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties, tempDir := setupBenchmarkDB(b)
	defer cleanupBenchmarkDB(b, tempDir)

	// Create query executor
	executor := NewExecutorWithAllIndexes(
		engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties)

	// Get a selection of node IDs to test with
	nodeIDs := []uint64{1, 5, 10, 15, 20, 25, 30, 35, 40, 45}
	directions := []string{DirectionOutgoing, DirectionIncoming, DirectionBoth}

	// Reset timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Vary the node ID and direction
		nodeID := nodeIDs[i%len(nodeIDs)]
		direction := directions[i%len(directions)]

		query := &Query{
			Type: QueryTypeFindNeighbors,
			Parameters: map[string]string{
				ParamNodeID:    fmt.Sprintf("%d", nodeID),
				ParamDirection: direction,
			},
		}

		_, err := executor.Execute(query)
		if err != nil {
			b.Fatalf("Error executing query: %v", err)
		}
	}
}

// BenchmarkQueryExecution_FindPath benchmarks finding paths between nodes
func BenchmarkQueryExecution_FindPath(b *testing.B) {
	engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties, tempDir := setupBenchmarkDB(b)
	defer cleanupBenchmarkDB(b, tempDir)

	// Create query executor
	executor := NewExecutorWithAllIndexes(
		engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties)

	// Prepare source and target node pairs for path finding
	pathPairs := []struct {
		source uint64
		target uint64
	}{
		{1, 5},
		{10, 15},
		{20, 25},
		{30, 35},
		{40, 45},
		{50, 55},
		{60, 65},
		{70, 75},
		{80, 85},
		{90, 95},
	}

	// Reset timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Vary the source/target pair
		pair := pathPairs[i%len(pathPairs)]

		query := &Query{
			Type: QueryTypeFindPath,
			Parameters: map[string]string{
				ParamSourceID: fmt.Sprintf("%d", pair.source),
				ParamTargetID: fmt.Sprintf("%d", pair.target),
				ParamMaxHops:  "3", // Use max hops of 3 for reasonable performance
			},
		}

		_, err := executor.Execute(query)
		if err != nil {
			// Skip errors as some paths might not exist
			b.Logf("Path not found: %v", err)
		}
	}
}

// BenchmarkQueryExecution_FindNodesByProperty benchmarks finding nodes by property
func BenchmarkQueryExecution_FindNodesByProperty(b *testing.B) {
	engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties, tempDir := setupBenchmarkDB(b)
	defer cleanupBenchmarkDB(b, tempDir)

	// Create query executor
	executor := NewExecutorWithAllIndexes(
		engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties)

	// Prepare some property searches
	propertySearches := []struct {
		name  string
		value string
	}{
		{"name", "Person-1"},
		{"name", "Company-5"},
		{"name", "Product-10"},
		{"name", "Location-15"},
		{"name", "Event-0"},
		{"id", "1"},
		{"id", "5"},
		{"id", "10"},
		{"id", "15"},
		{"id", "20"},
	}

	// Reset timer before the actual benchmark
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Vary the property being searched
		search := propertySearches[i%len(propertySearches)]

		query := &Query{
			Type: QueryTypeFindNodesByProperty,
			Parameters: map[string]string{
				ParamPropertyName:  search.name,
				ParamPropertyValue: search.value,
			},
		}

		_, err := executor.Execute(query)
		if err != nil {
			b.Fatalf("Error executing query: %v", err)
		}
	}
}
