package query

import (
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// BenchmarkTraversal_BFS benchmarks breadth-first traversal
func BenchmarkTraversal_BFS(b *testing.B) {
	engine, nodeIndex, edgeIndex, _, _, _, _, tempDir := setupBenchmarkDB(b)
	defer cleanupBenchmarkDB(b, tempDir)

	// Create traversal
	traversal := NewTraversal(engine, nodeIndex, edgeIndex)
	traversal.SetType(TraversalTypeBFS)

	// Try different starting nodes
	startNodeIDs := []uint64{1, 5, 10, 15, 20, 25, 30, 35, 40, 45}
	directions := []string{DirectionOutgoing, DirectionIncoming, DirectionBoth}
	maxDepths := []int{2, 3, 4} // Avoid large depths for benchmark performance

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Select test parameters that vary with each iteration
		startNodeID := startNodeIDs[i%len(startNodeIDs)]
		direction := directions[i%len(directions)]
		maxDepth := maxDepths[i%len(maxDepths)]
		
		// Set max depth for this iteration
		traversal.SetMaxDepth(maxDepth)
		
		// Count nodes to verify traversal works
		nodeCount := 0
		visitor := func(node model.Node, depth int) (bool, error) {
			nodeCount++
			return true, nil // Continue traversal
		}
		
		err := traversal.Run(startNodeID, direction, visitor)
		if err != nil {
			b.Fatalf("Error during traversal: %v", err)
		}
		
		// Make sure the traversal actually found nodes
		if nodeCount == 0 {
			b.Logf("Warning: No nodes visited during traversal from node %d", startNodeID)
		}
	}
}

// BenchmarkTraversal_DFS benchmarks depth-first traversal
func BenchmarkTraversal_DFS(b *testing.B) {
	engine, nodeIndex, edgeIndex, _, _, _, _, tempDir := setupBenchmarkDB(b)
	defer cleanupBenchmarkDB(b, tempDir)

	// Create traversal
	traversal := NewTraversal(engine, nodeIndex, edgeIndex)
	traversal.SetType(TraversalTypeDFS)

	// Try different starting nodes
	startNodeIDs := []uint64{1, 5, 10, 15, 20, 25, 30, 35, 40, 45}
	directions := []string{DirectionOutgoing, DirectionIncoming, DirectionBoth}
	maxDepths := []int{2, 3, 4} // Avoid large depths for benchmark performance

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Select test parameters that vary with each iteration
		startNodeID := startNodeIDs[i%len(startNodeIDs)]
		direction := directions[i%len(directions)]
		maxDepth := maxDepths[i%len(maxDepths)]
		
		// Set max depth for this iteration
		traversal.SetMaxDepth(maxDepth)
		
		// Count nodes to verify traversal works
		nodeCount := 0
		visitor := func(node model.Node, depth int) (bool, error) {
			nodeCount++
			return true, nil // Continue traversal
		}
		
		err := traversal.Run(startNodeID, direction, visitor)
		if err != nil {
			b.Fatalf("Error during traversal: %v", err)
		}
		
		// Make sure the traversal actually found nodes
		if nodeCount == 0 {
			b.Logf("Warning: No nodes visited during traversal from node %d", startNodeID)
		}
	}
}

// BenchmarkTraversal_PathFinding benchmarks path finding via direct BFS
func BenchmarkTraversal_PathFinding(b *testing.B) {
	engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, _, _, tempDir := setupBenchmarkDB(b)
	defer cleanupBenchmarkDB(b, tempDir)

	// Create executor for path finding
	executor := NewExecutor(engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels)

	// Path pairs to test with
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
	
	maxHops := []int{2, 3, 4}

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Select test parameters
		pair := pathPairs[i%len(pathPairs)]
		hops := maxHops[i%len(maxHops)]
		
		// Use the internal findPathBFS function directly
		_, err := executor.findPathBFS(pair.source, pair.target, hops)
		if err != nil {
			// Path might not exist for all pairs
			b.Logf("Error during path finding: %v", err)
		}
	}
}

// BenchmarkTraversal_EarlyTermination benchmarks traversal with early termination
func BenchmarkTraversal_EarlyTermination(b *testing.B) {
	engine, nodeIndex, edgeIndex, _, _, _, _, tempDir := setupBenchmarkDB(b)
	defer cleanupBenchmarkDB(b, tempDir)

	// Create traversal
	traversal := NewTraversal(engine, nodeIndex, edgeIndex)
	traversal.SetMaxDepth(5) // Use consistent max depth

	// Try different starting nodes
	startNodeIDs := []uint64{1, 5, 10, 15, 20, 25, 30, 35, 40, 45}
	targets := []string{"Person", "Company", "Product", "Location", "Event"}

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Select test parameters
		startNodeID := startNodeIDs[i%len(startNodeIDs)]
		targetLabel := targets[i%len(targets)]
		
		// Set visitor that terminates when finding a node with specific label
		visitor := func(node model.Node, depth int) (bool, error) {
			// Stop traversal if we find a node with the target label
			return node.Label != targetLabel, nil
		}
		
		// Run BFS traversal with early termination
		traversal.SetType(TraversalTypeBFS)
		err := traversal.Run(startNodeID, DirectionBoth, visitor)
		if err != nil {
			b.Fatalf("Error during traversal: %v", err)
		}
	}
}