package query

import (
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

func TestTraversal_BFS(t *testing.T) {
	// Skip during short tests
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	// Set up test database
	engine, nodeIndex, edgeIndex, _, _, tempDir := setupTestDB(t)
	defer cleanupTestDB(t, tempDir)

	// Create traversal
	traversal := NewTraversal(engine, nodeIndex, edgeIndex)
	traversal.SetMaxDepth(3)
	traversal.SetType(TraversalTypeBFS)

	// Test BFS traversal from Alice (1)
	visited := make(map[uint64]bool)
	depths := make(map[uint64]int)

	err := traversal.Run(1, DirectionBoth, func(node model.Node, depth int) (bool, error) {
		visited[node.ID] = true
		depths[node.ID] = depth
		return true, nil
	})

	if err != nil {
		t.Fatalf("Traversal.Run() error = %v", err)
	}

	// Check that all nodes were visited
	expectedNodes := map[uint64]bool{1: true, 2: true, 3: true, 4: true, 5: true}
	for id := range expectedNodes {
		if !visited[id] {
			t.Errorf("Node %d was not visited", id)
		}
	}

	// Check depths
	// Alice (1) is at depth 0
	if depths[1] != 0 {
		t.Errorf("Expected node 1 at depth 0, got %d", depths[1])
	}

	// Bob (2), Charlie (3), Acme (4) are at depth 1
	for _, id := range []uint64{2, 3, 4} {
		if depths[id] != 1 {
			t.Errorf("Expected node %d at depth 1, got %d", id, depths[id])
		}
	}

	// TechCorp (5) is at depth 2
	// It's reachable from Bob (2) or Charlie (3)
	if depths[5] != 2 {
		t.Errorf("Expected node 5 at depth 2, got %d", depths[5])
	}

	// Test early termination
	visitCount := 0
	err = traversal.Run(1, DirectionBoth, func(node model.Node, depth int) (bool, error) {
		visitCount++
		// Stop after the first node
		return false, nil
	})

	if err != nil {
		t.Fatalf("Traversal.Run() error = %v", err)
	}

	if visitCount != 1 {
		t.Errorf("Expected 1 node visit with early termination, got %d", visitCount)
	}
}

func TestTraversal_DFS(t *testing.T) {
	// Skip during short tests
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	// Set up test database
	engine, nodeIndex, edgeIndex, _, _, tempDir := setupTestDB(t)
	defer cleanupTestDB(t, tempDir)

	// Create traversal
	traversal := NewTraversal(engine, nodeIndex, edgeIndex)
	traversal.SetMaxDepth(3)
	traversal.SetType(TraversalTypeDFS)

	// Test DFS traversal from Alice (1)
	visited := make(map[uint64]bool)
	depths := make(map[uint64]int)

	err := traversal.Run(1, DirectionBoth, func(node model.Node, depth int) (bool, error) {
		visited[node.ID] = true
		depths[node.ID] = depth
		return true, nil
	})

	if err != nil {
		t.Fatalf("Traversal.Run() error = %v", err)
	}

	// Check that all nodes were visited
	expectedNodes := map[uint64]bool{1: true, 2: true, 3: true, 4: true, 5: true}
	for id := range expectedNodes {
		if !visited[id] {
			t.Errorf("Node %d was not visited", id)
		}
	}

	// Check depths
	// Alice (1) is at depth 0
	if depths[1] != 0 {
		t.Errorf("Expected node 1 at depth 0, got %d", depths[1])
	}

	// Test early termination
	visitCount := 0
	err = traversal.Run(1, DirectionBoth, func(node model.Node, depth int) (bool, error) {
		visitCount++
		// Stop after the first node
		return false, nil
	})

	if err != nil {
		t.Fatalf("Traversal.Run() error = %v", err)
	}

	if visitCount != 1 {
		t.Errorf("Expected 1 node visit with early termination, got %d", visitCount)
	}

	// Test max depth limit
	maxDepthVisited := 0
	traversal.SetMaxDepth(1)
	err = traversal.Run(1, DirectionBoth, func(node model.Node, depth int) (bool, error) {
		if depth > maxDepthVisited {
			maxDepthVisited = depth
		}
		return true, nil
	})

	if err != nil {
		t.Fatalf("Traversal.Run() error = %v", err)
	}

	if maxDepthVisited > 1 {
		t.Errorf("Expected max depth of 1, got %d", maxDepthVisited)
	}
}