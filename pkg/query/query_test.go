package query

import (
	"testing"
)

func TestEngine_API(t *testing.T) {
	// Skip during short tests
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	// Set up test database
	engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties, tempDir := setupTestDB(t)
	defer cleanupTestDB(t, tempDir)

	// Create query engine with all indexes
	queryEngine := NewEngineWithAllIndexes(engine, nodeIndex, edgeIndex, nodeLabels, edgeLabels, nodeProperties, edgeProperties)

	// Test FindNodesByLabel
	t.Run("FindNodesByLabel", func(t *testing.T) {
		result, err := queryEngine.FindNodesByLabel("Person")
		if err != nil {
			t.Fatalf("Engine.FindNodesByLabel() error = %v", err)
		}
		if len(result.Nodes) != 3 {
			t.Errorf("Expected 3 nodes, got %d", len(result.Nodes))
		}
	})

	// Test FindEdgesByLabel
	t.Run("FindEdgesByLabel", func(t *testing.T) {
		result, err := queryEngine.FindEdgesByLabel("KNOWS")
		if err != nil {
			t.Fatalf("Engine.FindEdgesByLabel() error = %v", err)
		}
		if len(result.Edges) != 3 {
			t.Errorf("Expected 3 edges, got %d", len(result.Edges))
		}
	})

	// Test FindNeighbors
	t.Run("FindNeighbors", func(t *testing.T) {
		result, err := queryEngine.FindNeighbors(1, DirectionOutgoing)
		if err != nil {
			t.Fatalf("Engine.FindNeighbors() error = %v", err)
		}
		if len(result.Nodes) < 3 {
			t.Errorf("Expected at least 3 nodes, got %d", len(result.Nodes))
		}
	})

	// Test FindPath
	t.Run("FindPath", func(t *testing.T) {
		result, err := queryEngine.FindPath(1, 5, 3)
		if err != nil {
			t.Fatalf("Engine.FindPath() error = %v", err)
		}
		if len(result.Paths) != 1 {
			t.Errorf("Expected 1 path, got %d", len(result.Paths))
		}
		if len(result.Paths[0].Nodes) < 3 {
			t.Errorf("Expected at least 3 nodes in path, got %d", len(result.Paths[0].Nodes))
		}
	})

	// Test Execute
	t.Run("Execute", func(t *testing.T) {
		result, err := queryEngine.Execute(`FIND_NODES_BY_LABEL(label: "Person")`)
		if err != nil {
			t.Fatalf("Engine.Execute() error = %v", err)
		}
		if len(result.Nodes) != 3 {
			t.Errorf("Expected 3 nodes, got %d", len(result.Nodes))
		}
	})

	// Test Execute with JSON
	t.Run("Execute with JSON", func(t *testing.T) {
		result, err := queryEngine.Execute(`{"type": "FIND_NODES_BY_LABEL", "parameters": {"label": "Person"}}`)
		if err != nil {
			t.Fatalf("Engine.Execute() error = %v", err)
		}
		if len(result.Nodes) != 3 {
			t.Errorf("Expected 3 nodes, got %d", len(result.Nodes))
		}
	})
}

func TestUtils(t *testing.T) {
	t.Run("FormatUint64", func(t *testing.T) {
		expected := "123"
		got := FormatUint64(123)
		if got != expected {
			t.Errorf("FormatUint64() = %v, want %v", got, expected)
		}
	})

	t.Run("FormatInt", func(t *testing.T) {
		expected := "123"
		got := FormatInt(123)
		if got != expected {
			t.Errorf("FormatInt() = %v, want %v", got, expected)
		}
	})

	t.Run("ParseUint64", func(t *testing.T) {
		expected := uint64(123)
		got, err := ParseUint64("123")
		if err != nil {
			t.Errorf("ParseUint64() error = %v", err)
		}
		if got != expected {
			t.Errorf("ParseUint64() = %v, want %v", got, expected)
		}
	})

	t.Run("ParseInt", func(t *testing.T) {
		expected := 123
		got, err := ParseInt("123")
		if err != nil {
			t.Errorf("ParseInt() error = %v", err)
		}
		if got != expected {
			t.Errorf("ParseInt() = %v, want %v", got, expected)
		}
	})

	t.Run("GetOrDefault", func(t *testing.T) {
		params := map[string]string{
			"key1": "value1",
		}
		got := GetOrDefault(params, "key1", "default")
		if got != "value1" {
			t.Errorf("GetOrDefault() = %v, want %v", got, "value1")
		}
		got = GetOrDefault(params, "key2", "default")
		if got != "default" {
			t.Errorf("GetOrDefault() = %v, want %v", got, "default")
		}
	})

	t.Run("FormatNodeKey", func(t *testing.T) {
		expected := "node:123"
		got := FormatNodeKey(123)
		if got != expected {
			t.Errorf("FormatNodeKey() = %v, want %v", got, expected)
		}
	})

	t.Run("FormatEdgeKey", func(t *testing.T) {
		expected := "edge:123"
		got := FormatEdgeKey("123")
		if got != expected {
			t.Errorf("FormatEdgeKey() = %v, want %v", got, expected)
		}
	})

	t.Run("FormatOutgoingEdgesKey", func(t *testing.T) {
		expected := "outgoing:123"
		got := FormatOutgoingEdgesKey(123)
		if got != expected {
			t.Errorf("FormatOutgoingEdgesKey() = %v, want %v", got, expected)
		}
	})

	t.Run("FormatIncomingEdgesKey", func(t *testing.T) {
		expected := "incoming:123"
		got := FormatIncomingEdgesKey(123)
		if got != expected {
			t.Errorf("FormatIncomingEdgesKey() = %v, want %v", got, expected)
		}
	})
}
