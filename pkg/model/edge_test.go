package model

import (
	"testing"
)

func TestNewEdge(t *testing.T) {
	sourceID := uint64(123)
	targetID := uint64(456)
	label := "KNOWS"

	edge := NewEdge(sourceID, targetID, label)

	if edge.SourceID != sourceID {
		t.Errorf("Expected source ID to be %d, got %d", sourceID, edge.SourceID)
	}

	if edge.TargetID != targetID {
		t.Errorf("Expected target ID to be %d, got %d", targetID, edge.TargetID)
	}

	if edge.Label != label {
		t.Errorf("Expected edge label to be %s, got %s", label, edge.Label)
	}

	if edge.Properties == nil {
		t.Error("Expected properties map to be initialized, got nil")
	}

	if len(edge.Properties) != 0 {
		t.Errorf("Expected empty properties map, got %d items", len(edge.Properties))
	}
}

func TestEdgeProperties(t *testing.T) {
	edge := NewEdge(123, 456, "KNOWS")

	// Test AddProperty and GetProperty
	edge.AddProperty("since", "2020-01-01")
	value, exists := edge.GetProperty("since")

	if !exists {
		t.Error("Expected property 'since' to exist")
	}

	if value != "2020-01-01" {
		t.Errorf("Expected property value to be '2020-01-01', got '%s'", value)
	}

	// Test non-existent property
	_, exists = edge.GetProperty("strength")
	if exists {
		t.Error("Expected property 'strength' to not exist")
	}

	// Test updating a property
	edge.AddProperty("since", "2019-06-15")
	value, _ = edge.GetProperty("since")
	if value != "2019-06-15" {
		t.Errorf("Expected updated property value to be '2019-06-15', got '%s'", value)
	}

	// Test RemoveProperty
	edge.RemoveProperty("since")
	_, exists = edge.GetProperty("since")
	if exists {
		t.Error("Expected property 'since' to be removed")
	}
}
