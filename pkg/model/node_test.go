package model

import (
	"testing"
)

func TestNewNode(t *testing.T) {
	id := uint64(123)
	label := "Person"

	node := NewNode(id, label)

	if node.ID != id {
		t.Errorf("Expected node ID to be %d, got %d", id, node.ID)
	}

	if node.Label != label {
		t.Errorf("Expected node label to be %s, got %s", label, node.Label)
	}

	if node.Properties == nil {
		t.Error("Expected properties map to be initialized, got nil")
	}

	if len(node.Properties) != 0 {
		t.Errorf("Expected empty properties map, got %d items", len(node.Properties))
	}
}

func TestNodeProperties(t *testing.T) {
	node := NewNode(1, "Person")

	// Test AddProperty and GetProperty
	node.AddProperty("name", "John Doe")
	value, exists := node.GetProperty("name")

	if !exists {
		t.Error("Expected property 'name' to exist")
	}

	if value != "John Doe" {
		t.Errorf("Expected property value to be 'John Doe', got '%s'", value)
	}

	// Test non-existent property
	_, exists = node.GetProperty("age")
	if exists {
		t.Error("Expected property 'age' to not exist")
	}

	// Test updating a property
	node.AddProperty("name", "Jane Doe")
	value, _ = node.GetProperty("name")
	if value != "Jane Doe" {
		t.Errorf("Expected updated property value to be 'Jane Doe', got '%s'", value)
	}

	// Test RemoveProperty
	node.RemoveProperty("name")
	_, exists = node.GetProperty("name")
	if exists {
		t.Error("Expected property 'name' to be removed")
	}
}
