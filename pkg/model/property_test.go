package model

import (
	"bytes"
	"testing"
)

func TestNewProperty(t *testing.T) {
	key := "name"
	value := "John Doe"

	property := NewProperty(key, value)

	if property.Key != key {
		t.Errorf("Expected property key to be %s, got %s", key, property.Key)
	}

	if property.Value != value {
		t.Errorf("Expected property value to be %s, got %s", value, property.Value)
	}
}

func TestPropertyAccessors(t *testing.T) {
	property := NewProperty("age", "30")

	if property.GetKey() != "age" {
		t.Errorf("Expected GetKey() to return 'age', got '%s'", property.GetKey())
	}

	if property.GetValue() != "30" {
		t.Errorf("Expected GetValue() to return '30', got '%s'", property.GetValue())
	}
}

func TestPropertySetValue(t *testing.T) {
	property := NewProperty("age", "30")

	property.SetValue("31")

	if property.Value != "31" {
		t.Errorf("Expected value to be '31' after SetValue, got '%s'", property.Value)
	}

	if property.GetValue() != "31" {
		t.Errorf("Expected GetValue() to return '31' after SetValue, got '%s'", property.GetValue())
	}
}

func TestPropertyContainer(t *testing.T) {
	pc := NewPropertyContainer()

	// Test initial state
	if pc.Properties == nil {
		t.Error("Expected properties map to be initialized, got nil")
	}

	if len(pc.Properties) != 0 {
		t.Errorf("Expected empty properties map, got %d items", len(pc.Properties))
	}

	// Test AddProperty and GetProperty
	pc.AddProperty("name", "John Doe")
	value, exists := pc.GetProperty("name")

	if !exists {
		t.Error("Expected property 'name' to exist")
	}

	if value != "John Doe" {
		t.Errorf("Expected property value to be 'John Doe', got '%s'", value)
	}

	// Test non-existent property
	_, exists = pc.GetProperty("age")
	if exists {
		t.Error("Expected property 'age' to not exist")
	}

	// Test updating a property
	pc.AddProperty("name", "Jane Doe")
	value, _ = pc.GetProperty("name")
	if value != "Jane Doe" {
		t.Errorf("Expected updated property value to be 'Jane Doe', got '%s'", value)
	}

	// Test RemoveProperty
	pc.RemoveProperty("name")
	_, exists = pc.GetProperty("name")
	if exists {
		t.Error("Expected property 'name' to be removed")
	}

	// Test GetAllProperties
	pc.AddProperty("name", "John Doe")
	pc.AddProperty("age", "30")

	props := pc.GetAllProperties()
	if len(props) != 2 {
		t.Errorf("Expected 2 properties, got %d", len(props))
	}

	if props["name"] != "John Doe" {
		t.Errorf("Expected 'name' property to be 'John Doe', got '%s'", props["name"])
	}

	if props["age"] != "30" {
		t.Errorf("Expected 'age' property to be '30', got '%s'", props["age"])
	}

	// Verify that changes to the returned map don't affect the original
	props["name"] = "Modified"
	value, _ = pc.GetProperty("name")
	if value != "John Doe" {
		t.Errorf("Expected original property to remain 'John Doe', got '%s'", value)
	}
}

func TestPropertyContainerSerialization(t *testing.T) {
	// Create a container with some properties
	pc := NewPropertyContainer()
	pc.AddProperty("name", "John Doe")
	pc.AddProperty("age", "30")
	pc.AddProperty("city", "New York")

	// Serialize
	var buf bytes.Buffer
	err := pc.SerializeProperties(&buf)
	if err != nil {
		t.Fatalf("Failed to serialize PropertyContainer: %v", err)
	}

	// Deserialize to a new container
	newPC := NewPropertyContainer()
	err = newPC.DeserializeProperties(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Failed to deserialize PropertyContainer: %v", err)
	}

	// Verify properties were preserved
	if len(newPC.Properties) != 3 {
		t.Errorf("Expected 3 properties, got %d", len(newPC.Properties))
	}

	value, exists := newPC.GetProperty("name")
	if !exists {
		t.Error("Expected 'name' property to exist after deserialization")
	}
	if value != "John Doe" {
		t.Errorf("Expected 'name' property to be 'John Doe', got '%s'", value)
	}

	value, exists = newPC.GetProperty("age")
	if !exists {
		t.Error("Expected 'age' property to exist after deserialization")
	}
	if value != "30" {
		t.Errorf("Expected 'age' property to be '30', got '%s'", value)
	}

	value, exists = newPC.GetProperty("city")
	if !exists {
		t.Error("Expected 'city' property to exist after deserialization")
	}
	if value != "New York" {
		t.Errorf("Expected 'city' property to be 'New York', got '%s'", value)
	}
}

func TestNodeWithPropertyContainer(t *testing.T) {
	// Create a node and add some properties
	node := NewNode(123, "Person")
	node.AddProperty("name", "John Doe")
	node.AddProperty("age", "30")

	// Check properties are accessible
	value, exists := node.GetProperty("name")
	if !exists {
		t.Error("Expected 'name' property to exist")
	}
	if value != "John Doe" {
		t.Errorf("Expected 'name' property to be 'John Doe', got '%s'", value)
	}

	// Serialize the node
	data, err := SerializeNode(node)
	if err != nil {
		t.Fatalf("Failed to serialize node: %v", err)
	}

	// Deserialize to a new node
	newNode, err := DeserializeNode(data)
	if err != nil {
		t.Fatalf("Failed to deserialize node: %v", err)
	}

	// Verify properties were preserved
	value, exists = newNode.GetProperty("name")
	if !exists {
		t.Error("Expected 'name' property to exist after deserialization")
	}
	if value != "John Doe" {
		t.Errorf("Expected 'name' property to be 'John Doe', got '%s'", value)
	}

	value, exists = newNode.GetProperty("age")
	if !exists {
		t.Error("Expected 'age' property to exist after deserialization")
	}
	if value != "30" {
		t.Errorf("Expected 'age' property to be '30', got '%s'", value)
	}
}

func TestEdgeWithPropertyContainer(t *testing.T) {
	// Create an edge and add some properties
	edge := NewEdge(123, 456, "KNOWS")
	edge.AddProperty("since", "2020-01-01")
	edge.AddProperty("strength", "close")

	// Check properties are accessible
	value, exists := edge.GetProperty("since")
	if !exists {
		t.Error("Expected 'since' property to exist")
	}
	if value != "2020-01-01" {
		t.Errorf("Expected 'since' property to be '2020-01-01', got '%s'", value)
	}

	// Serialize the edge
	data, err := SerializeEdge(edge)
	if err != nil {
		t.Fatalf("Failed to serialize edge: %v", err)
	}

	// Deserialize to a new edge
	newEdge, err := DeserializeEdge(data)
	if err != nil {
		t.Fatalf("Failed to deserialize edge: %v", err)
	}

	// Verify properties were preserved
	value, exists = newEdge.GetProperty("since")
	if !exists {
		t.Error("Expected 'since' property to exist after deserialization")
	}
	if value != "2020-01-01" {
		t.Errorf("Expected 'since' property to be '2020-01-01', got '%s'", value)
	}

	value, exists = newEdge.GetProperty("strength")
	if !exists {
		t.Error("Expected 'strength' property to exist after deserialization")
	}
	if value != "close" {
		t.Errorf("Expected 'strength' property to be 'close', got '%s'", value)
	}
}
