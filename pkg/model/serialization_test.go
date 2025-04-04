package model

import (
	"encoding/binary"
	"testing"
)

func TestNodeSerialization(t *testing.T) {
	// Create a test node
	node := NewNode(42, "Person")
	node.AddProperty("name", "John Doe")
	node.AddProperty("age", "30")

	// Serialize the node
	data, err := SerializeNode(node)
	if err != nil {
		t.Fatalf("Failed to serialize node: %v", err)
	}

	// Deserialize the node
	deserializedNode, err := DeserializeNode(data)
	if err != nil {
		t.Fatalf("Failed to deserialize node: %v", err)
	}

	// Verify the deserialized node matches the original
	if deserializedNode.ID != node.ID {
		t.Errorf("Expected node ID to be %d, got %d", node.ID, deserializedNode.ID)
	}

	if deserializedNode.Label != node.Label {
		t.Errorf("Expected node label to be %s, got %s", node.Label, deserializedNode.Label)
	}

	if len(deserializedNode.Properties) != len(node.Properties) {
		t.Errorf("Expected node to have %d properties, got %d", len(node.Properties), len(deserializedNode.Properties))
	}

	for key, expectedValue := range node.Properties {
		actualValue, exists := deserializedNode.GetProperty(key)
		if !exists {
			t.Errorf("Expected property %s to exist in deserialized node", key)
		}
		if actualValue != expectedValue {
			t.Errorf("Expected property %s to have value %s, got %s", key, expectedValue, actualValue)
		}
	}
}

func TestEdgeSerialization(t *testing.T) {
	// Create a test edge
	edge := NewEdge(1, 2, "KNOWS")
	edge.AddProperty("since", "2020-01-01")
	edge.AddProperty("strength", "close")

	// Serialize the edge
	data, err := SerializeEdge(edge)
	if err != nil {
		t.Fatalf("Failed to serialize edge: %v", err)
	}

	// Deserialize the edge
	deserializedEdge, err := DeserializeEdge(data)
	if err != nil {
		t.Fatalf("Failed to deserialize edge: %v", err)
	}

	// Verify the deserialized edge matches the original
	if deserializedEdge.SourceID != edge.SourceID {
		t.Errorf("Expected edge source ID to be %d, got %d", edge.SourceID, deserializedEdge.SourceID)
	}

	if deserializedEdge.TargetID != edge.TargetID {
		t.Errorf("Expected edge target ID to be %d, got %d", edge.TargetID, deserializedEdge.TargetID)
	}

	if deserializedEdge.Label != edge.Label {
		t.Errorf("Expected edge label to be %s, got %s", edge.Label, deserializedEdge.Label)
	}

	if len(deserializedEdge.Properties) != len(edge.Properties) {
		t.Errorf("Expected edge to have %d properties, got %d", len(edge.Properties), len(deserializedEdge.Properties))
	}

	for key, expectedValue := range edge.Properties {
		actualValue, exists := deserializedEdge.GetProperty(key)
		if !exists {
			t.Errorf("Expected property %s to exist in deserialized edge", key)
		}
		if actualValue != expectedValue {
			t.Errorf("Expected property %s to have value %s, got %s", key, expectedValue, actualValue)
		}
	}
}

func TestPropertySerialization(t *testing.T) {
	// Create a test property
	property := NewProperty("name", "Jane Smith")

	// Serialize the property
	data, err := SerializeProperty(property)
	if err != nil {
		t.Fatalf("Failed to serialize property: %v", err)
	}

	// Deserialize the property
	deserializedProperty, err := DeserializeProperty(data)
	if err != nil {
		t.Fatalf("Failed to deserialize property: %v", err)
	}

	// Verify the deserialized property matches the original
	if deserializedProperty.Key != property.Key {
		t.Errorf("Expected property key to be %s, got %s", property.Key, deserializedProperty.Key)
	}

	if deserializedProperty.Value != property.Value {
		t.Errorf("Expected property value to be %s, got %s", property.Value, deserializedProperty.Value)
	}
}

func TestGenericSerialization(t *testing.T) {
	// Test node serialization and deserialization
	node := NewNode(42, "Person")
	node.AddProperty("name", "John Doe")

	nodeData, err := SerializeToBytesForPage(node)
	if err != nil {
		t.Fatalf("Failed to serialize node using generic function: %v", err)
	}

	deserializedEntity, err := DeserializeFromPageBytes(nodeData)
	if err != nil {
		t.Fatalf("Failed to deserialize node using generic function: %v", err)
	}

	deserializedNode, ok := deserializedEntity.(*Node)
	if !ok {
		t.Fatalf("Expected deserialized entity to be *Node, got %T", deserializedEntity)
	}

	if deserializedNode.ID != node.ID {
		t.Errorf("Expected node ID to be %d, got %d", node.ID, deserializedNode.ID)
	}

	// Test edge serialization and deserialization
	edge := NewEdge(1, 2, "KNOWS")
	edge.AddProperty("since", "2020-01-01")

	edgeData, err := SerializeToBytesForPage(edge)
	if err != nil {
		t.Fatalf("Failed to serialize edge using generic function: %v", err)
	}

	deserializedEntity, err = DeserializeFromPageBytes(edgeData)
	if err != nil {
		t.Fatalf("Failed to deserialize edge using generic function: %v", err)
	}

	deserializedEdge, ok := deserializedEntity.(*Edge)
	if !ok {
		t.Fatalf("Expected deserialized entity to be *Edge, got %T", deserializedEntity)
	}

	if deserializedEdge.Label != edge.Label {
		t.Errorf("Expected edge label to be %s, got %s", edge.Label, deserializedEdge.Label)
	}

	// Test property serialization and deserialization
	property := NewProperty("name", "Jane Smith")

	propertyData, err := SerializeToBytesForPage(property)
	if err != nil {
		t.Fatalf("Failed to serialize property using generic function: %v", err)
	}

	deserializedEntity, err = DeserializeFromPageBytes(propertyData)
	if err != nil {
		t.Fatalf("Failed to deserialize property using generic function: %v", err)
	}

	deserializedProperty, ok := deserializedEntity.(*Property)
	if !ok {
		t.Fatalf("Expected deserialized entity to be *Property, got %T", deserializedEntity)
	}

	if deserializedProperty.Value != property.Value {
		t.Errorf("Expected property value to be %s, got %s", property.Value, deserializedProperty.Value)
	}
}

func TestErrorCases(t *testing.T) {
	// Test nil node
	_, err := SerializeNode(nil)
	if err == nil {
		t.Error("Expected error when serializing nil Node, got nil")
	}

	// Test nil edge
	_, err = SerializeEdge(nil)
	if err == nil {
		t.Error("Expected error when serializing nil Edge, got nil")
	}

	// Test nil property
	_, err = SerializeProperty(nil)
	if err == nil {
		t.Error("Expected error when serializing nil Property, got nil")
	}

	// Test invalid data for node deserialization
	_, err = DeserializeNode([]byte{1, 2, 3})
	if err == nil {
		t.Error("Expected error when deserializing invalid data as Node, got nil")
	}

	// Test invalid magic bytes
	invalidMagicData := make([]byte, 100)
	copy(invalidMagicData, []byte{0, 0, 0, 0, 1, 0, TypeNode})
	_, err = DeserializeNode(invalidMagicData)
	if err != ErrInvalidSerializedData {
		t.Errorf("Expected ErrInvalidSerializedData, got %v", err)
	}

	// Test invalid version
	invalidVersionData := make([]byte, 100)
	binary.LittleEndian.PutUint32(invalidVersionData, SerializationMagic)
	binary.LittleEndian.PutUint16(invalidVersionData[4:], 999) // Invalid version
	invalidVersionData[6] = TypeNode
	_, err = DeserializeNode(invalidVersionData)
	if err != ErrUnsupportedVersion {
		t.Errorf("Expected ErrUnsupportedVersion, got %v", err)
	}

	// Test invalid entity type
	invalidTypeData := make([]byte, 100)
	binary.LittleEndian.PutUint32(invalidTypeData, SerializationMagic)
	binary.LittleEndian.PutUint16(invalidTypeData[4:], SerializationVersion)
	invalidTypeData[6] = 99 // Invalid type
	_, err = DeserializeNode(invalidTypeData)
	if err != ErrInvalidEntityType {
		t.Errorf("Expected ErrInvalidEntityType, got %v", err)
	}

	// Test unsupported entity type for serialization
	_, err = SerializeToBytesForPage("not a valid entity")
	if err == nil {
		t.Error("Expected error when serializing unsupported type, got nil")
	}
}

func TestPageDataSerialization(t *testing.T) {
	// Create a page
	page := NewPage(1, 4096)
	
	// Create a node to serialize into the page
	node := NewNode(42, "Person")
	node.AddProperty("name", "John Doe")
	
	// Serialize the node
	nodeData, err := SerializeNode(node)
	if err != nil {
		t.Fatalf("Failed to serialize node: %v", err)
	}
	
	// Copy the serialized node data into the page
	if len(nodeData) > len(page.Data) {
		t.Fatalf("Serialized node data (%d bytes) exceeds page size (%d bytes)", len(nodeData), len(page.Data))
	}
	
	copy(page.Data, nodeData)
	
	// Mark where the serialized data ends
	dataLength := len(nodeData)
	
	// Deserialize the node from the page
	deserializedEntity, err := DeserializeFromPageBytes(page.Data[:dataLength])
	if err != nil {
		t.Fatalf("Failed to deserialize node from page: %v", err)
	}
	
	deserializedNode, ok := deserializedEntity.(*Node)
	if !ok {
		t.Fatalf("Expected deserialized entity to be *Node, got %T", deserializedEntity)
	}
	
	// Verify the deserialized node matches the original
	if deserializedNode.ID != node.ID {
		t.Errorf("Expected node ID to be %d, got %d", node.ID, deserializedNode.ID)
	}
	
	if deserializedNode.Label != node.Label {
		t.Errorf("Expected node label to be %s, got %s", node.Label, deserializedNode.Label)
	}
	
	name, exists := deserializedNode.GetProperty("name")
	if !exists {
		t.Error("Expected property 'name' to exist in deserialized node")
	}
	if name != "John Doe" {
		t.Errorf("Expected property 'name' to have value 'John Doe', got '%s'", name)
	}
}