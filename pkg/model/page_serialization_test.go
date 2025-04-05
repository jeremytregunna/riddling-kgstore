package model

import (
	"testing"
)

func TestPageSerialization(t *testing.T) {
	// Create a test page
	pageID := uint64(42)
	pageSize := 4096
	page := NewPage(pageID, pageSize)
	page.SetFlag(PageFlagDirty)

	// Fill the page with some sample data
	for i := 0; i < 100; i++ {
		page.Data[i] = byte(i % 256)
	}

	// Serialize the page
	serializedPage, err := SerializePage(page)
	if err != nil {
		t.Fatalf("Failed to serialize page: %v", err)
	}

	// Deserialize the page
	deserializedPage, err := DeserializePage(serializedPage)
	if err != nil {
		t.Fatalf("Failed to deserialize page: %v", err)
	}

	// Verify the deserialized page matches the original
	if deserializedPage.ID != page.ID {
		t.Errorf("Expected page ID to be %d, got %d", page.ID, deserializedPage.ID)
	}

	if deserializedPage.Flags != page.Flags {
		t.Errorf("Expected page flags to be %d, got %d", page.Flags, deserializedPage.Flags)
	}

	if len(deserializedPage.Data) != len(page.Data) {
		t.Errorf("Expected page data size to be %d, got %d", len(page.Data), len(deserializedPage.Data))
	}

	// Check the first 100 bytes of data
	for i := 0; i < 100; i++ {
		if deserializedPage.Data[i] != page.Data[i] {
			t.Errorf("Data mismatch at position %d: expected %d, got %d", i, page.Data[i], deserializedPage.Data[i])
			break
		}
	}
}

func TestNilPageSerialization(t *testing.T) {
	// Test serializing a nil page
	_, err := SerializePage(nil)
	if err == nil {
		t.Error("Expected error when serializing nil page, got nil")
	}
}

func TestInvalidPageDeserialization(t *testing.T) {
	// Test deserializing invalid data
	_, err := DeserializePage([]byte{1, 2, 3})
	if err == nil {
		t.Error("Expected error when deserializing invalid data, got nil")
	}
}

func TestWriteAndReadEntityToPage(t *testing.T) {
	// Create a page
	page := NewPage(1, 4096)

	// Create entities to store in the page
	node := NewNode(42, "Person")
	node.AddProperty("name", "John Doe")

	edge := NewEdge(42, 43, "KNOWS")
	edge.AddProperty("since", "2020-01-01")

	property := NewProperty("location", "New York")

	// Write the node to the page
	nodeOffset := 0
	nodeSize, err := WriteEntityToPage(page, node, nodeOffset)
	if err != nil {
		t.Fatalf("Failed to write node to page: %v", err)
	}

	// Write the edge to the page
	edgeOffset := nodeOffset + nodeSize
	edgeSize, err := WriteEntityToPage(page, edge, edgeOffset)
	if err != nil {
		t.Fatalf("Failed to write edge to page: %v", err)
	}

	// Write the property to the page
	propOffset := edgeOffset + edgeSize
	propSize, err := WriteEntityToPage(page, property, propOffset)
	if err != nil {
		t.Fatalf("Failed to write property to page: %v", err)
	}

	// Read the node from the page
	readNode, readNodeSize, err := ReadEntityFromPage(page, nodeOffset)
	if err != nil {
		t.Fatalf("Failed to read node from page: %v", err)
	}

	// Verify node
	if readNodeSize != nodeSize {
		t.Errorf("Expected node size to be %d, got %d", nodeSize, readNodeSize)
	}

	nodeFromPage, ok := readNode.(*Node)
	if !ok {
		t.Fatalf("Expected node to be *Node, got %T", readNode)
	}

	if nodeFromPage.ID != node.ID {
		t.Errorf("Expected node ID to be %d, got %d", node.ID, nodeFromPage.ID)
	}

	if nodeFromPage.Label != node.Label {
		t.Errorf("Expected node label to be %s, got %s", node.Label, nodeFromPage.Label)
	}

	// Read the edge from the page
	readEdge, readEdgeSize, err := ReadEntityFromPage(page, edgeOffset)
	if err != nil {
		t.Fatalf("Failed to read edge from page: %v", err)
	}

	// Verify edge
	if readEdgeSize != edgeSize {
		t.Errorf("Expected edge size to be %d, got %d", edgeSize, readEdgeSize)
	}

	edgeFromPage, ok := readEdge.(*Edge)
	if !ok {
		t.Fatalf("Expected edge to be *Edge, got %T", readEdge)
	}

	if edgeFromPage.SourceID != edge.SourceID {
		t.Errorf("Expected edge source ID to be %d, got %d", edge.SourceID, edgeFromPage.SourceID)
	}

	// Read the property from the page
	readProp, readPropSize, err := ReadEntityFromPage(page, propOffset)
	if err != nil {
		t.Fatalf("Failed to read property from page: %v", err)
	}

	// Verify property
	if readPropSize != propSize {
		t.Errorf("Expected property size to be %d, got %d", propSize, readPropSize)
	}

	propFromPage, ok := readProp.(*Property)
	if !ok {
		t.Fatalf("Expected property to be *Property, got %T", readProp)
	}

	if propFromPage.Key != property.Key {
		t.Errorf("Expected property key to be %s, got %s", property.Key, propFromPage.Key)
	}

	// Verify page is marked as dirty after writes
	if !page.IsDirty() {
		t.Error("Expected page to be marked as dirty after writes")
	}
}

func TestWriteEntityError(t *testing.T) {
	// Create a small page to test overflow
	page := NewPage(1, 32)

	// Create a large node with properties to overflow the page
	node := NewNode(42, "PersonWithVeryLongNameThatWillOverflowThePage")
	node.AddProperty("name", "John Doe with a very long name that will overflow the page")
	node.AddProperty("description", "This is a very long description that will definitely overflow the small page we created")

	// Try to write the node to the page (should fail due to size)
	_, err := WriteEntityToPage(page, node, 0)
	if err == nil {
		t.Error("Expected error when writing entity that exceeds page size, got nil")
	}

	// Check if the error is of the expected type
	_, ok := err.(ErrPageDataSizeExceeded)
	if !ok {
		t.Errorf("Expected error to be ErrPageDataSizeExceeded, got %T", err)
	}

	// Test writing to an invalid offset
	_, err = WriteEntityToPage(page, NewProperty("test", "value"), -1)
	if err == nil {
		t.Error("Expected error when writing to negative offset, got nil")
	}

	_, err = WriteEntityToPage(page, NewProperty("test", "value"), 100)
	if err == nil {
		t.Error("Expected error when writing to offset beyond page size, got nil")
	}

	// Test writing to a nil page
	_, err = WriteEntityToPage(nil, NewProperty("test", "value"), 0)
	if err == nil {
		t.Error("Expected error when writing to nil page, got nil")
	}
}

func TestReadEntityError(t *testing.T) {
	// Create a page
	page := NewPage(1, 100)

	// Test reading from an invalid offset
	_, _, err := ReadEntityFromPage(page, -1)
	if err == nil {
		t.Error("Expected error when reading from negative offset, got nil")
	}

	_, _, err = ReadEntityFromPage(page, 100)
	if err == nil {
		t.Error("Expected error when reading from offset beyond page size, got nil")
	}

	// Test reading from a nil page
	_, _, err = ReadEntityFromPage(nil, 0)
	if err == nil {
		t.Error("Expected error when reading from nil page, got nil")
	}

	// Test reading invalid entity data
	// Write some invalid data to the page
	for i := 0; i < 10; i++ {
		page.Data[i] = byte(i)
	}

	_, _, err = ReadEntityFromPage(page, 0)
	if err == nil {
		t.Error("Expected error when reading invalid entity data, got nil")
	}
}
