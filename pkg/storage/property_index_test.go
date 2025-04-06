package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestPropertyIndex(t *testing.T) {
	// Create a temporary directory for storage files
	tempDir, err := os.MkdirTemp("", "property_index_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a storage engine
	config := DefaultEngineConfig()
	config.DataDir = tempDir
	storage, err := NewStorageEngine(config)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer storage.Close()

	// Create a node property index
	nodeIndex, err := NewNodePropertyIndex(storage, nil)
	if err != nil {
		t.Fatalf("Failed to create node property index: %v", err)
	}

	// Test Put and Get
	t.Run("PutAndGet", func(t *testing.T) {
		// Add some properties
		err = nodeIndex.Put([]byte("name|1"), []byte("Alice"))
		if err != nil {
			t.Fatalf("Failed to put property: %v", err)
		}

		err = nodeIndex.Put([]byte("age|1"), []byte("30"))
		if err != nil {
			t.Fatalf("Failed to put property: %v", err)
		}

		err = nodeIndex.Put([]byte("name|2"), []byte("Bob"))
		if err != nil {
			t.Fatalf("Failed to put property: %v", err)
		}

		err = nodeIndex.Put([]byte("age|2"), []byte("25"))
		if err != nil {
			t.Fatalf("Failed to put property: %v", err)
		}

		// Get properties
		nodeID, err := nodeIndex.Get([]byte("name|Alice"))
		if err != nil {
			t.Fatalf("Failed to get property: %v", err)
		}
		if string(nodeID) != "1" {
			t.Errorf("Expected nodeID '1', got '%s'", nodeID)
		}

		nodeID, err = nodeIndex.Get([]byte("age|30"))
		if err != nil {
			t.Fatalf("Failed to get property: %v", err)
		}
		if string(nodeID) != "1" {
			t.Errorf("Expected nodeID '1', got '%s'", nodeID)
		}

		nodeID, err = nodeIndex.Get([]byte("name|Bob"))
		if err != nil {
			t.Fatalf("Failed to get property: %v", err)
		}
		if string(nodeID) != "2" {
			t.Errorf("Expected nodeID '2', got '%s'", nodeID)
		}
	})

	// Test GetAll
	t.Run("GetAll", func(t *testing.T) {
		// Get all nodes with name property
		nodeIDs, err := nodeIndex.GetAll([]byte("name"))
		if err != nil {
			t.Fatalf("Failed to get all nodes: %v", err)
		}
		if len(nodeIDs) != 2 {
			t.Errorf("Expected 2 nodes, got %d", len(nodeIDs))
		}

		// Verify node IDs
		found1, found2 := false, false
		for _, id := range nodeIDs {
			if string(id) == "1" {
				found1 = true
			}
			if string(id) == "2" {
				found2 = true
			}
		}
		if !found1 || !found2 {
			t.Errorf("Missing expected node IDs. Found1: %v, Found2: %v", found1, found2)
		}
	})

	// Test Contains
	t.Run("Contains", func(t *testing.T) {
		exists, err := nodeIndex.Contains([]byte("name"))
		if err != nil {
			t.Fatalf("Failed to check contains: %v", err)
		}
		if !exists {
			t.Errorf("Expected 'name' property to exist")
		}

		exists, err = nodeIndex.Contains([]byte("name|Alice"))
		if err != nil {
			t.Fatalf("Failed to check contains: %v", err)
		}
		if !exists {
			t.Errorf("Expected 'name|Alice' property to exist")
		}

		exists, err = nodeIndex.Contains([]byte("unknown"))
		if err != nil {
			t.Fatalf("Failed to check contains: %v", err)
		}
		if exists {
			t.Errorf("Expected 'unknown' property to not exist")
		}
	})

	// Test DeleteValue
	t.Run("DeleteValue", func(t *testing.T) {
		err = nodeIndex.DeleteValue([]byte("name|Alice"), []byte("1"))
		if err != nil {
			t.Fatalf("Failed to delete value: %v", err)
		}

		// Verify it's deleted
		exists, err := nodeIndex.Contains([]byte("name|Alice"))
		if err != nil {
			t.Fatalf("Failed to check contains: %v", err)
		}
		if exists {
			t.Errorf("Expected 'name|Alice' to be deleted")
		}

		// Other property should still exist
		nodeID, err := nodeIndex.Get([]byte("name|Bob"))
		if err != nil {
			t.Fatalf("Failed to get property: %v", err)
		}
		if string(nodeID) != "2" {
			t.Errorf("Expected nodeID '2', got '%s'", nodeID)
		}
	})

	// Test Delete
	t.Run("Delete", func(t *testing.T) {
		err = nodeIndex.Delete([]byte("age"))
		if err != nil {
			t.Fatalf("Failed to delete property: %v", err)
		}

		// Verify all age properties are deleted
		exists, err := nodeIndex.Contains([]byte("age"))
		if err != nil {
			t.Fatalf("Failed to check contains: %v", err)
		}
		if exists {
			t.Errorf("Expected 'age' property to be deleted")
		}

		// Name property for Bob should still exist
		exists, err = nodeIndex.Contains([]byte("name|Bob"))
		if err != nil {
			t.Fatalf("Failed to check contains: %v", err)
		}
		if !exists {
			t.Errorf("Expected 'name|Bob' to still exist")
		}
	})

	// Test flush and reload
	t.Run("FlushAndReload", func(t *testing.T) {
		// Flush the index
		err = nodeIndex.Flush()
		if err != nil {
			t.Fatalf("Failed to flush index: %v", err)
		}

		// Close the storage
		err = storage.Close()
		if err != nil {
			t.Fatalf("Failed to close storage: %v", err)
		}

		// Reopen the storage
		storage, err = NewStorageEngine(config)
		if err != nil {
			t.Fatalf("Failed to reopen storage: %v", err)
		}

		// Create a new node property index
		nodeIndex, err = NewNodePropertyIndex(storage, nil)
		if err != nil {
			t.Fatalf("Failed to create node property index: %v", err)
		}

		// Verify data is still there
		exists, err := nodeIndex.Contains([]byte("name|Bob"))
		if err != nil {
			t.Fatalf("Failed to check contains: %v", err)
		}
		if !exists {
			t.Errorf("Expected 'name|Bob' to still exist after reload")
		}
	})
}

func TestPropertyIndexWithDifferentValueTypes(t *testing.T) {
	// Create a temporary directory for storage files
	tempDir, err := os.MkdirTemp("", "property_index_value_types_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a storage engine
	config := DefaultEngineConfig()
	config.DataDir = tempDir
	storage, err := NewStorageEngine(config)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer storage.Close()

	// Create a node property index
	nodeIndex, err := NewNodePropertyIndex(storage, nil)
	if err != nil {
		t.Fatalf("Failed to create node property index: %v", err)
	}

	// Test different value types
	t.Run("ValueTypes", func(t *testing.T) {
		// Add properties with different value types
		// String
		err = nodeIndex.Put([]byte("name|1"), []byte("Alice"))
		if err != nil {
			t.Fatalf("Failed to put string property: %v", err)
		}

		// Integer
		err = nodeIndex.Put([]byte("age|1"), []byte("30"))
		if err != nil {
			t.Fatalf("Failed to put integer property: %v", err)
		}

		// Float
		err = nodeIndex.Put([]byte("score|1"), []byte("98.5"))
		if err != nil {
			t.Fatalf("Failed to put float property: %v", err)
		}

		// Boolean
		err = nodeIndex.Put([]byte("active|1"), []byte("true"))
		if err != nil {
			t.Fatalf("Failed to put boolean property: %v", err)
		}

		// Get properties
		nodeID, err := nodeIndex.Get([]byte("name|Alice"))
		if err != nil {
			t.Fatalf("Failed to get string property: %v", err)
		}
		if string(nodeID) != "1" {
			t.Errorf("Expected nodeID '1', got '%s'", nodeID)
		}

		nodeID, err = nodeIndex.Get([]byte("age|30"))
		if err != nil {
			t.Fatalf("Failed to get integer property: %v", err)
		}
		if string(nodeID) != "1" {
			t.Errorf("Expected nodeID '1', got '%s'", nodeID)
		}

		nodeID, err = nodeIndex.Get([]byte("score|98.5"))
		if err != nil {
			t.Fatalf("Failed to get float property: %v", err)
		}
		if string(nodeID) != "1" {
			t.Errorf("Expected nodeID '1', got '%s'", nodeID)
		}

		nodeID, err = nodeIndex.Get([]byte("active|true"))
		if err != nil {
			t.Fatalf("Failed to get boolean property: %v", err)
		}
		if string(nodeID) != "1" {
			t.Errorf("Expected nodeID '1', got '%s'", nodeID)
		}
	})
}

func TestFullTextPropertyIndex(t *testing.T) {
	// Create a temporary directory for storage files
	tempDir, err := os.MkdirTemp("", "fulltext_property_index_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a storage engine
	config := DefaultEngineConfig()
	config.DataDir = tempDir
	storage, err := NewStorageEngine(config)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer storage.Close()

	// Create a full-text property index
	nodeIndex, err := NewFullTextPropertyIndex(storage, nil, PropertyIndexTypeNode)
	if err != nil {
		t.Fatalf("Failed to create full-text property index: %v", err)
	}

	// Test basic full-text functionality (simple version for now)
	t.Run("BasicFullText", func(t *testing.T) {
		// Add some text properties
		err = nodeIndex.Put([]byte("description|1"), []byte("This is a sample text with important keywords"))
		if err != nil {
			t.Fatalf("Failed to put property: %v", err)
		}

		err = nodeIndex.Put([]byte("description|2"), []byte("Another example with different words"))
		if err != nil {
			t.Fatalf("Failed to put property: %v", err)
		}

		// Get properties
		nodeIDs, err := nodeIndex.GetAll([]byte("description"))
		if err != nil {
			t.Fatalf("Failed to get all nodes: %v", err)
		}
		if len(nodeIDs) != 2 {
			t.Errorf("Expected 2 nodes, got %d", len(nodeIDs))
		}

		// Verify node IDs
		found1, found2 := false, false
		for _, id := range nodeIDs {
			if string(id) == "1" {
				found1 = true
			}
			if string(id) == "2" {
				found2 = true
			}
		}
		if !found1 || !found2 {
			t.Errorf("Missing expected node IDs. Found1: %v, Found2: %v", found1, found2)
		}
	})
}

func BenchmarkPropertyIndexOperations(b *testing.B) {
	// Create a temporary directory for storage files
	tempDir, err := os.MkdirTemp("", "property_index_benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a storage engine
	config := DefaultEngineConfig()
	config.DataDir = tempDir
	storage, err := NewStorageEngine(config)
	if err != nil {
		b.Fatalf("Failed to create storage engine: %v", err)
	}
	defer storage.Close()

	// Create a node property index
	nodeIndex, err := NewNodePropertyIndex(storage, nil)
	if err != nil {
		b.Fatalf("Failed to create node property index: %v", err)
	}

	// Benchmark Put operation
	b.Run("Put", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("property%d|%d", i%100, i)
			value := fmt.Sprintf("value%d", i)
			err := nodeIndex.Put([]byte(key), []byte(value))
			if err != nil {
				b.Fatalf("Failed to put property: %v", err)
			}
		}
	})

	// Setup for Get benchmark
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("property%d|%d", i%100, i)
		value := fmt.Sprintf("value%d", i)
		err := nodeIndex.Put([]byte(key), []byte(value))
		if err != nil {
			b.Fatalf("Failed to put property: %v", err)
		}
	}

	// Benchmark Get operation
	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("property%d|value%d", i%100, i%1000)
			_, _ = nodeIndex.Get([]byte(key))
		}
	})

	// Benchmark GetAll operation
	b.Run("GetAll", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("property%d", i%100)
			_, _ = nodeIndex.GetAll([]byte(key))
		}
	})
}