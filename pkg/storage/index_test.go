package storage

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

func TestNodePrimaryIndex(t *testing.T) {
	// Create a temporary directory for the database
	dbDir, err := os.MkdirTemp("", "node_index_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dbDir)

	// Create a storage engine
	config := EngineConfig{
		DataDir:              dbDir,
		MemTableSize:         4096,
		SyncWrites:           true,
		Logger:               model.NewNoOpLogger(),
		Comparator:           DefaultComparator,
		BackgroundCompaction: false,
		BloomFilterFPR:       0.01,
	}

	engine, err := NewStorageEngine(config)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	// Create a node primary index
	index, err := NewNodeIndex(engine, model.NewNoOpLogger())
	if err != nil {
		t.Fatalf("Failed to create node index: %v", err)
	}

	// Test data
	nodeID1 := make([]byte, 8)
	binary.LittleEndian.PutUint64(nodeID1, 1)
	nodeData1 := []byte("node1-data")

	nodeID2 := make([]byte, 8)
	binary.LittleEndian.PutUint64(nodeID2, 2)
	nodeData2 := []byte("node2-data")

	// Test Put and Get
	err = index.Put(nodeID1, nodeData1)
	if err != nil {
		t.Errorf("Failed to put node1: %v", err)
	}

	err = index.Put(nodeID2, nodeData2)
	if err != nil {
		t.Errorf("Failed to put node2: %v", err)
	}

	// Verify the data
	retrievedData1, err := index.Get(nodeID1)
	if err != nil {
		t.Errorf("Failed to get node1: %v", err)
	}
	if !bytes.Equal(retrievedData1, nodeData1) {
		t.Errorf("Expected node1 data %q, got %q", nodeData1, retrievedData1)
	}

	retrievedData2, err := index.Get(nodeID2)
	if err != nil {
		t.Errorf("Failed to get node2: %v", err)
	}
	if !bytes.Equal(retrievedData2, nodeData2) {
		t.Errorf("Expected node2 data %q, got %q", nodeData2, retrievedData2)
	}

	// Test Contains
	exists, err := index.Contains(nodeID1)
	if err != nil {
		t.Errorf("Error checking if node1 exists: %v", err)
	}
	if !exists {
		t.Error("Node1 should exist in the index")
	}

	// Test GetAll (should return a single value for primary index)
	allData, err := index.GetAll(nodeID1)
	if err != nil {
		t.Errorf("Failed to GetAll for node1: %v", err)
	}
	if len(allData) != 1 || !bytes.Equal(allData[0], nodeData1) {
		t.Errorf("GetAll returned unexpected data: %v", allData)
	}

	// Test Delete
	err = index.Delete(nodeID1)
	if err != nil {
		t.Errorf("Failed to delete node1: %v", err)
	}

	// Verify node1 is gone
	_, err = index.Get(nodeID1)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for deleted node, got %v", err)
	}

	exists, err = index.Contains(nodeID1)
	if err != nil {
		t.Errorf("Error checking if deleted node exists: %v", err)
	}
	if exists {
		t.Error("Deleted node should not exist in the index")
	}

	// Test DeleteValue (should be same as Delete for primary index)
	err = index.DeleteValue(nodeID2, nodeData2)
	if err != nil {
		t.Errorf("Failed to DeleteValue for node2: %v", err)
	}

	// Verify node2 is gone
	_, err = index.Get(nodeID2)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for deleted node, got %v", err)
	}

	// Verify index type
	if index.GetType() != IndexTypeNodePrimary {
		t.Errorf("Expected index type %d, got %d", IndexTypeNodePrimary, index.GetType())
	}

	// Test Flush
	err = index.Flush()
	if err != nil {
		t.Errorf("Failed to flush index: %v", err)
	}

	// Test Close
	err = index.Close()
	if err != nil {
		t.Errorf("Failed to close index: %v", err)
	}

	// Test operations on closed index
	_, err = index.Get(nodeID1)
	if err != ErrIndexClosed {
		t.Errorf("Expected ErrIndexClosed for closed index, got %v", err)
	}
}

func TestNodeLabelIndex(t *testing.T) {
	// We'll run tests with both implementations
	implementations := []struct {
		name       string
		useLSMIndex bool
	}{
		{"Standard Index", false},
		{"LSM-tree Index", true},
	}
	
	for _, impl := range implementations {
		t.Run(impl.name, func(t *testing.T) {
			// Create a temporary directory for the database
			dbDir, err := os.MkdirTemp("", "node_label_index_test")
			if err != nil {
				t.Fatalf("Failed to create temp directory: %v", err)
			}
			defer os.RemoveAll(dbDir)

			// Create a storage engine
			config := EngineConfig{
				DataDir:              dbDir,
				MemTableSize:         4096,
				SyncWrites:           true,
				Logger:               model.NewNoOpLogger(),
				Comparator:           DefaultComparator,
				BackgroundCompaction: false,
				BloomFilterFPR:       0.01,
				UseLSMNodeLabelIndex: impl.useLSMIndex,
			}

			engine, err := NewStorageEngine(config)
			if err != nil {
				t.Fatalf("Failed to create storage engine: %v", err)
			}
			defer engine.Close()

			// Create a node label index
			index, err := NewNodeLabelIndex(engine, model.NewNoOpLogger())
			if err != nil {
				t.Fatalf("Failed to create node label index: %v", err)
			}

			// Test data
			label1 := []byte("Person")
			nodeID1 := make([]byte, 8)
			binary.LittleEndian.PutUint64(nodeID1, 1)
			nodeID2 := make([]byte, 8)
			binary.LittleEndian.PutUint64(nodeID2, 2)

			label2 := []byte("Organization")
			nodeID3 := make([]byte, 8)
			binary.LittleEndian.PutUint64(nodeID3, 3)

			// Test Put
			err = index.Put(label1, nodeID1)
			if err != nil {
				t.Errorf("Failed to put nodeID1 under label1: %v", err)
			}

			err = index.Put(label1, nodeID2)
			if err != nil {
				t.Errorf("Failed to put nodeID2 under label1: %v", err)
			}

			err = index.Put(label2, nodeID3)
			if err != nil {
				t.Errorf("Failed to put nodeID3 under label2: %v", err)
			}

			// Test GetAll
			nodesForLabel1, err := index.GetAll(label1)
			if err != nil {
				t.Errorf("Failed to GetAll for label1: %v", err)
			}
			if len(nodesForLabel1) != 2 {
				t.Errorf("Expected 2 nodes for label1, got %d", len(nodesForLabel1))
			}

			// Verify we got the expected node IDs
			found1 := false
			found2 := false
			for _, id := range nodesForLabel1 {
				if bytes.Equal(id, nodeID1) {
					found1 = true
				}
				if bytes.Equal(id, nodeID2) {
					found2 = true
				}
			}
			if !found1 || !found2 {
				t.Errorf("Did not find expected node IDs for label1: found1=%v, found2=%v", found1, found2)
			}

			// Test Get (should return the first node ID)
			firstNodeID, err := index.Get(label1)
			if err != nil {
				t.Errorf("Failed to Get for label1: %v", err)
			}
			if !bytes.Equal(firstNodeID, nodeID1) && !bytes.Equal(firstNodeID, nodeID2) {
				t.Errorf("Get returned unexpected node ID: %v", firstNodeID)
			}

			// Test Contains
			exists, err := index.Contains(label1)
			if err != nil {
				t.Errorf("Error checking if label1 exists: %v", err)
			}
			if !exists {
				t.Error("Label1 should exist in the index")
			}

			// Test DeleteValue
			err = index.DeleteValue(label1, nodeID1)
			if err != nil {
				t.Errorf("Failed to DeleteValue for nodeID1 under label1: %v", err)
			}

			// Verify nodeID1 is gone
			nodesForLabel1, err = index.GetAll(label1)
			if err != nil {
				t.Errorf("Failed to GetAll for label1 after delete: %v", err)
			}
			if len(nodesForLabel1) != 1 || !bytes.Equal(nodesForLabel1[0], nodeID2) {
				t.Errorf("Expected only nodeID2 for label1, got %v", nodesForLabel1)
			}

			// Test Delete
			err = index.Delete(label1)
			if err != nil {
				t.Errorf("Failed to Delete label1: %v", err)
			}

			// Verify label1 is gone
			exists, err = index.Contains(label1)
			if err != nil {
				t.Errorf("Error checking if deleted label exists: %v", err)
			}
			if exists {
				t.Error("Deleted label should not exist in the index")
			}

			// Verify index type
			if index.GetType() != IndexTypeNodeLabel {
				t.Errorf("Expected index type %d, got %d", IndexTypeNodeLabel, index.GetType())
			}

			// Test Flush
			err = index.Flush()
			if err != nil {
				t.Errorf("Failed to flush index: %v", err)
			}

			// Test Close
			err = index.Close()
			if err != nil {
				t.Errorf("Failed to close index: %v", err)
			}
		})
	}
}

func TestEdgeLabelIndex(t *testing.T) {
	// Create a temporary directory for the database
	dbDir, err := os.MkdirTemp("", "edge_label_index_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(dbDir)

	// Create a storage engine
	config := EngineConfig{
		DataDir:              dbDir,
		MemTableSize:         4096,
		SyncWrites:           true,
		Logger:               model.NewNoOpLogger(),
		Comparator:           DefaultComparator,
		BackgroundCompaction: false,
		BloomFilterFPR:       0.01,
	}

	engine, err := NewStorageEngine(config)
	if err != nil {
		t.Fatalf("Failed to create storage engine: %v", err)
	}
	defer engine.Close()

	// Create an edge label index
	index, err := NewEdgeLabelIndex(engine, model.NewNoOpLogger())
	if err != nil {
		t.Fatalf("Failed to create edge label index: %v", err)
	}

	// Test data
	label1 := []byte("KNOWS")
	edgeID1 := []byte("1-2") // source-target
	edgeID2 := []byte("1-3")

	label2 := []byte("WORKS_FOR")
	edgeID3 := []byte("2-4")

	// Test Put
	err = index.Put(label1, edgeID1)
	if err != nil {
		t.Errorf("Failed to put edgeID1 under label1: %v", err)
	}

	err = index.Put(label1, edgeID2)
	if err != nil {
		t.Errorf("Failed to put edgeID2 under label1: %v", err)
	}

	err = index.Put(label2, edgeID3)
	if err != nil {
		t.Errorf("Failed to put edgeID3 under label2: %v", err)
	}

	// Test GetAll
	edgesForLabel1, err := index.GetAll(label1)
	if err != nil {
		t.Errorf("Failed to GetAll for label1: %v", err)
	}
	if len(edgesForLabel1) != 2 {
		t.Errorf("Expected 2 edges for label1, got %d", len(edgesForLabel1))
	}

	// Verify we got the expected edge IDs
	found1 := false
	found2 := false
	for _, id := range edgesForLabel1 {
		if bytes.Equal(id, edgeID1) {
			found1 = true
		}
		if bytes.Equal(id, edgeID2) {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Errorf("Did not find expected edge IDs for label1: found1=%v, found2=%v", found1, found2)
	}

	// Test Get (should return the first edge ID)
	firstEdgeID, err := index.Get(label1)
	if err != nil {
		t.Errorf("Failed to Get for label1: %v", err)
	}
	if !bytes.Equal(firstEdgeID, edgeID1) && !bytes.Equal(firstEdgeID, edgeID2) {
		t.Errorf("Get returned unexpected edge ID: %v", firstEdgeID)
	}

	// Test Contains
	exists, err := index.Contains(label1)
	if err != nil {
		t.Errorf("Error checking if label1 exists: %v", err)
	}
	if !exists {
		t.Error("Label1 should exist in the index")
	}

	// Test DeleteValue
	err = index.DeleteValue(label1, edgeID1)
	if err != nil {
		t.Errorf("Failed to DeleteValue for edgeID1 under label1: %v", err)
	}

	// Verify edgeID1 is gone
	edgesForLabel1, err = index.GetAll(label1)
	if err != nil {
		t.Errorf("Failed to GetAll for label1 after delete: %v", err)
	}
	if len(edgesForLabel1) != 1 || !bytes.Equal(edgesForLabel1[0], edgeID2) {
		t.Errorf("Expected only edgeID2 for label1, got %v", edgesForLabel1)
	}

	// Test Delete
	err = index.Delete(label1)
	if err != nil {
		t.Errorf("Failed to Delete label1: %v", err)
	}

	// Verify label1 is gone
	exists, err = index.Contains(label1)
	if err != nil {
		t.Errorf("Error checking if deleted label exists: %v", err)
	}
	if exists {
		t.Error("Deleted label should not exist in the index")
	}

	// Verify index type
	if index.GetType() != IndexTypeEdgeLabel {
		t.Errorf("Expected index type %d, got %d", IndexTypeEdgeLabel, index.GetType())
	}

	// Test Flush
	err = index.Flush()
	if err != nil {
		t.Errorf("Failed to flush index: %v", err)
	}

	// Test Close
	err = index.Close()
	if err != nil {
		t.Errorf("Failed to close index: %v", err)
	}
}