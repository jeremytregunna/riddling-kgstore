package storage

import (
	"fmt"
	"os"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// setupLabelIndexBenchmark creates a storage engine and node/edge label indices for benchmarking
func setupLabelIndexBenchmark(b *testing.B, useLSMIndex bool) (*StorageEngine, Index, Index, func()) {
	// Create a temporary directory for the database
	tempDir, err := os.MkdirTemp("", "label_index_benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a storage engine
	config := EngineConfig{
		DataDir:              tempDir,
		MemTableSize:         1024 * 1024 * 10, // 10MB
		SyncWrites:           false,            // Disable syncing for performance
		Logger:               model.NewNoOpLogger(),
		Comparator:           DefaultComparator,
		BackgroundCompaction: false, // Disable background compaction
		BloomFilterFPR:       0.01,
		UseLSMNodeLabelIndex: useLSMIndex, // Control which implementation to use
	}

	engine, err := NewStorageEngine(config)
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create storage engine: %v", err)
	}

	// Create a node label index
	nodeLabelIndex, err := NewNodeLabelIndex(engine, model.NewNoOpLogger())
	if err != nil {
		engine.Close()
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create node label index: %v", err)
	}

	// Create an edge label index
	edgeLabelIndex, err := NewEdgeLabelIndex(engine, model.NewNoOpLogger())
	if err != nil {
		nodeLabelIndex.Close()
		engine.Close()
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create edge label index: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		nodeLabelIndex.Close()
		edgeLabelIndex.Close()
		engine.Close()
		os.RemoveAll(tempDir)
	}

	return engine, nodeLabelIndex, edgeLabelIndex, cleanup
}

// BenchmarkNodeLabelLookup measures node label index lookup performance
func BenchmarkNodeLabelLookup(b *testing.B) {
	// Test both implementations
	implementations := []struct {
		name        string
		useLSMIndex bool
	}{
		{"StandardIndex", false},
		{"LSMIndex", true},
	}

	// Test different data sizes
	dataSizes := []struct {
		name          string
		labelsCount   int // Number of different labels
		nodesPerLabel int // Number of nodes per label
	}{
		{"Small_10Labels_100Nodes", 10, 100},
		{"Medium_100Labels_100Nodes", 100, 100},
		{"Large_100Labels_1000Nodes", 100, 1000},
	}

	for _, impl := range implementations {
		for _, size := range dataSizes {
			testName := fmt.Sprintf("%s_%s", impl.name, size.name)
			b.Run(testName, func(b *testing.B) {
				_, nodeLabelIndex, _, cleanup := setupLabelIndexBenchmark(b, impl.useLSMIndex)
				defer cleanup()

				// Prepare benchmark data - populate the index
				for labelNum := 0; labelNum < size.labelsCount; labelNum++ {
					label := []byte(fmt.Sprintf("Label-%04d", labelNum))

					// Add nodes for this label
					for nodeNum := 0; nodeNum < size.nodesPerLabel; nodeNum++ {
						nodeID := []byte(fmt.Sprintf("Node-%04d-%06d", labelNum, nodeNum))
						if err := nodeLabelIndex.Put(label, nodeID); err != nil {
							b.Fatalf("Failed to add node to label index: %v", err)
						}
					}
				}

				// Random label to lookup during the benchmark
				targetLabel := []byte(fmt.Sprintf("Label-%04d", size.labelsCount/2)) // middle label

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Look up all nodes with the target label
					nodes, err := nodeLabelIndex.GetAll(targetLabel)
					if err != nil {
						b.Fatalf("Failed to get nodes by label: %v", err)
					}

					// Verify we got the expected number of nodes
					if len(nodes) != size.nodesPerLabel {
						b.Fatalf("Expected %d nodes, got %d", size.nodesPerLabel, len(nodes))
					}
				}
			})
		}
	}
}

// BenchmarkEdgeLabelLookup measures edge label index lookup performance
func BenchmarkEdgeLabelLookup(b *testing.B) {
	dataSizes := []struct {
		name          string
		labelsCount   int // Number of different edge labels
		edgesPerLabel int // Number of edges per label
	}{
		{"Small_10Labels_100Edges", 10, 100},
		{"Medium_100Labels_100Edges", 100, 100},
		{"Large_100Labels_1000Edges", 100, 1000},
	}

	for _, size := range dataSizes {
		b.Run(size.name, func(b *testing.B) {
			_, _, edgeLabelIndex, cleanup := setupLabelIndexBenchmark(b, false)
			defer cleanup()

			// Populate the edge label index
			for labelNum := 0; labelNum < size.labelsCount; labelNum++ {
				label := []byte(fmt.Sprintf("EdgeType-%04d", labelNum))

				// Add edges for this label
				for edgeNum := 0; edgeNum < size.edgesPerLabel; edgeNum++ {
					// Create an edge ID in a format like "source-target"
					edgeID := []byte(fmt.Sprintf("Edge-%04d-%06d", labelNum, edgeNum))
					if err := edgeLabelIndex.Put(label, edgeID); err != nil {
						b.Fatalf("Failed to add edge to label index: %v", err)
					}
				}
			}

			// Random label to lookup during the benchmark
			targetLabel := []byte(fmt.Sprintf("EdgeType-%04d", size.labelsCount/2)) // middle label

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Look up all edges with the target label
				edges, err := edgeLabelIndex.GetAll(targetLabel)
				if err != nil {
					b.Fatalf("Failed to get edges by label: %v", err)
				}

				// Verify we got the expected number of edges
				if len(edges) != size.edgesPerLabel {
					b.Fatalf("Expected %d edges, got %d", size.edgesPerLabel, len(edges))
				}
			}
		})
	}
}

// BenchmarkLabelIndexMixedOperations tests a mix of operations on the label index
func BenchmarkLabelIndexMixedOperations(b *testing.B) {
	implementations := []struct {
		name        string
		useLSMIndex bool
	}{
		{"StandardIndex", false},
		{"LSMIndex", true},
	}

	for _, impl := range implementations {
		b.Run(impl.name, func(b *testing.B) {
			_, nodeLabelIndex, _, cleanup := setupLabelIndexBenchmark(b, impl.useLSMIndex)
			defer cleanup()

			// Pre-populate with some data
			const initialLabels = 10
			const initialNodesPerLabel = 10

			for labelNum := 0; labelNum < initialLabels; labelNum++ {
				label := []byte(fmt.Sprintf("Label-%04d", labelNum))
				for nodeNum := 0; nodeNum < initialNodesPerLabel; nodeNum++ {
					nodeID := []byte(fmt.Sprintf("Node-%04d-%06d", labelNum, nodeNum))
					if err := nodeLabelIndex.Put(label, nodeID); err != nil {
						b.Fatalf("Failed to add initial node: %v", err)
					}
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Mix of operations based on iteration count
				op := i % 4 // 4 different operations

				// Label to operate on (rotating through them)
				labelNum := i % initialLabels
				label := []byte(fmt.Sprintf("Label-%04d", labelNum))

				switch op {
				case 0:
					// Add a new node to a label
					nodeID := []byte(fmt.Sprintf("NewNode-%d", i))
					if err := nodeLabelIndex.Put(label, nodeID); err != nil {
						b.Fatalf("Failed to add new node: %v", err)
					}
				case 1:
					// Get all nodes for a label
					_, err := nodeLabelIndex.GetAll(label)
					if err != nil {
						b.Fatalf("Failed to get nodes by label: %v", err)
					}
				case 2:
					// Check if a label exists
					_, err := nodeLabelIndex.Contains(label)
					if err != nil {
						b.Fatalf("Failed to check label existence: %v", err)
					}
				case 3:
					// Remove a node from a label (using the first node)
					nodeID := []byte(fmt.Sprintf("Node-%04d-%06d", labelNum, 0))
					err := nodeLabelIndex.DeleteValue(label, nodeID)
					if err != nil {
						// May fail if already deleted, which is acceptable
						continue
					}
				}
			}
		})
	}
}
