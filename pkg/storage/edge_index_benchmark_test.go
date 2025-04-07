package storage

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// setupEdgeIndexBenchmark creates a storage engine and edge index for benchmarking
func setupEdgeIndexBenchmark(b *testing.B) (Index, *StorageEngine, func()) {
	// Create a temporary directory for the database
	dbDir, err := os.MkdirTemp("", "edge_index_benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a storage engine with configuration optimized for benchmarking
	config := EngineConfig{
		DataDir:              dbDir,
		MemTableSize:         1024 * 1024 * 10, // 10MB
		SyncWrites:           false,            // Disable syncing for better performance
		Logger:               model.NewNoOpLogger(),
		Comparator:           DefaultComparator,
		BackgroundCompaction: false, // Disable background compaction for benchmarks
		BloomFilterFPR:       0.01,
	}

	engine, err := NewStorageEngine(config)
	if err != nil {
		os.RemoveAll(dbDir)
		b.Fatalf("Failed to create storage engine: %v", err)
	}

	// Create an edge index
	index, err := NewEdgeIndex(engine, model.NewNoOpLogger())
	if err != nil {
		engine.Close()
		os.RemoveAll(dbDir)
		b.Fatalf("Failed to create edge index: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		index.Close()
		engine.Close()
		os.RemoveAll(dbDir)
	}

	return index, engine, cleanup
}

// BenchmarkEdgeIndexPut measures the performance of adding edges to the index
func BenchmarkEdgeIndexPut(b *testing.B) {
	index, _, cleanup := setupEdgeIndexBenchmark(b)
	defer cleanup()

	// Prepare edge data
	edgeData := []byte("source:1,target:2,type:KNOWS,properties:{prop1:value1,prop2:value2}")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a unique edge ID for each iteration
		edgeID := make([]byte, 8)
		binary.LittleEndian.PutUint64(edgeID, uint64(i))

		if err := index.Put(edgeID, edgeData); err != nil {
			b.Fatalf("Failed to put edge: %v", err)
		}
	}
}

// BenchmarkEdgeIndexGet measures the performance of retrieving edges from the index
func BenchmarkEdgeIndexGet(b *testing.B) {
	index, _, cleanup := setupEdgeIndexBenchmark(b)
	defer cleanup()

	// Prepare test data
	numEdges := 1000
	edgeIDs := make([][]byte, numEdges)
	edgeData := []byte("source:1,target:2,type:KNOWS,properties:{prop1:value1,prop2:value2}")

	// Insert edges into the index
	for i := 0; i < numEdges; i++ {
		edgeID := make([]byte, 8)
		binary.LittleEndian.PutUint64(edgeID, uint64(i))
		edgeIDs[i] = edgeID

		if err := index.Put(edgeID, edgeData); err != nil {
			b.Fatalf("Failed to put edge during setup: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Access edges in a round-robin fashion
		edgeID := edgeIDs[i%numEdges]

		_, err := index.Get(edgeID)
		if err != nil {
			b.Fatalf("Failed to get edge: %v", err)
		}
	}
}

// BenchmarkEdgeIndexIterate measures the performance of iterating through all edges
func BenchmarkEdgeIndexIterate(b *testing.B) {
	index, engine, cleanup := setupEdgeIndexBenchmark(b)
	defer cleanup()

	// Prepare test data
	numEdges := 1000
	edgeData := []byte("source:1,target:2,type:KNOWS,properties:{prop1:value1,prop2:value2}")

	// Insert edges into the index
	for i := 0; i < numEdges; i++ {
		edgeID := make([]byte, 8)
		binary.LittleEndian.PutUint64(edgeID, uint64(i))

		if err := index.Put(edgeID, edgeData); err != nil {
			b.Fatalf("Failed to put edge during setup: %v", err)
		}
	}

	// Prepare a prefix for edge index keys
	prefix := []byte("e:")

	// Find all the sstables
	tempDir := engine.config.DataDir

	// Force a flush to ensure data is in sstables
	if err := engine.Flush(); err != nil {
		b.Fatalf("Failed to flush storage engine: %v", err)
	}

	// Now find the sstables after flush
	sstablePaths, err := filepath.Glob(filepath.Join(tempDir, "sstables", "*.data"))
	if err != nil {
		b.Fatalf("Failed to find sstables: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0

		// For each sstable, create an iterator and iterate through keys with the prefix
		for _, path := range sstablePaths {
			// Extract sstable ID from path
			base := filepath.Base(path)
			idStr := strings.TrimSuffix(base, ".data")
			id, err := strconv.ParseUint(idStr, 10, 64)
			if err != nil {
				b.Fatalf("Failed to parse sstable ID: %v", err)
			}

			// Create sstable config
			config := SSTableConfig{
				ID:         id,
				Path:       filepath.Dir(path),
				Logger:     model.NewNoOpLogger(),
				Comparator: DefaultComparator,
			}

			// Open the sstable
			sstable, err := OpenSSTable(config)
			if err != nil {
				b.Fatalf("Failed to open sstable: %v", err)
			}

			// Create an iterator
			iter, err := sstable.Iterator()
			if err != nil {
				sstable.Close()
				b.Fatalf("Failed to create iterator: %v", err)
			}

			// Seek to the first key with the prefix
			if err := iter.Seek(prefix); err != nil {
				iter.Close()
				sstable.Close()
				b.Fatalf("Failed to seek to first key: %v", err)
			}

			// Iterate through all keys with the prefix
			for iter.Valid() {
				key := iter.Key()
				if !bytes.HasPrefix(key, prefix) {
					break
				}
				count++
				if err := iter.Next(); err != nil {
					b.Fatalf("Failed to advance iterator: %v", err)
				}
			}

			iter.Close()
			sstable.Close()
		}

		// Skip counting keys in memory since we can't directly access the memTable
		// For benchmarking purposes, this is acceptable
	}
}

// BenchmarkEdgeIndexDelete measures the performance of deleting edges from the index
func BenchmarkEdgeIndexDelete(b *testing.B) {
	index, _, cleanup := setupEdgeIndexBenchmark(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// First, add an edge
		edgeID := make([]byte, 8)
		binary.LittleEndian.PutUint64(edgeID, uint64(i))
		edgeData := []byte("test-edge-data")

		if err := index.Put(edgeID, edgeData); err != nil {
			b.Fatalf("Failed to put edge: %v", err)
		}

		// Then delete it
		if err := index.Delete(edgeID); err != nil {
			b.Fatalf("Failed to delete edge: %v", err)
		}
	}
}

// BenchmarkEdgeIndexBulkOperations measures performance of bulk operations
func BenchmarkEdgeIndexBulkOperations(b *testing.B) {
	tests := []struct {
		name          string
		operationSize int // Number of operations in each batch
	}{
		{"Small_10Ops", 10},
		{"Medium_100Ops", 100},
		{"Large_1000Ops", 1000},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			index, _, cleanup := setupEdgeIndexBenchmark(b)
			defer cleanup()

			// Prepare edge data
			edgeData := []byte("source:1,target:2,type:KNOWS,properties:{prop1:value1,prop2:value2}")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Perform a batch of put operations
				for j := 0; j < tt.operationSize; j++ {
					edgeID := make([]byte, 8)
					binary.LittleEndian.PutUint64(edgeID, uint64(i*tt.operationSize+j))

					if err := index.Put(edgeID, edgeData); err != nil {
						b.Fatalf("Failed to put edge in batch: %v", err)
					}
				}
			}
		})
	}
}

// BenchmarkEdgeIndexMixedWorkload measures performance with a mix of operations
func BenchmarkEdgeIndexMixedWorkload(b *testing.B) {
	index, _, cleanup := setupEdgeIndexBenchmark(b)
	defer cleanup()

	// Pre-populate some data
	numInitialEdges := 1000
	edgeIDs := make([][]byte, numInitialEdges)
	edgeData := []byte("source:1,target:2,type:KNOWS,properties:{prop1:value1,prop2:value2}")

	for i := 0; i < numInitialEdges; i++ {
		edgeID := make([]byte, 8)
		binary.LittleEndian.PutUint64(edgeID, uint64(i))
		edgeIDs[i] = edgeID

		if err := index.Put(edgeID, edgeData); err != nil {
			b.Fatalf("Failed to put initial edge: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Perform a mix of operations based on the iteration number
		switch i % 4 {
		case 0:
			// Put operation
			edgeID := make([]byte, 8)
			binary.LittleEndian.PutUint64(edgeID, uint64(numInitialEdges+i))
			if err := index.Put(edgeID, edgeData); err != nil {
				b.Fatalf("Failed to put edge: %v", err)
			}
		case 1:
			// Get operation
			edgeID := edgeIDs[i%numInitialEdges]
			_, err := index.Get(edgeID)
			if err != nil {
				b.Fatalf("Failed to get edge: %v", err)
			}
		case 2:
			// Contains operation
			edgeID := edgeIDs[i%numInitialEdges]
			_, err := index.Contains(edgeID)
			if err != nil {
				b.Fatalf("Failed to check if edge exists: %v", err)
			}
		case 3:
			// Delete and recreate operation
			edgeID := edgeIDs[i%numInitialEdges]
			if err := index.Delete(edgeID); err != nil {
				b.Fatalf("Failed to delete edge: %v", err)
			}
			if err := index.Put(edgeID, edgeData); err != nil {
				b.Fatalf("Failed to recreate edge: %v", err)
			}
		}
	}
}
