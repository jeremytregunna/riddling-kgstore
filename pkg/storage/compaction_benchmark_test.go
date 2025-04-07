package storage

import (
	"fmt"
	"os"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// setupCompactionBenchmark creates a storage engine for compaction benchmarks
func setupCompactionBenchmark(b *testing.B, numberOfRecords int, recordSize int) (*StorageEngine, string, func()) {
	// Create a temporary directory for the database
	tempDir, err := os.MkdirTemp("", "compaction_benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create a storage engine with configuration optimized for benchmarking
	config := EngineConfig{
		DataDir:              tempDir,
		MemTableSize:         1024 * 1024, // Small size to force frequent compactions
		SyncWrites:           false,       // Disable syncing for performance
		Logger:               model.NewNoOpLogger(),
		Comparator:           DefaultComparator,
		BackgroundCompaction: false, // Disable background compaction for controlled tests
		BloomFilterFPR:       0.01,
	}

	engine, err := NewStorageEngine(config)
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create storage engine: %v", err)
	}

	// Generate test data and write to the storage engine
	for i := 0; i < numberOfRecords; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		value := make([]byte, recordSize)
		for j := 0; j < recordSize; j++ {
			value[j] = byte((i + j) % 256)
		}

		if err := engine.Put(key, value); err != nil {
			engine.Close()
			os.RemoveAll(tempDir)
			b.Fatalf("Failed to put data: %v", err)
		}

		// Flush the memtable after every 1000 records to create multiple SSTs
		if i > 0 && i%1000 == 0 {
			err := engine.Flush()
			if err != nil {
				engine.Close()
				os.RemoveAll(tempDir)
				b.Fatalf("Failed to flush memtable: %v", err)
			}
		}
	}

	// Final flush to ensure all data is in SSTables
	if err := engine.Flush(); err != nil {
		engine.Close()
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to do final flush: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		engine.Close()
		os.RemoveAll(tempDir)
	}

	return engine, tempDir, cleanup
}

// BenchmarkCompactionLogic measures the performance of the compaction algorithm
func BenchmarkCompactionLogic(b *testing.B) {
	tests := []struct {
		name           string
		numberOfRecords int
		recordSize     int
	}{
		{"Small_1K_Records_100B", 1000, 100},
		{"Medium_10K_Records_100B", 10000, 100},
		{"Large_10K_Records_1KB", 10000, 1024},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			engine, _, cleanup := setupCompactionBenchmark(b, tt.numberOfRecords, tt.recordSize)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Force compaction
				err := engine.Compact()
				if err != nil {
					b.Fatalf("Compaction failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkCompactionWithDifferentSizeSSTables measures compaction with varying SSTable sizes
func BenchmarkCompactionWithDifferentSizeSSTables(b *testing.B) {
	tests := []struct {
		name          string
		ssTableSizes  []int // Number of records per SSTable
		recordSize    int   // Size of each record in bytes
	}{
		{"EqualSizedSSTables", []int{1000, 1000, 1000, 1000}, 100},
		{"IncreasingSSTables", []int{500, 1000, 2000, 4000}, 100},
		{"DecreasingSSTables", []int{4000, 2000, 1000, 500}, 100},
		{"MixedSSTables", []int{500, 3000, 1000, 2000}, 100},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			// Create a temporary directory for the database
			tempDir, err := os.MkdirTemp("", "compaction_size_benchmark")
			if err != nil {
				b.Fatalf("Failed to create temp directory: %v", err)
			}
			defer os.RemoveAll(tempDir)

			// Create a storage engine with configuration optimized for benchmarking
			config := EngineConfig{
				DataDir:              tempDir,
				MemTableSize:         8 * 1024 * 1024, // Larger size to accommodate all records
				SyncWrites:           false,            // Disable syncing for performance
				Logger:               model.NewNoOpLogger(),
				Comparator:           DefaultComparator,
				BackgroundCompaction: false, // Disable background compaction for controlled tests
				BloomFilterFPR:       0.01,
			}

			engine, err := NewStorageEngine(config)
			if err != nil {
				b.Fatalf("Failed to create storage engine: %v", err)
			}
			defer engine.Close()

			// Create SSTables of different sizes
			recordCounter := 0
			for _, sstSize := range tt.ssTableSizes {
				// Add the specified number of records for this SSTable
				for i := 0; i < sstSize; i++ {
					key := []byte(fmt.Sprintf("key-%08d", recordCounter))
					value := make([]byte, tt.recordSize)
					for j := 0; j < tt.recordSize; j++ {
						value[j] = byte((recordCounter + j) % 256)
					}

					if err := engine.Put(key, value); err != nil {
						b.Fatalf("Failed to put data: %v", err)
					}
					recordCounter++
				}

				// Flush to create a new SSTable
				if err := engine.Flush(); err != nil {
					b.Fatalf("Failed to flush memtable: %v", err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Force compaction of all the different sized SSTables
				err := engine.Compact()
				if err != nil {
					b.Fatalf("Compaction failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkCompactionWithVersioning measures compaction performance with versioned records
func BenchmarkCompactionWithVersioning(b *testing.B) {
	tests := []struct {
		name             string
		numberOfRecords  int
		updatePercentage int // Percentage of records to update
	}{
		{"SmallUpdates_10Percent", 10000, 10},
		{"MediumUpdates_50Percent", 10000, 50},
		{"LargeUpdates_90Percent", 10000, 90},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			// Create a temporary directory for the database
			tempDir, err := os.MkdirTemp("", "compaction_version_benchmark")
			if err != nil {
				b.Fatalf("Failed to create temp directory: %v", err)
			}
			defer os.RemoveAll(tempDir)

			// Create a storage engine with configuration optimized for benchmarking
			config := EngineConfig{
				DataDir:              tempDir,
				MemTableSize:         1024 * 1024, // Small size to force frequent compactions
				SyncWrites:           false,        // Disable syncing for performance
				Logger:               model.NewNoOpLogger(),
				Comparator:           DefaultComparator,
				BackgroundCompaction: false, // Disable background compaction for controlled tests
				BloomFilterFPR:       0.01,
			}

			engine, err := NewStorageEngine(config)
			if err != nil {
				b.Fatalf("Failed to create storage engine: %v", err)
			}
			defer engine.Close()

			// First, create an SSTable with the initial versions of all records
			memTable := NewMemTable(MemTableConfig{
				MaxSize:    8 * 1024 * 1024,
				Logger:     model.NewNoOpLogger(),
				Comparator: DefaultComparator,
			})

			for i := 0; i < tt.numberOfRecords; i++ {
				key := []byte(fmt.Sprintf("key-%08d", i))
				value := []byte(fmt.Sprintf("value-v1-%08d", i))
				
				// Use version 1 for all initial records
				err := memTable.PutWithVersion(key, value, 1)
				if err != nil {
					b.Fatalf("Failed to put initial record: %v", err)
				}
			}

			// Create an SSTable from the memTable
			sstConfig := SSTableConfig{
				ID:         1,
				Path:       tempDir + "/sstables",
				Logger:     model.NewNoOpLogger(),
				Comparator: DefaultComparator,
			}

			_, err = CreateSSTable(sstConfig, memTable)
			if err != nil {
				b.Fatalf("Failed to create initial SSTable: %v", err)
			}

			// Now create a second SSTable with updated versions of some records
			updatedMemTable := NewMemTable(MemTableConfig{
				MaxSize:    8 * 1024 * 1024,
				Logger:     model.NewNoOpLogger(),
				Comparator: DefaultComparator,
			})

			// Calculate how many records to update
			recordsToUpdate := tt.numberOfRecords * tt.updatePercentage / 100

			for i := 0; i < recordsToUpdate; i++ {
				key := []byte(fmt.Sprintf("key-%08d", i))
				value := []byte(fmt.Sprintf("value-v2-%08d", i))
				
				// Use version 2 for updated records
				err := updatedMemTable.PutWithVersion(key, value, 2)
				if err != nil {
					b.Fatalf("Failed to put updated record: %v", err)
				}
			}

			// Create a second SSTable with the updates
			sstConfig.ID = 2
			_, err = CreateSSTable(sstConfig, updatedMemTable)
			if err != nil {
				b.Fatalf("Failed to create updated SSTable: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Force compaction - this will need to preserve the higher versions
				err := engine.Compact()
				if err != nil {
					b.Fatalf("Compaction failed: %v", err)
				}
			}
		})
	}
}