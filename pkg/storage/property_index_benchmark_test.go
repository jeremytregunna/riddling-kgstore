package storage

import (
	"fmt"
	"os"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// setupPropertyIndexBenchmark creates a storage engine and property indices for benchmarking
func setupPropertyIndexBenchmark(b *testing.B, fullTextSearch bool) (*StorageEngine, Index, func()) {
	// Create a temporary directory for the database
	tempDir, err := os.MkdirTemp("", "property_index_benchmark")
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
	}

	engine, err := NewStorageEngine(config)
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create storage engine: %v", err)
	}

	// Create a property index - use node property index for benchmarks
	var propIndex Index
	if fullTextSearch {
		propIndex, err = NewFullTextPropertyIndex(engine, model.NewNoOpLogger(), PropertyIndexTypeNode)
	} else {
		propIndex, err = NewNodePropertyIndex(engine, model.NewNoOpLogger())
	}
	
	if err != nil {
		engine.Close()
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create property index: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		propIndex.Close()
		engine.Close()
		os.RemoveAll(tempDir)
	}

	return engine, propIndex, cleanup
}

// BenchmarkFullTextIndexing measures the performance of adding data to a full-text index
func BenchmarkFullTextIndexing(b *testing.B) {
	tests := []struct {
		name          string
		entityCount   int // Number of entities to index
		propertyCount int // Number of properties per entity
	}{
		{"Small_100Entities_5Props", 100, 5},
		{"Medium_1000Entities_5Props", 1000, 5},
		{"Large_1000Entities_20Props", 1000, 20},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			_, propIndex, cleanup := setupPropertyIndexBenchmark(b, true)
			defer cleanup()

			// Generate test data for the benchmark
			entityIDs := make([][]byte, tt.entityCount)
			for i := 0; i < tt.entityCount; i++ {
				entityIDs[i] = []byte(fmt.Sprintf("node-%06d", i))
			}

			properties := []string{
				"name", "description", "title", "content", "tags",
				"category", "creator", "email", "address", "phone",
				"location", "department", "status", "notes", "keywords",
				"text", "language", "country", "type", "identifier",
			}

			// Ensure we only use as many properties as requested
			if len(properties) > tt.propertyCount {
				properties = properties[:tt.propertyCount]
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Index properties for each entity
				for j := 0; j < tt.entityCount; j++ {
					entityID := entityIDs[j]
					
					// Add each property for this entity
					for k, propName := range properties {
						// Create unique but deterministic values
						propValue := fmt.Sprintf("%s value for entity %d with words like searchable indexed content item %d",
							propName, j, k)
						
						// Create the key in the format "propertyName|entityID"
						key := []byte(fmt.Sprintf("%s|%s", propName, entityID))
						
						if err := propIndex.Put(key, []byte(propValue)); err != nil {
							b.Fatalf("Failed to index property: %v", err)
						}
					}
				}
			}
		})
	}
}

// BenchmarkFullTextSearch measures the performance of searching a full-text index
func BenchmarkFullTextSearch(b *testing.B) {
	tests := []struct {
		name            string
		dataSize        int  // Number of entities to pre-index
		fullTextEnabled bool // Whether to enable full-text search
	}{
		{"Standard_SmallData", 100, false},
		{"Standard_LargeData", 1000, false},
		{"FullText_SmallData", 100, true},
		{"FullText_LargeData", 1000, true},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			_, propIndex, cleanup := setupPropertyIndexBenchmark(b, tt.fullTextEnabled)
			defer cleanup()

			// Prepare data - index a set of entities with properties
			for i := 0; i < tt.dataSize; i++ {
				entityID := []byte(fmt.Sprintf("node-%06d", i))
				
				// Add standard properties
				nameKey := []byte(fmt.Sprintf("name|%s", entityID))
				descKey := []byte(fmt.Sprintf("description|%s", entityID))
				
				// Create property values with some searchable terms
				nameVal := fmt.Sprintf("Entity %d", i)
				descVal := fmt.Sprintf("This is entity %d with searchable terms like example text content %d", 
					i, i%10)
				
				if err := propIndex.Put(nameKey, []byte(nameVal)); err != nil {
					b.Fatalf("Failed to index name property: %v", err)
				}
				
				if err := propIndex.Put(descKey, []byte(descVal)); err != nil {
					b.Fatalf("Failed to index description property: %v", err)
				}
			}
			
			// Different search terms to benchmark
			searchTerms := []struct {
				property string
				value    string
			}{
				{"name", "Entity 50"},                   // Exact match on name
				{"description", "searchable"},           // Common term in many entities
				{"description", "example text content"}, // Multiple terms
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Rotate through different search terms
				term := searchTerms[i%len(searchTerms)]
				
				// Create the search key in the format "propertyName|propertyValue"
				searchKey := []byte(fmt.Sprintf("%s|%s", term.property, term.value))
				
				// Perform the search
				results, err := propIndex.GetAll(searchKey)
				if err != nil {
					b.Fatalf("Failed to search: %v", err)
				}
				
				// Ensure we got some results
				if len(results) == 0 && !tt.fullTextEnabled {
					// If not using full-text search, we should still find exact matches
					if term.property == "name" {
						b.Fatalf("Expected at least one result for exact match search")
					}
				}
			}
		})
	}
}

// BenchmarkPropertyIndexRange measures the performance of range queries on property values
func BenchmarkPropertyIndexRange(b *testing.B) {
	_, propIndex, cleanup := setupPropertyIndexBenchmark(b, false)
	defer cleanup()

	// Prepare data - create numerical properties for range queries
	const entityCount = 1000
	
	// Index entities with numerical age property
	for i := 0; i < entityCount; i++ {
		entityID := []byte(fmt.Sprintf("node-%06d", i))
		
		// Add an age property with incrementing values
		ageKey := []byte(fmt.Sprintf("age|%s", entityID))
		ageVal := fmt.Sprintf("%d", i%100) // Ages from 0-99
		
		// Add a price property with incrementing values
		priceKey := []byte(fmt.Sprintf("price|%s", entityID))
		priceVal := fmt.Sprintf("%d.99", (i%50)*10) // Prices like 0.99, 10.99, etc.
		
		if err := propIndex.Put(ageKey, []byte(ageVal)); err != nil {
			b.Fatalf("Failed to index age property: %v", err)
		}
		
		if err := propIndex.Put(priceKey, []byte(priceVal)); err != nil {
			b.Fatalf("Failed to index price property: %v", err)
		}
	}

	// Range query cases to benchmark
	rangeQueries := []struct {
		property string
		min      string
		max      string
	}{
		{"age", "20", "30"},    // Age between 20-30
		{"age", "50", "99"},    // Age between 50-99
		{"price", "100", "300"}, // Price between 100-300
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Rotate through different range queries
		query := rangeQueries[i%len(rangeQueries)]
		
		// For a real range query, we'd need to implement it in the property index
		// But for benchmarking, we'll simulate by getting all values and filtering
		
		// First get all entities with the property
		allResults, err := propIndex.GetAll([]byte(query.property + "|"))
		if err != nil {
			b.Fatalf("Failed to retrieve entities: %v", err)
		}
		
		// Filter results (simulating range query)
		minVal := query.min
		maxVal := query.max
		
		// Count the results in range (we need to do this work to ensure
		// the compiler doesn't optimize out our benchmark loop)
		matchCount := 0
		for _, result := range allResults {
			// In a real implementation, we'd extract the property value more efficiently
			// For the benchmark, this simple approach is sufficient
			if string(result) >= minVal && string(result) <= maxVal {
				matchCount++
			}
		}
	}
}