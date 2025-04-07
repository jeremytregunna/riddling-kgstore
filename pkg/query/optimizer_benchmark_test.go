package query

import (
	"testing"
)

// BenchmarkOptimizer_Optimize benchmarks the query optimization process
func BenchmarkOptimizer_Optimize(b *testing.B) {
	// Create optimizer
	optimizer := NewOptimizer()

	// Define a set of queries to optimize
	queries := []*Query{
		{
			Type: QueryTypeFindNodesByLabel,
			Parameters: map[string]string{
				ParamLabel: "Person",
			},
		},
		{
			Type: QueryTypeFindEdgesByLabel,
			Parameters: map[string]string{
				ParamLabel: "KNOWS",
			},
		},
		{
			Type: QueryTypeFindNodesByProperty,
			Parameters: map[string]string{
				ParamPropertyName:  "name",
				ParamPropertyValue: "Alice",
			},
		},
		{
			Type: QueryTypeFindEdgesByProperty,
			Parameters: map[string]string{
				ParamPropertyName:  "weight",
				ParamPropertyValue: "5",
			},
		},
		{
			Type: QueryTypeFindNeighbors,
			Parameters: map[string]string{
				ParamNodeID:    "42",
				ParamDirection: DirectionOutgoing,
			},
		},
		{
			Type: QueryTypeFindPath,
			Parameters: map[string]string{
				ParamSourceID: "1",
				ParamTargetID: "100",
			},
		},
		{
			Type: QueryTypeFindPath,
			Parameters: map[string]string{
				ParamSourceID: "1",
				ParamTargetID: "100",
				ParamMaxHops:  "10", // Test with custom max hops
			},
		},
	}

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Optimize each query in our test set
		query := queries[i%len(queries)]
		
		_, err := optimizer.Optimize(query)
		if err != nil {
			b.Fatalf("Error optimizing query: %v", err)
		}
	}
}

// BenchmarkQueryParsing_Parse benchmarks the query parsing process
func BenchmarkQueryParsing_Parse(b *testing.B) {
	// Define a set of query strings to parse
	queryStrings := []string{
		`FIND_NODES_BY_LABEL(label: "Person")`,
		`FIND_EDGES_BY_LABEL(label: "KNOWS")`,
		`FIND_NODES_BY_PROPERTY(propertyName: "name", propertyValue: "Alice")`,
		`FIND_EDGES_BY_PROPERTY(propertyName: "weight", propertyValue: "5")`,
		`FIND_NEIGHBORS(nodeId: "42", direction: "outgoing")`,
		`FIND_NEIGHBORS(nodeId: "42")`, // Test default parameters
		`FIND_PATH(sourceId: "1", targetId: "100")`,
		`FIND_PATH(sourceId: "1", targetId: "100", maxHops: "5")`,
	}

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Parse each query string in our test set
		queryStr := queryStrings[i%len(queryStrings)]
		
		_, err := Parse(queryStr)
		if err != nil {
			b.Fatalf("Error parsing query: %v", err)
		}
	}
}

// BenchmarkQueryParsing_ParseJSON benchmarks parsing JSON queries
func BenchmarkQueryParsing_ParseJSON(b *testing.B) {
	// Define a set of JSON query strings to parse
	jsonQueries := []string{
		`{"type":"FIND_NODES_BY_LABEL","parameters":{"label":"Person"}}`,
		`{"type":"FIND_EDGES_BY_LABEL","parameters":{"label":"KNOWS"}}`,
		`{"type":"FIND_NODES_BY_PROPERTY","parameters":{"propertyName":"name","propertyValue":"Alice"}}`,
		`{"type":"FIND_EDGES_BY_PROPERTY","parameters":{"propertyName":"weight","propertyValue":"5"}}`,
		`{"type":"FIND_NEIGHBORS","parameters":{"nodeId":"42","direction":"outgoing"}}`,
		`{"type":"FIND_PATH","parameters":{"sourceId":"1","targetId":"100"}}`,
		`{"type":"FIND_PATH","parameters":{"sourceId":"1","targetId":"100","maxHops":"5"}}`,
	}

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Parse each JSON query in our test set
		jsonQuery := jsonQueries[i%len(jsonQueries)]
		
		// Use the standard Parse function which handles JSON input
		_, err := Parse(jsonQuery)
		if err != nil {
			b.Fatalf("Error parsing JSON query: %v", err)
		}
	}
}

// BenchmarkOptimizer_OptimizePathQuery benchmarks optimizing path queries specifically
func BenchmarkOptimizer_OptimizePathQuery(b *testing.B) {
	// Create optimizer
	optimizer := NewOptimizer()

	// Test different hop limits
	queries := []*Query{
		{
			Type: QueryTypeFindPath,
			Parameters: map[string]string{
				ParamSourceID: "1",
				ParamTargetID: "100",
				// No maxHops - should use default
			},
		},
		{
			Type: QueryTypeFindPath,
			Parameters: map[string]string{
				ParamSourceID: "1",
				ParamTargetID: "100",
				ParamMaxHops:  "3",
			},
		},
		{
			Type: QueryTypeFindPath,
			Parameters: map[string]string{
				ParamSourceID: "1",
				ParamTargetID: "100",
				ParamMaxHops:  "10",
			},
		},
		{
			Type: QueryTypeFindPath,
			Parameters: map[string]string{
				ParamSourceID: "1",
				ParamTargetID: "100",
				ParamMaxHops:  "20", // High value - should be capped
			},
		},
		{
			Type: QueryTypeFindPath,
			Parameters: map[string]string{
				ParamSourceID: "1",
				ParamTargetID: "100",
				ParamMaxHops:  "invalid", // Invalid value - should use default
			},
		},
	}

	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Optimize each path query
		query := queries[i%len(queries)]
		
		_, err := optimizer.Optimize(query)
		if err != nil {
			b.Fatalf("Error optimizing query: %v", err)
		}
	}
}