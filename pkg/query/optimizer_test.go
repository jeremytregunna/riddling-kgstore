package query

import (
	"strconv"
	"testing"
)

func TestOptimizer_Optimize(t *testing.T) {
	optimizer := NewOptimizer()

	tests := []struct {
		name     string
		query    *Query
		validate func(*Query, *testing.T)
	}{
		{
			name: "Find nodes by label - No optimization needed",
			query: &Query{
				Type: QueryTypeFindNodesByLabel,
				Parameters: map[string]string{
					ParamLabel: "Person",
				},
			},
			validate: func(optimized *Query, t *testing.T) {
				if optimized.Type != QueryTypeFindNodesByLabel {
					t.Errorf("Expected query type %s, got %s", QueryTypeFindNodesByLabel, optimized.Type)
				}
				if optimized.Parameters[ParamLabel] != "Person" {
					t.Errorf("Expected label 'Person', got '%s'", optimized.Parameters[ParamLabel])
				}
			},
		},
		{
			name: "Find neighbors - Set default direction",
			query: &Query{
				Type: QueryTypeFindNeighbors,
				Parameters: map[string]string{
					ParamNodeID: "1",
				},
			},
			validate: func(optimized *Query, t *testing.T) {
				if optimized.Parameters[ParamDirection] != DirectionBoth {
					t.Errorf("Expected direction '%s', got '%s'", DirectionBoth, optimized.Parameters[ParamDirection])
				}
			},
		},
		{
			name: "Find path - Set default max hops",
			query: &Query{
				Type: QueryTypeFindPath,
				Parameters: map[string]string{
					ParamSourceID: "1",
					ParamTargetID: "5",
				},
			},
			validate: func(optimized *Query, t *testing.T) {
				if optimized.Parameters[ParamMaxHops] != strconv.Itoa(optimizer.DefaultMaxHops) {
					t.Errorf("Expected max hops '%d', got '%s'", optimizer.DefaultMaxHops, optimized.Parameters[ParamMaxHops])
				}
			},
		},
		{
			name: "Find path - Cap excessive max hops",
			query: &Query{
				Type: QueryTypeFindPath,
				Parameters: map[string]string{
					ParamSourceID: "1",
					ParamTargetID: "5",
					ParamMaxHops:  "100", // Excessive value
				},
			},
			validate: func(optimized *Query, t *testing.T) {
				maxHops, _ := strconv.Atoi(optimized.Parameters[ParamMaxHops])
				if maxHops > optimizer.DefaultMaxHops*2 {
					t.Errorf("Expected max hops to be capped at %d, got %d", optimizer.DefaultMaxHops*2, maxHops)
				}
			},
		},
		{
			name: "Find path - Invalid max hops replaced with default",
			query: &Query{
				Type: QueryTypeFindPath,
				Parameters: map[string]string{
					ParamSourceID: "1",
					ParamTargetID: "5",
					ParamMaxHops:  "invalid", // Not a number
				},
			},
			validate: func(optimized *Query, t *testing.T) {
				if optimized.Parameters[ParamMaxHops] != strconv.Itoa(optimizer.DefaultMaxHops) {
					t.Errorf("Expected max hops '%d', got '%s'", optimizer.DefaultMaxHops, optimized.Parameters[ParamMaxHops])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			optimized, err := optimizer.Optimize(tt.query)
			if err != nil {
				t.Fatalf("Optimizer.Optimize() error = %v", err)
			}

			tt.validate(optimized, t)
		})
	}

	// Test error case - nil query
	_, err := optimizer.Optimize(nil)
	if err == nil {
		t.Errorf("Expected error for nil query")
	}

	// Test error case - unsupported query type
	_, err = optimizer.Optimize(&Query{Type: "UNSUPPORTED"})
	if err == nil {
		t.Errorf("Expected error for unsupported query type")
	}
}

func TestOptimizer_PropertyQueries(t *testing.T) {
	optimizer := NewOptimizer()

	tests := []struct {
		name     string
		query    *Query
		validate func(*Query, *testing.T)
	}{
		{
			name: "Find nodes by property - Trim property name",
			query: &Query{
				Type: QueryTypeFindNodesByProperty,
				Parameters: map[string]string{
					ParamPropertyName:  " name ", // Extra spaces
					ParamPropertyValue: "Alice",
				},
			},
			validate: func(optimized *Query, t *testing.T) {
				if optimized.Parameters[ParamPropertyName] != "name" {
					t.Errorf("Expected property name to be trimmed to 'name', got '%s'", optimized.Parameters[ParamPropertyName])
				}
			},
		},
		{
			name: "Find edges by property - Maintain property values",
			query: &Query{
				Type: QueryTypeFindEdgesByProperty,
				Parameters: map[string]string{
					ParamPropertyName:  "role",
					ParamPropertyValue: "Developer",
				},
			},
			validate: func(optimized *Query, t *testing.T) {
				if optimized.Parameters[ParamPropertyName] != "role" {
					t.Errorf("Expected property name 'role', got '%s'", optimized.Parameters[ParamPropertyName])
				}
				if optimized.Parameters[ParamPropertyValue] != "Developer" {
					t.Errorf("Expected property value 'Developer', got '%s'", optimized.Parameters[ParamPropertyValue])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			optimized, err := optimizer.Optimize(tt.query)
			if err != nil {
				t.Fatalf("Optimizer.Optimize() error = %v", err)
			}

			tt.validate(optimized, t)
		})
	}
}
