package query

import (
	"fmt"
	"strconv"
	"strings"
)

// Optimizer optimizes queries by applying transformation rules
type Optimizer struct {
	// Configuration options for optimization
	EnablePathLengthLimit bool
	DefaultMaxHops        int
}

// NewOptimizer creates a new query optimizer
func NewOptimizer() *Optimizer {
	return &Optimizer{
		EnablePathLengthLimit: true,
		DefaultMaxHops:        5,
	}
}

// Optimize applies optimization rules to a query
func (o *Optimizer) Optimize(query *Query) (*Query, error) {
	// Validate the query
	if query == nil {
		return nil, fmt.Errorf("query is nil")
	}

	// Create a copy of the query for optimization
	optimized := &Query{
		Type:       query.Type,
		Parameters: make(map[string]string),
	}

	// Copy parameters
	for k, v := range query.Parameters {
		optimized.Parameters[k] = v
	}

	// Apply optimization rules based on query type
	switch query.Type {
	// Read operations
	case QueryTypeFindNodesByLabel:
		// No specific optimizations for this query type yet
	case QueryTypeFindEdgesByLabel:
		// No specific optimizations for this query type yet
	case QueryTypeFindNodesByProperty:
		// Use property index optimizations
		o.optimizePropertyQuery(optimized)
	case QueryTypeFindEdgesByProperty:
		// Use property index optimizations
		o.optimizePropertyQuery(optimized)
	case QueryTypeFindNeighbors:
		o.optimizeNeighborsQuery(optimized)
	case QueryTypeFindPath:
		o.optimizePathQuery(optimized)
		
	// Transaction operations
	case QueryTypeBeginTransaction, QueryTypeCommitTransaction, QueryTypeRollbackTransaction:
		// No specific optimizations for transaction operations
		
	// Write operations
	case QueryTypeCreateNode, QueryTypeCreateEdge, QueryTypeDeleteNode, 
	     QueryTypeDeleteEdge, QueryTypeSetProperty, QueryTypeRemoveProperty:
		// No specific optimizations for write operations yet
		
	default:
		return nil, fmt.Errorf("unsupported query type: %s", query.Type)
	}

	return optimized, nil
}

// optimizeNeighborsQuery optimizes a FIND_NEIGHBORS query
func (o *Optimizer) optimizeNeighborsQuery(query *Query) {
	// Set default direction if not specified
	if _, ok := query.Parameters[ParamDirection]; !ok {
		query.Parameters[ParamDirection] = DirectionBoth
	}
}

// optimizePathQuery optimizes a FIND_PATH query
func (o *Optimizer) optimizePathQuery(query *Query) {
	// Add maximum hop limit for safety if not specified
	if o.EnablePathLengthLimit {
		if _, ok := query.Parameters[ParamMaxHops]; !ok {
			query.Parameters[ParamMaxHops] = strconv.Itoa(o.DefaultMaxHops)
		} else {
			// Validate and potentially cap the max hops
			if maxHopsStr, ok := query.Parameters[ParamMaxHops]; ok {
				maxHops, err := strconv.Atoi(maxHopsStr)
				if err != nil || maxHops <= 0 {
					// Invalid max hops, use default
					query.Parameters[ParamMaxHops] = strconv.Itoa(o.DefaultMaxHops)
				} else if maxHops > o.DefaultMaxHops*2 {
					// Cap maximum hops to avoid performance issues
					query.Parameters[ParamMaxHops] = strconv.Itoa(o.DefaultMaxHops * 2)
				}
			}
		}
	}
}

// optimizePropertyQuery optimizes property-based queries
func (o *Optimizer) optimizePropertyQuery(query *Query) {
	// No specific optimizations yet, but we could add:
	// - Type-specific optimizations for numeric ranges
	// - Full-text search capabilities for string properties
	// - Caching commonly accessed property queries

	// For now, just ensure the parameters are properly formatted
	if propName, ok := query.Parameters[ParamPropertyName]; ok {
		// Normalize property name if needed (e.g., trim spaces)
		query.Parameters[ParamPropertyName] = strings.TrimSpace(propName)
	}
}
