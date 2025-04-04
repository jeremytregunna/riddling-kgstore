package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// QueryType represents the different types of queries supported
type QueryType string

const (
	QueryTypeFindNodesByLabel  QueryType = "FIND_NODES_BY_LABEL"
	QueryTypeFindEdgesByLabel  QueryType = "FIND_EDGES_BY_LABEL"
	QueryTypeFindNeighbors     QueryType = "FIND_NEIGHBORS"
	QueryTypeFindPath          QueryType = "FIND_PATH"
)

// Query represents a parsed query
type Query struct {
	Type       QueryType         `json:"type"`
	Parameters map[string]string `json:"parameters"`
}

// Parameter keys
const (
	ParamLabel      = "label"
	ParamNodeID     = "nodeId"
	ParamDirection  = "direction"
	ParamMaxHops    = "maxHops"
	ParamSourceID   = "sourceId"
	ParamTargetID   = "targetId"
)

// Direction types for traversal
const (
	DirectionOutgoing = "outgoing"
	DirectionIncoming = "incoming"
	DirectionBoth     = "both"
)

// ErrInvalidQuery indicates that the query is invalid
var ErrInvalidQuery = errors.New("invalid query")

// Parse parses a query string into a Query struct
// The query language is a simple string format:
// FIND_NODES_BY_LABEL(label: "Person")
// FIND_EDGES_BY_LABEL(label: "KNOWS")
// FIND_NEIGHBORS(nodeId: "1", direction: "outgoing")
// FIND_PATH(sourceId: "1", targetId: "2", maxHops: "3")
func Parse(queryStr string) (*Query, error) {
	queryStr = strings.TrimSpace(queryStr)
	if queryStr == "" {
		return nil, fmt.Errorf("%w: empty query", ErrInvalidQuery)
	}

	// Check if it's a JSON query
	if strings.HasPrefix(queryStr, "{") {
		var query Query
		if err := json.Unmarshal([]byte(queryStr), &query); err != nil {
			return nil, fmt.Errorf("%w: invalid JSON: %v", ErrInvalidQuery, err)
		}
		return &query, nil
	}

	// Parse text-based query
	openParenIndex := strings.Index(queryStr, "(")
	if openParenIndex == -1 {
		return nil, fmt.Errorf("%w: missing parameters", ErrInvalidQuery)
	}

	closeParenIndex := strings.LastIndex(queryStr, ")")
	if closeParenIndex == -1 || closeParenIndex <= openParenIndex {
		return nil, fmt.Errorf("%w: missing closing parenthesis", ErrInvalidQuery)
	}

	queryType := QueryType(strings.TrimSpace(queryStr[:openParenIndex]))
	paramsStr := queryStr[openParenIndex+1 : closeParenIndex]

	// Validate query type
	switch queryType {
	case QueryTypeFindNodesByLabel, QueryTypeFindEdgesByLabel, QueryTypeFindNeighbors, QueryTypeFindPath:
		// Valid query type
	default:
		return nil, fmt.Errorf("%w: unknown query type: %s", ErrInvalidQuery, queryType)
	}

	// Parse parameters
	params := make(map[string]string)
	if paramsStr != "" {
		paramPairs := strings.Split(paramsStr, ",")
		for _, pair := range paramPairs {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				continue
			}

			kv := strings.SplitN(pair, ":", 2)
			if len(kv) != 2 {
				return nil, fmt.Errorf("%w: invalid parameter format: %s", ErrInvalidQuery, pair)
			}

			key := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])

			// Remove quotes if present
			if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
				value = value[1 : len(value)-1]
			}

			params[key] = value
		}
	}

	query := &Query{
		Type:       queryType,
		Parameters: params,
	}

	// Validate parameters based on query type
	if err := validateQueryParameters(query); err != nil {
		return nil, err
	}

	return query, nil
}

// validateQueryParameters validates that the query has all required parameters
func validateQueryParameters(query *Query) error {
	switch query.Type {
	case QueryTypeFindNodesByLabel, QueryTypeFindEdgesByLabel:
		if _, ok := query.Parameters[ParamLabel]; !ok {
			return fmt.Errorf("%w: missing required parameter 'label'", ErrInvalidQuery)
		}
	case QueryTypeFindNeighbors:
		if _, ok := query.Parameters[ParamNodeID]; !ok {
			return fmt.Errorf("%w: missing required parameter 'nodeId'", ErrInvalidQuery)
		}
		
		// Direction is optional, defaults to "both"
		if dir, ok := query.Parameters[ParamDirection]; ok {
			if dir != DirectionOutgoing && dir != DirectionIncoming && dir != DirectionBoth {
				return fmt.Errorf("%w: invalid direction parameter, must be 'outgoing', 'incoming', or 'both'", ErrInvalidQuery)
			}
		}
	case QueryTypeFindPath:
		if _, ok := query.Parameters[ParamSourceID]; !ok {
			return fmt.Errorf("%w: missing required parameter 'sourceId'", ErrInvalidQuery)
		}
		if _, ok := query.Parameters[ParamTargetID]; !ok {
			return fmt.Errorf("%w: missing required parameter 'targetId'", ErrInvalidQuery)
		}
		// MaxHops is optional, defaults to a reasonable value in the executor
	}

	return nil
}

// String returns a string representation of the query
func (q *Query) String() string {
	var sb strings.Builder
	sb.WriteString(string(q.Type))
	sb.WriteString("(")
	
	paramCount := 0
	for k, v := range q.Parameters {
		if paramCount > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(k)
		sb.WriteString(": \"")
		sb.WriteString(v)
		sb.WriteString("\"")
		paramCount++
	}
	
	sb.WriteString(")")
	return sb.String()
}