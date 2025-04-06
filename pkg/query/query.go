package query

import (
	"git.canoozie.net/riddling/kgstore/pkg/storage"
)

// Engine represents the query processing engine
type Engine struct {
	Executor  *Executor
	Optimizer *Optimizer
	Traversal *Traversal
}

// NewEngine creates a new query engine with basic indexes
func NewEngine(storageEngine *storage.StorageEngine, nodeIndex, edgeIndex, nodeLabels, edgeLabels storage.Index) *Engine {
	executor := NewExecutor(storageEngine, nodeIndex, edgeIndex, nodeLabels, edgeLabels)
	optimizer := NewOptimizer()
	traversal := NewTraversal(storageEngine, nodeIndex, edgeIndex)

	return &Engine{
		Executor:  executor,
		Optimizer: optimizer,
		Traversal: traversal,
	}
}

// NewEngineWithAllIndexes creates a new query engine with all available indexes
func NewEngineWithAllIndexes(
	storageEngine *storage.StorageEngine,
	nodeIndex,
	edgeIndex,
	nodeLabels,
	edgeLabels,
	nodeProperties,
	edgeProperties storage.Index,
) *Engine {
	executor := NewExecutorWithAllIndexes(
		storageEngine,
		nodeIndex,
		edgeIndex,
		nodeLabels,
		edgeLabels,
		nodeProperties,
		edgeProperties,
	)
	optimizer := NewOptimizer()
	traversal := NewTraversal(storageEngine, nodeIndex, edgeIndex)

	return &Engine{
		Executor:  executor,
		Optimizer: optimizer,
		Traversal: traversal,
	}
}

// Execute parses and executes a query
func (e *Engine) Execute(queryStr string) (*Result, error) {
	// Parse the query
	query, err := Parse(queryStr)
	if err != nil {
		return nil, err
	}

	// Execute the query
	return e.Executor.Execute(query)
}

// FindNodesByLabel finds all nodes with the given label
func (e *Engine) FindNodesByLabel(label string) (*Result, error) {
	query := &Query{
		Type: QueryTypeFindNodesByLabel,
		Parameters: map[string]string{
			ParamLabel: label,
		},
	}

	return e.Executor.Execute(query)
}

// FindEdgesByLabel finds all edges with the given label
func (e *Engine) FindEdgesByLabel(label string) (*Result, error) {
	query := &Query{
		Type: QueryTypeFindEdgesByLabel,
		Parameters: map[string]string{
			ParamLabel: label,
		},
	}

	return e.Executor.Execute(query)
}

// FindNeighbors finds all neighbors of the given node
func (e *Engine) FindNeighbors(nodeID uint64, direction string) (*Result, error) {
	query := &Query{
		Type: QueryTypeFindNeighbors,
		Parameters: map[string]string{
			ParamNodeID:    convertUint64ToString(nodeID),
			ParamDirection: direction,
		},
	}

	return e.Executor.Execute(query)
}

// FindPath finds a path between two nodes
func (e *Engine) FindPath(sourceID, targetID uint64, maxHops int) (*Result, error) {
	query := &Query{
		Type: QueryTypeFindPath,
		Parameters: map[string]string{
			ParamSourceID: convertUint64ToString(sourceID),
			ParamTargetID: convertUint64ToString(targetID),
			ParamMaxHops:  convertIntToString(maxHops),
		},
	}

	return e.Executor.Execute(query)
}

// FindNodesByProperty finds all nodes with a specific property value
func (e *Engine) FindNodesByProperty(propertyName string, propertyValue string) (*Result, error) {
	query := &Query{
		Type: QueryTypeFindNodesByProperty,
		Parameters: map[string]string{
			ParamPropertyName:  propertyName,
			ParamPropertyValue: propertyValue,
		},
	}

	return e.Executor.Execute(query)
}

// FindEdgesByProperty finds all edges with a specific property value
func (e *Engine) FindEdgesByProperty(propertyName string, propertyValue string) (*Result, error) {
	query := &Query{
		Type: QueryTypeFindEdgesByProperty,
		Parameters: map[string]string{
			ParamPropertyName:  propertyName,
			ParamPropertyValue: propertyValue,
		},
	}

	return e.Executor.Execute(query)
}

// convertUint64ToString converts a uint64 to a string
func convertUint64ToString(value uint64) string {
	return FormatUint64(value)
}

// convertIntToString converts an int to a string
func convertIntToString(value int) string {
	return FormatInt(value)
}
