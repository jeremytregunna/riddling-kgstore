package query

import (
	"fmt"

	"git.canoozie.net/riddling/kgstore/pkg/model"
	"git.canoozie.net/riddling/kgstore/pkg/storage"
)

// CreateIndexes creates all the required indexes for querying
func CreateIndexes(dataDir string) (*Engine, error) {
	// Create the storage engine
	config := storage.EngineConfig{
		DataDir: dataDir,
		Logger:  model.DefaultLoggerInstance,
	}

	storageEngine, err := storage.NewStorageEngine(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage engine: %w", err)
	}

	// Create the primary node index
	nodeIndex, err := storage.NewNodeIndex(storageEngine, model.DefaultLoggerInstance)
	if err != nil {
		storageEngine.Close()
		return nil, fmt.Errorf("failed to create node index: %w", err)
	}

	// Create the primary edge index
	edgeIndex, err := storage.NewEdgeIndex(storageEngine, model.DefaultLoggerInstance)
	if err != nil {
		storageEngine.Close()
		return nil, fmt.Errorf("failed to create edge index: %w", err)
	}

	// Create the node label index
	nodeLabels, err := storage.NewNodeLabelIndex(storageEngine, model.DefaultLoggerInstance)
	if err != nil {
		storageEngine.Close()
		return nil, fmt.Errorf("failed to create node label index: %w", err)
	}

	// Create the edge label index
	edgeLabels, err := storage.NewEdgeLabelIndex(storageEngine, model.DefaultLoggerInstance)
	if err != nil {
		storageEngine.Close()
		return nil, fmt.Errorf("failed to create edge label index: %w", err)
	}
    
	// Create the node property index
	nodeProperties, err := storage.NewNodePropertyIndex(storageEngine, model.DefaultLoggerInstance)
	if err != nil {
		storageEngine.Close()
		return nil, fmt.Errorf("failed to create node property index: %w", err)
	}

	// Create the edge property index
	edgeProperties, err := storage.NewEdgePropertyIndex(storageEngine, model.DefaultLoggerInstance)
	if err != nil {
		storageEngine.Close()
		return nil, fmt.Errorf("failed to create edge property index: %w", err)
	}

	// Create the query engine with all the indexes
	queryEngine := NewEngineWithAllIndexes(
		storageEngine, 
		nodeIndex, 
		edgeIndex, 
		nodeLabels, 
		edgeLabels,
		nodeProperties,
		edgeProperties,
	)

	return queryEngine, nil
}

// OpenIndexes opens existing indexes from the specified directory
func OpenIndexes(dataDir string) (*Engine, error) {
	// Open the storage engine
	config := storage.EngineConfig{
		DataDir: dataDir,
		Logger:  model.DefaultLoggerInstance,
	}

	storageEngine, err := storage.NewStorageEngine(config)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage engine: %w", err)
	}

	// Open the primary node index
	nodeIndex, err := storage.NewNodeIndex(storageEngine, model.DefaultLoggerInstance)
	if err != nil {
		storageEngine.Close()
		return nil, fmt.Errorf("failed to open node index: %w", err)
	}

	// Open the primary edge index
	edgeIndex, err := storage.NewEdgeIndex(storageEngine, model.DefaultLoggerInstance)
	if err != nil {
		storageEngine.Close()
		return nil, fmt.Errorf("failed to open edge index: %w", err)
	}

	// Open the node label index
	nodeLabels, err := storage.NewNodeLabelIndex(storageEngine, model.DefaultLoggerInstance)
	if err != nil {
		storageEngine.Close()
		return nil, fmt.Errorf("failed to open node label index: %w", err)
	}

	// Open the edge label index
	edgeLabels, err := storage.NewEdgeLabelIndex(storageEngine, model.DefaultLoggerInstance)
	if err != nil {
		storageEngine.Close()
		return nil, fmt.Errorf("failed to open edge label index: %w", err)
	}
    
	// Open the node property index
	nodeProperties, err := storage.NewNodePropertyIndex(storageEngine, model.DefaultLoggerInstance)
	if err != nil {
		storageEngine.Close()
		return nil, fmt.Errorf("failed to open node property index: %w", err)
	}

	// Open the edge property index
	edgeProperties, err := storage.NewEdgePropertyIndex(storageEngine, model.DefaultLoggerInstance)
	if err != nil {
		storageEngine.Close()
		return nil, fmt.Errorf("failed to open edge property index: %w", err)
	}

	// Create the query engine with all the indexes
	queryEngine := NewEngineWithAllIndexes(
		storageEngine, 
		nodeIndex, 
		edgeIndex, 
		nodeLabels, 
		edgeLabels,
		nodeProperties,
		edgeProperties,
	)

	return queryEngine, nil
}

// CloseIndexes closes all the indexes and the storage engine
func CloseIndexes(engine *Engine) error {
	if engine == nil {
		return nil
	}

	// Get the storage engine
	storageEngine := engine.Executor.Engine

	// Close the storage engine (which will also close all indexes)
	if err := storageEngine.Close(); err != nil {
		return fmt.Errorf("failed to close storage engine: %w", err)
	}

	return nil
}
