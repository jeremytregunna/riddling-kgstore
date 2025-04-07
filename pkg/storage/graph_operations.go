package storage

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

var (
	// ErrNodeNotFound indicates that a node with the given ID was not found
	ErrNodeNotFound = errors.New("node not found")

	// ErrEdgeNotFound indicates that an edge with the given ID was not found
	ErrEdgeNotFound = errors.New("edge not found")

	// ErrInvalidTarget indicates that the target parameter is invalid
	ErrInvalidTarget = errors.New("invalid target, must be 'node' or 'edge'")
)

// GraphOperations interface defines the high-level graph operations
type GraphOperations interface {
	// Node operations
	CreateNode(tx *Transaction, label string) (string, error)
	GetNode(nodeID string) (*model.Node, error)
	DeleteNode(tx *Transaction, nodeID string) error

	// Edge operations
	CreateEdge(tx *Transaction, sourceID, targetID, label string) (string, error)
	GetEdge(edgeID string) (*model.Edge, error)
	DeleteEdge(tx *Transaction, edgeID string) error

	// Property operations
	SetNodeProperty(tx *Transaction, nodeID, name, value string) error
	SetEdgeProperty(tx *Transaction, edgeID, name, value string) error
	RemoveNodeProperty(tx *Transaction, nodeID, name string) error
	RemoveEdgeProperty(tx *Transaction, edgeID, name string) error
}

// GraphStore implements the GraphOperations interface using the StorageEngine
type GraphStore struct {
	engine        *StorageEngine
	logger        model.Logger
	nodeIDCounter *atomic.Uint64
	edgeIDCounter *atomic.Uint64
	mu            sync.RWMutex // Protects ID counters
}

// NewGraphStore creates a new GraphStore with the given storage engine
func NewGraphStore(engine *StorageEngine, logger model.Logger) (*GraphStore, error) {
	if logger == nil {
		logger = model.DefaultLoggerInstance
	}

	// Create ID counters
	nodeIDCounter := atomic.Uint64{}
	nodeIDCounter.Store(1) // Start from 1

	edgeIDCounter := atomic.Uint64{}
	edgeIDCounter.Store(1) // Start from 1

	// TODO: Scan the database to find the highest node and edge IDs
	// This would require scanning all keys that represent nodes and edges

	return &GraphStore{
		engine:        engine,
		logger:        logger,
		nodeIDCounter: &nodeIDCounter,
		edgeIDCounter: &edgeIDCounter,
	}, nil
}

// Key format constants for serialization
const (
	nodeKeyPrefix    = "n:"  // Prefix for node keys
	edgeKeyPrefix    = "e:"  // Prefix for edge keys
	nodeLabelPrefix  = "nl:" // Prefix for node label index
	edgeLabelPrefix  = "el:" // Prefix for edge label index
	nodePropPrefix   = "np:" // Prefix for node property index
	edgePropPrefix   = "ep:" // Prefix for edge property index
	sourceNodePrefix = "sn:" // Prefix for source node index
	targetNodePrefix = "tn:" // Prefix for target node index
)

// CreateNode creates a new node with the given label
func (g *GraphStore) CreateNode(tx *Transaction, label string) (string, error) {
	// Generate a new node ID
	nodeID := g.nodeIDCounter.Add(1) // Atomic increment
	node := model.NewNode(nodeID, label)
	nodeIDStr := fmt.Sprintf("%d", nodeID)

	// Serialize the node
	nodeData, err := model.SerializeNode(node)
	if err != nil {
		return "", fmt.Errorf("failed to serialize node: %w", err)
	}

	// Create the node key
	nodeKey := []byte(nodeKeyPrefix + nodeIDStr)

	// Store the node in the database
	if err := g.engine.Put(nodeKey, nodeData); err != nil {
		return "", fmt.Errorf("failed to store node: %w", err)
	}

	// Add to the label index
	labelKey := []byte(nodeLabelPrefix + label + ":" + nodeIDStr)
	if err := g.engine.Put(labelKey, []byte{}); err != nil {
		return "", fmt.Errorf("failed to update label index: %w", err)
	}

	g.logger.Debug("Created node %s with label %s", nodeIDStr, label)
	return nodeIDStr, nil
}

// GetNode retrieves a node by ID
func (g *GraphStore) GetNode(nodeID string) (*model.Node, error) {
	// Create the node key
	nodeKey := []byte(nodeKeyPrefix + nodeID)

	// Retrieve the node data from the database
	nodeData, err := g.engine.Get(nodeKey)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, ErrNodeNotFound
		}
		return nil, fmt.Errorf("failed to retrieve node: %w", err)
	}

	// Deserialize the node
	node, err := model.DeserializeNode(nodeData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize node: %w", err)
	}

	return node, nil
}

// DeleteNode deletes a node by ID
func (g *GraphStore) DeleteNode(tx *Transaction, nodeID string) error {
	// First, check if the node exists
	node, err := g.GetNode(nodeID)
	if err != nil {
		return err // Either ErrNodeNotFound or another error
	}

	// Create the node key
	nodeKey := []byte(nodeKeyPrefix + nodeID)

	// Delete the node from the database
	if err := g.engine.Delete(nodeKey); err != nil {
		return fmt.Errorf("failed to delete node: %w", err)
	}

	// Remove from the label index
	labelKey := []byte(nodeLabelPrefix + node.Label + ":" + nodeID)
	if err := g.engine.Delete(labelKey); err != nil {
		g.logger.Warn("Failed to remove node %s from label index: %v", nodeID, err)
	}

	// TODO: Delete all properties of the node
	// This would require scanning all keys with the nodePropPrefix + nodeID

	// TODO: Delete all edges connected to this node
	// This would require scanning all keys with sourceNodePrefix + nodeID
	// and targetNodePrefix + nodeID

	g.logger.Debug("Deleted node %s", nodeID)
	return nil
}

// CreateEdge creates a new edge between two nodes
func (g *GraphStore) CreateEdge(tx *Transaction, sourceID, targetID, label string) (string, error) {
	// Check if source and target nodes exist
	if _, err := g.GetNode(sourceID); err != nil {
		return "", fmt.Errorf("source node: %w", err)
	}
	if _, err := g.GetNode(targetID); err != nil {
		return "", fmt.Errorf("target node: %w", err)
	}

	// Generate a new edge ID
	edgeID := g.edgeIDCounter.Add(1) // Atomic increment
	sourceNodeID, _ := strconv.ParseUint(sourceID, 10, 64)
	targetNodeID, _ := strconv.ParseUint(targetID, 10, 64)
	edge := model.NewEdge(sourceNodeID, targetNodeID, label)
	edgeIDStr := fmt.Sprintf("%d", edgeID)

	// Serialize the edge
	edgeData, err := model.SerializeEdge(edge)
	if err != nil {
		return "", fmt.Errorf("failed to serialize edge: %w", err)
	}

	// Create the edge key
	edgeKey := []byte(edgeKeyPrefix + edgeIDStr)

	// Store the edge in the database
	if err := g.engine.Put(edgeKey, edgeData); err != nil {
		return "", fmt.Errorf("failed to store edge: %w", err)
	}

	// Add to the label index
	labelKey := []byte(edgeLabelPrefix + label + ":" + edgeIDStr)
	if err := g.engine.Put(labelKey, []byte{}); err != nil {
		return "", fmt.Errorf("failed to update label index: %w", err)
	}

	// Add to the source node index
	sourceKey := []byte(sourceNodePrefix + sourceID + ":" + edgeIDStr)
	if err := g.engine.Put(sourceKey, []byte{}); err != nil {
		return "", fmt.Errorf("failed to update source node index: %w", err)
	}

	// Add to the target node index
	targetKey := []byte(targetNodePrefix + targetID + ":" + edgeIDStr)
	if err := g.engine.Put(targetKey, []byte{}); err != nil {
		return "", fmt.Errorf("failed to update target node index: %w", err)
	}

	g.logger.Debug("Created edge %s from %s to %s with label %s", edgeIDStr, sourceID, targetID, label)
	return edgeIDStr, nil
}

// GetEdge retrieves an edge by ID
func (g *GraphStore) GetEdge(edgeID string) (*model.Edge, error) {
	// Create the edge key
	edgeKey := []byte(edgeKeyPrefix + edgeID)

	// Retrieve the edge data from the database
	edgeData, err := g.engine.Get(edgeKey)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, ErrEdgeNotFound
		}
		return nil, fmt.Errorf("failed to retrieve edge: %w", err)
	}

	// Deserialize the edge
	edge, err := model.DeserializeEdge(edgeData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize edge: %w", err)
	}

	return edge, nil
}

// DeleteEdge deletes an edge by ID
func (g *GraphStore) DeleteEdge(tx *Transaction, edgeID string) error {
	// First, check if the edge exists
	edge, err := g.GetEdge(edgeID)
	if err != nil {
		return err // Either ErrEdgeNotFound or another error
	}

	// Create the edge key
	edgeKey := []byte(edgeKeyPrefix + edgeID)

	// Delete the edge from the database
	if err := g.engine.Delete(edgeKey); err != nil {
		return fmt.Errorf("failed to delete edge: %w", err)
	}

	// Remove from the label index
	labelKey := []byte(edgeLabelPrefix + edge.Label + ":" + edgeID)
	if err := g.engine.Delete(labelKey); err != nil {
		g.logger.Warn("Failed to remove edge %s from label index: %v", edgeID, err)
	}

	// Get source and target node IDs as strings
	sourceID := fmt.Sprintf("%d", edge.SourceID)
	targetID := fmt.Sprintf("%d", edge.TargetID)

	// Remove from the source node index
	sourceKey := []byte(sourceNodePrefix + sourceID + ":" + edgeID)
	if err := g.engine.Delete(sourceKey); err != nil {
		g.logger.Warn("Failed to remove edge %s from source node index: %v", edgeID, err)
	}

	// Remove from the target node index
	targetKey := []byte(targetNodePrefix + targetID + ":" + edgeID)
	if err := g.engine.Delete(targetKey); err != nil {
		g.logger.Warn("Failed to remove edge %s from target node index: %v", edgeID, err)
	}

	// TODO: Delete all properties of the edge
	// This would require scanning all keys with the edgePropPrefix + edgeID

	g.logger.Debug("Deleted edge %s", edgeID)
	return nil
}

// SetNodeProperty sets a property on a node
func (g *GraphStore) SetNodeProperty(tx *Transaction, nodeID, name, value string) error {
	// First, check if the node exists
	node, err := g.GetNode(nodeID)
	if err != nil {
		return err
	}

	// Set the property on the node
	node.PropertyContainer.AddProperty(name, value)

	// Serialize the updated node
	nodeData, err := model.SerializeNode(node)
	if err != nil {
		return fmt.Errorf("failed to serialize node: %w", err)
	}

	// Update the node in the database
	nodeKey := []byte(nodeKeyPrefix + nodeID)
	if err := g.engine.Put(nodeKey, nodeData); err != nil {
		return fmt.Errorf("failed to update node: %w", err)
	}

	// Add to the property index
	propKey := []byte(nodePropPrefix + name + ":" + value + ":" + nodeID)
	if err := g.engine.Put(propKey, []byte{}); err != nil {
		return fmt.Errorf("failed to update property index: %w", err)
	}

	g.logger.Debug("Set property %s=%s on node %s", name, value, nodeID)
	return nil
}

// SetEdgeProperty sets a property on an edge
func (g *GraphStore) SetEdgeProperty(tx *Transaction, edgeID, name, value string) error {
	// First, check if the edge exists
	edge, err := g.GetEdge(edgeID)
	if err != nil {
		return err
	}

	// Set the property on the edge
	edge.PropertyContainer.AddProperty(name, value)

	// Serialize the updated edge
	edgeData, err := model.SerializeEdge(edge)
	if err != nil {
		return fmt.Errorf("failed to serialize edge: %w", err)
	}

	// Update the edge in the database
	edgeKey := []byte(edgeKeyPrefix + edgeID)
	if err := g.engine.Put(edgeKey, edgeData); err != nil {
		return fmt.Errorf("failed to update edge: %w", err)
	}

	// Add to the property index
	propKey := []byte(edgePropPrefix + name + ":" + value + ":" + edgeID)
	if err := g.engine.Put(propKey, []byte{}); err != nil {
		return fmt.Errorf("failed to update property index: %w", err)
	}

	g.logger.Debug("Set property %s=%s on edge %s", name, value, edgeID)
	return nil
}

// RemoveNodeProperty removes a property from a node
func (g *GraphStore) RemoveNodeProperty(tx *Transaction, nodeID, name string) error {
	// First, check if the node exists
	node, err := g.GetNode(nodeID)
	if err != nil {
		return err
	}

	// Check if the property exists and get its value
	oldValue, exists := node.PropertyContainer.GetProperty(name)
	if !exists {
		g.logger.Debug("Property %s does not exist on node %s", name, nodeID)
		return nil // No error if property doesn't exist
	}

	// Remove the property from the node
	node.PropertyContainer.RemoveProperty(name)

	// Serialize the updated node
	nodeData, err := model.SerializeNode(node)
	if err != nil {
		return fmt.Errorf("failed to serialize node: %w", err)
	}

	// Update the node in the database
	nodeKey := []byte(nodeKeyPrefix + nodeID)
	if err := g.engine.Put(nodeKey, nodeData); err != nil {
		return fmt.Errorf("failed to update node: %w", err)
	}

	// Remove from the property index
	propKey := []byte(nodePropPrefix + name + ":" + oldValue + ":" + nodeID)
	if err := g.engine.Delete(propKey); err != nil {
		g.logger.Warn("Failed to remove node %s from property index: %v", nodeID, err)
	}

	g.logger.Debug("Removed property %s from node %s", name, nodeID)
	return nil
}

// RemoveEdgeProperty removes a property from an edge
func (g *GraphStore) RemoveEdgeProperty(tx *Transaction, edgeID, name string) error {
	// First, check if the edge exists
	edge, err := g.GetEdge(edgeID)
	if err != nil {
		return err
	}

	// Check if the property exists and get its value
	oldValue, exists := edge.PropertyContainer.GetProperty(name)
	if !exists {
		g.logger.Debug("Property %s does not exist on edge %s", name, edgeID)
		return nil // No error if property doesn't exist
	}

	// Remove the property from the edge
	edge.PropertyContainer.RemoveProperty(name)

	// Serialize the updated edge
	edgeData, err := model.SerializeEdge(edge)
	if err != nil {
		return fmt.Errorf("failed to serialize edge: %w", err)
	}

	// Update the edge in the database
	edgeKey := []byte(edgeKeyPrefix + edgeID)
	if err := g.engine.Put(edgeKey, edgeData); err != nil {
		return fmt.Errorf("failed to update edge: %w", err)
	}

	// Remove from the property index
	propKey := []byte(edgePropPrefix + name + ":" + oldValue + ":" + edgeID)
	if err := g.engine.Delete(propKey); err != nil {
		g.logger.Warn("Failed to remove edge %s from property index: %v", edgeID, err)
	}

	g.logger.Debug("Removed property %s from edge %s", name, edgeID)
	return nil
}
