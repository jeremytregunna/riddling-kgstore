package storage

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"git.canoozie.net/riddling/kgstore/pkg/common"
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


// CreateNode creates a new node with the given label
func (g *GraphStore) CreateNode(tx *Transaction, label string) (string, error) {
	g.logger.Debug("GraphStore.CreateNode - Starting with tx %d", tx.id)
	
	// Generate a new node ID
	nodeID := g.nodeIDCounter.Add(1) // Atomic increment
	node := model.NewNode(nodeID, label)
	nodeIDStr := fmt.Sprintf("%d", nodeID)

	// Serialize the node
	nodeData, err := model.SerializeNode(node)
	if err != nil {
		return "", fmt.Errorf("failed to serialize node: %w", err)
	}

	// Create the node key with prefix using common package
	nodeKey := []byte(common.FormatNodeKey(nodeID))

	g.logger.Debug("GraphStore.CreateNode - Storing node %s with key %s using tx %d", 
		nodeIDStr, string(nodeKey), tx.id)
	
	// Store the node in the database with the prefix, using the transaction
	if err := g.engine.PutWithTx(tx, nodeKey, nodeData); err != nil {
		g.logger.Debug("GraphStore.CreateNode - Failed to store node: %v", err)
		return "", fmt.Errorf("failed to store node: %w", err)
	}

	// Also store with just the ID as key for compatibility with some access patterns
	nodeKeyWithoutPrefix := []byte(nodeIDStr)
	g.logger.Debug("Also storing node with raw ID key: %s", nodeIDStr)
	if err := g.engine.PutWithTx(tx, nodeKeyWithoutPrefix, nodeData); err != nil {
		g.logger.Warn("Failed to store node with raw ID key: %v", err)
		// Continue even if this fails, not critical
	}

	// Add to the label index - this is for compatibility with old index approach
	labelKey := []byte(common.FormatNodeLabelKey(label, nodeID))
	g.logger.Debug("GraphStore.CreateNode - Updating label index with key %s using tx %d", 
		string(labelKey), tx.id)
	
	if err := g.engine.PutWithTx(tx, labelKey, []byte{}); err != nil {
		return "", fmt.Errorf("failed to update label index: %w", err)
	}

	// Also add to node label index (needed especially for LSM-based index)
	// This was using a direct Put which was causing the deadlock!
	// Use a specific transaction-based operation to add to the index
	g.logger.Debug("GraphStore.CreateNode - Adding to node label index for label %s -> %s using tx %d", 
		label, nodeIDStr, tx.id)
	
	// Instead of using the LSM-based index directly which uses engine.Put,
	// store it with the transaction, same as we did above
	nodeLabelKey := []byte(common.FormatNodeLabelScanKey(label) + nodeIDStr)
	if err := g.engine.PutWithTx(tx, nodeLabelKey, []byte{}); err != nil {
		g.logger.Warn("Failed to add node %s to label index for label %s: %v", nodeIDStr, label, err)
	}

	g.logger.Debug("Created node %s with label %s", nodeIDStr, label)
	g.logger.Debug("GraphStore.CreateNode - Successfully created node %s using tx %d", 
		nodeIDStr, tx.id)
	return nodeIDStr, nil
}

// GetNode retrieves a node by ID
func (g *GraphStore) GetNode(nodeID string) (*model.Node, error) {
	// Parse node ID as uint64
	nodeIDUint, err := common.ParseUint64(nodeID)
	if err != nil {
		return nil, fmt.Errorf("invalid node ID: %w", err)
	}

	// Create the node key using common package
	nodeKey := []byte(common.FormatNodeKey(nodeIDUint))
	g.logger.Debug("GraphStore.GetNode - Looking up node with key: %s", string(nodeKey))

	// Retrieve the node data from the database
	nodeData, err := g.engine.Get(nodeKey)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			// Try alternative key format (without prefix)
			rawNodeKey := []byte(nodeID)
			g.logger.Debug("GraphStore.GetNode - Node not found with key %s, trying raw ID: %s", 
				string(nodeKey), string(rawNodeKey))
			
			rawNodeData, rawErr := g.engine.Get(rawNodeKey)
			if rawErr != nil {
				if errors.Is(rawErr, ErrKeyNotFound) {
					g.logger.Debug("GraphStore.GetNode - Node not found with either key format: %s or %s", 
						string(nodeKey), string(rawNodeKey))
					return nil, ErrNodeNotFound
				}
				return nil, fmt.Errorf("failed to retrieve node with raw ID: %w", rawErr)
			}
			
			// Use the raw node data instead
			g.logger.Debug("GraphStore.GetNode - Found node with raw ID: %s", string(rawNodeKey))
			nodeData = rawNodeData
		} else {
			return nil, fmt.Errorf("failed to retrieve node: %w", err)
		}
	} else {
		g.logger.Debug("GraphStore.GetNode - Found node with prefixed key: %s", string(nodeKey))
	}

	// Deserialize the node
	node, err := model.DeserializeNode(nodeData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize node: %w", err)
	}

	g.logger.Debug("GraphStore.GetNode - Successfully deserialized node %s with label %s", 
		nodeID, node.Label)
	return node, nil
}

// DeleteNode deletes a node by ID
func (g *GraphStore) DeleteNode(tx *Transaction, nodeID string) error {
	// First, check if the node exists
	node, err := g.GetNode(nodeID)
	if err != nil {
		return err // Either ErrNodeNotFound or another error
	}

	// Parse node ID as uint64
	nodeIDUint, err := common.ParseUint64(nodeID)
	if err != nil {
		return fmt.Errorf("invalid node ID: %w", err)
	}

	// Create the node key using common package
	nodeKey := []byte(common.FormatNodeKey(nodeIDUint))

	// Delete the node from the database
	if err := g.engine.DeleteWithTx(tx, nodeKey); err != nil {
		return fmt.Errorf("failed to delete node: %w", err)
	}

	// Remove from the label index
	labelKey := []byte(common.FormatNodeLabelKey(node.Label, nodeIDUint))
	if err := g.engine.DeleteWithTx(tx, labelKey); err != nil {
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
	g.logger.Debug("GraphStore.CreateEdge - Starting with tx %d, source %s, target %s, label %s", 
		tx.id, sourceID, targetID, label)
		
	// Check if source and target nodes exist
	sourceNode, err := g.GetNode(sourceID)
	if err != nil {
		g.logger.Debug("GraphStore.CreateEdge - Source node %s not found: %v", sourceID, err)
		return "", fmt.Errorf("source node: %w", err)
	}
	g.logger.Debug("GraphStore.CreateEdge - Found source node %s with label %s", 
		sourceID, sourceNode.Label)
	
	targetNode, err := g.GetNode(targetID)
	if err != nil {
		g.logger.Debug("GraphStore.CreateEdge - Target node %s not found: %v", targetID, err)
		return "", fmt.Errorf("target node: %w", err)
	}
	g.logger.Debug("GraphStore.CreateEdge - Found target node %s with label %s", 
		targetID, targetNode.Label)

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

	// Create the edge key with prefix using common package
	edgeKey := []byte(common.FormatEdgeKey(edgeIDStr))
	g.logger.Debug("GraphStore.CreateEdge - Storing edge with key: %s", string(edgeKey))

	// Store the edge in the database with the prefix, using the transaction
	if err := g.engine.PutWithTx(tx, edgeKey, edgeData); err != nil {
		return "", fmt.Errorf("failed to store edge: %w", err)
	}

	// Add to the label index using common package
	labelKey := []byte(common.FormatEdgeLabelKey(label, edgeIDStr))
	g.logger.Debug("GraphStore.CreateEdge - Adding to edge label index with key: %s", string(labelKey))
	
	if err := g.engine.PutWithTx(tx, labelKey, []byte{}); err != nil {
		return "", fmt.Errorf("failed to update label index: %w", err)
	}
	
	// Also add to edge label scan index (needed for LSM-based index and direct scans)
	edgeLabelScanKey := []byte(common.FormatEdgeLabelScanKey(label) + edgeIDStr)
	g.logger.Debug("GraphStore.CreateEdge - Adding to edge label scan index with key: %s", string(edgeLabelScanKey))
	
	if err := g.engine.PutWithTx(tx, edgeLabelScanKey, []byte{}); err != nil {
		g.logger.Warn("Failed to add edge %s to label scan index for label %s: %v", edgeIDStr, label, err)
	}

	// Parse source ID as uint64
	sourceIDUint, err := common.ParseUint64(sourceID)
	if err != nil {
		return "", fmt.Errorf("invalid source ID: %w", err)
	}

	// Add to the source node index using common package
	sourceKey := []byte(common.FormatSourceEdgeKey(sourceIDUint, edgeIDStr))
	if err := g.engine.PutWithTx(tx, sourceKey, []byte{}); err != nil {
		return "", fmt.Errorf("failed to update source node index: %w", err)
	}

	// Also store in outgoing edges list
	outgoingKey := []byte(common.FormatOutgoingEdgesKey(sourceIDUint))
	outgoingEdges, err := g.engine.GetWithTx(tx, outgoingKey)
	if err != nil && err != ErrKeyNotFound {
		g.logger.Warn("Failed to get outgoing edges for node %s: %v", sourceID, err)
	}

	// Create or update outgoing edges list
	var outEdgeIDs []string
	if err == nil {
		// Deserialize existing edges if found
		g.logger.Debug("Found existing outgoing edges for node %s, deserializing", sourceID)
		err = model.Deserialize(outgoingEdges, &outEdgeIDs)
		if err != nil {
			g.logger.Warn("Failed to deserialize outgoing edges for node %s: %v", sourceID, err)
			outEdgeIDs = make([]string, 0)
		} else {
			g.logger.Debug("Found %d existing outgoing edges for node %s: %v", len(outEdgeIDs), sourceID, outEdgeIDs)
		}
	} else {
		// Create a new list if not found
		g.logger.Debug("No existing outgoing edges found for node %s, creating new list", sourceID)
		outEdgeIDs = make([]string, 0)
	}

	// Add this edge ID to the list
	outEdgeIDs = append(outEdgeIDs, edgeIDStr)

	// Serialize and store the updated list
	outData, err := model.Serialize(outEdgeIDs)
	if err != nil {
		g.logger.Warn("Failed to serialize outgoing edges for node %s: %v", sourceID, err)
	} else {
		g.logger.Debug("Storing %d outgoing edges for node %s with key %s: %v",
			len(outEdgeIDs), sourceID, string(outgoingKey), outEdgeIDs)
		if err := g.engine.PutWithTx(tx, outgoingKey, outData); err != nil {
			g.logger.Warn("Failed to update outgoing edges for node %s: %v", sourceID, err)
		} else {
			g.logger.Debug("Successfully stored outgoing edges for node %s", sourceID)
		}
	}

	// Parse target ID as uint64
	targetIDUint, err := common.ParseUint64(targetID)
	if err != nil {
		return "", fmt.Errorf("invalid target ID: %w", err)
	}

	// Add to the target node index using common package
	targetKey := []byte(common.FormatTargetEdgeKey(targetIDUint, edgeIDStr))
	if err := g.engine.PutWithTx(tx, targetKey, []byte{}); err != nil {
		return "", fmt.Errorf("failed to update target node index: %w", err)
	}

	// Also store in incoming edges list
	incomingKey := []byte(common.FormatIncomingEdgesKey(targetIDUint))
	incomingEdges, err := g.engine.GetWithTx(tx, incomingKey)
	if err != nil && err != ErrKeyNotFound {
		g.logger.Warn("Failed to get incoming edges for node %s: %v", targetID, err)
	}

	// Create or update incoming edges list
	var inEdgeIDs []string
	if err == nil {
		// Deserialize existing edges if found
		err = model.Deserialize(incomingEdges, &inEdgeIDs)
		if err != nil {
			g.logger.Warn("Failed to deserialize incoming edges for node %s: %v", targetID, err)
			inEdgeIDs = make([]string, 0)
		}
	} else {
		// Create a new list if not found
		inEdgeIDs = make([]string, 0)
	}

	// Add this edge ID to the list
	inEdgeIDs = append(inEdgeIDs, edgeIDStr)

	// Serialize and store the updated list
	inData, err := model.Serialize(inEdgeIDs)
	if err != nil {
		g.logger.Warn("Failed to serialize incoming edges for node %s: %v", targetID, err)
	} else {
		if err := g.engine.PutWithTx(tx, incomingKey, inData); err != nil {
			g.logger.Warn("Failed to update incoming edges for node %s: %v", targetID, err)
		}
	}

	g.logger.Debug("Created edge %s from %s to %s with label %s", edgeIDStr, sourceID, targetID, label)
	return edgeIDStr, nil
}

// GetEdge retrieves an edge by ID
func (g *GraphStore) GetEdge(edgeID string) (*model.Edge, error) {
	// Create the edge key using common package
	edgeKey := []byte(common.FormatEdgeKey(edgeID))
	g.logger.Debug("GraphStore.GetEdge - Looking up edge with key: %s", string(edgeKey))

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

	// Create the edge key using common package
	edgeKey := []byte(common.FormatEdgeKey(edgeID))

	// Delete the edge from the database
	if err := g.engine.DeleteWithTx(tx, edgeKey); err != nil {
		return fmt.Errorf("failed to delete edge: %w", err)
	}

	// Remove from the label index using common package
	labelKey := []byte(common.FormatEdgeLabelKey(edge.Label, edgeID))
	if err := g.engine.DeleteWithTx(tx, labelKey); err != nil {
		g.logger.Warn("Failed to remove edge %s from label index: %v", edgeID, err)
	}

	// Using common package to generate keys from source and target IDs directly

	// Remove from the source node index using common package
	sourceKey := []byte(common.FormatSourceEdgeKey(edge.SourceID, edgeID))
	if err := g.engine.DeleteWithTx(tx, sourceKey); err != nil {
		g.logger.Warn("Failed to remove edge %s from source node index: %v", edgeID, err)
	}

	// Remove from the target node index using common package
	targetKey := []byte(common.FormatTargetEdgeKey(edge.TargetID, edgeID))
	if err := g.engine.DeleteWithTx(tx, targetKey); err != nil {
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

	// Parse node ID as uint64
	nodeIDUint, err := common.ParseUint64(nodeID)
	if err != nil {
		return fmt.Errorf("invalid node ID: %w", err)
	}

	// Update the node in the database using common package
	nodeKey := []byte(common.FormatNodeKey(nodeIDUint))
	if err := g.engine.PutWithTx(tx, nodeKey, nodeData); err != nil {
		return fmt.Errorf("failed to update node: %w", err)
	}

	// Add to the property index using common package
	propKey := []byte(common.FormatNodePropertyKey(name, value, nodeID))
	if err := g.engine.PutWithTx(tx, propKey, []byte{}); err != nil {
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

	// Update the edge in the database using common package
	edgeKey := []byte(common.FormatEdgeKey(edgeID))
	if err := g.engine.PutWithTx(tx, edgeKey, edgeData); err != nil {
		return fmt.Errorf("failed to update edge: %w", err)
	}

	// Add to the property index using common package
	propKey := []byte(common.FormatEdgePropertyKey(name, value, edgeID))
	if err := g.engine.PutWithTx(tx, propKey, []byte{}); err != nil {
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

	// Parse node ID as uint64
	nodeIDUint, err := common.ParseUint64(nodeID)
	if err != nil {
		return fmt.Errorf("invalid node ID: %w", err)
	}

	// Update the node in the database using common package
	nodeKey := []byte(common.FormatNodeKey(nodeIDUint))
	if err := g.engine.PutWithTx(tx, nodeKey, nodeData); err != nil {
		return fmt.Errorf("failed to update node: %w", err)
	}

	// Remove from the property index using common package
	propKey := []byte(common.FormatNodePropertyKey(name, oldValue, nodeID))
	if err := g.engine.DeleteWithTx(tx, propKey); err != nil {
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

	// Update the edge in the database using common package
	edgeKey := []byte(common.FormatEdgeKey(edgeID))
	if err := g.engine.PutWithTx(tx, edgeKey, edgeData); err != nil {
		return fmt.Errorf("failed to update edge: %w", err)
	}

	// Remove from the property index using common package
	propKey := []byte(common.FormatEdgePropertyKey(name, oldValue, edgeID))
	if err := g.engine.DeleteWithTx(tx, propKey); err != nil {
		g.logger.Warn("Failed to remove edge %s from property index: %v", edgeID, err)
	}

	g.logger.Debug("Removed property %s from edge %s", name, edgeID)
	return nil
}
