package query

import (
	"fmt"

	"git.canoozie.net/riddling/kgstore/pkg/model"
	"git.canoozie.net/riddling/kgstore/pkg/storage"
)

// TraversalType represents different graph traversal algorithms
type TraversalType string

const (
	TraversalTypeBFS TraversalType = "BFS" // Breadth-First Search
	TraversalTypeDFS TraversalType = "DFS" // Depth-First Search
)

// TraversalVisitor is a function called during traversal for each node visited
type TraversalVisitor func(node model.Node, depth int) (bool, error)

// Traversal manages graph traversals
type Traversal struct {
	Engine      *storage.StorageEngine
	NodeIndex   storage.Index
	EdgeIndex   storage.Index
	MaxDepth    int
	Type        TraversalType
}

// NewTraversal creates a new traversal with the given parameters
func NewTraversal(engine *storage.StorageEngine, nodeIndex, edgeIndex storage.Index) *Traversal {
	return &Traversal{
		Engine:    engine,
		NodeIndex: nodeIndex,
		EdgeIndex: edgeIndex,
		MaxDepth:  10,
		Type:      TraversalTypeBFS,
	}
}

// SetMaxDepth sets the maximum traversal depth
func (t *Traversal) SetMaxDepth(depth int) {
	if depth > 0 {
		t.MaxDepth = depth
	}
}

// SetType sets the traversal algorithm type
func (t *Traversal) SetType(traversalType TraversalType) {
	t.Type = traversalType
}

// Run starts a traversal from the given source node
func (t *Traversal) Run(sourceID uint64, direction string, visitor TraversalVisitor) error {
	// First check if the source node exists
	sourceKey := []byte(fmt.Sprintf("node:%d", sourceID))
	sourceBytes, err := t.NodeIndex.Get(sourceKey)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return fmt.Errorf("source node not found: %d", sourceID)
		}
		return fmt.Errorf("error getting source node %d: %w", sourceID, err)
	}

	var sourceNode model.Node
	err = model.Deserialize(sourceBytes, &sourceNode)
	if err != nil {
		return fmt.Errorf("error deserializing source node %d: %w", sourceID, err)
	}

	// Initialize traversal based on type
	switch t.Type {
	case TraversalTypeBFS:
		return t.runBFS(sourceNode, direction, visitor)
	case TraversalTypeDFS:
		return t.runDFS(sourceNode, direction, visitor, 0, make(map[uint64]bool))
	default:
		return fmt.Errorf("unsupported traversal type: %s", t.Type)
	}
}

// runBFS performs a breadth-first traversal
func (t *Traversal) runBFS(sourceNode model.Node, direction string, visitor TraversalVisitor) error {
	// Initialize queue with source node
	queue := []struct {
		NodeID uint64
		Depth  int
	}{
		{NodeID: sourceNode.ID, Depth: 0},
	}

	// Keep track of visited nodes
	visited := make(map[uint64]bool)
	visited[sourceNode.ID] = true

	// Visit source node
	continueTraversal, err := visitor(sourceNode, 0)
	if err != nil {
		return err
	}
	if !continueTraversal {
		return nil
	}

	// Process queue
	for len(queue) > 0 {
		// Dequeue
		current := queue[0]
		queue = queue[1:]

		// Skip if we've reached max depth
		if current.Depth >= t.MaxDepth {
			continue
		}

		// Process neighbors
		neighbors, err := t.getNeighbors(current.NodeID, direction)
		if err != nil {
			return err
		}

		for _, neighbor := range neighbors {
			// Skip if already visited
			if visited[neighbor.ID] {
				continue
			}

			// Mark as visited
			visited[neighbor.ID] = true

			// Visit the node
			continueTraversal, err := visitor(neighbor, current.Depth+1)
			if err != nil {
				return err
			}
			if !continueTraversal {
				return nil
			}

			// Enqueue
			queue = append(queue, struct {
				NodeID uint64
				Depth  int
			}{
				NodeID: neighbor.ID,
				Depth:  current.Depth + 1,
			})
		}
	}

	return nil
}

// runDFS performs a depth-first traversal
func (t *Traversal) runDFS(node model.Node, direction string, visitor TraversalVisitor, depth int, visited map[uint64]bool) error {
	// Skip if we've reached max depth
	if depth >= t.MaxDepth {
		return nil
	}

	// Visit the node
	continueTraversal, err := visitor(node, depth)
	if err != nil {
		return err
	}
	if !continueTraversal {
		return nil
	}

	// Mark as visited
	visited[node.ID] = true

	// Process neighbors
	neighbors, err := t.getNeighbors(node.ID, direction)
	if err != nil {
		return err
	}

	for _, neighbor := range neighbors {
		// Skip if already visited
		if visited[neighbor.ID] {
			continue
		}

		// Recursive DFS
		err = t.runDFS(neighbor, direction, visitor, depth+1, visited)
		if err != nil {
			return err
		}
	}

	return nil
}

// getNeighbors returns the neighbors of a node
func (t *Traversal) getNeighbors(nodeID uint64, direction string) ([]model.Node, error) {
	neighbors := make([]model.Node, 0)
	neighborIDs := make(map[uint64]bool)

	if direction == DirectionOutgoing || direction == DirectionBoth {
		// Get outgoing edges
		outKey := []byte(fmt.Sprintf("outgoing:%d", nodeID))
		outEdgeIDsBytes, err := t.EdgeIndex.Get(outKey)
		if err != nil && err != storage.ErrKeyNotFound {
			return nil, fmt.Errorf("error getting outgoing edges for node %d: %w", nodeID, err)
		}
		
		if err == nil {
			var outEdgeIDs []string
			err = model.Deserialize(outEdgeIDsBytes, &outEdgeIDs)
			if err != nil {
				return nil, fmt.Errorf("error deserializing outgoing edge IDs: %w", err)
			}

			// Process each outgoing edge
			for _, edgeID := range outEdgeIDs {
				edgeKey := []byte(fmt.Sprintf("edge:%s", edgeID))
				edgeBytes, err := t.EdgeIndex.Get(edgeKey)
				if err != nil {
					if err != storage.ErrKeyNotFound {
						return nil, fmt.Errorf("error getting edge %s: %w", edgeID, err)
					}
					continue
				}

				var edge model.Edge
				err = model.Deserialize(edgeBytes, &edge)
				if err != nil {
					return nil, fmt.Errorf("error deserializing edge %s: %w", edgeID, err)
				}

				// Add target node ID if not already added
				if !neighborIDs[edge.TargetID] {
					neighborIDs[edge.TargetID] = true
				}
			}
		}
	}

	if direction == DirectionIncoming || direction == DirectionBoth {
		// Get incoming edges
		inKey := []byte(fmt.Sprintf("incoming:%d", nodeID))
		inEdgeIDsBytes, err := t.EdgeIndex.Get(inKey)
		if err != nil && err != storage.ErrKeyNotFound {
			return nil, fmt.Errorf("error getting incoming edges for node %d: %w", nodeID, err)
		}
		
		if err == nil {
			var inEdgeIDs []string
			err = model.Deserialize(inEdgeIDsBytes, &inEdgeIDs)
			if err != nil {
				return nil, fmt.Errorf("error deserializing incoming edge IDs: %w", err)
			}

			// Process each incoming edge
			for _, edgeID := range inEdgeIDs {
				edgeKey := []byte(fmt.Sprintf("edge:%s", edgeID))
				edgeBytes, err := t.EdgeIndex.Get(edgeKey)
				if err != nil {
					if err != storage.ErrKeyNotFound {
						return nil, fmt.Errorf("error getting edge %s: %w", edgeID, err)
					}
					continue
				}

				var edge model.Edge
				err = model.Deserialize(edgeBytes, &edge)
				if err != nil {
					return nil, fmt.Errorf("error deserializing edge %s: %w", edgeID, err)
				}

				// Add source node ID if not already added
				if !neighborIDs[edge.SourceID] {
					neighborIDs[edge.SourceID] = true
				}
			}
		}
	}

	// Get nodes for the neighbor IDs
	for id := range neighborIDs {
		nodeKey := []byte(fmt.Sprintf("node:%d", id))
		nodeBytes, err := t.NodeIndex.Get(nodeKey)
		if err != nil {
			if err != storage.ErrKeyNotFound {
				return nil, fmt.Errorf("error getting node %d: %w", id, err)
			}
			continue
		}

		var node model.Node
		err = model.Deserialize(nodeBytes, &node)
		if err != nil {
			return nil, fmt.Errorf("error deserializing node %d: %w", id, err)
		}

		neighbors = append(neighbors, node)
	}

	return neighbors, nil
}