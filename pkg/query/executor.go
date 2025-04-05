package query

import (
	"fmt"
	"strconv"

	"git.canoozie.net/riddling/kgstore/pkg/model"
	"git.canoozie.net/riddling/kgstore/pkg/storage"
)

// Result represents a query result
type Result struct {
	Nodes []model.Node `json:"nodes,omitempty"`
	Edges []model.Edge `json:"edges,omitempty"`
	Paths []Path       `json:"paths,omitempty"`
	Error string       `json:"error,omitempty"`
}

// Path represents a path in the graph
type Path struct {
	Nodes []model.Node `json:"nodes"`
	Edges []model.Edge `json:"edges"`
}

// Executor executes queries against the storage engine
type Executor struct {
	Engine      *storage.StorageEngine
	NodeIndex   storage.Index
	EdgeIndex   storage.Index
	NodeLabels  storage.Index
	EdgeLabels  storage.Index
	Optimizer   *Optimizer
	maxPathHops int
}

// NewExecutor creates a new query executor
func NewExecutor(engine *storage.StorageEngine, nodeIndex, edgeIndex, nodeLabels, edgeLabels storage.Index) *Executor {
	return &Executor{
		Engine:      engine,
		NodeIndex:   nodeIndex,
		EdgeIndex:   edgeIndex,
		NodeLabels:  nodeLabels,
		EdgeLabels:  edgeLabels,
		Optimizer:   NewOptimizer(),
		maxPathHops: 5, // Default maximum path hops
	}
}

// SetMaxPathHops sets the maximum number of hops for path queries
func (e *Executor) SetMaxPathHops(hops int) {
	if hops > 0 {
		e.maxPathHops = hops
	}
}

// Execute executes a query and returns the result
func (e *Executor) Execute(query *Query) (*Result, error) {
	// Create optimized query plan
	plan, err := e.Optimizer.Optimize(query)
	if err != nil {
		return nil, fmt.Errorf("optimization error: %w", err)
	}

	// Execute the query plan
	switch plan.Type {
	case QueryTypeFindNodesByLabel:
		return e.executeNodesByLabel(plan)
	case QueryTypeFindEdgesByLabel:
		return e.executeEdgesByLabel(plan)
	case QueryTypeFindNeighbors:
		return e.executeNeighbors(plan)
	case QueryTypeFindPath:
		return e.executePath(plan)
	default:
		return nil, fmt.Errorf("unsupported query type: %s", plan.Type)
	}
}

// executeNodesByLabel finds nodes by label
func (e *Executor) executeNodesByLabel(query *Query) (*Result, error) {
	label := query.Parameters[ParamLabel]

	// Get node IDs for the label
	key := []byte(label)
	nodeIDsBytes, err := e.NodeLabels.Get(key)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			// No nodes with this label
			return &Result{Nodes: []model.Node{}}, nil
		}
		return nil, fmt.Errorf("error getting nodes for label %s: %w", label, err)
	}

	// Deserialize node IDs
	var nodeIDs []uint64
	err = model.Deserialize(nodeIDsBytes, &nodeIDs)
	if err != nil {
		return nil, fmt.Errorf("error deserializing node IDs: %w", err)
	}

	// Get nodes
	nodes := make([]model.Node, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		key := []byte(fmt.Sprintf("node:%d", id))
		nodeBytes, err := e.NodeIndex.Get(key)
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

		nodes = append(nodes, node)
	}

	return &Result{Nodes: nodes}, nil
}

// executeEdgesByLabel finds edges by label
func (e *Executor) executeEdgesByLabel(query *Query) (*Result, error) {
	label := query.Parameters[ParamLabel]

	// Get edge IDs for the label
	key := []byte(label)
	edgeIDsBytes, err := e.EdgeLabels.Get(key)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			// No edges with this label
			return &Result{Edges: []model.Edge{}}, nil
		}
		return nil, fmt.Errorf("error getting edges for label %s: %w", label, err)
	}

	// Deserialize edge IDs
	var edgeIDs []string
	err = model.Deserialize(edgeIDsBytes, &edgeIDs)
	if err != nil {
		return nil, fmt.Errorf("error deserializing edge IDs: %w", err)
	}

	// Get edges
	edges := make([]model.Edge, 0, len(edgeIDs))
	for _, id := range edgeIDs {
		key := []byte(fmt.Sprintf("edge:%s", id))
		edgeBytes, err := e.EdgeIndex.Get(key)
		if err != nil {
			if err != storage.ErrKeyNotFound {
				return nil, fmt.Errorf("error getting edge %s: %w", id, err)
			}
			continue
		}

		var edge model.Edge
		err = model.Deserialize(edgeBytes, &edge)
		if err != nil {
			return nil, fmt.Errorf("error deserializing edge %s: %w", id, err)
		}

		edges = append(edges, edge)
	}

	return &Result{Edges: edges}, nil
}

// executeNeighbors finds neighbors of a node
func (e *Executor) executeNeighbors(query *Query) (*Result, error) {
	nodeIDStr := query.Parameters[ParamNodeID]
	nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid node ID: %s", nodeIDStr)
	}

	direction := DirectionBoth
	if dir, ok := query.Parameters[ParamDirection]; ok {
		direction = dir
	}

	// Get the node first to ensure it exists
	key := []byte(fmt.Sprintf("node:%d", nodeID))
	nodeBytes, err := e.NodeIndex.Get(key)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, fmt.Errorf("node not found: %d", nodeID)
		}
		return nil, fmt.Errorf("error getting node %d: %w", nodeID, err)
	}

	var node model.Node
	err = model.Deserialize(nodeBytes, &node)
	if err != nil {
		return nil, fmt.Errorf("error deserializing node %d: %w", nodeID, err)
	}

	// Find all edges where this node is the source or target
	var edgeIDs []string
	if direction == DirectionOutgoing || direction == DirectionBoth {
		// Get outgoing edges
		outKey := []byte(fmt.Sprintf("outgoing:%d", nodeID))
		outEdgeIDsBytes, err := e.EdgeIndex.Get(outKey)
		if err != nil && err != storage.ErrKeyNotFound {
			return nil, fmt.Errorf("error getting outgoing edges for node %d: %w", nodeID, err)
		}

		if err == nil {
			var outEdgeIDs []string
			err = model.Deserialize(outEdgeIDsBytes, &outEdgeIDs)
			if err != nil {
				return nil, fmt.Errorf("error deserializing outgoing edge IDs: %w", err)
			}
			edgeIDs = append(edgeIDs, outEdgeIDs...)
		}
	}

	if direction == DirectionIncoming || direction == DirectionBoth {
		// Get incoming edges
		inKey := []byte(fmt.Sprintf("incoming:%d", nodeID))
		inEdgeIDsBytes, err := e.EdgeIndex.Get(inKey)
		if err != nil && err != storage.ErrKeyNotFound {
			return nil, fmt.Errorf("error getting incoming edges for node %d: %w", nodeID, err)
		}

		if err == nil {
			var inEdgeIDs []string
			err = model.Deserialize(inEdgeIDsBytes, &inEdgeIDs)
			if err != nil {
				return nil, fmt.Errorf("error deserializing incoming edge IDs: %w", err)
			}
			edgeIDs = append(edgeIDs, inEdgeIDs...)
		}
	}

	// Get the edges
	edges := make([]model.Edge, 0, len(edgeIDs))
	nodeIDs := make(map[uint64]bool) // Unique neighbor IDs

	for _, id := range edgeIDs {
		edgeKey := []byte(fmt.Sprintf("edge:%s", id))
		edgeBytes, err := e.EdgeIndex.Get(edgeKey)
		if err != nil {
			if err != storage.ErrKeyNotFound {
				return nil, fmt.Errorf("error getting edge %s: %w", id, err)
			}
			continue
		}

		var edge model.Edge
		err = model.Deserialize(edgeBytes, &edge)
		if err != nil {
			return nil, fmt.Errorf("error deserializing edge %s: %w", id, err)
		}

		edges = append(edges, edge)

		// Add neighbor node ID
		if edge.SourceID == nodeID {
			nodeIDs[edge.TargetID] = true
		} else if edge.TargetID == nodeID {
			nodeIDs[edge.SourceID] = true
		}
	}

	// Get the neighbor nodes
	nodes := make([]model.Node, 0, len(nodeIDs))
	for id := range nodeIDs {
		nodeKey := []byte(fmt.Sprintf("node:%d", id))
		nodeBytes, err := e.NodeIndex.Get(nodeKey)
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

		nodes = append(nodes, node)
	}

	return &Result{Nodes: nodes, Edges: edges}, nil
}

// executePath finds a path between two nodes
func (e *Executor) executePath(query *Query) (*Result, error) {
	sourceIDStr := query.Parameters[ParamSourceID]
	sourceID, err := strconv.ParseUint(sourceIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid source node ID: %s", sourceIDStr)
	}

	targetIDStr := query.Parameters[ParamTargetID]
	targetID, err := strconv.ParseUint(targetIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid target node ID: %s", targetIDStr)
	}

	maxHops := e.maxPathHops
	if maxHopsStr, ok := query.Parameters[ParamMaxHops]; ok {
		maxHopsParsed, err := strconv.Atoi(maxHopsStr)
		if err == nil && maxHopsParsed > 0 {
			maxHops = maxHopsParsed
		}
	}

	// Get the source and target nodes first to ensure they exist
	sourceKey := []byte(fmt.Sprintf("node:%d", sourceID))
	sourceBytes, err := e.NodeIndex.Get(sourceKey)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, fmt.Errorf("source node not found: %d", sourceID)
		}
		return nil, fmt.Errorf("error getting source node %d: %w", sourceID, err)
	}

	var sourceNode model.Node
	err = model.Deserialize(sourceBytes, &sourceNode)
	if err != nil {
		return nil, fmt.Errorf("error deserializing source node %d: %w", sourceID, err)
	}

	targetKey := []byte(fmt.Sprintf("node:%d", targetID))
	targetBytes, err := e.NodeIndex.Get(targetKey)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, fmt.Errorf("target node not found: %d", targetID)
		}
		return nil, fmt.Errorf("error getting target node %d: %w", targetID, err)
	}

	var targetNode model.Node
	err = model.Deserialize(targetBytes, &targetNode)
	if err != nil {
		return nil, fmt.Errorf("error deserializing target node %d: %w", targetID, err)
	}

	// Perform breadth-first search to find a path
	path, err := e.findPathBFS(sourceID, targetID, maxHops)
	if err != nil {
		return nil, err
	}

	if path == nil {
		// No path found
		return &Result{Paths: []Path{}}, nil
	}

	return &Result{Paths: []Path{*path}}, nil
}

// findPathBFS finds a path between two nodes using breadth-first search
func (e *Executor) findPathBFS(sourceID, targetID uint64, maxHops int) (*Path, error) {
	if sourceID == targetID {
		// Source and target are the same node
		sourceKey := []byte(fmt.Sprintf("node:%d", sourceID))
		sourceBytes, err := e.NodeIndex.Get(sourceKey)
		if err != nil {
			return nil, fmt.Errorf("error getting node %d: %w", sourceID, err)
		}

		var sourceNode model.Node
		err = model.Deserialize(sourceBytes, &sourceNode)
		if err != nil {
			return nil, fmt.Errorf("error deserializing node %d: %w", sourceID, err)
		}

		return &Path{
			Nodes: []model.Node{sourceNode},
			Edges: []model.Edge{},
		}, nil
	}

	// Keep track of visited nodes to avoid cycles
	visited := make(map[uint64]bool)
	visited[sourceID] = true

	// Keep track of the parent node and edge for each node in the path
	parents := make(map[uint64]struct {
		NodeID uint64
		Edge   model.Edge
	})

	// Queue for BFS
	queue := []uint64{sourceID}
	found := false

	// BFS
	for i := 0; i < maxHops && len(queue) > 0 && !found; i++ {
		levelSize := len(queue)

		for j := 0; j < levelSize; j++ {
			currentID := queue[0]
			queue = queue[1:]

			// Get outgoing edges
			outKey := []byte(fmt.Sprintf("outgoing:%d", currentID))
			outEdgeIDsBytes, err := e.EdgeIndex.Get(outKey)
			if err != nil && err != storage.ErrKeyNotFound {
				return nil, fmt.Errorf("error getting outgoing edges for node %d: %w", currentID, err)
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
					edgeBytes, err := e.EdgeIndex.Get(edgeKey)
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

					// Skip if target node is already visited
					if visited[edge.TargetID] {
						continue
					}

					// Record the parent
					parents[edge.TargetID] = struct {
						NodeID uint64
						Edge   model.Edge
					}{
						NodeID: currentID,
						Edge:   edge,
					}

					// Check if we reached the target node
					if edge.TargetID == targetID {
						found = true
						break
					}

					// Mark target as visited and add to queue
					visited[edge.TargetID] = true
					queue = append(queue, edge.TargetID)
				}
			}

			if found {
				break
			}

			// Get incoming edges
			inKey := []byte(fmt.Sprintf("incoming:%d", currentID))
			inEdgeIDsBytes, err := e.EdgeIndex.Get(inKey)
			if err != nil && err != storage.ErrKeyNotFound {
				return nil, fmt.Errorf("error getting incoming edges for node %d: %w", currentID, err)
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
					edgeBytes, err := e.EdgeIndex.Get(edgeKey)
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

					// Skip if source node is already visited
					if visited[edge.SourceID] {
						continue
					}

					// Record the parent
					parents[edge.SourceID] = struct {
						NodeID uint64
						Edge   model.Edge
					}{
						NodeID: currentID,
						Edge:   edge,
					}

					// Check if we reached the target node
					if edge.SourceID == targetID {
						found = true
						break
					}

					// Mark source as visited and add to queue
					visited[edge.SourceID] = true
					queue = append(queue, edge.SourceID)
				}
			}

			if found {
				break
			}
		}
	}

	if !found {
		// No path found
		return nil, nil
	}

	// Reconstruct the path
	path := &Path{
		Nodes: []model.Node{},
		Edges: []model.Edge{},
	}

	// Start from the target and work backwards
	currentID := targetID
	for currentID != sourceID {
		// Get the node
		nodeKey := []byte(fmt.Sprintf("node:%d", currentID))
		nodeBytes, err := e.NodeIndex.Get(nodeKey)
		if err != nil {
			return nil, fmt.Errorf("error getting node %d: %w", currentID, err)
		}

		var node model.Node
		err = model.Deserialize(nodeBytes, &node)
		if err != nil {
			return nil, fmt.Errorf("error deserializing node %d: %w", currentID, err)
		}

		// Add node to the path
		path.Nodes = append([]model.Node{node}, path.Nodes...)

		// Add edge to the path
		parent := parents[currentID]
		path.Edges = append([]model.Edge{parent.Edge}, path.Edges...)

		// Move to the parent node
		currentID = parent.NodeID
	}

	// Add the source node at the beginning
	sourceNodeKey := []byte(fmt.Sprintf("node:%d", sourceID))
	sourceNodeBytes, err := e.NodeIndex.Get(sourceNodeKey)
	if err != nil {
		return nil, fmt.Errorf("error getting node %d: %w", sourceID, err)
	}

	var sourceNode model.Node
	err = model.Deserialize(sourceNodeBytes, &sourceNode)
	if err != nil {
		return nil, fmt.Errorf("error deserializing node %d: %w", sourceID, err)
	}

	path.Nodes = append([]model.Node{sourceNode}, path.Nodes...)

	return path, nil
}
