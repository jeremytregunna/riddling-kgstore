package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// Index errors
var (
	ErrIndexClosed = errors.New("index is closed")
)

// IndexType represents the type of index
type IndexType uint8

const (
	// Primary index for Node ID -> Node data
	IndexTypeNodePrimary IndexType = 1

	// Secondary index for Node Label -> List of Node IDs
	IndexTypeNodeLabel IndexType = 2

	// Secondary index for Edge Label -> List of Edge IDs
	IndexTypeEdgeLabel IndexType = 3

	// Primary index for Edge ID -> Edge data
	IndexTypeEdgePrimary IndexType = 4
)

// Index is a generic interface for different index types
type Index interface {
	// Put adds or updates a key-value pair in the index
	Put(key, value []byte) error

	// Get retrieves a value by key from the index
	Get(key []byte) ([]byte, error)

	// GetAll retrieves all values for a key (for multi-value indexes)
	GetAll(key []byte) ([][]byte, error)

	// Delete removes a key from the index
	Delete(key []byte) error

	// DeleteValue removes a specific key-value pair from the index
	DeleteValue(key, value []byte) error

	// Contains checks if a key exists in the index
	Contains(key []byte) (bool, error)

	// Close closes the index
	Close() error

	// Flush flushes the index to the underlying storage
	Flush() error

	// GetType returns the type of the index
	GetType() IndexType
}

// nodeIndex implements a primary index for Node ID -> Node data
type nodeIndex struct {
	mu        sync.RWMutex
	storage   *StorageEngine
	isOpen    bool
	logger    model.Logger
	keyPrefix []byte // Prefix for node index keys
}

// NewNodeIndex creates a new primary index for nodes
func NewNodeIndex(storage *StorageEngine, logger model.Logger) (Index, error) {
	if logger == nil {
		logger = model.DefaultLoggerInstance
	}

	index := &nodeIndex{
		storage:   storage,
		isOpen:    true,
		logger:    logger,
		keyPrefix: []byte("n:"), // Prefix for node index keys
	}

	return index, nil
}

// Put adds or updates a node ID to node data mapping
func (idx *nodeIndex) Put(nodeID, nodeData []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(nodeID)

	// Store in the underlying storage
	err := idx.storage.Put(key, nodeData)
	if err != nil {
		return fmt.Errorf("failed to store node index entry: %w", err)
	}

	idx.logger.Debug("Added node index entry for node ID %s", nodeID)
	return nil
}

// Get retrieves node data by node ID
func (idx *nodeIndex) Get(nodeID []byte) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return nil, ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(nodeID)

	// Get from the underlying storage
	data, err := idx.storage.Get(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("failed to get node index entry: %w", err)
	}

	return data, nil
}

// GetAll is not applicable to the primary node index (one node per ID)
func (idx *nodeIndex) GetAll(nodeID []byte) ([][]byte, error) {
	// For primary index, there's only one value per key
	data, err := idx.Get(nodeID)
	if err != nil {
		return nil, err
	}
	return [][]byte{data}, nil
}

// Delete removes a node from the index
func (idx *nodeIndex) Delete(nodeID []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(nodeID)

	// Delete from the underlying storage
	err := idx.storage.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete node index entry: %w", err)
	}

	idx.logger.Debug("Deleted node index entry for node ID %s", nodeID)
	return nil
}

// DeleteValue is not applicable to the primary node index (can use Delete instead)
func (idx *nodeIndex) DeleteValue(nodeID, _ []byte) error {
	return idx.Delete(nodeID)
}

// Contains checks if a node ID exists in the index
func (idx *nodeIndex) Contains(nodeID []byte) (bool, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return false, ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(nodeID)

	// Check in the underlying storage
	exists, err := idx.storage.Contains(key)
	if err != nil {
		return false, fmt.Errorf("failed to check node index entry: %w", err)
	}

	return exists, nil
}

// Close closes the index
func (idx *nodeIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return nil
	}

	idx.isOpen = false
	idx.logger.Info("Closed node primary index")
	return nil
}

// Flush flushes the index to the underlying storage
func (idx *nodeIndex) Flush() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	err := idx.storage.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush node index: %w", err)
	}

	idx.logger.Info("Flushed node primary index")
	return nil
}

// GetType returns the type of the index
func (idx *nodeIndex) GetType() IndexType {
	return IndexTypeNodePrimary
}

// makeKey creates a key for the node index with the prefix
func (idx *nodeIndex) makeKey(nodeID []byte) []byte {
	return append(idx.keyPrefix, nodeID...)
}

// nodeLabelIndex implements a secondary index for Node Label -> List of Node IDs
type nodeLabelIndex struct {
	mu        sync.RWMutex
	storage   *StorageEngine
	isOpen    bool
	logger    model.Logger
	keyPrefix []byte // Prefix for node label index keys
}

// NewNodeLabelIndex creates a new secondary index for node labels
func NewNodeLabelIndex(storage *StorageEngine, logger model.Logger) (Index, error) {
	if logger == nil {
		logger = model.DefaultLoggerInstance
	}

	index := &nodeLabelIndex{
		storage:   storage,
		isOpen:    true,
		logger:    logger,
		keyPrefix: []byte("nl:"), // Prefix for node label index keys
	}

	return index, nil
}

// Put adds a node ID to a label's list
func (idx *nodeLabelIndex) Put(label, nodeID []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(label)

	// Get the current list of node IDs for this label
	nodeIDs, err := idx.getNodeIDsLocked(key)
	if err != nil && err != ErrKeyNotFound {
		return fmt.Errorf("failed to get node IDs for label: %w", err)
	}

	// Add the new node ID if it doesn't already exist
	found := false
	for _, id := range nodeIDs {
		if bytes.Equal(id, nodeID) {
			found = true
			break
		}
	}

	if !found {
		nodeIDs = append(nodeIDs, nodeID)
	} else {
		// If the node ID already exists, we don't need to update anything
		return nil
	}

	// Serialize the list of node IDs
	data, err := idx.serializeNodeIDs(nodeIDs)
	if err != nil {
		return fmt.Errorf("failed to serialize node IDs: %w", err)
	}

	// Store in the underlying storage
	err = idx.storage.Put(key, data)
	if err != nil {
		return fmt.Errorf("failed to store node label index entry: %w", err)
	}

	idx.logger.Debug("Added node ID %s to label %s index", nodeID, label)
	return nil
}

// Get retrieves the first node ID for a label (not typically used)
func (idx *nodeLabelIndex) Get(label []byte) ([]byte, error) {
	nodeIDs, err := idx.GetAll(label)
	if err != nil {
		return nil, err
	}

	if len(nodeIDs) == 0 {
		return nil, ErrKeyNotFound
	}

	return nodeIDs[0], nil
}

// GetAll retrieves all node IDs for a label
func (idx *nodeLabelIndex) GetAll(label []byte) ([][]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return nil, ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(label)

	// Get the node IDs from the storage
	nodeIDs, err := idx.getNodeIDsLocked(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return [][]byte{}, nil
		}
		return nil, fmt.Errorf("failed to get node IDs for label: %w", err)
	}

	return nodeIDs, nil
}

// Delete removes all node IDs for a label
func (idx *nodeLabelIndex) Delete(label []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(label)

	// Delete from the underlying storage
	err := idx.storage.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete node label index entry: %w", err)
	}

	idx.logger.Debug("Deleted node label index entry for label %s", label)
	return nil
}

// DeleteValue removes a specific node ID from a label's list
func (idx *nodeLabelIndex) DeleteValue(label, nodeID []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(label)

	// Get the current list of node IDs for this label
	nodeIDs, err := idx.getNodeIDsLocked(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return nil // Nothing to delete
		}
		return fmt.Errorf("failed to get node IDs for label: %w", err)
	}

	// Find and remove the node ID
	found := false
	newNodeIDs := make([][]byte, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		if bytes.Equal(id, nodeID) {
			found = true
		} else {
			newNodeIDs = append(newNodeIDs, id)
		}
	}

	if !found {
		return nil // Nothing to delete
	}

	// If there are no more node IDs, delete the key
	if len(newNodeIDs) == 0 {
		err = idx.storage.Delete(key)
		if err != nil {
			return fmt.Errorf("failed to delete node label index entry: %w", err)
		}
	} else {
		// Serialize the updated list of node IDs
		data, err := idx.serializeNodeIDs(newNodeIDs)
		if err != nil {
			return fmt.Errorf("failed to serialize node IDs: %w", err)
		}

		// Store in the underlying storage
		err = idx.storage.Put(key, data)
		if err != nil {
			return fmt.Errorf("failed to update node label index entry: %w", err)
		}
	}

	idx.logger.Debug("Removed node ID %s from label %s index", nodeID, label)
	return nil
}

// Contains checks if a label exists in the index
func (idx *nodeLabelIndex) Contains(label []byte) (bool, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return false, ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(label)

	// Check in the underlying storage
	exists, err := idx.storage.Contains(key)
	if err != nil {
		return false, fmt.Errorf("failed to check node label index entry: %w", err)
	}

	return exists, nil
}

// Close closes the index
func (idx *nodeLabelIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return nil
	}

	idx.isOpen = false
	idx.logger.Info("Closed node label index")
	return nil
}

// Flush flushes the index to the underlying storage
func (idx *nodeLabelIndex) Flush() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	err := idx.storage.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush node label index: %w", err)
	}

	idx.logger.Info("Flushed node label index")
	return nil
}

// GetType returns the type of the index
func (idx *nodeLabelIndex) GetType() IndexType {
	return IndexTypeNodeLabel
}

// makeKey creates a key for the node label index with the prefix
func (idx *nodeLabelIndex) makeKey(label []byte) []byte {
	return append(idx.keyPrefix, label...)
}

// getNodeIDsLocked retrieves the list of node IDs for a label
// Caller must hold the lock
func (idx *nodeLabelIndex) getNodeIDsLocked(key []byte) ([][]byte, error) {
	data, err := idx.storage.Get(key)
	if err != nil {
		return nil, err
	}

	// Deserialize the list of node IDs
	return idx.deserializeNodeIDs(data)
}

// serializeNodeIDs serializes a list of node IDs
func (idx *nodeLabelIndex) serializeNodeIDs(nodeIDs [][]byte) ([]byte, error) {
	// Calculate total size
	totalSize := 4 // Count (uint32)
	for _, id := range nodeIDs {
		totalSize += 4 + len(id) // Length (uint32) + data
	}

	// Create buffer
	buf := make([]byte, totalSize)
	offset := 0

	// Write count
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(nodeIDs)))
	offset += 4

	// Write node IDs
	for _, id := range nodeIDs {
		// Write length
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(id)))
		offset += 4

		// Write data
		copy(buf[offset:], id)
		offset += len(id)
	}

	return buf, nil
}

// deserializeNodeIDs deserializes a list of node IDs
func (idx *nodeLabelIndex) deserializeNodeIDs(data []byte) ([][]byte, error) {
	if len(data) < 4 {
		return nil, errors.New("invalid node IDs data")
	}

	// Read count
	count := binary.LittleEndian.Uint32(data[0:4])
	offset := 4

	// Read node IDs
	nodeIDs := make([][]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		if offset+4 > len(data) {
			return nil, errors.New("invalid node IDs data")
		}

		// Read length
		length := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(length) > len(data) {
			return nil, errors.New("invalid node IDs data")
		}

		// Read data
		id := make([]byte, length)
		copy(id, data[offset:offset+int(length)])
		offset += int(length)

		nodeIDs = append(nodeIDs, id)
	}

	return nodeIDs, nil
}

// edgeLabelIndex implements a secondary index for Edge Label -> List of Edge IDs
type edgeLabelIndex struct {
	mu        sync.RWMutex
	storage   *StorageEngine
	isOpen    bool
	logger    model.Logger
	keyPrefix []byte // Prefix for edge label index keys
}

// NewEdgeLabelIndex creates a new secondary index for edge labels
func NewEdgeLabelIndex(storage *StorageEngine, logger model.Logger) (Index, error) {
	if logger == nil {
		logger = model.DefaultLoggerInstance
	}

	index := &edgeLabelIndex{
		storage:   storage,
		isOpen:    true,
		logger:    logger,
		keyPrefix: []byte("el:"), // Prefix for edge label index keys
	}

	return index, nil
}

// Put adds an edge ID to a label's list
// For edges, the "ID" is encoded as sourceID-targetID
func (idx *edgeLabelIndex) Put(label, edgeID []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(label)

	// Get the current list of edge IDs for this label
	edgeIDs, err := idx.getEdgeIDsLocked(key)
	if err != nil && err != ErrKeyNotFound {
		return fmt.Errorf("failed to get edge IDs for label: %w", err)
	}

	// Add the new edge ID if it doesn't already exist
	found := false
	for _, id := range edgeIDs {
		if bytes.Equal(id, edgeID) {
			found = true
			break
		}
	}

	if !found {
		edgeIDs = append(edgeIDs, edgeID)
	} else {
		// If the edge ID already exists, we don't need to update anything
		return nil
	}

	// Serialize the list of edge IDs
	data, err := idx.serializeEdgeIDs(edgeIDs)
	if err != nil {
		return fmt.Errorf("failed to serialize edge IDs: %w", err)
	}

	// Store in the underlying storage
	err = idx.storage.Put(key, data)
	if err != nil {
		return fmt.Errorf("failed to store edge label index entry: %w", err)
	}

	idx.logger.Debug("Added edge ID %s to label %s index", edgeID, label)
	return nil
}

// Get retrieves the first edge ID for a label (not typically used)
func (idx *edgeLabelIndex) Get(label []byte) ([]byte, error) {
	edgeIDs, err := idx.GetAll(label)
	if err != nil {
		return nil, err
	}

	if len(edgeIDs) == 0 {
		return nil, ErrKeyNotFound
	}

	return edgeIDs[0], nil
}

// GetAll retrieves all edge IDs for a label
func (idx *edgeLabelIndex) GetAll(label []byte) ([][]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return nil, ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(label)

	// Get the edge IDs from the storage
	edgeIDs, err := idx.getEdgeIDsLocked(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return [][]byte{}, nil
		}
		return nil, fmt.Errorf("failed to get edge IDs for label: %w", err)
	}

	return edgeIDs, nil
}

// Delete removes all edge IDs for a label
func (idx *edgeLabelIndex) Delete(label []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(label)

	// Delete from the underlying storage
	err := idx.storage.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete edge label index entry: %w", err)
	}

	idx.logger.Debug("Deleted edge label index entry for label %s", label)
	return nil
}

// DeleteValue removes a specific edge ID from a label's list
func (idx *edgeLabelIndex) DeleteValue(label, edgeID []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(label)

	// Get the current list of edge IDs for this label
	edgeIDs, err := idx.getEdgeIDsLocked(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return nil // Nothing to delete
		}
		return fmt.Errorf("failed to get edge IDs for label: %w", err)
	}

	// Find and remove the edge ID
	found := false
	newEdgeIDs := make([][]byte, 0, len(edgeIDs))
	for _, id := range edgeIDs {
		if bytes.Equal(id, edgeID) {
			found = true
		} else {
			newEdgeIDs = append(newEdgeIDs, id)
		}
	}

	if !found {
		return nil // Nothing to delete
	}

	// If there are no more edge IDs, delete the key
	if len(newEdgeIDs) == 0 {
		err = idx.storage.Delete(key)
		if err != nil {
			return fmt.Errorf("failed to delete edge label index entry: %w", err)
		}
	} else {
		// Serialize the updated list of edge IDs
		data, err := idx.serializeEdgeIDs(newEdgeIDs)
		if err != nil {
			return fmt.Errorf("failed to serialize edge IDs: %w", err)
		}

		// Store in the underlying storage
		err = idx.storage.Put(key, data)
		if err != nil {
			return fmt.Errorf("failed to update edge label index entry: %w", err)
		}
	}

	idx.logger.Debug("Removed edge ID %s from label %s index", edgeID, label)
	return nil
}

// Contains checks if a label exists in the index
func (idx *edgeLabelIndex) Contains(label []byte) (bool, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return false, ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.makeKey(label)

	// Check in the underlying storage
	exists, err := idx.storage.Contains(key)
	if err != nil {
		return false, fmt.Errorf("failed to check edge label index entry: %w", err)
	}

	return exists, nil
}

// Close closes the index
func (idx *edgeLabelIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return nil
	}

	idx.isOpen = false
	idx.logger.Info("Closed edge label index")
	return nil
}

// Flush flushes the index to the underlying storage
func (idx *edgeLabelIndex) Flush() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	err := idx.storage.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush edge label index: %w", err)
	}

	idx.logger.Info("Flushed edge label index")
	return nil
}

// GetType returns the type of the index
func (idx *edgeLabelIndex) GetType() IndexType {
	return IndexTypeEdgeLabel
}

// makeKey creates a key for the edge label index with the prefix
func (idx *edgeLabelIndex) makeKey(label []byte) []byte {
	return append(idx.keyPrefix, label...)
}

// getEdgeIDsLocked retrieves the list of edge IDs for a label
// Caller must hold the lock
func (idx *edgeLabelIndex) getEdgeIDsLocked(key []byte) ([][]byte, error) {
	data, err := idx.storage.Get(key)
	if err != nil {
		return nil, err
	}

	// Deserialize the list of edge IDs
	return idx.deserializeEdgeIDs(data)
}

// serializeEdgeIDs serializes a list of edge IDs
func (idx *edgeLabelIndex) serializeEdgeIDs(edgeIDs [][]byte) ([]byte, error) {
	// Calculate total size
	totalSize := 4 // Count (uint32)
	for _, id := range edgeIDs {
		totalSize += 4 + len(id) // Length (uint32) + data
	}

	// Create buffer
	buf := make([]byte, totalSize)
	offset := 0

	// Write count
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(edgeIDs)))
	offset += 4

	// Write edge IDs
	for _, id := range edgeIDs {
		// Write length
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(id)))
		offset += 4

		// Write data
		copy(buf[offset:], id)
		offset += len(id)
	}

	return buf, nil
}

// deserializeEdgeIDs deserializes a list of edge IDs
func (idx *edgeLabelIndex) deserializeEdgeIDs(data []byte) ([][]byte, error) {
	if len(data) < 4 {
		return nil, errors.New("invalid edge IDs data")
	}

	// Read count
	count := binary.LittleEndian.Uint32(data[0:4])
	offset := 4

	// Read edge IDs
	edgeIDs := make([][]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		if offset+4 > len(data) {
			return nil, errors.New("invalid edge IDs data")
		}

		// Read length
		length := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(length) > len(data) {
			return nil, errors.New("invalid edge IDs data")
		}

		// Read data
		id := make([]byte, length)
		copy(id, data[offset:offset+int(length)])
		offset += int(length)

		edgeIDs = append(edgeIDs, id)
	}

	return edgeIDs, nil
}
