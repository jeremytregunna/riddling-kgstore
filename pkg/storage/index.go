package storage

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// IndexCache is a generic cache for index results
type IndexCache struct {
	mu        sync.RWMutex
	items     map[string][][]byte
	maxSize   int
	hitCount  int
	missCount int
}

// NewIndexCache creates a new cache for index results
func NewIndexCache(maxSize int) *IndexCache {
	return &IndexCache{
		items:   make(map[string][][]byte),
		maxSize: maxSize,
	}
}

// Get retrieves cached IDs for a key
func (c *IndexCache) Get(key []byte) ([][]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ids, ok := c.items[string(key)]
	if ok {
		c.hitCount++
	} else {
		c.missCount++
	}
	return ids, ok
}

// Put adds IDs for a key to the cache
func (c *IndexCache) Put(key []byte, ids [][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Simple eviction strategy if we reach max size
	if len(c.items) >= c.maxSize {
		// Remove a random item (we could implement LRU in the future)
		for k := range c.items {
			delete(c.items, k)
			break
		}
	}

	c.items[string(key)] = ids
}

// Invalidate removes a key from the cache
func (c *IndexCache) Invalidate(key []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, string(key))
}

// Clear removes all items from the cache
func (c *IndexCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string][][]byte)
}

// GetStats returns cache statistics
func (c *IndexCache) GetStats() (int, int, int) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.items), c.hitCount, c.missCount
}

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

	// Secondary index for Property -> List of Node/Edge IDs
	IndexTypePropertyValue IndexType = 5
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

// BaseIndex implements common functionality for all indexes
type BaseIndex struct {
	mu        sync.RWMutex
	storage   *StorageEngine
	isOpen    bool
	logger    model.Logger
	keyPrefix []byte
	indexType IndexType
}

// NewBaseIndex creates a new base index with common functionality
func NewBaseIndex(storage *StorageEngine, logger model.Logger, prefix []byte, indexType IndexType) *BaseIndex {
	if logger == nil {
		logger = model.DefaultLoggerInstance
	}

	return &BaseIndex{
		storage:   storage,
		isOpen:    true,
		logger:    logger,
		keyPrefix: prefix,
		indexType: indexType,
	}
}

// MakeKey creates a key with the index prefix
func (idx *BaseIndex) MakeKey(key []byte) []byte {
	return model.SerializeKeyPrefix(idx.keyPrefix, key)
}

// IsOpen returns whether the index is open
func (idx *BaseIndex) IsOpen() bool {
	return idx.isOpen
}

// GetIndexType returns the type of the index
func (idx *BaseIndex) GetIndexType() IndexType {
	return idx.indexType
}

// Close closes the index
func (idx *BaseIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return nil
	}

	idx.isOpen = false
	idx.logger.Info("Closed index of type %d", idx.indexType)
	return nil
}

// Flush flushes the index to the underlying storage
func (idx *BaseIndex) Flush() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	err := idx.storage.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush index: %w", err)
	}

	idx.logger.Info("Flushed index of type %d", idx.indexType)
	return nil
}

// SerializeIDs serializes a list of IDs using a standardized format
// This can be used for node IDs, edge IDs, or other list serialization
func SerializeIDs(ids [][]byte) ([]byte, error) {
	return model.SerializeIDs(ids)
}

// DeserializeIDs deserializes a list of IDs using the standardized format
func DeserializeIDs(data []byte) ([][]byte, error) {
	return model.DeserializeIDs(data)
}

// nodeIndex implements a primary index for Node ID -> Node data
type nodeIndex struct {
	*BaseIndex
}

// NewNodeIndex creates a new primary index for nodes
func NewNodeIndex(storage *StorageEngine, logger model.Logger) (Index, error) {
	base := NewBaseIndex(storage, logger, []byte("n:"), IndexTypeNodePrimary)
	return &nodeIndex{BaseIndex: base}, nil
}

// Put adds or updates a node ID to node data mapping
func (idx *nodeIndex) Put(nodeID, nodeData []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.MakeKey(nodeID)

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
	key := idx.MakeKey(nodeID)

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
	key := idx.MakeKey(nodeID)

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
	key := idx.MakeKey(nodeID)

	// Check in the underlying storage
	exists, err := idx.storage.Contains(key)
	if err != nil {
		return false, fmt.Errorf("failed to check node index entry: %w", err)
	}

	return exists, nil
}

// GetType returns the type of the index
func (idx *nodeIndex) GetType() IndexType {
	return idx.indexType
}

// nodeLabelIndex implements a secondary index for Node Label -> List of Node IDs
type nodeLabelIndex struct {
	*BaseIndex
}

// NewNodeLabelIndex creates a new secondary index for node labels
func NewNodeLabelIndex(storage *StorageEngine, logger model.Logger) (Index, error) {
	if logger == nil {
		logger = model.DefaultLoggerInstance
	}

	// Use LSM-tree based index if configured
	if storage.useLSMNodeLabelIndex {
		logger.Info("Using LSM-tree based node label index")
		return NewLSMNodeLabelIndex(storage, logger)
	}

	base := NewBaseIndex(storage, logger, []byte("nl:"), IndexTypeNodeLabel)
	return &nodeLabelIndex{BaseIndex: base}, nil
}

// Put adds a node ID to a label's list
func (idx *nodeLabelIndex) Put(label, nodeID []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.MakeKey(label)

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
	data, err := SerializeIDs(nodeIDs)
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
	key := idx.MakeKey(label)

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
	key := idx.MakeKey(label)

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
	key := idx.MakeKey(label)

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
		data, err := SerializeIDs(newNodeIDs)
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
	key := idx.MakeKey(label)

	// Check in the underlying storage
	exists, err := idx.storage.Contains(key)
	if err != nil {
		return false, fmt.Errorf("failed to check node label index entry: %w", err)
	}

	return exists, nil
}

// GetType returns the type of the index
func (idx *nodeLabelIndex) GetType() IndexType {
	return idx.indexType
}

// getNodeIDsLocked retrieves the list of node IDs for a label
// Caller must hold the lock
func (idx *nodeLabelIndex) getNodeIDsLocked(key []byte) ([][]byte, error) {
	data, err := idx.storage.Get(key)
	if err != nil {
		return nil, err
	}

	// Deserialize the list of node IDs
	return DeserializeIDs(data)
}

// edgeLabelIndex implements a secondary index for Edge Label -> List of Edge IDs
type edgeLabelIndex struct {
	*BaseIndex
}

// NewEdgeLabelIndex creates a new secondary index for edge labels
func NewEdgeLabelIndex(storage *StorageEngine, logger model.Logger) (Index, error) {
	base := NewBaseIndex(storage, logger, []byte("el:"), IndexTypeEdgeLabel)
	return &edgeLabelIndex{BaseIndex: base}, nil
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
	key := idx.MakeKey(label)

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
	data, err := SerializeIDs(edgeIDs)
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
	key := idx.MakeKey(label)

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
	key := idx.MakeKey(label)

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
	key := idx.MakeKey(label)

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
		data, err := SerializeIDs(newEdgeIDs)
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
	key := idx.MakeKey(label)

	// Check in the underlying storage
	exists, err := idx.storage.Contains(key)
	if err != nil {
		return false, fmt.Errorf("failed to check edge label index entry: %w", err)
	}

	return exists, nil
}

// GetType returns the type of the index
func (idx *edgeLabelIndex) GetType() IndexType {
	return idx.indexType
}

// getEdgeIDsLocked retrieves the list of edge IDs for a label
// Caller must hold the lock
func (idx *edgeLabelIndex) getEdgeIDsLocked(key []byte) ([][]byte, error) {
	data, err := idx.storage.Get(key)
	if err != nil {
		return nil, err
	}

	// Deserialize the list of edge IDs
	return DeserializeIDs(data)
}
