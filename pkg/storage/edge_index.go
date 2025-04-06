package storage

import (
	"fmt"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// edgeIndex implements a primary index for Edge ID -> Edge data
type edgeIndex struct {
	*BaseIndex
}

// NewEdgeIndex creates a new primary index for edges
func NewEdgeIndex(storage *StorageEngine, logger model.Logger) (Index, error) {
	base := NewBaseIndex(storage, logger, []byte("e:"), IndexTypeEdgePrimary)
	return &edgeIndex{BaseIndex: base}, nil
}

// Put adds or updates an edge ID to edge data mapping
func (idx *edgeIndex) Put(edgeID, edgeData []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.MakeKey(edgeID)

	// Store in the underlying storage
	err := idx.storage.Put(key, edgeData)
	if err != nil {
		return fmt.Errorf("failed to store edge index entry: %w", err)
	}

	idx.logger.Debug("Added edge index entry for edge ID %s", edgeID)
	return nil
}

// Get retrieves edge data by edge ID
func (idx *edgeIndex) Get(edgeID []byte) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return nil, ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.MakeKey(edgeID)

	// Get from the underlying storage
	data, err := idx.storage.Get(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("failed to get edge index entry: %w", err)
	}

	return data, nil
}

// GetAll is not applicable to the primary edge index (one edge per ID)
func (idx *edgeIndex) GetAll(edgeID []byte) ([][]byte, error) {
	// For primary index, there's only one value per key
	data, err := idx.Get(edgeID)
	if err != nil {
		return nil, err
	}
	return [][]byte{data}, nil
}

// Delete removes an edge from the index
func (idx *edgeIndex) Delete(edgeID []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.MakeKey(edgeID)

	// Delete from the underlying storage
	err := idx.storage.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete edge index entry: %w", err)
	}

	idx.logger.Debug("Deleted edge index entry for edge ID %s", edgeID)
	return nil
}

// DeleteValue is not applicable to the primary edge index (can use Delete instead)
func (idx *edgeIndex) DeleteValue(edgeID, _ []byte) error {
	return idx.Delete(edgeID)
}

// Contains checks if an edge ID exists in the index
func (idx *edgeIndex) Contains(edgeID []byte) (bool, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return false, ErrIndexClosed
	}

	// Create a key with the prefix
	key := idx.MakeKey(edgeID)

	// Check in the underlying storage
	exists, err := idx.storage.Contains(key)
	if err != nil {
		return false, fmt.Errorf("failed to check edge index entry: %w", err)
	}

	return exists, nil
}

// GetType returns the type of the index
func (idx *edgeIndex) GetType() IndexType {
	return idx.indexType
}