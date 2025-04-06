package storage

import (
	"bytes"
	"errors"
	"fmt"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// lsmNodeLabelIndex implements a secondary index for Node Label -> List of Node IDs
// using an LSM-tree based structure for better performance
type lsmNodeLabelIndex struct {
	BaseIndex
	cache *IndexCache
}

// NewLSMNodeLabelIndex creates a new LSM-tree based secondary index for node labels
func NewLSMNodeLabelIndex(storage *StorageEngine, logger model.Logger) (Index, error) {
	base := NewBaseIndex(storage, logger, []byte("nl:"), IndexTypeNodeLabel)
	
	index := &lsmNodeLabelIndex{
		BaseIndex: base,
		cache:     NewIndexCache(1000), // Cache up to 1000 labels
	}

	return index, nil
}

// makeCompositeKey creates a composite key for the node label index in format nl:{label}:{nodeID}
// This enables efficient range scans by label
func (idx *lsmNodeLabelIndex) makeCompositeKey(label, nodeID []byte) []byte {
	// Create a composite key: nl:{label}:{nodeID}
	// This allows for range scans by label prefix
	key := make([]byte, 0, len(idx.keyPrefix)+len(label)+1+len(nodeID))
	key = append(key, idx.keyPrefix...)
	key = append(key, label...)
	key = append(key, ':')
	key = append(key, nodeID...)
	return key
}

// makeLabelPrefix creates a prefix key for range scans in format nl:{label}:
func (idx *lsmNodeLabelIndex) makeLabelPrefix(label []byte) []byte {
	key := make([]byte, 0, len(idx.keyPrefix)+len(label)+1)
	key = append(key, idx.keyPrefix...)
	key = append(key, label...)
	key = append(key, ':')
	return key
}

// extractNodeID extracts the node ID from a composite key
func (idx *lsmNodeLabelIndex) extractNodeID(key []byte) ([]byte, error) {
	// The key format is nl:{label}:{nodeID}
	// Find the last ':' separator
	lastSep := bytes.LastIndexByte(key, ':')
	if lastSep == -1 || lastSep+1 >= len(key) {
		return nil, errors.New("invalid key format for node label index")
	}
	
	// Return the part after the last ':'
	return key[lastSep+1:], nil
}

// Put adds a node ID to a label's index
func (idx *lsmNodeLabelIndex) Put(label, nodeID []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create a composite key
	key := idx.makeCompositeKey(label, nodeID)

	// Store a simple marker value (1 byte) - the actual data is in the key
	marker := []byte{1}
	
	// Store in the underlying storage
	err := idx.storage.Put(key, marker)
	if err != nil {
		return fmt.Errorf("failed to store node label index entry: %w", err)
	}

	// Invalidate cache for this label
	idx.cache.Invalidate(label)

	idx.logger.Debug("Added node ID %s to label %s index using LSM structure", nodeID, label)
	return nil
}

// Get retrieves the first node ID for a label (not typically used)
func (idx *lsmNodeLabelIndex) Get(label []byte) ([]byte, error) {
	nodeIDs, err := idx.GetAll(label)
	if err != nil {
		return nil, err
	}

	if len(nodeIDs) == 0 {
		return nil, ErrKeyNotFound
	}

	return nodeIDs[0], nil
}

// scanKeysWithPrefix scans all keys with a specific prefix and extracts the node IDs
func (idx *lsmNodeLabelIndex) scanKeysWithPrefix(prefix []byte) ([][]byte, error) {
	// Implementation using prefix-based iteration through the underlying storage
	result := make([][]byte, 0)
	
	// First check the current MemTable
	memTableNodeIDs, err := idx.scanMemTableWithPrefix(idx.storage.memTable, prefix)
	if err != nil {
		return nil, err
	}
	result = append(result, memTableNodeIDs...)
	
	// Then check immutable MemTables
	for i := len(idx.storage.immMemTables) - 1; i >= 0; i-- {
		memTableNodeIDs, err := idx.scanMemTableWithPrefix(idx.storage.immMemTables[i], prefix)
		if err != nil {
			return nil, err
		}
		result = append(result, memTableNodeIDs...)
	}
	
	// Finally check SSTables (newest to oldest)
	for i := len(idx.storage.sstables) - 1; i >= 0; i-- {
		sstableNodeIDs, err := idx.scanSSTableWithPrefix(idx.storage.sstables[i], prefix)
		if err != nil {
			return nil, err
		}
		result = append(result, sstableNodeIDs...)
	}
	
	// Remove duplicates (nodeIDs might appear in multiple layers)
	return idx.deduplicateNodeIDs(result), nil
}

// scanMemTableWithPrefix scans a MemTable for keys with a specific prefix
func (idx *lsmNodeLabelIndex) scanMemTableWithPrefix(memTable MemTableInterface, prefix []byte) ([][]byte, error) {
	// This implementation assumes we can iterate through the MemTable
	result := make([][]byte, 0)
	
	// For MemTable, use the GetEntries method and filter by prefix
	entries := memTable.GetEntries()
	
	// Process entries in pairs (key, value)
	for i := 0; i < len(entries); i += 2 {
		key := entries[i]
		
		// Check if this key has our prefix
		if bytes.HasPrefix(key, prefix) {
			// Extract the node ID from the key
			nodeID, err := idx.extractNodeID(key)
			if err != nil {
				continue // Skip invalid keys
			}
			
			// Get the value to check if it's a tombstone
			value := entries[i+1]
			if len(value) > 0 && value[0] == 1 { // Not a tombstone
				result = append(result, nodeID)
			}
		}
	}
	
	return result, nil
}

// scanSSTableWithPrefix scans an SSTable for keys with a specific prefix
func (idx *lsmNodeLabelIndex) scanSSTableWithPrefix(sstable *SSTable, prefix []byte) ([][]byte, error) {
	result := make([][]byte, 0)
	
	// Create an iterator for the SSTable
	opts := DefaultIteratorOptions()
	opts.IncludeTombstones = false
	
	iter, err := sstable.IteratorWithOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable iterator: %w", err)
	}
	defer iter.Close()
	
	// Position at the first key with our prefix
	err = iter.Seek(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to prefix: %w", err)
	}
	
	// Iterate through matching keys
	for iter.Valid() {
		key := iter.Key()
		
		// Check if we've moved past keys with our prefix
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		
		// Extract the node ID from the key
		nodeID, err := idx.extractNodeID(key)
		if err == nil {
			result = append(result, nodeID)
		}
		
		// Move to next entry
		if err := iter.Next(); err != nil {
			break
		}
	}
	
	return result, nil
}

// deduplicateNodeIDs removes duplicate node IDs from a list
func (idx *lsmNodeLabelIndex) deduplicateNodeIDs(nodeIDs [][]byte) [][]byte {
	if len(nodeIDs) <= 1 {
		return nodeIDs
	}
	
	// Use a map for deduplication
	unique := make(map[string]struct{})
	result := make([][]byte, 0, len(nodeIDs))
	
	for _, id := range nodeIDs {
		idStr := string(id)
		if _, exists := unique[idStr]; !exists {
			unique[idStr] = struct{}{}
			result = append(result, id)
		}
	}
	
	return result
}

// GetAll retrieves all node IDs for a label
func (idx *lsmNodeLabelIndex) GetAll(label []byte) ([][]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return nil, ErrIndexClosed
	}

	// Check cache first
	if cached, ok := idx.cache.Get(label); ok {
		return cached, nil
	}

	// Create a prefix for range scan
	prefix := idx.makeLabelPrefix(label)
	
	// Scan for all keys with this prefix
	nodeIDs, err := idx.scanKeysWithPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to scan for node IDs: %w", err)
	}
	
	// Cache the results
	idx.cache.Put(label, nodeIDs)

	return nodeIDs, nil
}

// Delete removes all node IDs for a label
func (idx *lsmNodeLabelIndex) Delete(label []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Get all existing node IDs for this label
	prefix := idx.makeLabelPrefix(label)
	nodeIDs, err := idx.scanKeysWithPrefix(prefix)
	if err != nil {
		return fmt.Errorf("failed to scan for node IDs: %w", err)
	}
	
	// Delete each node ID entry
	for _, nodeID := range nodeIDs {
		key := idx.makeCompositeKey(label, nodeID)
		err := idx.storage.Delete(key)
		if err != nil {
			return fmt.Errorf("failed to delete node label index entry: %w", err)
		}
	}

	// Invalidate cache
	idx.cache.Invalidate(label)

	idx.logger.Debug("Deleted %d node IDs for label %s from index", len(nodeIDs), label)
	return nil
}

// DeleteValue removes a specific node ID from a label's list
func (idx *lsmNodeLabelIndex) DeleteValue(label, nodeID []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Create the composite key
	key := idx.makeCompositeKey(label, nodeID)
	
	// Delete from the underlying storage
	err := idx.storage.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete node ID from label index: %w", err)
	}

	// Invalidate cache
	idx.cache.Invalidate(label)

	idx.logger.Debug("Removed node ID %s from label %s index", nodeID, label)
	return nil
}

// Contains checks if a label exists in the index
func (idx *lsmNodeLabelIndex) Contains(label []byte) (bool, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return false, ErrIndexClosed
	}

	// Check cache first for performance
	if cached, ok := idx.cache.Get(label); ok {
		return len(cached) > 0, nil
	}

	// Create a prefix for range scan
	prefix := idx.makeLabelPrefix(label)
	
	// We just need to find one key with this prefix
	// First check the current MemTable
	nodeIDs, err := idx.scanMemTableWithPrefix(idx.storage.memTable, prefix)
	if err == nil && len(nodeIDs) > 0 {
		return true, nil
	}
	
	// Then check immutable MemTables
	for i := len(idx.storage.immMemTables) - 1; i >= 0; i-- {
		nodeIDs, err := idx.scanMemTableWithPrefix(idx.storage.immMemTables[i], prefix)
		if err == nil && len(nodeIDs) > 0 {
			return true, nil
		}
	}
	
	// Finally check SSTables (newest to oldest)
	for i := len(idx.storage.sstables) - 1; i >= 0; i-- {
		sstable := idx.storage.sstables[i]
		
		// Create an iterator for the SSTable
		iter, err := sstable.Iterator()
		if err != nil {
			continue
		}
		
		// Try to find at least one key with our prefix
		err = iter.Seek(prefix)
		if err == nil && iter.Valid() {
			key := iter.Key()
			if bytes.HasPrefix(key, prefix) {
				iter.Close()
				return true, nil
			}
		}
		
		iter.Close()
	}

	return false, nil
}

// Close closes the index
func (idx *lsmNodeLabelIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return nil
	}

	idx.isOpen = false
	idx.cache.Clear()
	idx.logger.Info("Closed LSM-based node label index")
	return nil
}

// Flush flushes the index to the underlying storage
func (idx *lsmNodeLabelIndex) Flush() error {
	return idx.BaseIndex.Flush()
}

// GetType returns the type of the index
func (idx *lsmNodeLabelIndex) GetType() IndexType {
	return idx.indexType
}