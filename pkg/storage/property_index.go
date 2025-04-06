package storage

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// Property index constants are defined in index.go

// PropertyIndexType indicates whether the property is for a node or an edge
type PropertyIndexType uint8

const (
	PropertyIndexTypeNode PropertyIndexType = 1
	PropertyIndexTypeEdge PropertyIndexType = 2
)

// PropertyValueType represents the type of property value being indexed
type PropertyValueType uint8

const (
	PropertyValueTypeString  PropertyValueType = 1
	PropertyValueTypeNumeric PropertyValueType = 2
	PropertyValueTypeBoolean PropertyValueType = 3
)

// propertyIndex implements a specialized SSTable format for indexing property values
// It follows an LSM-tree based structure similar to the node label index
type propertyIndex struct {
	BaseIndex
	entityType     PropertyIndexType
	cache          *IndexCache
	fullTextSearch bool // Whether to enable full-text search capabilities
}

// NewNodePropertyIndex creates a new property index for node properties
func NewNodePropertyIndex(storage *StorageEngine, logger model.Logger) (Index, error) {
	return newPropertyIndex(storage, logger, PropertyIndexTypeNode, false)
}

// NewEdgePropertyIndex creates a new property index for edge properties
func NewEdgePropertyIndex(storage *StorageEngine, logger model.Logger) (Index, error) {
	return newPropertyIndex(storage, logger, PropertyIndexTypeEdge, false)
}

// NewFullTextPropertyIndex creates a new property index with full-text search capabilities
func NewFullTextPropertyIndex(storage *StorageEngine, logger model.Logger, entityType PropertyIndexType) (Index, error) {
	return newPropertyIndex(storage, logger, entityType, true)
}

// newPropertyIndex creates a new property index with the specified configuration
func newPropertyIndex(storage *StorageEngine, logger model.Logger, entityType PropertyIndexType, fullTextSearch bool) (Index, error) {
	var entityPrefix string
	if entityType == PropertyIndexTypeNode {
		entityPrefix = "np"
	} else {
		entityPrefix = "ep"
	}

	// Create the base index
	base := NewBaseIndex(storage, logger, []byte(entityPrefix + ":"), IndexTypePropertyValue)

	index := &propertyIndex{
		BaseIndex:      base,
		entityType:     entityType,
		cache:          NewIndexCache(1000), // Cache up to 1000 property queries
		fullTextSearch: fullTextSearch,
	}

	return index, nil
}

// makeKey creates a composite key for the property index
// Format: {prefix}:{propertyName}:{valueType}:{propertyValue}:{entityID}
// This enables efficient range scans by property name and value
func (idx *propertyIndex) makeKey(propertyName, propertyValue, entityID []byte, valueType PropertyValueType) []byte {
	// Create a composite key using the serialization functions
	return model.SerializeCompositeKey(
		idx.keyPrefix,
		propertyName,
		[]byte{byte(valueType)},
		propertyValue,
		entityID,
	)
}

// makePropertyPrefix creates a prefix key for range scans by property name
// Format: {prefix}:{propertyName}:
func (idx *propertyIndex) makePropertyPrefix(propertyName []byte) []byte {
	result := model.SerializeCompositeKey(idx.keyPrefix, propertyName)
	return append(result, ':') // Add trailing colon for prefix scan
}

// makePropertyValuePrefix creates a prefix key for range scans by property name and value
// Format: {prefix}:{propertyName}:{valueType}:{propertyValue}:
func (idx *propertyIndex) makePropertyValuePrefix(propertyName, propertyValue []byte, valueType PropertyValueType) []byte {
	result := model.SerializeCompositeKey(
		idx.keyPrefix,
		propertyName,
		[]byte{byte(valueType)},
		propertyValue,
	)
	return append(result, ':') // Add trailing colon for prefix scan
}

// extractEntityID extracts the entity ID from a composite key
func (idx *propertyIndex) extractEntityID(key []byte) ([]byte, error) {
	// The key format is {prefix}:{propertyName}:{valueType}:{propertyValue}:{entityID}
	parts := model.SplitCompositeKey(key)
	if len(parts) < 5 {
		return nil, errors.New("invalid key format for property index")
	}
	
	// Return the last part (entity ID)
	return parts[len(parts)-1], nil
}

// detectValueType tries to determine the value type based on its content
func (idx *propertyIndex) detectValueType(value []byte) PropertyValueType {
	// Try to parse as numeric (integer or float)
	strVal := string(value)
	if _, err := strconv.ParseInt(strVal, 10, 64); err == nil {
		return PropertyValueTypeNumeric
	}
	if _, err := strconv.ParseFloat(strVal, 64); err == nil {
		return PropertyValueTypeNumeric
	}
	
	// Check for boolean values
	lowerStr := strings.ToLower(strVal)
	if lowerStr == "true" || lowerStr == "false" {
		return PropertyValueTypeBoolean
	}
	
	// Default to string type
	return PropertyValueTypeString
}

// Put adds an entity ID to the property index
// key is the property name, value is the property value, entityID is the entity (node/edge) ID
func (idx *propertyIndex) Put(key, value []byte) error {
	// For the property index, we expect the "key" to be in the format "propertyName|entityID"
	// and the "value" to be the property value
	parts := bytes.Split(key, []byte("|"))
	if len(parts) != 2 {
		return fmt.Errorf("invalid key format for property index: %s", key)
	}
	
	propertyName := parts[0]
	entityID := parts[1]
	propertyValue := value
	
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}

	// Detect the value type
	valueType := idx.detectValueType(propertyValue)
	
	// Create a composite key
	indexKey := idx.makeKey(propertyName, propertyValue, entityID, valueType)

	// Store a simple marker value (1 byte) - the actual data is in the key
	marker := []byte{1}
	
	// Store in the underlying storage
	err := idx.storage.Put(indexKey, marker)
	if err != nil {
		return fmt.Errorf("failed to store property index entry: %w", err)
	}

	// Invalidate cache for this property
	idx.cache.Invalidate(idx.makePropertyPrefix(propertyName))
	idx.cache.Invalidate(idx.makePropertyValuePrefix(propertyName, propertyValue, valueType))

	idx.logger.Debug("Added entity ID %s to property index for %s=%s", entityID, propertyName, propertyValue)
	return nil
}

// Get retrieves the first entity ID for a property name and value
func (idx *propertyIndex) Get(key []byte) ([]byte, error) {
	// For the property index, we expect the "key" to be in the format "propertyName|propertyValue"
	parts := bytes.Split(key, []byte("|"))
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid key format for property index: %s", key)
	}
	
	propertyName := parts[0]
	propertyValue := parts[1]
	
	// Try with all value types (the caller might not know which value type it is)
	for _, valueType := range []PropertyValueType{
		PropertyValueTypeString,
		PropertyValueTypeNumeric,
		PropertyValueTypeBoolean,
	} {
		entityIDs, err := idx.getEntitiesByPropertyValue(propertyName, propertyValue, valueType)
		if err == nil && len(entityIDs) > 0 {
			return entityIDs[0], nil
		}
	}
	
	return nil, ErrKeyNotFound
}

// getEntitiesByPropertyValue retrieves all entity IDs for a property name and value with a specific type
func (idx *propertyIndex) getEntitiesByPropertyValue(propertyName, propertyValue []byte, valueType PropertyValueType) ([][]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return nil, ErrIndexClosed
	}
	
	// Create a prefix for range scan
	prefix := idx.makePropertyValuePrefix(propertyName, propertyValue, valueType)
	
	// Check cache first for this specific query
	if cached, ok := idx.cache.Get(prefix); ok {
		return cached, nil
	}
	
	// Scan for all keys with this prefix
	entityIDs, err := idx.scanKeysWithPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to scan for entity IDs: %w", err)
	}
	
	// Cache the results
	idx.cache.Put(prefix, entityIDs)

	return entityIDs, nil
}

// GetAll retrieves all entity IDs for a property name (with any value)
func (idx *propertyIndex) GetAll(key []byte) ([][]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return nil, ErrIndexClosed
	}
	
	// Check if the key contains a value filter
	parts := bytes.Split(key, []byte("|"))
	if len(parts) == 2 {
		// If it's a name+value query, delegate to Get and wrap the result
		idx.mu.RUnlock() // Unlock before calling Get
		result, err := idx.Get(key)
		idx.mu.RLock() // Lock again
		
		if err != nil {
			return [][]byte{}, nil
		}
		return [][]byte{result}, nil
	}
	
	// Otherwise, we're looking for all entities with a specific property name
	propertyName := key
	
	// Check cache first
	prefix := idx.makePropertyPrefix(propertyName)
	if cached, ok := idx.cache.Get(prefix); ok {
		return cached, nil
	}
	
	// Scan for all keys with this prefix
	entityIDs, err := idx.scanKeysWithPrefix(prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to scan for entity IDs: %w", err)
	}
	
	// Cache the results
	idx.cache.Put(prefix, entityIDs)

	return entityIDs, nil
}

// scanKeysWithPrefix scans all keys with a specific prefix and extracts the entity IDs
func (idx *propertyIndex) scanKeysWithPrefix(prefix []byte) ([][]byte, error) {
	// Implementation using prefix-based iteration through the underlying storage
	result := make([][]byte, 0)
	
	// First check the current MemTable
	memTableIDs, err := idx.scanMemTableWithPrefix(idx.storage.memTable, prefix)
	if err != nil {
		return nil, err
	}
	result = append(result, memTableIDs...)
	
	// Then check immutable MemTables
	for i := len(idx.storage.immMemTables) - 1; i >= 0; i-- {
		memTableIDs, err := idx.scanMemTableWithPrefix(idx.storage.immMemTables[i], prefix)
		if err != nil {
			return nil, err
		}
		result = append(result, memTableIDs...)
	}
	
	// Finally check SSTables (newest to oldest)
	for i := len(idx.storage.sstables) - 1; i >= 0; i-- {
		sstableIDs, err := idx.scanSSTableWithPrefix(idx.storage.sstables[i], prefix)
		if err != nil {
			return nil, err
		}
		result = append(result, sstableIDs...)
	}
	
	// Remove duplicates (entityIDs might appear in multiple layers)
	return idx.deduplicateEntityIDs(result), nil
}

// scanMemTableWithPrefix scans a MemTable for keys with a specific prefix
func (idx *propertyIndex) scanMemTableWithPrefix(memTable MemTableInterface, prefix []byte) ([][]byte, error) {
	// This implementation assumes we can iterate through the MemTable
	result := make([][]byte, 0)
	
	// For MemTable, use the GetEntries method and filter by prefix
	entries := memTable.GetEntries()
	
	// Process entries in pairs (key, value)
	for i := 0; i < len(entries); i += 2 {
		key := entries[i]
		
		// Check if this key has our prefix
		if bytes.HasPrefix(key, prefix) {
			// Extract the entity ID from the key
			entityID, err := idx.extractEntityID(key)
			if err != nil {
				continue // Skip invalid keys
			}
			
			// Get the value to check if it's a tombstone
			value := entries[i+1]
			if len(value) > 0 && value[0] == 1 { // Not a tombstone
				result = append(result, entityID)
			}
		}
	}
	
	return result, nil
}

// scanSSTableWithPrefix scans an SSTable for keys with a specific prefix
func (idx *propertyIndex) scanSSTableWithPrefix(sstable *SSTable, prefix []byte) ([][]byte, error) {
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
		
		// Extract the entity ID from the key
		entityID, err := idx.extractEntityID(key)
		if err == nil {
			result = append(result, entityID)
		}
		
		// Move to next entry
		if err := iter.Next(); err != nil {
			break
		}
	}
	
	return result, nil
}

// deduplicateEntityIDs removes duplicate entity IDs from a list
func (idx *propertyIndex) deduplicateEntityIDs(entityIDs [][]byte) [][]byte {
	if len(entityIDs) <= 1 {
		return entityIDs
	}
	
	// Use a map for deduplication
	unique := make(map[string]struct{})
	result := make([][]byte, 0, len(entityIDs))
	
	for _, id := range entityIDs {
		idStr := string(id)
		if _, exists := unique[idStr]; !exists {
			unique[idStr] = struct{}{}
			result = append(result, id)
		}
	}
	
	return result
}

// Delete removes all entity IDs for a property name
func (idx *propertyIndex) Delete(key []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}
	
	// For property index, the key could be either:
	// 1. A property name (delete all entities with this property)
	// 2. A property name|property value combination (delete all entities with this prop=value)
	
	parts := bytes.Split(key, []byte("|"))
	if len(parts) == 2 {
		// Handle deletion by property name + value
		propertyName := parts[0]
		propertyValue := parts[1]
		
		// Try with all value types
		for _, valueType := range []PropertyValueType{
			PropertyValueTypeString,
			PropertyValueTypeNumeric,
			PropertyValueTypeBoolean,
		} {
			prefix := idx.makePropertyValuePrefix(propertyName, propertyValue, valueType)
			err := idx.deleteByPrefix(prefix)
			if err != nil {
				return err
			}
		}
		
		return nil
	}
	
	// Handle deletion by property name only
	propertyName := key
	prefix := idx.makePropertyPrefix(propertyName)
	return idx.deleteByPrefix(prefix)
}

// deleteByPrefix removes all keys with a specific prefix
func (idx *propertyIndex) deleteByPrefix(prefix []byte) error {
	// Get all existing entity IDs for this prefix
	entityIDs, err := idx.scanKeysWithPrefix(prefix)
	if err != nil {
		return fmt.Errorf("failed to scan for entity IDs: %w", err)
	}
	
	// We need to find and delete each individual key
	// This is inefficient but necessary because we can't do range deletes directly
	
	// Check current MemTable and immutable MemTables
	keysToDelete := make([][]byte, 0)
	
	// Collect keys from current MemTable
	memKeys := idx.collectKeysWithPrefixFromMemTable(idx.storage.memTable, prefix)
	keysToDelete = append(keysToDelete, memKeys...)
	
	// Collect keys from immutable MemTables
	for i := len(idx.storage.immMemTables) - 1; i >= 0; i-- {
		immKeys := idx.collectKeysWithPrefixFromMemTable(idx.storage.immMemTables[i], prefix)
		keysToDelete = append(keysToDelete, immKeys...)
	}
	
	// Collect keys from SSTables
	for i := len(idx.storage.sstables) - 1; i >= 0; i-- {
		sstKeys := idx.collectKeysWithPrefixFromSSTable(idx.storage.sstables[i], prefix)
		keysToDelete = append(keysToDelete, sstKeys...)
	}
	
	// Delete each key
	for _, key := range keysToDelete {
		err := idx.storage.Delete(key)
		if err != nil {
			return fmt.Errorf("failed to delete property index entry: %w", err)
		}
	}
	
	// Invalidate cache for this prefix
	idx.cache.Invalidate(prefix)
	
	idx.logger.Debug("Deleted %d entity IDs for property index with prefix %s", len(entityIDs), prefix)
	return nil
}

// collectKeysWithPrefixFromMemTable collects all keys with a given prefix from a MemTable
func (idx *propertyIndex) collectKeysWithPrefixFromMemTable(memTable MemTableInterface, prefix []byte) [][]byte {
	result := make([][]byte, 0)
	
	// For MemTable, use the GetEntries method and filter by prefix
	entries := memTable.GetEntries()
	
	// Process entries in pairs (key, value)
	for i := 0; i < len(entries); i += 2 {
		key := entries[i]
		
		// Check if this key has our prefix
		if bytes.HasPrefix(key, prefix) {
			result = append(result, key)
		}
	}
	
	return result
}

// collectKeysWithPrefixFromSSTable collects all keys with a given prefix from an SSTable
func (idx *propertyIndex) collectKeysWithPrefixFromSSTable(sstable *SSTable, prefix []byte) [][]byte {
	result := make([][]byte, 0)
	
	// Create an iterator for the SSTable
	iter, err := sstable.Iterator()
	if err != nil {
		return result
	}
	defer iter.Close()
	
	// Position at the first key with our prefix
	err = iter.Seek(prefix)
	if err != nil {
		return result
	}
	
	// Iterate through matching keys
	for iter.Valid() {
		key := iter.Key()
		
		// Check if we've moved past keys with our prefix
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		
		result = append(result, key)
		
		// Move to next entry
		if err := iter.Next(); err != nil {
			break
		}
	}
	
	return result
}

// DeleteValue removes a specific entity ID from the property index
func (idx *propertyIndex) DeleteValue(key, entityID []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return ErrIndexClosed
	}
	
	// For the property index, we expect the "key" to be in the format "propertyName|propertyValue"
	parts := bytes.Split(key, []byte("|"))
	if len(parts) != 2 {
		return fmt.Errorf("invalid key format for property index: %s", key)
	}
	
	propertyName := parts[0]
	propertyValue := parts[1]
	
	// Try each value type
	for _, valueType := range []PropertyValueType{
		PropertyValueTypeString,
		PropertyValueTypeNumeric,
		PropertyValueTypeBoolean,
	} {
		indexKey := idx.makeKey(propertyName, propertyValue, entityID, valueType)
		
		// Check if this key exists
		exists, err := idx.storage.Contains(indexKey)
		if err != nil {
			return fmt.Errorf("failed to check property index entry: %w", err)
		}
		
		if exists {
			// Delete from the underlying storage
			err := idx.storage.Delete(indexKey)
			if err != nil {
				return fmt.Errorf("failed to delete property index entry: %w", err)
			}
			
			// Invalidate cache
			idx.cache.Invalidate(idx.makePropertyPrefix(propertyName))
			idx.cache.Invalidate(idx.makePropertyValuePrefix(propertyName, propertyValue, valueType))
			
			idx.logger.Debug("Removed entity ID %s from property %s=%s index", entityID, propertyName, propertyValue)
			return nil
		}
	}
	
	// If we get here, no matching entry was found
	return nil
}

// Contains checks if a property exists in the index
func (idx *propertyIndex) Contains(key []byte) (bool, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.isOpen {
		return false, ErrIndexClosed
	}
	
	// For property index, the key could be either:
	// 1. A property name (check if any entity has this property)
	// 2. A property name|property value combination (check if any entity has this prop=value)
	
	parts := bytes.Split(key, []byte("|"))
	if len(parts) == 2 {
		// Check if any entity has this property name and value
		propertyName := parts[0]
		propertyValue := parts[1]
		
		// Try with all value types
		for _, valueType := range []PropertyValueType{
			PropertyValueTypeString,
			PropertyValueTypeNumeric,
			PropertyValueTypeBoolean,
		} {
			prefix := idx.makePropertyValuePrefix(propertyName, propertyValue, valueType)
			exists, err := idx.containsWithPrefix(prefix)
			if err != nil {
				return false, err
			}
			if exists {
				return true, nil
			}
		}
		
		return false, nil
	}
	
	// Check if any entity has this property name
	propertyName := key
	prefix := idx.makePropertyPrefix(propertyName)
	return idx.containsWithPrefix(prefix)
}

// containsWithPrefix checks if any key with the given prefix exists
func (idx *propertyIndex) containsWithPrefix(prefix []byte) (bool, error) {
	// Check cache first for performance
	if cached, ok := idx.cache.Get(prefix); ok {
		return len(cached) > 0, nil
	}
	
	// We just need to find one key with this prefix
	// First check the current MemTable
	memKeys := idx.collectKeysWithPrefixFromMemTable(idx.storage.memTable, prefix)
	if len(memKeys) > 0 {
		return true, nil
	}
	
	// Then check immutable MemTables
	for i := len(idx.storage.immMemTables) - 1; i >= 0; i-- {
		immKeys := idx.collectKeysWithPrefixFromMemTable(idx.storage.immMemTables[i], prefix)
		if len(immKeys) > 0 {
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
func (idx *propertyIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.isOpen {
		return nil
	}

	idx.isOpen = false
	idx.cache.Clear()
	idx.logger.Info("Closed property index")
	return nil
}

// Flush flushes the index to the underlying storage
func (idx *propertyIndex) Flush() error {
	return idx.BaseIndex.Flush()
}

// GetType returns the type of the index
func (idx *propertyIndex) GetType() IndexType {
	return idx.indexType
}