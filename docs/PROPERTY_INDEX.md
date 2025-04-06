# Property Index Implementation

This document describes the implementation of a specialized SSTable format for indexing node and edge properties in KGStore.

## Overview

The property index is designed to efficiently retrieve nodes and edges by their property values, similar to how a secondary index works in a relational database. The implementation follows a pattern similar to the LSM-tree based node label index, with optimizations specific to property value lookups.

## Implementation Status

The property index structure is fully implemented in the current codebase:

✅ **Implemented**:
- Complete property index data structure in `pkg/storage/property_index.go`
- Support for node and edge properties through separate indexes
- Three value types: string, numeric, and boolean
- Prefix-based key encoding for efficient lookups
- Built-in caching mechanism for frequent queries
- Integration with the existing storage engine

⚠️ **Partially Implemented**:
- Full integration with the query engine (see PROPERTY_INDEX_QUERY.md)

❌ **Not Yet Implemented**:
- Advanced full-text search capabilities
- Specialized range query optimizations
- Property value statistics for query optimization

## Key Features

1. **Specialized Key Format**:
   - Composite key format: `{prefix}:{propertyName}:{valueType}:{propertyValue}:{entityID}`
   - Enables efficient range scans by property name and value
   - Supports different entity types (nodes and edges)

2. **Multiple Value Types**:
   - String values (standard text properties)
   - Numeric values (integers, floats)
   - Boolean values (true/false)
   - Each type has its own encoding for optimized searches

3. **Performance Optimizations**:
   - In-memory cache for frequently accessed property queries
   - Prefix-based scanning for efficient lookups
   - Integration with the two-phase deletion mechanism for consistent reads

## Implementation Details

### Key Components

1. **propertyIndex Struct**:
   - Main implementation of the specialized SSTable format for properties
   - Implements the Index interface from the storage package
   - Supports both node and edge properties with a type designator

2. **Key Encoding**:
   - Properties are indexed using a composite key structure
   - Format allows efficient range queries by property name or property value
   - Value type is encoded in the key for type-specific comparisons

3. **Property Cache**:
   - Simple in-memory cache for frequently accessed property queries
   - Reduces disk reads for common property lookups
   - Automatically invalidated when properties are modified

### Key Operations

1. **Put**:
   - Adds an entity ID to the property index with a specified property name and value
   - Detects and encodes the value type appropriately
   - Invalidates cache for affected property names

2. **Get/GetAll**:
   - Retrieves entity IDs by property name and optionally property value
   - Supports queries for all entities with a given property or specific property value
   - Uses cache for improved performance

3. **Delete/DeleteValue**:
   - Removes property entries from the index
   - Can delete all entities with a specific property or just a single entity's property

4. **Contains**:
   - Efficiently checks if any entity has a specific property or property value
   - Uses prefix scanning to quickly determine existence

### Integration with Storage Engine

The property index is integrated with the KGStore storage engine through configuration options:

1. **Config Flag**:
   - `UsePropertyIndex` in EngineConfig controls whether to use the specialized property index
   - Enabled by default for optimal property lookup performance

2. **Index Creation**:
   - `NewNodePropertyIndex` and `NewEdgePropertyIndex` factory functions
   - `NewFullTextPropertyIndex` for text search capabilities (basic implementation)

3. **Compatibility**:
   - Works with the existing SSTable format and storage mechanisms
   - Leverages the two-phase deletion approach for consistent reads during compaction

## Using the Property Index

Applications can use the property index in two ways:

1. **Direct Index Access**:
   ```go
   // Getting property indexes from storage engine
   nodePropertyIdx, _ := engine.GetIndex(storage.IndexTypePropertyValue)
   
   // Format: propertyName|propertyValue (or just propertyName to get all entities with that property)
   key := []byte("name|Alice")
   nodeIDs, _ := nodePropertyIdx.Get(key)
   ```

2. **Query Integration** (Partially implemented):
   Once fully integrated with the query engine, applications will be able to use:
   ```
   FIND_NODES_BY_PROPERTY(propertyName: "name", propertyValue: "Alice")
   ```

## Future Enhancements

1. **Advanced Text Search**:
   - Implement tokenization and indexing of individual words
   - Add support for phrase matching and fuzzy search

2. **Range Queries**:
   - Optimize numeric range queries for properties (e.g., age > 25)
   - Add specialized iterators for efficient range scanning

3. **Composite Property Queries**:
   - Support for querying on multiple properties simultaneously
   - Optimize intersection and union operations on property results

4. **Statistics and Optimization**:
   - Collect statistics on property values distribution
   - Use statistics for query optimization and data compression