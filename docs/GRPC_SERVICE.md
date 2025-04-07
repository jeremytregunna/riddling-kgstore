# KGStore gRPC Service

This document describes the gRPC service for KGStore, a knowledge graph database. The service provides a high-performance, language-agnostic interface to the database.

## Protocol Buffers Definition

The service is defined in `kgstore.proto` which specifies the RPC service methods, message types, and operations supported by the database.

## Service Methods

The gRPC service provides the following methods:

1. **ExecuteQuery**: Executes a query against the database and returns the result
2. **ExecuteStreamingQuery**: Similar to ExecuteQuery but returns results as a stream, useful for large result sets
3. **BeginTransaction**: Starts a new transaction
4. **CommitTransaction**: Commits a transaction
5. **RollbackTransaction**: Rolls back a transaction

## Query Types

The service supports the following query types:

### Read Operations
- **FIND_NODES_BY_LABEL**: Find all nodes with a specific label
- **FIND_EDGES_BY_LABEL**: Find all edges with a specific label
- **FIND_NODES_BY_PROPERTY**: Find all nodes with a specific property value
- **FIND_EDGES_BY_PROPERTY**: Find all edges with a specific property value
- **FIND_NEIGHBORS**: Find all neighbors of a node
- **FIND_PATH**: Find a path between two nodes

### Write Operations
- **CREATE_NODE**: Create a new node
- **CREATE_EDGE**: Create a new edge between two nodes
- **DELETE_NODE**: Delete a node
- **DELETE_EDGE**: Delete an edge
- **SET_PROPERTY**: Set a property on a node or edge
- **REMOVE_PROPERTY**: Remove a property from a node or edge

## Command-line Client

The `client` command-line tool provides an easy way to interact with the gRPC service:

```
Usage: client [OPTIONS]

Options:
  -params string
        JSON-encoded parameters for the query
  -server string
        The server address in the format of host:port (default "localhost:50051")
  -type string
        The type of query to execute (default "FIND_NODES_BY_LABEL")
  -txid string
        Transaction ID (optional)

Examples:
  client -type=BEGIN_TRANSACTION
  client -type=FIND_NODES_BY_LABEL -params='{"label":"Person"}'
  client -type=CREATE_NODE -params='{"label":"Person"}' -txid=tx-1
  client -type=COMMIT_TRANSACTION -txid=tx-1
```

## Transaction Example

Here's how to use transactions with the service:

1. Begin a transaction:
```
client -type=BEGIN_TRANSACTION
```

2. Create a node within the transaction (using the transaction ID from step 1):
```
client -type=CREATE_NODE -params='{"label":"Person"}' -txid=tx-1
```

3. Commit the transaction:
```
client -type=COMMIT_TRANSACTION -txid=tx-1
```

Or roll it back:
```
client -type=ROLLBACK_TRANSACTION -txid=tx-1
```

## Query Parameters

Different query types require different parameters:

- **FIND_NODES_BY_LABEL**: `label`
- **FIND_EDGES_BY_LABEL**: `label`
- **FIND_NODES_BY_PROPERTY**: `propertyName`, `propertyValue`
- **FIND_EDGES_BY_PROPERTY**: `propertyName`, `propertyValue`
- **FIND_NEIGHBORS**: `nodeId`, `direction` (optional, values: "outgoing", "incoming", "both")
- **FIND_PATH**: `sourceId`, `targetId`, `maxHops` (optional)
- **CREATE_NODE**: `label`, `ref` (optional, reference name for future queries)
- **CREATE_EDGE**: `sourceId` or `source`, `targetId`, `label`, `ref` (optional, reference name for future queries)
- **DELETE_NODE**: `id`
- **DELETE_EDGE**: `id`
- **SET_PROPERTY**: `target` ("node" or "edge"), `id`, `name`, `value`
- **REMOVE_PROPERTY**: `target` ("node" or "edge"), `id`, `name`

### Using References in Transactions

Any entity (node or edge) created within a transaction can be assigned a reference name using the `ref` parameter. This reference can then be used in subsequent queries within the same transaction by prefixing it with `$`.

Example:
```
# Create a node with reference name "person1"
CREATE_NODE(label: "Person", ref: "person1")

# Reference this node in a subsequent query
CREATE_EDGE(sourceId: "$person1", targetId: "2", label: "KNOWS")
```

## Running the Server

To start the gRPC server:

```
./bin/server
```

The server accepts the following environment variables:
- `PORT`: The port to listen on (default: 50051)
- `DB_PATH`: The path to the database (default: ./data/kgstore.db)

## Demo Script

A demo script is provided in `scripts/demo_grpc.sh` that demonstrates the usage of the gRPC service. Run it with:

```
./scripts/demo_grpc.sh
```

The script shows how to:
1. Start the server
2. Begin transactions
3. Create nodes and edges
4. Set properties
5. Query the database 
6. Commit and rollback transactions
7. Shut down the server

## Using from Other Languages

Because gRPC is language-agnostic, you can generate client code for various languages using the Protocol Buffers compiler (`protoc`). See the [gRPC documentation](https://grpc.io/docs/) for more information on generating client code for your language of choice.

## Efficiency Considerations

The service is designed to be efficient:

1. Uses protocol buffers for compact, efficient message serialization
2. Supports streaming for large result sets
3. Provides transaction support for atomic operations
4. Optimizes network usage with binary protocol
5. All operations use type enums and parameter maps rather than string parsing

## Error Handling

All operations return status codes and error messages that follow the gRPC status code conventions. Error details are provided in the `error` field of responses.