#!/bin/bash

# Function to handle errors gracefully
run_client() {
  echo "Running: $*"
  output=$("$@" 2>&1)
  status=$?
  echo "$output"
  if [ $status -ne 0 ]; then
    echo "Command failed with status $status"
    return 1
  fi
  return 0
}

# Clean up any existing data directory to start fresh
echo "Cleaning up old data..."
rm -rf ./data

# Build the server and client
echo "Building server and client..."
mkdir -p bin
go build -o bin/server ./cmd/server
go build -o bin/client ./cmd/client

# Check if build was successful
if [ $? -ne 0 ]; then
  echo "Build failed. Exiting."
  exit 1
fi

# Start the server in the background with debug logging
echo "Starting server with debug logging..."
mkdir -p data
LOG_LEVEL=debug ./bin/server > server_debug.log 2>&1 &
SERVER_PID=$!

# Wait for server to start
sleep 2
echo "Server started with PID $SERVER_PID"

# Begin a transaction
echo "Beginning a transaction..."
# Set extra debug for more detailed logging
export LOG_LEVEL=debug
TX_OUTPUT=$(run_client ./bin/client -type=BEGIN_TRANSACTION || echo '{"transaction_id": "error"}')
TX_ID=$(echo "$TX_OUTPUT" | grep -o '"transaction_id": "[^"]*"' | cut -d '"' -f 4)
if [ "$TX_ID" = "error" ]; then
  echo "Failed to begin transaction. Exiting."
  kill $SERVER_PID
  exit 1
fi
echo "Transaction ID: $TX_ID"

# Create the first node with ref "person"
echo "Creating a Person node with reference 'person'..."
NODE1_OUTPUT=$(run_client ./bin/client -type=CREATE_NODE -params='{"label":"Person","ref":"person"}' -txid="$TX_ID" || echo '{"node_id": ""}')
NODE1_ID=$(echo "$NODE1_OUTPUT" | grep -o '"node_id": "[^"]*"' | cut -d '"' -f 4)
echo "Created Person node with ID: $NODE1_ID"

# Create the second node with ref "city"
echo "Creating a City node with reference 'city'..."
NODE2_OUTPUT=$(run_client ./bin/client -type=CREATE_NODE -params='{"label":"City","ref":"city"}' -txid="$TX_ID" || echo '{"node_id": ""}')
NODE2_ID=$(echo "$NODE2_OUTPUT" | grep -o '"node_id": "[^"]*"' | cut -d '"' -f 4)
echo "Created City node with ID: $NODE2_ID"

# Create an edge between the nodes using references
echo "Creating LIVES_IN edge between nodes using references..."
EDGE_OUTPUT=$(run_client ./bin/client -type=CREATE_EDGE -params='{"sourceId":"$person","targetId":"$city","label":"LIVES_IN","ref":"lives_in"}' -txid="$TX_ID" || echo '{"edge_id": ""}')
EDGE_ID=$(echo "$EDGE_OUTPUT" | grep -o '"edge_id": "[^"]*"' | cut -d '"' -f 4)
echo "Created LIVES_IN edge with ID: $EDGE_ID"

# Commit the transaction
echo "Committing the transaction..."
run_client ./bin/client -type=COMMIT_TRANSACTION -txid="$TX_ID" || true

# Query operations
echo "Finding nodes by label 'Person'..."
run_client ./bin/client -type=FIND_NODES_BY_LABEL -params='{"label":"Person"}' || true

echo "Finding edges by label 'LIVES_IN'..."
run_client ./bin/client -type=FIND_EDGES_BY_LABEL -params='{"label":"LIVES_IN"}' || true

if [ -n "$NODE1_ID" ]; then
  echo "Finding neighbors of node $NODE1_ID..."
  run_client ./bin/client -type=FIND_NEIGHBORS -params="{\"nodeId\":\"$NODE1_ID\",\"direction\":\"outgoing\"}" || true
else
  echo "Skipping neighbor search as node ID is missing"
fi

if [ -n "$NODE1_ID" ] && [ -n "$NODE2_ID" ]; then
  echo "Finding a path between nodes $NODE1_ID and $NODE2_ID..."
  run_client ./bin/client -type=FIND_PATH -params="{\"sourceId\":\"$NODE1_ID\",\"targetId\":\"$NODE2_ID\"}" || true
else
  echo "Skipping path finding as node IDs are missing"
fi

# Begin a new transaction for property operations
echo "Beginning another transaction for property operations..."
TX_OUTPUT=$(run_client ./bin/client -type=BEGIN_TRANSACTION || echo '{"transaction_id": "error"}')
TX_ID=$(echo "$TX_OUTPUT" | grep -o '"transaction_id": "[^"]*"' | cut -d '"' -f 4)
if [ "$TX_ID" = "error" ]; then
  echo "Failed to begin transaction. Skipping property operations."
else
  echo "Transaction ID: $TX_ID"

  # Create new entities with references in this transaction
  echo "Creating another Person node with reference 'person2'..."
  run_client ./bin/client -type=CREATE_NODE -params='{"label":"Person","ref":"person2"}' -txid="$TX_ID" || true
  
  echo "Creating a Company node with reference 'company'..."
  run_client ./bin/client -type=CREATE_NODE -params='{"label":"Company","ref":"company"}' -txid="$TX_ID" || true
  
  # Create an edge using references
  echo "Creating WORKS_AT edge using references..."
  run_client ./bin/client -type=CREATE_EDGE -params='{"sourceId":"$person2","targetId":"$company","label":"WORKS_AT","ref":"works_at"}' -txid="$TX_ID" || true

  # Set properties using references
  echo "Setting name property on node using reference..."
  run_client ./bin/client -type=SET_PROPERTY -params='{"target":"node","id":"$person2","name":"name","value":"John Doe"}' -txid="$TX_ID" || true
  
  echo "Setting name property on company using reference..."
  run_client ./bin/client -type=SET_PROPERTY -params='{"target":"node","id":"$company","name":"name","value":"Acme Inc."}' -txid="$TX_ID" || true

  echo "Setting since property on edge using reference..."
  run_client ./bin/client -type=SET_PROPERTY -params='{"target":"edge","id":"$works_at","name":"since","value":"2020"}' -txid="$TX_ID" || true

  echo "Finding nodes by property name=John Doe..."
  run_client ./bin/client -type=FIND_NODES_BY_PROPERTY -params='{"propertyName":"name","propertyValue":"John Doe"}' -txid="$TX_ID" || true

  echo "Committing property transaction..."
  run_client ./bin/client -type=COMMIT_TRANSACTION -txid="$TX_ID" || true
fi

# Test rollback with another transaction - Note this will not work properly yet,
# but we keep it as it shows that entity reference resolution works properly
echo "Beginning transaction for demonstrating rollback..."
TX_OUTPUT=$(run_client ./bin/client -type=BEGIN_TRANSACTION || echo '{"transaction_id": "error"}')
TX_ID=$(echo "$TX_OUTPUT" | grep -o '"transaction_id": "[^"]*"' | cut -d '"' -f 4)
if [ "$TX_ID" = "error" ]; then
  echo "Failed to begin transaction. Skipping rollback demonstration."
else
  echo "Transaction ID: $TX_ID"

  echo "Creating a TempNode that will be rolled back..."
  run_client ./bin/client -type=CREATE_NODE -params='{"label":"TempNode"}' -txid="$TX_ID" || true

  echo "Rolling back transaction..."
  run_client ./bin/client -type=ROLLBACK_TRANSACTION -txid="$TX_ID" || true

  echo "Checking if node was rolled back (should not find any TempNode)..."
  echo "Note: Rollback is not yet properly implemented, so TempNode might still show up - this is a known issue"
  run_client ./bin/client -type=FIND_NODES_BY_LABEL -params='{"label":"TempNode"}' || true
fi

# Terminate the server gracefully
echo "Shutting down server..."
kill -SIGINT $SERVER_PID
sleep 2

# Make sure server is really gone
if ps -p $SERVER_PID > /dev/null; then
  echo "Forcing server shutdown..."
  kill -9 $SERVER_PID > /dev/null 2>&1
fi

echo "Demo completed!"