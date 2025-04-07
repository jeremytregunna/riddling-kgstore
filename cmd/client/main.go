package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	pb "git.canoozie.net/riddling/kgstore/pkg/proto/git.canoozie.net/riddling/kgstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	serverAddr = flag.String("server", "localhost:50051", "The server address in the format of host:port")
	queryType  = flag.String("type", "FIND_NODES_BY_LABEL", "The type of query to execute")
	params     = flag.String("params", "", "JSON-encoded parameters for the query")
	txID       = flag.String("txid", "", "Transaction ID (optional)")
)

func main() {
	flag.Parse()

	// Set up a connection to the server
	conn, err := grpc.Dial(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewKGStoreServiceClient(conn)

	// Parse query type
	var queryTypeEnum pb.QueryType
	switch *queryType {
	case "FIND_NODES_BY_LABEL":
		queryTypeEnum = pb.QueryType_FIND_NODES_BY_LABEL
	case "FIND_EDGES_BY_LABEL":
		queryTypeEnum = pb.QueryType_FIND_EDGES_BY_LABEL
	case "FIND_NODES_BY_PROPERTY":
		queryTypeEnum = pb.QueryType_FIND_NODES_BY_PROPERTY
	case "FIND_EDGES_BY_PROPERTY":
		queryTypeEnum = pb.QueryType_FIND_EDGES_BY_PROPERTY
	case "FIND_NEIGHBORS":
		queryTypeEnum = pb.QueryType_FIND_NEIGHBORS
	case "FIND_PATH":
		queryTypeEnum = pb.QueryType_FIND_PATH
	case "CREATE_NODE":
		queryTypeEnum = pb.QueryType_CREATE_NODE
	case "CREATE_EDGE":
		queryTypeEnum = pb.QueryType_CREATE_EDGE
	case "DELETE_NODE":
		queryTypeEnum = pb.QueryType_DELETE_NODE
	case "DELETE_EDGE":
		queryTypeEnum = pb.QueryType_DELETE_EDGE
	case "SET_PROPERTY":
		queryTypeEnum = pb.QueryType_SET_PROPERTY
	case "REMOVE_PROPERTY":
		queryTypeEnum = pb.QueryType_REMOVE_PROPERTY
	case "BEGIN_TRANSACTION":
		handleBeginTransaction(client)
		return
	case "COMMIT_TRANSACTION":
		if *txID == "" {
			log.Fatalf("Transaction ID is required for COMMIT_TRANSACTION")
		}
		handleCommitTransaction(client, *txID)
		return
	case "ROLLBACK_TRANSACTION":
		if *txID == "" {
			log.Fatalf("Transaction ID is required for ROLLBACK_TRANSACTION")
		}
		handleRollbackTransaction(client, *txID)
		return
	default:
		log.Fatalf("Unknown query type: %s", *queryType)
	}

	// Parse parameters
	parameters := make(map[string]string)
	if *params != "" {
		if err := json.Unmarshal([]byte(*params), &parameters); err != nil {
			log.Fatalf("Failed to parse parameters: %v", err)
		}
	}

	// Create query request
	request := &pb.QueryRequest{
		Type:          queryTypeEnum,
		Parameters:    parameters,
		TransactionId: *txID,
	}

	// Execute query with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := client.ExecuteQuery(ctx, request)
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			// Create an error response with the error message from the status
			errorResponse := map[string]interface{}{
				"error":       st.Message(),
				"operation":   *queryType,
				"success":     false,
				"status_code": st.Code().String(),
			}
			respJson, _ := json.MarshalIndent(errorResponse, "", "  ")
			fmt.Println(string(respJson))
		} else {
			// For non-status errors, still create a structured response
			errorResponse := map[string]interface{}{
				"error":     err.Error(),
				"operation": *queryType,
				"success":   false,
			}
			respJson, _ := json.MarshalIndent(errorResponse, "", "  ")
			fmt.Println(string(respJson))
		}
		os.Exit(1)
	}

	// Print response
	prettyResponse, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal response: %v", err)
	}
	fmt.Println(string(prettyResponse))
}

func handleBeginTransaction(client pb.KGStoreServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	response, err := client.BeginTransaction(ctx, &pb.BeginTransactionRequest{})
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			// Create an error response with the error message from the status
			errorResponse := map[string]interface{}{
				"error":       st.Message(),
				"operation":   "BEGIN_TRANSACTION",
				"success":     false,
				"status_code": st.Code().String(),
			}
			respJson, _ := json.MarshalIndent(errorResponse, "", "  ")
			fmt.Println(string(respJson))
		} else {
			// For non-status errors, still create a structured response
			errorResponse := map[string]interface{}{
				"error":     err.Error(),
				"operation": "BEGIN_TRANSACTION",
				"success":   false,
			}
			respJson, _ := json.MarshalIndent(errorResponse, "", "  ")
			fmt.Println(string(respJson))
		}
		os.Exit(1)
	}

	prettyResponse, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal response: %v", err)
	}
	fmt.Println(string(prettyResponse))
}

func handleCommitTransaction(client pb.KGStoreServiceClient, txID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	response, err := client.CommitTransaction(ctx, &pb.TransactionRequest{
		TransactionId: txID,
	})
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			// Create an error response with the error message from the status
			errorResponse := map[string]interface{}{
				"error":          st.Message(),
				"operation":      "COMMIT_TRANSACTION",
				"success":        false,
				"status_code":    st.Code().String(),
				"transaction_id": txID,
			}
			respJson, _ := json.MarshalIndent(errorResponse, "", "  ")
			fmt.Println(string(respJson))
		} else {
			// For non-status errors, still create a structured response
			errorResponse := map[string]interface{}{
				"error":          err.Error(),
				"operation":      "COMMIT_TRANSACTION",
				"success":        false,
				"transaction_id": txID,
			}
			respJson, _ := json.MarshalIndent(errorResponse, "", "  ")
			fmt.Println(string(respJson))
		}
		os.Exit(1)
	}

	prettyResponse, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal response: %v", err)
	}
	fmt.Println(string(prettyResponse))
}

func handleRollbackTransaction(client pb.KGStoreServiceClient, txID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	response, err := client.RollbackTransaction(ctx, &pb.TransactionRequest{
		TransactionId: txID,
	})
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			// Create an error response with the error message from the status
			errorResponse := map[string]interface{}{
				"error":          st.Message(),
				"operation":      "ROLLBACK_TRANSACTION",
				"success":        false,
				"status_code":    st.Code().String(),
				"transaction_id": txID,
			}
			respJson, _ := json.MarshalIndent(errorResponse, "", "  ")
			fmt.Println(string(respJson))
		} else {
			// For non-status errors, still create a structured response
			errorResponse := map[string]interface{}{
				"error":          err.Error(),
				"operation":      "ROLLBACK_TRANSACTION",
				"success":        false,
				"transaction_id": txID,
			}
			respJson, _ := json.MarshalIndent(errorResponse, "", "  ")
			fmt.Println(string(respJson))
		}
		os.Exit(1)
	}

	prettyResponse, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal response: %v", err)
	}
	fmt.Println(string(prettyResponse))
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nExamples:\n")
	fmt.Fprintf(os.Stderr, "  %s -type=BEGIN_TRANSACTION\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s -type=FIND_NODES_BY_LABEL -params='{\"label\":\"Person\"}'\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s -type=CREATE_NODE -params='{\"label\":\"Person\"}' -txid=tx-1\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s -type=COMMIT_TRANSACTION -txid=tx-1\n", os.Args[0])
	os.Exit(1)
}
