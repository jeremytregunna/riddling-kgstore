package server

import (
	"context"
	"fmt"

	"git.canoozie.net/riddling/kgstore/pkg/model"
	pb "git.canoozie.net/riddling/kgstore/pkg/proto/git.canoozie.net/riddling/kgstore/proto"
	"git.canoozie.net/riddling/kgstore/pkg/query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KGStoreServer implements the gRPC server for the knowledge graph store
type KGStoreServer struct {
	pb.UnimplementedKGStoreServiceServer
	queryEngine *query.Engine
}

// NewKGStoreServer creates a new instance of the KGStore gRPC server
func NewKGStoreServer(queryEngine *query.Engine) *KGStoreServer {
	return &KGStoreServer{
		queryEngine: queryEngine,
	}
}

// RegisterServer registers the server with the provided gRPC server
func RegisterServer(grpcServer *grpc.Server, queryEngine *query.Engine) {
	server := NewKGStoreServer(queryEngine)
	pb.RegisterKGStoreServiceServer(grpcServer, server)

	// No specific shutdown handler needed - grpcServer.GracefulStop() handles shutdown
}

// ExecuteQuery executes a query against the database
func (s *KGStoreServer) ExecuteQuery(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	// Convert proto QueryType to query.QueryType
	queryType, err := convertQueryType(req.Type)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid query type: %v", err)
	}

	// Build query
	parameters := req.Parameters
	if parameters == nil {
		parameters = make(map[string]string)
	}

	// Add transaction ID to parameters if provided
	if req.TransactionId != "" {
		parameters["txId"] = req.TransactionId
	}

	q := &query.Query{
		Type:       queryType,
		Parameters: parameters,
	}

	// Execute query
	result, err := s.queryEngine.Executor.Execute(q)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query execution failed: %v", err)
	}

	// Convert query result to gRPC response
	response, err := convertQueryResult(result)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to convert query result: %v", err)
	}

	return response, nil
}

// ExecuteStreamingQuery executes a query that may return a large result set
func (s *KGStoreServer) ExecuteStreamingQuery(req *pb.QueryRequest, stream pb.KGStoreService_ExecuteStreamingQueryServer) error {
	// Convert proto QueryType to query.QueryType
	queryType, err := convertQueryType(req.Type)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid query type: %v", err)
	}

	// Build query
	parameters := req.Parameters
	if parameters == nil {
		parameters = make(map[string]string)
	}

	// Add transaction ID to parameters if provided
	if req.TransactionId != "" {
		parameters["txId"] = req.TransactionId
	}

	q := &query.Query{
		Type:       queryType,
		Parameters: parameters,
	}

	// Execute query
	result, err := s.queryEngine.Executor.Execute(q)
	if err != nil {
		return status.Errorf(codes.Internal, "query execution failed: %v", err)
	}

	// For simplicity, we're sending the entire result as one message
	// In a production system, you would batch the results and send multiple messages
	response, err := convertQueryResult(result)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to convert query result: %v", err)
	}

	return stream.Send(response)
}

// BeginTransaction starts a new transaction
func (s *KGStoreServer) BeginTransaction(ctx context.Context, req *pb.BeginTransactionRequest) (*pb.TransactionResponse, error) {
	txID, err := s.queryEngine.Executor.BeginTransaction()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %v", err)
	}

	return &pb.TransactionResponse{
		TransactionId: txID,
		Success:       true,
		Operation:     "BEGIN_TRANSACTION",
	}, nil
}

// CommitTransaction commits a transaction
func (s *KGStoreServer) CommitTransaction(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	err := s.queryEngine.Executor.CommitTransaction(req.TransactionId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to commit transaction: %v", err)
	}

	return &pb.TransactionResponse{
		TransactionId: req.TransactionId,
		Success:       true,
		Operation:     "COMMIT_TRANSACTION",
	}, nil
}

// RollbackTransaction rolls back a transaction
func (s *KGStoreServer) RollbackTransaction(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	err := s.queryEngine.Executor.RollbackTransaction(req.TransactionId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to rollback transaction: %v", err)
	}

	return &pb.TransactionResponse{
		TransactionId: req.TransactionId,
		Success:       true,
		Operation:     "ROLLBACK_TRANSACTION",
	}, nil
}

// Helper functions to convert between protobuf and domain types

// convertQueryType converts a protobuf QueryType to a query.QueryType
func convertQueryType(protoType pb.QueryType) (query.QueryType, error) {
	switch protoType {
	case pb.QueryType_FIND_NODES_BY_LABEL:
		return query.QueryTypeFindNodesByLabel, nil
	case pb.QueryType_FIND_EDGES_BY_LABEL:
		return query.QueryTypeFindEdgesByLabel, nil
	case pb.QueryType_FIND_NODES_BY_PROPERTY:
		return query.QueryTypeFindNodesByProperty, nil
	case pb.QueryType_FIND_EDGES_BY_PROPERTY:
		return query.QueryTypeFindEdgesByProperty, nil
	case pb.QueryType_FIND_NEIGHBORS:
		return query.QueryTypeFindNeighbors, nil
	case pb.QueryType_FIND_PATH:
		return query.QueryTypeFindPath, nil
	case pb.QueryType_CREATE_NODE:
		return query.QueryTypeCreateNode, nil
	case pb.QueryType_CREATE_EDGE:
		return query.QueryTypeCreateEdge, nil
	case pb.QueryType_DELETE_NODE:
		return query.QueryTypeDeleteNode, nil
	case pb.QueryType_DELETE_EDGE:
		return query.QueryTypeDeleteEdge, nil
	case pb.QueryType_SET_PROPERTY:
		return query.QueryTypeSetProperty, nil
	case pb.QueryType_REMOVE_PROPERTY:
		return query.QueryTypeRemoveProperty, nil
	default:
		return "", fmt.Errorf("unknown query type: %v", protoType)
	}
}

// convertNode converts a model.Node to a protobuf Node
func convertNode(node model.Node) *pb.Node {
	return &pb.Node{
		Id:         node.ID,
		Label:      node.Label,
		Properties: node.Properties,
	}
}

// convertEdge converts a model.Edge to a protobuf Edge
func convertEdge(edge model.Edge) *pb.Edge {
	return &pb.Edge{
		SourceId:   edge.SourceID,
		TargetId:   edge.TargetID,
		Label:      edge.Label,
		Properties: edge.Properties,
	}
}

// convertPath converts a query.Path to a protobuf Path
func convertPath(path query.Path) *pb.Path {
	protoPath := &pb.Path{
		Nodes: make([]*pb.Node, len(path.Nodes)),
		Edges: make([]*pb.Edge, len(path.Edges)),
	}

	for i, node := range path.Nodes {
		protoPath.Nodes[i] = convertNode(node)
	}

	for i, edge := range path.Edges {
		protoPath.Edges[i] = convertEdge(edge)
	}

	return protoPath
}

// convertQueryResult converts a query.Result to a protobuf QueryResponse
func convertQueryResult(result *query.Result) (*pb.QueryResponse, error) {
	response := &pb.QueryResponse{
		NodeId:    result.NodeID,
		EdgeId:    result.EdgeID,
		Success:   result.Success,
		Operation: result.Operation,
		Error:     result.Error,
	}

	// Convert nodes
	if len(result.Nodes) > 0 {
		response.Nodes = make([]*pb.Node, len(result.Nodes))
		for i, node := range result.Nodes {
			response.Nodes[i] = convertNode(node)
		}
	}

	// Convert edges
	if len(result.Edges) > 0 {
		response.Edges = make([]*pb.Edge, len(result.Edges))
		for i, edge := range result.Edges {
			response.Edges[i] = convertEdge(edge)
		}
	}

	// Convert paths
	if len(result.Paths) > 0 {
		response.Paths = make([]*pb.Path, len(result.Paths))
		for i, path := range result.Paths {
			response.Paths[i] = convertPath(path)
		}
	}

	return response, nil
}
