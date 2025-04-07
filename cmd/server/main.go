package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"git.canoozie.net/riddling/kgstore/pkg/model"
	"git.canoozie.net/riddling/kgstore/pkg/query"
	"git.canoozie.net/riddling/kgstore/pkg/server"
	"git.canoozie.net/riddling/kgstore/pkg/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	defaultPort = "50051"
	defaultPath = "./data/kgstore.db"
)

func main() {
	// Initialize logger with level from environment variable
	logLevel := model.LogLevelInfo
	logLevelEnv := os.Getenv("LOG_LEVEL")
	if logLevelEnv == "debug" {
		logLevel = model.LogLevelDebug
	}

	logger := model.NewDefaultLogger(logLevel)
	logger.Info("Starting KGStore gRPC server")

	// Get port from environment
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	// Get database path from environment
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = defaultPath
	}

	// Create directory for the database if it doesn't exist
	dbDir := filepath.Dir(dbPath)
	err := os.MkdirAll(dbDir, 0755)
	if err != nil {
		log.Fatalf("Failed to create database directory: %v", err)
	}

	// Initialize storage engine
	engineConfig := storage.DefaultEngineConfig()
	engineConfig.DataDir = dbDir
	engineConfig.Logger = logger

	storageEngine, err := storage.NewStorageEngine(engineConfig)
	if err != nil {
		log.Fatalf("Failed to initialize storage engine: %v", err)
	}
	defer storageEngine.Close()

	// Initialize indexes
	nodeIndex, err := storage.NewNodeIndex(storageEngine, logger)
	if err != nil {
		log.Fatalf("Failed to initialize node index: %v", err)
	}

	edgeIndex, err := storage.NewEdgeIndex(storageEngine, logger)
	if err != nil {
		log.Fatalf("Failed to initialize edge index: %v", err)
	}

	// Use the LSM-based node label index
	storageEngine.SetUseLSMNodeLabelIndex(true)
	nodeLabelIndex, err := storage.NewNodeLabelIndex(storageEngine, logger)
	if err != nil {
		log.Fatalf("Failed to initialize node label index: %v", err)
	}

	// Edge label index
	edgeLabelIndex, err := storage.NewEdgeLabelIndex(storageEngine, logger)
	if err != nil {
		log.Fatalf("Failed to initialize edge label index: %v", err)
	}

	// Create property indexes
	nodePropertyIndex, err := storage.NewNodePropertyIndex(storageEngine, logger)
	if err != nil {
		log.Fatalf("Failed to initialize node property index: %v", err)
	}

	edgePropertyIndex, err := storage.NewEdgePropertyIndex(storageEngine, logger)
	if err != nil {
		log.Fatalf("Failed to initialize edge property index: %v", err)
	}

	// Initialize query engine with all indexes
	queryEngine := query.NewEngineWithAllIndexes(
		storageEngine,
		nodeIndex,
		edgeIndex,
		nodeLabelIndex,
		edgeLabelIndex,
		nodePropertyIndex,
		edgePropertyIndex,
	)

	// Create a gRPC server
	grpcServer := grpc.NewServer()

	// Register our server
	server.RegisterServer(grpcServer, queryEngine)

	// Register reflection service on gRPC server
	reflection.Register(grpcServer)

	// Start listening
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Handle graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		logger.Info("Shutting down gRPC server")

		// Just stop the gRPC server without trying to flush data
		// This avoids panic in storage engine during flush on empty memtables
		os.Exit(0)
	}()

	logger.Info("Starting gRPC server on port %s", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
