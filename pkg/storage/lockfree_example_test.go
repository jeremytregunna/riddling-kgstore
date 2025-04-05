package storage_test

import (
	"fmt"
	"os"
	"path/filepath"

	"git.canoozie.net/riddling/kgstore/pkg/storage"
)

func Example_lockFreeMemTable() {
	// Create a temporary directory for the database
	tempDir, err := os.MkdirTemp("", "lockfree-example")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		return
	}
	defer os.RemoveAll(tempDir)

	// Create a storage engine with lock-free MemTable
	config := storage.DefaultEngineConfig()
	config.DataDir = tempDir
	config.UseLockFreeMemTable = true // Enable lock-free MemTable

	// Create the storage engine
	engine, err := storage.NewStorageEngine(config)
	if err != nil {
		fmt.Printf("Failed to create storage engine: %v\n", err)
		return
	}
	defer engine.Close()

	// Add some data
	key1 := []byte("user:1001")
	value1 := []byte(`{"name":"John Doe","email":"john@example.com"}`)
	if err := engine.Put(key1, value1); err != nil {
		fmt.Printf("Failed to put key1: %v\n", err)
		return
	}

	key2 := []byte("user:1002")
	value2 := []byte(`{"name":"Jane Smith","email":"jane@example.com"}`)
	if err := engine.Put(key2, value2); err != nil {
		fmt.Printf("Failed to put key2: %v\n", err)
		return
	}

	// Retrieve the data
	result1, err := engine.Get(key1)
	if err != nil {
		fmt.Printf("Failed to get key1: %v\n", err)
		return
	}
	fmt.Printf("User 1001: %s\n", result1)

	result2, err := engine.Get(key2)
	if err != nil {
		fmt.Printf("Failed to get key2: %v\n", err)
		return
	}
	fmt.Printf("User 1002: %s\n", result2)

	// Delete some data
	if err := engine.Delete(key1); err != nil {
		fmt.Printf("Failed to delete key1: %v\n", err)
		return
	}

	// Check if key exists
	exists, err := engine.Contains(key1)
	if err != nil {
		fmt.Printf("Error checking key1: %v\n", err)
		return
	}
	
	fmt.Printf("User 1001 exists: %v\n", exists)

	// Flush MemTable to disk
	if err := engine.Flush(); err != nil {
		fmt.Printf("Failed to flush: %v\n", err)
		return
	}

	// Check that the SSTable was created
	sstableDir := filepath.Join(tempDir, "sstables")
	files, err := os.ReadDir(sstableDir)
	if err != nil {
		fmt.Printf("Failed to read sstable directory: %v\n", err)
		return
	}

	fmt.Printf("Created %d SSTable files\n", len(files))

	// Output:
	// User 1001: {"name":"John Doe","email":"john@example.com"}
	// User 1002: {"name":"Jane Smith","email":"jane@example.com"}
	// User 1001 exists: false
	// Created 0 SSTable files
}