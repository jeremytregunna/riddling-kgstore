package model

import (
	"fmt"
)

// ErrInvalidNodeID is returned when an operation is performed with an invalid node ID
type ErrInvalidNodeID struct {
	ID uint64
}

func (e ErrInvalidNodeID) Error() string {
	return fmt.Sprintf("invalid node ID: %d", e.ID)
}

// ErrInvalidEdgeSource is returned when an edge is created with an invalid source node ID
type ErrInvalidEdgeSource struct {
	SourceID uint64
}

func (e ErrInvalidEdgeSource) Error() string {
	return fmt.Sprintf("invalid edge source node ID: %d", e.SourceID)
}

// ErrInvalidEdgeTarget is returned when an edge is created with an invalid target node ID
type ErrInvalidEdgeTarget struct {
	TargetID uint64
}

func (e ErrInvalidEdgeTarget) Error() string {
	return fmt.Sprintf("invalid edge target node ID: %d", e.TargetID)
}

// ErrInvalidPropertyKey is returned when a property operation is performed with an invalid key
type ErrInvalidPropertyKey struct {
	Key string
}

func (e ErrInvalidPropertyKey) Error() string {
	return fmt.Sprintf("invalid property key: %s", e.Key)
}

// ErrPropertyNotFound is returned when a property is not found for a given key
type ErrPropertyNotFound struct {
	Key string
}

func (e ErrPropertyNotFound) Error() string {
	return fmt.Sprintf("property not found for key: %s", e.Key)
}

// ErrPageDataSizeExceeded is returned when attempting to write data that exceeds the page size
type ErrPageDataSizeExceeded struct {
	DataSize int
	PageSize int
}

func (e ErrPageDataSizeExceeded) Error() string {
	return fmt.Sprintf("data size (%d) exceeds page size (%d)", e.DataSize, e.PageSize)
}

// MemTable errors
var (
	// ErrKeyNotFound is returned when a key is not found in a MemTable
	ErrKeyNotFound = fmt.Errorf("key not found in MemTable")

	// ErrNilValue is returned when attempting to add a nil value to a MemTable
	ErrNilValue = fmt.Errorf("cannot add nil value to MemTable")

	// ErrNilKey is returned when attempting to use a nil key in a MemTable
	ErrNilKey = fmt.Errorf("cannot use nil key in MemTable")

	// ErrMemTableFull is returned when a MemTable has reached its maximum size
	ErrMemTableFull = fmt.Errorf("MemTable is full")

	// ErrMemTableFlushed is returned when attempting to modify a MemTable that has been flushed
	ErrMemTableFlushed = fmt.Errorf("MemTable has been flushed and is read-only")
)
