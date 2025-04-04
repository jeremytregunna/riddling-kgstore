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

// ErrInvalidPageID is returned when an operation is performed with an invalid page ID
type ErrInvalidPageID struct {
	ID uint64
}

func (e ErrInvalidPageID) Error() string {
	return fmt.Sprintf("invalid page ID: %d", e.ID)
}

// ErrPageDataSizeExceeded is returned when attempting to write data that exceeds the page size
type ErrPageDataSizeExceeded struct {
	DataSize int
	PageSize int
}

func (e ErrPageDataSizeExceeded) Error() string {
	return fmt.Sprintf("data size (%d) exceeds page size (%d)", e.DataSize, e.PageSize)
}
