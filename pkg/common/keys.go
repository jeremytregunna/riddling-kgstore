package common

import (
	"fmt"
	"strconv"
)

// Key format constants for serialization
const (
	NodeKeyPrefix    = "n:"  // Prefix for node keys
	EdgeKeyPrefix    = "e:"  // Prefix for edge keys
	NodeLabelPrefix  = "nl:" // Prefix for node label index
	EdgeLabelPrefix  = "el:" // Prefix for edge label index
	NodePropPrefix   = "np:" // Prefix for node property index
	EdgePropPrefix   = "ep:" // Prefix for edge property index
	SourceNodePrefix = "sn:" // Prefix for source node index
	TargetNodePrefix = "tn:" // Prefix for target node index
)

// FormatUint64 formats a uint64 as a string
func FormatUint64(value uint64) string {
	return strconv.FormatUint(value, 10)
}

// FormatInt formats an int as a string
func FormatInt(value int) string {
	return strconv.Itoa(value)
}

// ParseUint64 parses a string as a uint64
func ParseUint64(value string) (uint64, error) {
	return strconv.ParseUint(value, 10, 64)
}

// ParseInt parses a string as an int
func ParseInt(value string) (int, error) {
	return strconv.Atoi(value)
}

// FormatNodeKey formats a node key for storage
func FormatNodeKey(nodeID uint64) string {
	return fmt.Sprintf("%s%d", NodeKeyPrefix, nodeID)
}

// FormatEdgeKey formats an edge key for storage
func FormatEdgeKey(edgeID string) string {
	return fmt.Sprintf("%s%s", EdgeKeyPrefix, edgeID)
}

// FormatOutgoingEdgesKey formats a key for outgoing edges
func FormatOutgoingEdgesKey(nodeID uint64) string {
	return fmt.Sprintf("%s%d", SourceNodePrefix, nodeID)
}

// FormatIncomingEdgesKey formats a key for incoming edges
func FormatIncomingEdgesKey(nodeID uint64) string {
	return fmt.Sprintf("%s%d", TargetNodePrefix, nodeID)
}

// FormatNodeLabelKey formats a key for node label index
func FormatNodeLabelKey(label string, nodeID uint64) string {
	return fmt.Sprintf("%s%s:%d", NodeLabelPrefix, label, nodeID)
}

// FormatEdgeLabelKey formats a key for edge label index
func FormatEdgeLabelKey(label string, edgeID string) string {
	return fmt.Sprintf("%s%s:%s", EdgeLabelPrefix, label, edgeID)
}

// FormatNodePropertyKey formats a key for node property index
func FormatNodePropertyKey(propertyName, propertyValue, nodeID string) string {
	return fmt.Sprintf("%s%s:%s:%s", NodePropPrefix, propertyName, propertyValue, nodeID)
}

// FormatEdgePropertyKey formats a key for edge property index
func FormatEdgePropertyKey(propertyName, propertyValue, edgeID string) string {
	return fmt.Sprintf("%s%s:%s:%s", EdgePropPrefix, propertyName, propertyValue, edgeID)
}

// FormatSourceEdgeKey formats a key for storing edge ID in source node index
func FormatSourceEdgeKey(sourceID uint64, edgeID string) string {
	return fmt.Sprintf("%s%d:%s", SourceNodePrefix, sourceID, edgeID)
}

// FormatTargetEdgeKey formats a key for storing edge ID in target node index
func FormatTargetEdgeKey(targetID uint64, edgeID string) string {
	return fmt.Sprintf("%s%d:%s", TargetNodePrefix, targetID, edgeID)
}

// FormatNodeLabelScanKey formats a key for scanning node labels
func FormatNodeLabelScanKey(label string) string {
	return fmt.Sprintf("%s%s:", NodeLabelPrefix, label)
}

// FormatEdgeLabelScanKey formats a key for scanning edge labels
func FormatEdgeLabelScanKey(label string) string {
	return fmt.Sprintf("%s%s:", EdgeLabelPrefix, label)
}