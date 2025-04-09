package query

import (
	"git.canoozie.net/riddling/kgstore/pkg/common"
)

// GetOrDefault gets a value from a map or returns a default if not found
func GetOrDefault(params map[string]string, key, defaultValue string) string {
	if value, ok := params[key]; ok {
		return value
	}
	return defaultValue
}

// FormatUint64 formats a uint64 as a string (wrapper for common package)
func FormatUint64(value uint64) string {
	return common.FormatUint64(value)
}

// FormatInt formats an int as a string (wrapper for common package)
func FormatInt(value int) string {
	return common.FormatInt(value)
}

// ParseUint64 parses a string as a uint64 (wrapper for common package)
func ParseUint64(value string) (uint64, error) {
	return common.ParseUint64(value)
}

// ParseInt parses a string as an int (wrapper for common package)
func ParseInt(value string) (int, error) {
	return common.ParseInt(value)
}

// FormatNodeKey formats a node key for storage (wrapper for common package)
func FormatNodeKey(nodeID uint64) string {
	return common.FormatNodeKey(nodeID)
}

// FormatEdgeKey formats an edge key for storage (wrapper for common package)
func FormatEdgeKey(edgeID string) string {
	return common.FormatEdgeKey(edgeID)
}

// FormatOutgoingEdgesKey formats a key for outgoing edges (wrapper for common package)
func FormatOutgoingEdgesKey(nodeID uint64) string {
	return common.FormatOutgoingEdgesKey(nodeID)
}

// FormatIncomingEdgesKey formats a key for incoming edges (wrapper for common package)
func FormatIncomingEdgesKey(nodeID uint64) string {
	return common.FormatIncomingEdgesKey(nodeID)
}

// FormatNodeLabelKey formats a key for node label index (wrapper for common package)
func FormatNodeLabelKey(label string, nodeID uint64) string {
	return common.FormatNodeLabelKey(label, nodeID)
}

// FormatEdgeLabelKey formats a key for edge label index (wrapper for common package)
func FormatEdgeLabelKey(label string, edgeID string) string {
	return common.FormatEdgeLabelKey(label, edgeID)
}

// FormatNodeLabelScanKey formats a key for scanning node labels (wrapper for common package)
func FormatNodeLabelScanKey(label string) string {
	return common.FormatNodeLabelScanKey(label)
}

// FormatEdgeLabelScanKey formats a key for scanning edge labels (wrapper for common package)
func FormatEdgeLabelScanKey(label string) string {
	return common.FormatEdgeLabelScanKey(label)
}
