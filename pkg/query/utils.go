package query

import (
	"fmt"
	"strconv"
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

// GetOrDefault gets a value from a map or returns a default if not found
func GetOrDefault(params map[string]string, key, defaultValue string) string {
	if value, ok := params[key]; ok {
		return value
	}
	return defaultValue
}

// FormatNodeKey formats a node key for storage
func FormatNodeKey(nodeID uint64) string {
	return fmt.Sprintf("n:%d", nodeID)
}

// FormatEdgeKey formats an edge key for storage
func FormatEdgeKey(edgeID string) string {
	return fmt.Sprintf("e:%s", edgeID)
}

// FormatOutgoingEdgesKey formats a key for outgoing edges
func FormatOutgoingEdgesKey(nodeID uint64) string {
	return fmt.Sprintf("sn:%d", nodeID)
}

// FormatIncomingEdgesKey formats a key for incoming edges
func FormatIncomingEdgesKey(nodeID uint64) string {
	return fmt.Sprintf("tn:%d", nodeID)
}
