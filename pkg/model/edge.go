package model

// Edge represents a relationship between two nodes in the knowledge graph
type Edge struct {
	SourceID   uint64            // ID of the source node
	TargetID   uint64            // ID of the target node
	Label      string            // Type or category of the relationship
	Properties map[string]string // Key-value pairs of edge properties
}

// NewEdge creates a new Edge between source and target nodes with the given label
func NewEdge(sourceID, targetID uint64, label string) *Edge {
	return &Edge{
		SourceID:   sourceID,
		TargetID:   targetID,
		Label:      label,
		Properties: make(map[string]string),
	}
}

// AddProperty adds or updates a property on the edge
func (e *Edge) AddProperty(key, value string) {
	e.Properties[key] = value
}

// GetProperty retrieves a property value by key
func (e *Edge) GetProperty(key string) (string, bool) {
	value, exists := e.Properties[key]
	return value, exists
}

// RemoveProperty removes a property from the edge
func (e *Edge) RemoveProperty(key string) {
	delete(e.Properties, key)
}
