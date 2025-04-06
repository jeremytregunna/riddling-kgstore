package model

// Edge represents a relationship between two nodes in the knowledge graph
type Edge struct {
	SourceID uint64 // ID of the source node
	TargetID uint64 // ID of the target node
	Label    string // Type or category of the relationship
	*PropertyContainer
}

// NewEdge creates a new Edge between source and target nodes with the given label
func NewEdge(sourceID, targetID uint64, label string) *Edge {
	return &Edge{
		SourceID:          sourceID,
		TargetID:          targetID,
		Label:             label,
		PropertyContainer: NewPropertyContainer(),
	}
}

// For backward compatibility with code that directly accesses Properties
func (e *Edge) GetProperties() map[string]string {
	return e.Properties
}
