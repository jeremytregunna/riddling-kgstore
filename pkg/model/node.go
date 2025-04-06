package model

// Node represents a vertex in the knowledge graph
type Node struct {
	ID    uint64 // Unique identifier for the node
	Label string // Type or category of the node
	*PropertyContainer
}

// NewNode creates a new Node with the given ID and label
func NewNode(id uint64, label string) *Node {
	return &Node{
		ID:                id,
		Label:             label,
		PropertyContainer: NewPropertyContainer(),
	}
}

// For backward compatibility with code that directly accesses Properties
func (n *Node) GetProperties() map[string]string {
	return n.Properties
}

