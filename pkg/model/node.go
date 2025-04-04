package model

// Node represents a vertex in the knowledge graph
type Node struct {
	ID         uint64            // Unique identifier for the node
	Label      string            // Type or category of the node
	Properties map[string]string // Key-value pairs of node properties
}

// NewNode creates a new Node with the given ID and label
func NewNode(id uint64, label string) *Node {
	return &Node{
		ID:         id,
		Label:      label,
		Properties: make(map[string]string),
	}
}

// AddProperty adds or updates a property on the node
func (n *Node) AddProperty(key, value string) {
	n.Properties[key] = value
}

// GetProperty retrieves a property value by key
func (n *Node) GetProperty(key string) (string, bool) {
	value, exists := n.Properties[key]
	return value, exists
}

// RemoveProperty removes a property from the node
func (n *Node) RemoveProperty(key string) {
	delete(n.Properties, key)
}
