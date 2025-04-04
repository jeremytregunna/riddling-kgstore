package model

// Property represents a key-value pair in the knowledge graph
// This is a separate type to allow for different value types in the future
type Property struct {
	Key   string // The property key
	Value string // The property value (string for now)
}

// NewProperty creates a new Property with the given key and value
func NewProperty(key, value string) *Property {
	return &Property{
		Key:   key,
		Value: value,
	}
}

// GetKey returns the property's key
func (p *Property) GetKey() string {
	return p.Key
}

// GetValue returns the property's value
func (p *Property) GetValue() string {
	return p.Value
}

// SetValue updates the property's value
func (p *Property) SetValue(value string) {
	p.Value = value
}
