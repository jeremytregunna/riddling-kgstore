package model

import (
	"bytes"
	"io"
)

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

// PropertyContainer provides a common way to handle properties across different entity types
type PropertyContainer struct {
	Properties map[string]string // Key-value pairs of properties
}

// NewPropertyContainer creates a new PropertyContainer
func NewPropertyContainer() *PropertyContainer {
	return &PropertyContainer{
		Properties: make(map[string]string),
	}
}

// AddProperty adds or updates a property
func (pc *PropertyContainer) AddProperty(key, value string) {
	pc.Properties[key] = value
}

// GetProperty retrieves a property value by key
func (pc *PropertyContainer) GetProperty(key string) (string, bool) {
	value, exists := pc.Properties[key]
	return value, exists
}

// RemoveProperty removes a property
func (pc *PropertyContainer) RemoveProperty(key string) {
	delete(pc.Properties, key)
}

// GetAllProperties returns a copy of all properties
func (pc *PropertyContainer) GetAllProperties() map[string]string {
	result := make(map[string]string, len(pc.Properties))
	for k, v := range pc.Properties {
		result[k] = v
	}
	return result
}

// SerializeProperties serializes the properties map to bytes
func (pc *PropertyContainer) SerializeProperties(buf *bytes.Buffer) error {
	return WriteStringMap(buf, pc.Properties)
}

// DeserializeProperties deserializes properties from a reader
func (pc *PropertyContainer) DeserializeProperties(r io.Reader) error {
	properties, err := ReadStringMap(r)
	if err != nil {
		return err
	}
	
	// Add each property
	for key, value := range properties {
		pc.AddProperty(key, value)
	}
	
	return nil
}
