package model

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
)

const (
	// Magic bytes that identify our serialized format
	SerializationMagic uint32 = 0x4B4753DB // "KGSDB"

	// Version of the serialization format
	SerializationVersion uint16 = 1

	// Type constants for serialized entities
	TypeNode     uint8 = 1
	TypeEdge     uint8 = 2
	TypeProperty uint8 = 3
)

// ErrInvalidSerializedData is returned when attempting to deserialize invalid data
var ErrInvalidSerializedData = errors.New("invalid serialized data")

// ErrUnsupportedVersion is returned when attempting to deserialize data with an unsupported version
var ErrUnsupportedVersion = errors.New("unsupported serialization version")

// ErrInvalidEntityType is returned when encountering an invalid entity type during deserialization
var ErrInvalidEntityType = errors.New("invalid entity type")

// SerializeNode serializes a Node into a binary format
func SerializeNode(node *Node) ([]byte, error) {
	if node == nil {
		return nil, errors.New("cannot serialize nil Node")
	}

	var buf bytes.Buffer

	// Write header
	binary.Write(&buf, binary.LittleEndian, SerializationMagic)
	binary.Write(&buf, binary.LittleEndian, SerializationVersion)
	binary.Write(&buf, binary.LittleEndian, TypeNode)

	// Write node ID and label
	binary.Write(&buf, binary.LittleEndian, node.ID)

	// Write label length and label bytes
	labelBytes := []byte(node.Label)
	binary.Write(&buf, binary.LittleEndian, uint16(len(labelBytes)))
	buf.Write(labelBytes)

	// Write property count
	binary.Write(&buf, binary.LittleEndian, uint16(len(node.Properties)))

	// Write each property
	for key, value := range node.Properties {
		keyBytes := []byte(key)
		valueBytes := []byte(value)

		binary.Write(&buf, binary.LittleEndian, uint16(len(keyBytes)))
		buf.Write(keyBytes)

		binary.Write(&buf, binary.LittleEndian, uint16(len(valueBytes)))
		buf.Write(valueBytes)
	}

	return buf.Bytes(), nil
}

// DeserializeNode deserializes a binary representation into a Node
func DeserializeNode(data []byte) (*Node, error) {
	if len(data) < 9 { // Magic(4) + Version(2) + Type(1) + Min data(2)
		return nil, ErrInvalidSerializedData
	}

	buf := bytes.NewReader(data)

	// Read and validate header
	var magic uint32
	var version uint16
	var entityType uint8

	if err := binary.Read(buf, binary.LittleEndian, &magic); err != nil {
		return nil, err
	}
	if magic != SerializationMagic {
		return nil, ErrInvalidSerializedData
	}

	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return nil, err
	}
	if version != SerializationVersion {
		return nil, ErrUnsupportedVersion
	}

	if err := binary.Read(buf, binary.LittleEndian, &entityType); err != nil {
		return nil, err
	}
	if entityType != TypeNode {
		return nil, ErrInvalidEntityType
	}

	// Read node ID
	var id uint64
	if err := binary.Read(buf, binary.LittleEndian, &id); err != nil {
		return nil, err
	}

	// Read label
	var labelLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &labelLen); err != nil {
		return nil, err
	}

	labelBytes := make([]byte, labelLen)
	if _, err := io.ReadFull(buf, labelBytes); err != nil {
		return nil, err
	}
	label := string(labelBytes)

	// Create node
	node := NewNode(id, label)

	// Read properties
	var propCount uint16
	if err := binary.Read(buf, binary.LittleEndian, &propCount); err != nil {
		return nil, err
	}

	for i := uint16(0); i < propCount; i++ {
		// Read key
		var keyLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(buf, keyBytes); err != nil {
			return nil, err
		}
		key := string(keyBytes)

		// Read value
		var valueLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
			return nil, err
		}

		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(buf, valueBytes); err != nil {
			return nil, err
		}
		value := string(valueBytes)

		// Add property to node
		node.AddProperty(key, value)
	}

	return node, nil
}

// SerializeEdge serializes an Edge into a binary format
func SerializeEdge(edge *Edge) ([]byte, error) {
	if edge == nil {
		return nil, errors.New("cannot serialize nil Edge")
	}

	var buf bytes.Buffer

	// Write header
	binary.Write(&buf, binary.LittleEndian, SerializationMagic)
	binary.Write(&buf, binary.LittleEndian, SerializationVersion)
	binary.Write(&buf, binary.LittleEndian, TypeEdge)

	// Write source and target IDs
	binary.Write(&buf, binary.LittleEndian, edge.SourceID)
	binary.Write(&buf, binary.LittleEndian, edge.TargetID)

	// Write label length and label bytes
	labelBytes := []byte(edge.Label)
	binary.Write(&buf, binary.LittleEndian, uint16(len(labelBytes)))
	buf.Write(labelBytes)

	// Write property count
	binary.Write(&buf, binary.LittleEndian, uint16(len(edge.Properties)))

	// Write each property
	for key, value := range edge.Properties {
		keyBytes := []byte(key)
		valueBytes := []byte(value)

		binary.Write(&buf, binary.LittleEndian, uint16(len(keyBytes)))
		buf.Write(keyBytes)

		binary.Write(&buf, binary.LittleEndian, uint16(len(valueBytes)))
		buf.Write(valueBytes)
	}

	return buf.Bytes(), nil
}

// DeserializeEdge deserializes a binary representation into an Edge
func DeserializeEdge(data []byte) (*Edge, error) {
	if len(data) < 17 { // Magic(4) + Version(2) + Type(1) + SourceID(8) + TargetID(8) + Min data(2)
		return nil, ErrInvalidSerializedData
	}

	buf := bytes.NewReader(data)

	// Read and validate header
	var magic uint32
	var version uint16
	var entityType uint8

	if err := binary.Read(buf, binary.LittleEndian, &magic); err != nil {
		return nil, err
	}
	if magic != SerializationMagic {
		return nil, ErrInvalidSerializedData
	}

	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return nil, err
	}
	if version != SerializationVersion {
		return nil, ErrUnsupportedVersion
	}

	if err := binary.Read(buf, binary.LittleEndian, &entityType); err != nil {
		return nil, err
	}
	if entityType != TypeEdge {
		return nil, ErrInvalidEntityType
	}

	// Read source and target IDs
	var sourceID, targetID uint64
	if err := binary.Read(buf, binary.LittleEndian, &sourceID); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &targetID); err != nil {
		return nil, err
	}

	// Read label
	var labelLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &labelLen); err != nil {
		return nil, err
	}

	labelBytes := make([]byte, labelLen)
	if _, err := io.ReadFull(buf, labelBytes); err != nil {
		return nil, err
	}
	label := string(labelBytes)

	// Create edge
	edge := NewEdge(sourceID, targetID, label)

	// Read properties
	var propCount uint16
	if err := binary.Read(buf, binary.LittleEndian, &propCount); err != nil {
		return nil, err
	}

	for i := uint16(0); i < propCount; i++ {
		// Read key
		var keyLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(buf, keyBytes); err != nil {
			return nil, err
		}
		key := string(keyBytes)

		// Read value
		var valueLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
			return nil, err
		}

		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(buf, valueBytes); err != nil {
			return nil, err
		}
		value := string(valueBytes)

		// Add property to edge
		edge.AddProperty(key, value)
	}

	return edge, nil
}

// SerializeProperty serializes a Property into a binary format
func SerializeProperty(property *Property) ([]byte, error) {
	if property == nil {
		return nil, errors.New("cannot serialize nil Property")
	}

	var buf bytes.Buffer

	// Write header
	binary.Write(&buf, binary.LittleEndian, SerializationMagic)
	binary.Write(&buf, binary.LittleEndian, SerializationVersion)
	binary.Write(&buf, binary.LittleEndian, TypeProperty)

	// Write key length and key bytes
	keyBytes := []byte(property.Key)
	binary.Write(&buf, binary.LittleEndian, uint16(len(keyBytes)))
	buf.Write(keyBytes)

	// Write value length and value bytes
	valueBytes := []byte(property.Value)
	binary.Write(&buf, binary.LittleEndian, uint16(len(valueBytes)))
	buf.Write(valueBytes)

	return buf.Bytes(), nil
}

// DeserializeProperty deserializes a binary representation into a Property
func DeserializeProperty(data []byte) (*Property, error) {
	if len(data) < 9 { // Magic(4) + Version(2) + Type(1) + Min data(2)
		return nil, ErrInvalidSerializedData
	}

	buf := bytes.NewReader(data)

	// Read and validate header
	var magic uint32
	var version uint16
	var entityType uint8

	if err := binary.Read(buf, binary.LittleEndian, &magic); err != nil {
		return nil, err
	}
	if magic != SerializationMagic {
		return nil, ErrInvalidSerializedData
	}

	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return nil, err
	}
	if version != SerializationVersion {
		return nil, ErrUnsupportedVersion
	}

	if err := binary.Read(buf, binary.LittleEndian, &entityType); err != nil {
		return nil, err
	}
	if entityType != TypeProperty {
		return nil, ErrInvalidEntityType
	}

	// Read key
	var keyLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
		return nil, err
	}

	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(buf, keyBytes); err != nil {
		return nil, err
	}
	key := string(keyBytes)

	// Read value
	var valueLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
		return nil, err
	}

	valueBytes := make([]byte, valueLen)
	if _, err := io.ReadFull(buf, valueBytes); err != nil {
		return nil, err
	}
	value := string(valueBytes)

	// Create property
	return NewProperty(key, value), nil
}

// SerializeToBytesForPage serializes an entity (Node, Edge, Property) to bytes for storing in a Page
func SerializeToBytesForPage(entity interface{}) ([]byte, error) {
	switch e := entity.(type) {
	case *Node:
		return SerializeNode(e)
	case *Edge:
		return SerializeEdge(e)
	case *Property:
		return SerializeProperty(e)
	default:
		return nil, errors.New("unsupported entity type for serialization")
	}
}

// DeserializeFromPageBytes deserializes bytes from a Page into the appropriate entity
func DeserializeFromPageBytes(data []byte) (interface{}, error) {
	if len(data) < 7 { // Magic(4) + Version(2) + Type(1)
		return nil, ErrInvalidSerializedData
	}

	// Read entity type without consuming the data
	buf := bytes.NewReader(data)

	// Skip magic and version
	if _, err := buf.Seek(6, io.SeekStart); err != nil {
		return nil, err
	}

	// Read entity type
	var entityType uint8
	if err := binary.Read(buf, binary.LittleEndian, &entityType); err != nil {
		return nil, err
	}

	// Deserialize based on entity type
	switch entityType {
	case TypeNode:
		return DeserializeNode(data)
	case TypeEdge:
		return DeserializeEdge(data)
	case TypeProperty:
		return DeserializeProperty(data)
	default:
		return nil, ErrInvalidEntityType
	}
}

// Serialize is a generic function that serializes various types to bytes
func Serialize(data interface{}) ([]byte, error) {
	switch v := data.(type) {
	case *Node:
		return SerializeNode(v)
	case Node:
		return SerializeNode(&v)
	case *Edge:
		return SerializeEdge(v)
	case Edge:
		return SerializeEdge(&v)
	case *Property:
		return SerializeProperty(v)
	case Property:
		return SerializeProperty(&v)
	case []uint64, []string, map[string]string:
		// Generic binary encoding for other types
		var buf bytes.Buffer
		if err := binary.Write(&buf, binary.LittleEndian, SerializationMagic); err != nil {
			return nil, err
		}
		if err := binary.Write(&buf, binary.LittleEndian, SerializationVersion); err != nil {
			return nil, err
		}

		// Use gob encoding for generic types
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(v); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	default:
		return nil, errors.New("unsupported type for serialization")
	}
}

// Deserialize is a generic function that deserializes bytes into the specified type
func Deserialize(data []byte, out interface{}) error {
	if len(data) < 6 { // Magic(4) + Version(2)
		return ErrInvalidSerializedData
	}

	// Read and validate magic and version
	buf := bytes.NewReader(data)
	var magic uint32
	var version uint16

	if err := binary.Read(buf, binary.LittleEndian, &magic); err != nil {
		return err
	}
	if magic != SerializationMagic {
		return ErrInvalidSerializedData
	}

	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return err
	}
	if version != SerializationVersion {
		return ErrUnsupportedVersion
	}

	// Handle different types based on the output parameter
	switch out := out.(type) {
	case *Node:
		node, err := DeserializeNode(data)
		if err != nil {
			return err
		}
		*out = *node
		return nil
	case *Edge:
		edge, err := DeserializeEdge(data)
		if err != nil {
			return err
		}
		*out = *edge
		return nil
	case *Property:
		property, err := DeserializeProperty(data)
		if err != nil {
			return err
		}
		*out = *property
		return nil
	case *[]uint64, *[]string, *map[string]string:
		// Generic binary decoding for other types
		// Skip the header (already validated)
		dec := gob.NewDecoder(bytes.NewReader(data[6:]))
		return dec.Decode(out)
	default:
		return errors.New("unsupported type for deserialization")
	}
}
