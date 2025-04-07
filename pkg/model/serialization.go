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

// Common serialization errors
var (
	ErrInvalidSerializedData = errors.New("invalid serialized data")
	ErrUnsupportedVersion    = errors.New("unsupported serialization version")
	ErrInvalidEntityType     = errors.New("invalid entity type")
	ErrInvalidHeader         = errors.New("invalid header")
	ErrBufferTooSmall        = errors.New("buffer too small")
)

// Header represents the standardized header for all serialized entities
type SerializationHeader struct {
	Magic   uint32
	Version uint16
	Type    uint8
}

// WriteHeader writes a standardized header to a buffer
func WriteHeader(buf *bytes.Buffer, entityType uint8) error {
	if err := binary.Write(buf, binary.LittleEndian, SerializationMagic); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, SerializationVersion); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, entityType); err != nil {
		return err
	}
	return nil
}

// ReadHeader reads and validates a standardized header from a reader
func ReadHeader(r io.Reader) (SerializationHeader, error) {
	var header SerializationHeader

	if err := binary.Read(r, binary.LittleEndian, &header.Magic); err != nil {
		return header, err
	}
	if header.Magic != SerializationMagic {
		return header, ErrInvalidHeader
	}

	if err := binary.Read(r, binary.LittleEndian, &header.Version); err != nil {
		return header, err
	}
	if header.Version != SerializationVersion {
		return header, ErrUnsupportedVersion
	}

	if err := binary.Read(r, binary.LittleEndian, &header.Type); err != nil {
		return header, err
	}

	return header, nil
}

// WriteString writes a length-prefixed string to a buffer
func WriteString(buf *bytes.Buffer, s string) error {
	strBytes := []byte(s)
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(strBytes))); err != nil {
		return err
	}
	_, err := buf.Write(strBytes)
	return err
}

// ReadString reads a length-prefixed string from a reader
func ReadString(r io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}

	strBytes := make([]byte, length)
	if _, err := io.ReadFull(r, strBytes); err != nil {
		return "", err
	}

	return string(strBytes), nil
}

// WriteStringMap writes a map of string key-value pairs to a buffer
func WriteStringMap(buf *bytes.Buffer, m map[string]string) error {
	// Write the count of items
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(m))); err != nil {
		return err
	}

	// Write each key-value pair
	for key, value := range m {
		if err := WriteString(buf, key); err != nil {
			return err
		}
		if err := WriteString(buf, value); err != nil {
			return err
		}
	}

	return nil
}

// ReadStringMap reads a map of string key-value pairs from a reader
func ReadStringMap(r io.Reader) (map[string]string, error) {
	var count uint16
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	m := make(map[string]string, count)
	for i := uint16(0); i < count; i++ {
		key, err := ReadString(r)
		if err != nil {
			return nil, err
		}

		value, err := ReadString(r)
		if err != nil {
			return nil, err
		}

		m[key] = value
	}

	return m, nil
}

// WriteUint64List writes a list of uint64 values to a buffer
func WriteUint64List(buf *bytes.Buffer, ids []uint64) error {
	// Write the count
	count := uint32(len(ids))
	if err := binary.Write(buf, binary.LittleEndian, count); err != nil {
		return err
	}

	// Write each ID
	for i := uint32(0); i < count; i++ {
		id := ids[i]
		if err := binary.Write(buf, binary.LittleEndian, id); err != nil {
			return err
		}
	}

	return nil
}

// ReadUint64List reads a list of uint64 values from a reader
func ReadUint64List(r io.Reader) ([]uint64, error) {
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	ids := make([]uint64, count)
	for i := uint32(0); i < count; i++ {
		if err := binary.Read(r, binary.LittleEndian, &ids[i]); err != nil {
			return nil, err
		}
	}

	return ids, nil
}

// WriteBytesList writes a list of byte slices to a buffer
func WriteBytesList(buf *bytes.Buffer, items [][]byte) error {
	// Write the count
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(items))); err != nil {
		return err
	}

	// Write each byte slice with its length prefix
	for _, item := range items {
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(item))); err != nil {
			return err
		}
		if _, err := buf.Write(item); err != nil {
			return err
		}
	}

	return nil
}

// ReadBytesList reads a list of byte slices from a reader
func ReadBytesList(r io.Reader) ([][]byte, error) {
	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	items := make([][]byte, count)
	for i := uint32(0); i < count; i++ {
		var length uint32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			return nil, err
		}

		items[i] = make([]byte, length)
		if _, err := io.ReadFull(r, items[i]); err != nil {
			return nil, err
		}
	}

	return items, nil
}

// SerializeNode serializes a Node into a binary format
func SerializeNode(node *Node) ([]byte, error) {
	if node == nil {
		return nil, errors.New("cannot serialize nil Node")
	}

	var buf bytes.Buffer

	// Write header
	if err := WriteHeader(&buf, TypeNode); err != nil {
		return nil, err
	}

	// Write node ID
	if err := binary.Write(&buf, binary.LittleEndian, node.ID); err != nil {
		return nil, err
	}

	// Write label
	if err := WriteString(&buf, node.Label); err != nil {
		return nil, err
	}

	// Write properties
	if err := node.SerializeProperties(&buf); err != nil {
		return nil, err
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
	header, err := ReadHeader(buf)
	if err != nil {
		return nil, err
	}
	if header.Type != TypeNode {
		return nil, ErrInvalidEntityType
	}

	// Read node ID
	var id uint64
	if err := binary.Read(buf, binary.LittleEndian, &id); err != nil {
		return nil, err
	}

	// Read label
	label, err := ReadString(buf)
	if err != nil {
		return nil, err
	}

	// Create node
	node := NewNode(id, label)

	// Read properties
	if err := node.DeserializeProperties(buf); err != nil {
		return nil, err
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
	if err := WriteHeader(&buf, TypeEdge); err != nil {
		return nil, err
	}

	// Write source and target IDs
	if err := binary.Write(&buf, binary.LittleEndian, edge.SourceID); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, edge.TargetID); err != nil {
		return nil, err
	}

	// Write label
	if err := WriteString(&buf, edge.Label); err != nil {
		return nil, err
	}

	// Write properties
	if err := edge.SerializeProperties(&buf); err != nil {
		return nil, err
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
	header, err := ReadHeader(buf)
	if err != nil {
		return nil, err
	}
	if header.Type != TypeEdge {
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
	label, err := ReadString(buf)
	if err != nil {
		return nil, err
	}

	// Create edge
	edge := NewEdge(sourceID, targetID, label)

	// Read properties
	if err := edge.DeserializeProperties(buf); err != nil {
		return nil, err
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
	if err := WriteHeader(&buf, TypeProperty); err != nil {
		return nil, err
	}

	// Write key and value
	if err := WriteString(&buf, property.Key); err != nil {
		return nil, err
	}
	if err := WriteString(&buf, property.Value); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DeserializeProperty deserializes a binary representation into a Property
func DeserializeProperty(data []byte) (*Property, error) {
	if len(data) < 9 { // Magic(4) + Version(2) + Type(1) + Min data(2)
		return nil, ErrInvalidSerializedData
	}

	buf := bytes.NewReader(data)

	// Read and validate header
	header, err := ReadHeader(buf)
	if err != nil {
		return nil, err
	}
	if header.Type != TypeProperty {
		return nil, ErrInvalidEntityType
	}

	// Read key
	key, err := ReadString(buf)
	if err != nil {
		return nil, err
	}

	// Read value
	value, err := ReadString(buf)
	if err != nil {
		return nil, err
	}

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

// SerializeGeneric is a generic function that serializes various types to bytes using gob encoding
func SerializeGeneric(data interface{}) ([]byte, error) {
	var buf bytes.Buffer

	// Write header
	if err := WriteHeader(&buf, 0); err != nil {
		return nil, err
	}

	// Use gob encoding for generic types
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DeserializeGeneric is a generic function that deserializes bytes into the specified type using gob encoding
func DeserializeGeneric(data []byte, out interface{}) error {
	if len(data) < 7 { // Magic(4) + Version(2) + Type(1)
		return ErrInvalidSerializedData
	}

	buf := bytes.NewReader(data)

	// Read and validate header
	_, err := ReadHeader(buf)
	if err != nil {
		return err
	}

	// Use gob decoding
	dec := gob.NewDecoder(buf)
	return dec.Decode(out)
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
		return SerializeGeneric(v)
	default:
		return nil, errors.New("unsupported type for serialization")
	}
}

// Deserialize is a generic function that deserializes bytes into the specified type
func Deserialize(data []byte, out interface{}) error {
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
		return DeserializeGeneric(data, out)
	default:
		return errors.New("unsupported type for deserialization")
	}
}

// SerializeIDs serializes a list of IDs (byte slices) using a standardized format
// This is used for node IDs, edge IDs, or other list serialization in indexes
func SerializeIDs(ids [][]byte) ([]byte, error) {
	var buf bytes.Buffer

	// Write the list to the buffer
	if err := WriteBytesList(&buf, ids); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DeserializeIDs deserializes a list of IDs (byte slices) using the standardized format
func DeserializeIDs(data []byte) ([][]byte, error) {
	if len(data) < 4 {
		return nil, errors.New("invalid ID list data: too short")
	}

	buf := bytes.NewReader(data)

	// Read the list from the buffer
	return ReadBytesList(buf)
}

// SerializeKeyPrefix creates a key with a prefix
// Used by indexes to create namespaced keys
func SerializeKeyPrefix(prefix, key []byte) []byte {
	result := make([]byte, 0, len(prefix)+len(key))
	result = append(result, prefix...)
	result = append(result, key...)
	return result
}

// SerializeCompositeKey creates a composite key with multiple components separated by a delimiter
// Format: component1:component2:component3...
func SerializeCompositeKey(components ...[]byte) []byte {
	// Calculate total size with delimiters
	totalSize := 0
	for _, comp := range components {
		totalSize += len(comp) + 1 // Add 1 for delimiter
	}
	if len(components) > 0 {
		totalSize-- // Last component doesn't need delimiter
	}

	// Build the key
	result := make([]byte, 0, totalSize)
	for i, comp := range components {
		result = append(result, comp...)
		if i < len(components)-1 {
			result = append(result, ':') // Delimiter
		}
	}

	return result
}

// SplitCompositeKey splits a composite key into its components
func SplitCompositeKey(key []byte) [][]byte {
	// Split by delimiter
	parts := bytes.Split(key, []byte{':'})
	return parts
}

// SerializeUint64 converts a uint64 to a byte slice (big-endian for better sorting)
func SerializeUint64(id uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, id)
	return buf
}

// DeserializeUint64 converts a byte slice to a uint64 (big-endian)
func DeserializeUint64(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, errors.New("invalid uint64 data: must be 8 bytes")
	}
	return binary.BigEndian.Uint64(data), nil
}

// SerializeUint32 converts a uint32 to a byte slice (big-endian for better sorting)
func SerializeUint32(id uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, id)
	return buf
}

// DeserializeUint32 converts a byte slice to a uint32 (big-endian)
func DeserializeUint32(data []byte) (uint32, error) {
	if len(data) != 4 {
		return 0, errors.New("invalid uint32 data: must be 4 bytes")
	}
	return binary.BigEndian.Uint32(data), nil
}
