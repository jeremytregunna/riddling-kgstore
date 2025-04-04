package model

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

const (
	// PageHeaderSize is the size of the page header in bytes
	PageHeaderSize = 16 // ID (8) + Flags (1) + Reserved (7)
)

// SerializePage serializes a Page into a byte slice
func SerializePage(page *Page) ([]byte, error) {
	if page == nil {
		return nil, errors.New("cannot serialize nil Page")
	}

	// Calculate total size needed for serialization
	dataSize := len(page.Data)
	totalSize := PageHeaderSize + dataSize

	// Create the buffer
	buffer := make([]byte, totalSize)

	// Write the page ID
	binary.LittleEndian.PutUint64(buffer[0:8], page.ID)

	// Write the flags
	buffer[8] = byte(page.Flags)

	// Reserved bytes (9-15) are left as zeros

	// Copy the data
	copy(buffer[PageHeaderSize:], page.Data)

	return buffer, nil
}

// DeserializePage deserializes a byte slice into a Page
func DeserializePage(data []byte) (*Page, error) {
	if len(data) < PageHeaderSize {
		return nil, errors.New("data too small to contain a valid page")
	}

	// Read the page ID
	id := binary.LittleEndian.Uint64(data[0:8])

	// Read the flags
	flags := PageFlag(data[8])

	// Create the page with the correct size
	dataSize := len(data) - PageHeaderSize
	page := NewPage(id, dataSize)
	page.Flags = flags

	// Copy the data
	copy(page.Data, data[PageHeaderSize:])

	return page, nil
}

// WriteEntityToPage writes an entity (Node, Edge, Property) to a page at the specified offset
func WriteEntityToPage(page *Page, entity interface{}, offset int) (int, error) {
	if page == nil {
		return 0, errors.New("page is nil")
	}

	if offset < 0 || offset >= len(page.Data) {
		return 0, errors.New("offset out of range")
	}

	// Serialize the entity
	entityData, err := SerializeToBytesForPage(entity)
	if err != nil {
		return 0, err
	}

	// Check if there's enough space in the page
	if offset+len(entityData) > len(page.Data) {
		return 0, ErrPageDataSizeExceeded{
			DataSize: len(entityData),
			PageSize: len(page.Data) - offset,
		}
	}

	// Copy the serialized entity to the page
	copy(page.Data[offset:], entityData)

	// Mark the page as dirty
	page.MarkDirty()

	// Return the number of bytes written
	return len(entityData), nil
}

// ReadEntityFromPage reads an entity from a page at the specified offset
func ReadEntityFromPage(page *Page, offset int) (interface{}, int, error) {
	if page == nil {
		return nil, 0, errors.New("page is nil")
	}

	if offset < 0 || offset >= len(page.Data) {
		return nil, 0, errors.New("offset out of range")
	}

	// Check if there's enough data for a valid header
	if offset+7 > len(page.Data) { // Magic(4) + Version(2) + Type(1)
		return nil, 0, errors.New("not enough data for entity header")
	}

	// Read the entity type to determine its size
	entityType := page.Data[offset+6]

	// Create a buffer to read the data
	buf := bytes.NewReader(page.Data[offset:])

	// Skip to after the header
	if _, err := buf.Seek(7, io.SeekStart); err != nil {
		return nil, 0, err
	}

	var entitySize int

	switch entityType {
	case TypeNode:
		// Read node ID (8 bytes)
		if _, err := buf.Seek(8, io.SeekCurrent); err != nil {
			return nil, 0, err
		}

		// Read label length (2 bytes)
		var labelLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &labelLen); err != nil {
			return nil, 0, err
		}

		// Skip label
		if _, err := buf.Seek(int64(labelLen), io.SeekCurrent); err != nil {
			return nil, 0, err
		}

		// Read property count (2 bytes)
		var propCount uint16
		if err := binary.Read(buf, binary.LittleEndian, &propCount); err != nil {
			return nil, 0, err
		}

		// For each property, read key length, key, value length, value
		for i := uint16(0); i < propCount; i++ {
			var keyLen, valueLen uint16
			if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
				return nil, 0, err
			}
			if _, err := buf.Seek(int64(keyLen), io.SeekCurrent); err != nil {
				return nil, 0, err
			}
			if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
				return nil, 0, err
			}
			if _, err := buf.Seek(int64(valueLen), io.SeekCurrent); err != nil {
				return nil, 0, err
			}
		}

	case TypeEdge:
		// Read source ID and target ID (16 bytes)
		if _, err := buf.Seek(16, io.SeekCurrent); err != nil {
			return nil, 0, err
		}

		// Read label length (2 bytes)
		var labelLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &labelLen); err != nil {
			return nil, 0, err
		}

		// Skip label
		if _, err := buf.Seek(int64(labelLen), io.SeekCurrent); err != nil {
			return nil, 0, err
		}

		// Read property count (2 bytes)
		var propCount uint16
		if err := binary.Read(buf, binary.LittleEndian, &propCount); err != nil {
			return nil, 0, err
		}

		// For each property, read key length, key, value length, value
		for i := uint16(0); i < propCount; i++ {
			var keyLen, valueLen uint16
			if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
				return nil, 0, err
			}
			if _, err := buf.Seek(int64(keyLen), io.SeekCurrent); err != nil {
				return nil, 0, err
			}
			if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
				return nil, 0, err
			}
			if _, err := buf.Seek(int64(valueLen), io.SeekCurrent); err != nil {
				return nil, 0, err
			}
		}

	case TypeProperty:
		// Read key length (2 bytes)
		var keyLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return nil, 0, err
		}

		// Skip key
		if _, err := buf.Seek(int64(keyLen), io.SeekCurrent); err != nil {
			return nil, 0, err
		}

		// Read value length (2 bytes)
		var valueLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
			return nil, 0, err
		}

		// Skip value
		if _, err := buf.Seek(int64(valueLen), io.SeekCurrent); err != nil {
			return nil, 0, err
		}

	default:
		return nil, 0, ErrInvalidEntityType
	}

	// Calculate entity size based on how far we've read
	entitySize = int(buf.Size()) - int(buf.Len())

	// Deserialize the entity
	entity, err := DeserializeFromPageBytes(page.Data[offset : offset+entitySize])
	if err != nil {
		return nil, 0, err
	}

	return entity, entitySize, nil
}