package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// SSTable errors
var (
	ErrSSTableCorrupted  = errors.New("SSTable file is corrupted")
	ErrKeyNotFoundInSSTable = errors.New("key not found in SSTable")
	ErrInvalidSSTableFormat = errors.New("invalid SSTable format")
	ErrIOOperation = errors.New("IO operation failed")
)

// SSTableMagic is a magic number that identifies an SSTable file
const SSTableMagic uint32 = 0x5354424C // "STBL"

// SSTableVersion is the current version of the SSTable format
const SSTableVersion uint16 = 1

// SSTable is an immutable Sorted String Table stored on disk
// It consists of a data file containing key-value pairs and an index
// file that allows for binary search lookups
type SSTable struct {
	id         uint64           // Unique identifier for this SSTable
	path       string           // Path to the SSTable directory
	dataFile   string           // Path to the data file
	indexFile  string           // Path to the index file
	filterFile string           // Path to the bloom filter file
	logger     model.Logger     // Logger for SSTable operations
	comparator Comparator       // Comparator for key comparison
	mu         sync.RWMutex     // Mutex for concurrent access
	isOpen     bool             // Whether the SSTable is open
	
	// Meta information
	keyCount   uint32           // Number of key-value pairs in the SSTable
	dataSize   uint64           // Size of the data file in bytes
	minKey     []byte           // Minimum key in the SSTable
	maxKey     []byte           // Maximum key in the SSTable
	
	// Index cache (loaded on demand)
	indexCache map[string]uint64
}

// SSTableHeader represents the header of an SSTable data file
type SSTableHeader struct {
	Magic      uint32    // Magic number to identify SSTable files
	Version    uint16    // Version of the SSTable format
	KeyCount   uint32    // Number of key-value pairs
	MinKeyLen  uint16    // Length of the minimum key
	MaxKeyLen  uint16    // Length of the maximum key
	MinKey     []byte    // Minimum key in the SSTable
	MaxKey     []byte    // Maximum key in the SSTable
}

// SSTableConfig holds configuration options for creating an SSTable
type SSTableConfig struct {
	ID         uint64         // Unique identifier for this SSTable
	Path       string         // Directory where SSTable files will be stored
	Logger     model.Logger   // Logger for SSTable operations
	Comparator Comparator     // Comparator for key comparison
}

// CreateSSTable creates a new SSTable from a MemTable
func CreateSSTable(config SSTableConfig, memTable *MemTable) (*SSTable, error) {
	if memTable == nil {
		return nil, errors.New("cannot create SSTable from nil MemTable")
	}
	
	if config.Logger == nil {
		config.Logger = model.DefaultLoggerInstance
	}
	
	if config.Comparator == nil {
		config.Comparator = DefaultComparator
	}
	
	// Ensure the directory exists
	if err := os.MkdirAll(config.Path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create SSTable directory: %w", err)
	}
	
	// Create file paths
	dataFile := filepath.Join(config.Path, fmt.Sprintf("%d.data", config.ID))
	indexFile := filepath.Join(config.Path, fmt.Sprintf("%d.index", config.ID))
	filterFile := filepath.Join(config.Path, fmt.Sprintf("%d.filter", config.ID))
	
	// Create a new SSTable
	sst := &SSTable{
		id:         config.ID,
		path:       config.Path,
		dataFile:   dataFile,
		indexFile:  indexFile,
		filterFile: filterFile,
		logger:     config.Logger,
		comparator: config.Comparator,
		isOpen:     false,
		indexCache: make(map[string]uint64),
	}
	
	// Get entries from the MemTable
	entries := memTable.GetEntries()
	if len(entries) == 0 {
		// Create empty SSTable files
		sst.createEmptySSTable()
		sst.logger.Info("Created empty SSTable with ID %d", config.ID)
		return sst, nil
	}
	
	// Build the SSTable files
	if err := sst.buildFromEntries(entries); err != nil {
		// Clean up files in case of error
		os.Remove(dataFile)
		os.Remove(indexFile)
		os.Remove(filterFile)
		return nil, fmt.Errorf("failed to build SSTable: %w", err)
	}
	
	sst.logger.Info("Created SSTable with ID %d, %d keys, %d bytes", 
		config.ID, sst.keyCount, sst.dataSize)
	
	return sst, nil
}

// OpenSSTable opens an existing SSTable
func OpenSSTable(config SSTableConfig) (*SSTable, error) {
	// Create file paths
	dataFile := filepath.Join(config.Path, fmt.Sprintf("%d.data", config.ID))
	indexFile := filepath.Join(config.Path, fmt.Sprintf("%d.index", config.ID))
	filterFile := filepath.Join(config.Path, fmt.Sprintf("%d.filter", config.ID))
	
	// Check if files exist
	if _, err := os.Stat(dataFile); err != nil {
		return nil, fmt.Errorf("data file not found: %w", err)
	}
	if _, err := os.Stat(indexFile); err != nil {
		return nil, fmt.Errorf("index file not found: %w", err)
	}
	
	// Create a new SSTable
	sst := &SSTable{
		id:         config.ID,
		path:       config.Path,
		dataFile:   dataFile,
		indexFile:  indexFile,
		filterFile: filterFile,
		logger:     config.Logger,
		comparator: config.Comparator,
		isOpen:     true,
		indexCache: make(map[string]uint64),
	}
	
	// Read header to get metadata
	if err := sst.readHeader(); err != nil {
		return nil, fmt.Errorf("failed to read SSTable header: %w", err)
	}
	
	return sst, nil
}

// Get retrieves a value from the SSTable by key
func (sst *SSTable) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, ErrNilKey
	}
	
	sst.mu.RLock()
	defer sst.mu.RUnlock()
	
	if !sst.isOpen {
		return nil, errors.New("SSTable is not open")
	}
	
	// Check if key is within range of this SSTable
	if (sst.minKey != nil && sst.comparator(key, sst.minKey) < 0) ||
	   (sst.maxKey != nil && sst.comparator(key, sst.maxKey) > 0) {
		return nil, ErrKeyNotFoundInSSTable
	}
	
	// Look up key in index
	offset, err := sst.findKeyInIndex(key)
	if err != nil {
		return nil, err
	}
	
	// Read value from data file at offset
	value, err := sst.readValueAtOffset(offset)
	if err != nil {
		return nil, err
	}
	
	return value, nil
}

// Contains checks if a key exists in the SSTable
func (sst *SSTable) Contains(key []byte) (bool, error) {
	if key == nil {
		return false, nil
	}
	
	sst.mu.RLock()
	defer sst.mu.RUnlock()
	
	if !sst.isOpen {
		return false, errors.New("SSTable is not open")
	}
	
	// Check if key is within range of this SSTable
	if (sst.minKey != nil && sst.comparator(key, sst.minKey) < 0) ||
	   (sst.maxKey != nil && sst.comparator(key, sst.maxKey) > 0) {
		return false, nil
	}
	
	// Look up key in index
	_, err := sst.findKeyInIndex(key)
	if err == ErrKeyNotFoundInSSTable {
		return false, nil
	} else if err != nil {
		return false, err
	}
	
	return true, nil
}

// Close closes the SSTable
func (sst *SSTable) Close() error {
	sst.mu.Lock()
	defer sst.mu.Unlock()
	
	sst.isOpen = false
	sst.indexCache = make(map[string]uint64)
	
	return nil
}

// ID returns the ID of the SSTable
func (sst *SSTable) ID() uint64 {
	return sst.id
}

// KeyCount returns the number of keys in the SSTable
func (sst *SSTable) KeyCount() uint32 {
	return sst.keyCount
}

// Size returns the size of the SSTable data file in bytes
func (sst *SSTable) Size() uint64 {
	return sst.dataSize
}

// MinKey returns the minimum key in the SSTable
func (sst *SSTable) MinKey() []byte {
	if sst.minKey == nil {
		return nil
	}
	return append([]byte{}, sst.minKey...)
}

// MaxKey returns the maximum key in the SSTable
func (sst *SSTable) MaxKey() []byte {
	if sst.maxKey == nil {
		return nil
	}
	return append([]byte{}, sst.maxKey...)
}

// buildFromEntries builds the SSTable files from a set of key-value entries
func (sst *SSTable) buildFromEntries(entries [][]byte) error {
	// Ensure entries is not empty and contains key-value pairs
	if len(entries) == 0 || len(entries)%2 != 0 {
		return errors.New("invalid entries: must be non-empty and contain key-value pairs")
	}
	
	// Create data file
	dataFile, err := os.Create(sst.dataFile)
	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}
	defer dataFile.Close()
	
	// Create index file
	indexFile, err := os.Create(sst.indexFile)
	if err != nil {
		return fmt.Errorf("failed to create index file: %w", err)
	}
	defer indexFile.Close()
	
	// Create buffered writers
	dataWriter := bufio.NewWriter(dataFile)
	indexWriter := bufio.NewWriter(indexFile)
	
	// Reserve space for header in data file
	headerSize := 14 // Magic(4) + Version(2) + KeyCount(4) + MinKeyLen(2) + MaxKeyLen(2)
	dataOffset := uint64(headerSize)
	
	// Get min and max keys
	minKey := entries[0]
	maxKey := entries[0]
	
	for i := 0; i < len(entries); i += 2 {
		key := entries[i]
		if sst.comparator(key, minKey) < 0 {
			minKey = key
		}
		if sst.comparator(key, maxKey) > 0 {
			maxKey = key
		}
	}
	
	// Adjust header size to include min and max keys
	dataOffset += uint64(len(minKey) + len(maxKey))
	
	// Write placeholder header (will be updated later)
	for i := 0; i < int(dataOffset); i++ {
		dataWriter.WriteByte(0)
	}
	
	// Write key-value pairs to data file and build index
	keyCount := uint32(0)
	
	for i := 0; i < len(entries); i += 2 {
		key := entries[i]
		value := entries[i+1]
		
		// Write index entry: key length, key, data offset
		binary.Write(indexWriter, binary.LittleEndian, uint16(len(key)))
		indexWriter.Write(key)
		binary.Write(indexWriter, binary.LittleEndian, dataOffset)
		
		// Write data entry: key length, key, value length, value
		binary.Write(dataWriter, binary.LittleEndian, uint16(len(key)))
		dataWriter.Write(key)
		binary.Write(dataWriter, binary.LittleEndian, uint32(len(value)))
		dataWriter.Write(value)
		
		// Update offset - make sure all values are uint64 to avoid type mismatches
		dataOffset += uint64(2) + uint64(len(key)) + uint64(4) + uint64(len(value))
		keyCount++
	}
	
	// Flush writers
	indexWriter.Flush()
	
	// Rewrite header with actual data
	dataFile.Seek(0, io.SeekStart)
	
	// Write magic and version
	binary.Write(dataFile, binary.LittleEndian, SSTableMagic)
	binary.Write(dataFile, binary.LittleEndian, SSTableVersion)
	
	// Write key count
	binary.Write(dataFile, binary.LittleEndian, keyCount)
	
	// Write min key length and min key
	binary.Write(dataFile, binary.LittleEndian, uint16(len(minKey)))
	binary.Write(dataFile, binary.LittleEndian, uint16(len(maxKey)))
	dataFile.Write(minKey)
	dataFile.Write(maxKey)
	
	// Flush data writer
	dataWriter.Flush()
	
	// Update SSTable metadata
	sst.keyCount = keyCount
	sst.dataSize = dataOffset
	sst.minKey = append([]byte{}, minKey...)
	sst.maxKey = append([]byte{}, maxKey...)
	sst.isOpen = true
	
	return nil
}

// createEmptySSTable creates empty SSTable files
func (sst *SSTable) createEmptySSTable() error {
	// Create data file
	dataFile, err := os.Create(sst.dataFile)
	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}
	defer dataFile.Close()
	
	// Create index file
	indexFile, err := os.Create(sst.indexFile)
	if err != nil {
		return fmt.Errorf("failed to create index file: %w", err)
	}
	defer indexFile.Close()
	
	// Write header to data file
	binary.Write(dataFile, binary.LittleEndian, SSTableMagic)
	binary.Write(dataFile, binary.LittleEndian, SSTableVersion)
	binary.Write(dataFile, binary.LittleEndian, uint32(0)) // Key count
	binary.Write(dataFile, binary.LittleEndian, uint16(0)) // Min key length
	binary.Write(dataFile, binary.LittleEndian, uint16(0)) // Max key length
	
	// Update SSTable metadata
	sst.keyCount = 0
	sst.dataSize = 14 // Size of header
	sst.minKey = nil
	sst.maxKey = nil
	sst.isOpen = true
	
	return nil
}

// readHeader reads the SSTable header from the data file
func (sst *SSTable) readHeader() error {
	// Open data file
	dataFile, err := os.Open(sst.dataFile)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer dataFile.Close()
	
	// Read magic and version
	var magic uint32
	var version uint16
	
	if err := binary.Read(dataFile, binary.LittleEndian, &magic); err != nil {
		return fmt.Errorf("failed to read magic: %w", err)
	}
	
	if magic != SSTableMagic {
		return ErrInvalidSSTableFormat
	}
	
	if err := binary.Read(dataFile, binary.LittleEndian, &version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	
	if version != SSTableVersion {
		return ErrInvalidSSTableFormat
	}
	
	// Read key count
	var keyCount uint32
	if err := binary.Read(dataFile, binary.LittleEndian, &keyCount); err != nil {
		return fmt.Errorf("failed to read key count: %w", err)
	}
	
	// Read min and max key lengths
	var minKeyLen, maxKeyLen uint16
	if err := binary.Read(dataFile, binary.LittleEndian, &minKeyLen); err != nil {
		return fmt.Errorf("failed to read min key length: %w", err)
	}
	
	if err := binary.Read(dataFile, binary.LittleEndian, &maxKeyLen); err != nil {
		return fmt.Errorf("failed to read max key length: %w", err)
	}
	
	// Read min and max keys if they exist
	if keyCount > 0 {
		if minKeyLen > 0 {
			minKey := make([]byte, minKeyLen)
			if _, err := io.ReadFull(dataFile, minKey); err != nil {
				return fmt.Errorf("failed to read min key: %w", err)
			}
			sst.minKey = minKey
		}
		
		if maxKeyLen > 0 {
			maxKey := make([]byte, maxKeyLen)
			if _, err := io.ReadFull(dataFile, maxKey); err != nil {
				return fmt.Errorf("failed to read max key: %w", err)
			}
			sst.maxKey = maxKey
		}
	}
	
	// Get file size
	fileInfo, err := dataFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	
	// Update SSTable metadata
	sst.keyCount = keyCount
	sst.dataSize = uint64(fileInfo.Size())
	
	return nil
}

// findKeyInIndex finds a key in the SSTable index
// Returns the offset in the data file where the key's value is stored
func (sst *SSTable) findKeyInIndex(key []byte) (uint64, error) {
	// Check cache first
	keyStr := string(key)
	if offset, ok := sst.indexCache[keyStr]; ok {
		return offset, nil
	}
	
	// Open index file
	indexFile, err := os.Open(sst.indexFile)
	if err != nil {
		return 0, fmt.Errorf("failed to open index file: %w", err)
	}
	defer indexFile.Close()
	
	// Read the entire index into memory for binary search
	indexData, err := io.ReadAll(indexFile)
	if err != nil {
		return 0, fmt.Errorf("failed to read index file: %w", err)
	}
	
	// If the index is empty or just has a header, there are no entries
	if len(indexData) <= 2 {
		return 0, ErrKeyNotFoundInSSTable
	}
	
	// Binary search for the key
	var offset uint64
	var found bool
	
	// Start at the beginning of the index
	pos := 0
	for pos < len(indexData) {
		// Make sure there's enough data for key length
		if pos+2 > len(indexData) {
			break
		}
		
		keyLen := binary.LittleEndian.Uint16(indexData[pos:])
		pos += 2
		
		// Make sure there's enough data for the key
		if pos+int(keyLen) > len(indexData) {
			break
		}
		
		indexKey := indexData[pos : pos+int(keyLen)]
		pos += int(keyLen)
		
		// Make sure there's enough data for the offset
		if pos+8 > len(indexData) {
			break
		}
		
		indexOffset := binary.LittleEndian.Uint64(indexData[pos:])
		pos += 8
		
		// Compare keys
		cmp := sst.comparator(key, indexKey)
		if cmp == 0 {
			// Key found
			offset = indexOffset
			found = true
			
			// Cache the result
			sst.indexCache[keyStr] = offset
			
			break
		}
	}
	
	if !found {
		return 0, ErrKeyNotFoundInSSTable
	}
	
	return offset, nil
}

// readValueAtOffset reads a value from the data file at the specified offset
func (sst *SSTable) readValueAtOffset(offset uint64) ([]byte, error) {
	// Open data file
	dataFile, err := os.Open(sst.dataFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}
	defer dataFile.Close()
	
	// Get file size to make sure we don't read past the end
	fileInfo, err := dataFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}
	
	fileSize := fileInfo.Size()
	if int64(offset) >= fileSize {
		return nil, fmt.Errorf("offset %d is beyond file size %d", offset, fileSize)
	}
	
	// Seek to the offset
	if _, err := dataFile.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d: %w", offset, err)
	}
	
	// Read key length
	var keyLen uint16
	if err := binary.Read(dataFile, binary.LittleEndian, &keyLen); err != nil {
		return nil, fmt.Errorf("failed to read key length: %w", err)
	}
	
	// Validate key length
	if int64(offset) + int64(2) + int64(keyLen) >= fileSize {
		return nil, fmt.Errorf("key length %d would read past end of file", keyLen)
	}
	
	// Skip key
	if _, err := dataFile.Seek(int64(keyLen), io.SeekCurrent); err != nil {
		return nil, fmt.Errorf("failed to skip key: %w", err)
	}
	
	// Read value length
	var valueLen uint32
	if err := binary.Read(dataFile, binary.LittleEndian, &valueLen); err != nil {
		return nil, fmt.Errorf("failed to read value length: %w", err)
	}
	
	// Validate value length
	currentPos, err := dataFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to get current position: %w", err)
	}
	
	if currentPos + int64(valueLen) > fileSize {
		return nil, fmt.Errorf("value length %d would read past end of file", valueLen)
	}
	
	// For safety, limit value length to a reasonable size
	if valueLen > 10*1024*1024 { // 10MB max
		return nil, fmt.Errorf("value length %d exceeds maximum allowed size", valueLen)
	}
	
	// Read value
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(dataFile, value); err != nil {
		return nil, fmt.Errorf("failed to read value: %w", err)
	}
	
	return value, nil
}

// Iterator returns an iterator for the SSTable
func (sst *SSTable) Iterator() (*SSTableIterator, error) {
	// Check if SSTable is open
	if !sst.isOpen {
		return nil, errors.New("SSTable is not open")
	}
	
	// Create and initialize an iterator
	iter := &SSTableIterator{
		sst:      sst,
		position: 0,
		valid:    false,
	}
	
	// Open the data file
	dataFile, err := os.Open(sst.dataFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}
	
	// Skip the header
	headerSize := 14 // Magic(4) + Version(2) + KeyCount(4) + MinKeyLen(2) + MaxKeyLen(2)
	if sst.minKey != nil {
		headerSize += len(sst.minKey)
	}
	if sst.maxKey != nil {
		headerSize += len(sst.maxKey)
	}
	
	// Seek to the beginning of the data
	if _, err := dataFile.Seek(int64(headerSize), io.SeekStart); err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to seek to data: %w", err)
	}
	
	// Store the data file
	iter.dataFile = dataFile
	iter.position = uint64(headerSize)
	
	// Move to the first entry
	if sst.keyCount > 0 {
		if err := iter.Next(); err != nil {
			dataFile.Close()
			return nil, fmt.Errorf("failed to initialize iterator: %w", err)
		}
	}
	
	return iter, nil
}

// SSTableIterator allows iterating over the key-value pairs in an SSTable
type SSTableIterator struct {
	sst       *SSTable
	dataFile  *os.File
	position  uint64
	key       []byte
	value     []byte
	valid     bool
}

// Valid returns true if the iterator is pointing to a valid key-value pair
func (iter *SSTableIterator) Valid() bool {
	return iter.valid
}

// Key returns the current key
func (iter *SSTableIterator) Key() []byte {
	if !iter.valid {
		return nil
	}
	return append([]byte{}, iter.key...)
}

// Value returns the current value
func (iter *SSTableIterator) Value() []byte {
	if !iter.valid {
		return nil
	}
	return append([]byte{}, iter.value...)
}

// Next moves the iterator to the next key-value pair
func (iter *SSTableIterator) Next() error {
	// Check if we've reached the end of the file
	fileInfo, err := iter.dataFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	
	if iter.position >= uint64(fileInfo.Size()) {
		iter.valid = false
		return nil
	}
	
	// Seek to the current position
	if _, err := iter.dataFile.Seek(int64(iter.position), io.SeekStart); err != nil {
		iter.valid = false
		return fmt.Errorf("failed to seek to position %d: %w", iter.position, err)
	}
	
	// Read key length
	var keyLen uint16
	if err := binary.Read(iter.dataFile, binary.LittleEndian, &keyLen); err != nil {
		iter.valid = false
		return fmt.Errorf("failed to read key length: %w", err)
	}
	
	// Read key
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(iter.dataFile, key); err != nil {
		iter.valid = false
		return fmt.Errorf("failed to read key: %w", err)
	}
	
	// Read value length
	var valueLen uint32
	if err := binary.Read(iter.dataFile, binary.LittleEndian, &valueLen); err != nil {
		iter.valid = false
		return fmt.Errorf("failed to read value length: %w", err)
	}
	
	// Read value
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(iter.dataFile, value); err != nil {
		iter.valid = false
		return fmt.Errorf("failed to read value: %w", err)
	}
	
	// Update iterator state
	iter.key = key
	iter.value = value
	iter.valid = true
	iter.position += uint64(2) + uint64(keyLen) + uint64(4) + uint64(valueLen)
	
	return nil
}

// Seek positions the iterator at the first key that is >= the specified key
func (iter *SSTableIterator) Seek(key []byte) error {
	// Check if key is within range of this SSTable
	if (iter.sst.minKey != nil && iter.sst.comparator(key, iter.sst.minKey) < 0) {
		// Key is less than min key, position at start
		return iter.SeekToFirst()
	}
	
	if (iter.sst.maxKey != nil && iter.sst.comparator(key, iter.sst.maxKey) > 0) {
		// Key is greater than max key, invalidate iterator
		iter.valid = false
		return nil
	}
	
	// Find key in index
	offset, err := iter.sst.findKeyInIndex(key)
	if err == nil {
		// Key found exactly, position iterator there
		iter.position = offset
		
		// Reset to this position and read the entry
		if _, err := iter.dataFile.Seek(int64(offset), io.SeekStart); err != nil {
			iter.valid = false
			return fmt.Errorf("failed to seek to offset %d: %w", offset, err)
		}
		
		return iter.Next()
	} else if err == ErrKeyNotFoundInSSTable {
		// Key not found, need to scan to find the first key >= target key
		return iter.seekToKeyOrGreater(key)
	} else {
		// Other error
		iter.valid = false
		return err
	}
}

// SeekToFirst positions the iterator at the first key in the SSTable
func (iter *SSTableIterator) SeekToFirst() error {
	// Seek to the beginning of the data
	headerSize := 14 // Magic(4) + Version(2) + KeyCount(4) + MinKeyLen(2) + MaxKeyLen(2)
	if iter.sst.minKey != nil {
		headerSize += len(iter.sst.minKey)
	}
	if iter.sst.maxKey != nil {
		headerSize += len(iter.sst.maxKey)
	}
	
	iter.position = uint64(headerSize)
	
	if _, err := iter.dataFile.Seek(int64(headerSize), io.SeekStart); err != nil {
		iter.valid = false
		return fmt.Errorf("failed to seek to beginning of data: %w", err)
	}
	
	// Read the first entry
	if iter.sst.keyCount > 0 {
		return iter.Next()
	}
	
	iter.valid = false
	return nil
}

// SeekToLast positions the iterator at the last key in the SSTable
func (iter *SSTableIterator) SeekToLast() error {
	// We need to scan through the entire file to find the last key
	// since we don't maintain a reverse index
	if err := iter.SeekToFirst(); err != nil {
		return err
	}
	
	if !iter.valid {
		// Empty SSTable
		return nil
	}
	
	var lastKey []byte
	var lastValue []byte
	
	// Scan through all entries
	for iter.valid {
		lastKey = append([]byte{}, iter.key...)
		lastValue = append([]byte{}, iter.value...)
		
		if err := iter.Next(); err != nil {
			return err
		}
	}
	
	// Restore the last position
	iter.key = lastKey
	iter.value = lastValue
	iter.valid = true
	
	return nil
}

// Close closes the iterator and releases resources
func (iter *SSTableIterator) Close() error {
	if iter.dataFile != nil {
		err := iter.dataFile.Close()
		iter.dataFile = nil
		iter.valid = false
		return err
	}
	return nil
}

// seekToKeyOrGreater positions the iterator at the first key that is >= the specified key
// using a linear scan through the data file
func (iter *SSTableIterator) seekToKeyOrGreater(key []byte) error {
	// Reset to the beginning
	if err := iter.SeekToFirst(); err != nil {
		return err
	}
	
	// Scan until we find a key >= target
	for iter.valid {
		if iter.sst.comparator(iter.key, key) >= 0 {
			// Found a key >= target
			return nil
		}
		
		if err := iter.Next(); err != nil {
			return err
		}
	}
	
	// No key >= target found
	return nil
}