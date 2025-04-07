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
	"time"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// SSTable errors
var (
	ErrSSTableCorrupted     = errors.New("SSTable file is corrupted")
	ErrKeyNotFoundInSSTable = errors.New("key not found in SSTable")
	ErrInvalidSSTableFormat = errors.New("invalid SSTable format")
	ErrIOOperation          = errors.New("IO operation failed")
)

// SSTableMagic is a magic number that identifies an SSTable file
const SSTableMagic uint32 = 0x5354424C // "STBL"

// SSTableVersion is the current version of the SSTable format
const SSTableVersion uint16 = 1

// SSTable is an immutable Sorted String Table stored on disk
// It consists of a data file containing key-value pairs and an index
// file that allows for binary search lookups
type SSTable struct {
	id         uint64       // Unique identifier for this SSTable
	path       string       // Path to the SSTable directory
	dataFile   string       // Path to the data file
	indexFile  string       // Path to the index file
	filterFile string       // Path to the bloom filter file
	logger     model.Logger // Logger for SSTable operations
	comparator Comparator   // Comparator for key comparison
	mu         sync.RWMutex // Mutex for concurrent access
	isOpen     bool         // Whether the SSTable is open
	timestamp  int64        // Creation timestamp for this SSTable

	// Meta information
	keyCount uint32 // Number of key-value pairs in the SSTable
	dataSize uint64 // Size of the data file in bytes
	minKey   []byte // Minimum key in the SSTable
	maxKey   []byte // Maximum key in the SSTable

	// Index cache (loaded on demand)
	indexCache map[string]uint64
}

// SSTableHeader represents the header of an SSTable data file
type SSTableHeader struct {
	Magic     uint32 // Magic number to identify SSTable files
	Version   uint16 // Version of the SSTable format
	KeyCount  uint32 // Number of key-value pairs
	MinKeyLen uint16 // Length of the minimum key
	MaxKeyLen uint16 // Length of the maximum key
	Timestamp int64  // Creation timestamp of the SSTable
	MinKey    []byte // Minimum key in the SSTable
	MaxKey    []byte // Maximum key in the SSTable
}

// SSTableConfig holds configuration options for creating an SSTable
type SSTableConfig struct {
	ID         uint64       // Unique identifier for this SSTable
	Path       string       // Directory where SSTable files will be stored
	Logger     model.Logger // Logger for SSTable operations
	Comparator Comparator   // Comparator for key comparison
	Timestamp  int64        // Creation timestamp, defaults to current time if 0
}

// CreateSSTOptions defines options for SSTable creation
type CreateSSTOptions struct {
	// IncludeDeleted determines whether to include deleted entries as tombstones
	IncludeDeleted bool
}

// DefaultCreateSSTOptions returns default options for creating an SSTable
func DefaultCreateSSTOptions() CreateSSTOptions {
	return CreateSSTOptions{
		IncludeDeleted: false, // By default, don't include deleted entries
	}
}

// CreateSSTableWithOptions creates a new SSTable from a MemTable with specific options
// to ensure that crashes during creation don't leave partial SSTables
func CreateSSTableWithOptions(config SSTableConfig, memTable MemTableInterface, options CreateSSTOptions) (*SSTable, error) {
	if memTable == nil {
		return nil, errors.New("cannot create SSTable from nil MemTable")
	}

	if config.Logger == nil {
		config.Logger = model.DefaultLoggerInstance
	}

	if config.Comparator == nil {
		config.Comparator = DefaultComparator
	}

	// Set timestamp if not provided
	if config.Timestamp == 0 {
		config.Timestamp = time.Now().UnixNano()
	}

	// Ensure the SSTable directory exists
	if err := os.MkdirAll(config.Path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create SSTable directory: %w", err)
	}

	// Create a temporary directory for atomicity
	tmpDir, err := os.MkdirTemp(config.Path, fmt.Sprintf("sstable_%d_tmp", config.ID))
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary directory: %w", err)
	}

	// Create temporary file paths
	tmpDataFile := filepath.Join(tmpDir, fmt.Sprintf("%d.data", config.ID))
	tmpIndexFile := filepath.Join(tmpDir, fmt.Sprintf("%d.index", config.ID))
	tmpFilterFile := filepath.Join(tmpDir, fmt.Sprintf("%d.filter", config.ID))

	// Create final file paths
	dataFile := filepath.Join(config.Path, fmt.Sprintf("%d.data", config.ID))
	indexFile := filepath.Join(config.Path, fmt.Sprintf("%d.index", config.ID))
	filterFile := filepath.Join(config.Path, fmt.Sprintf("%d.filter", config.ID))

	// Create a new SSTable
	sst := &SSTable{
		id:         config.ID,
		path:       config.Path,
		dataFile:   tmpDataFile, // Temporarily use the tmp file paths
		indexFile:  tmpIndexFile,
		filterFile: tmpFilterFile,
		logger:     config.Logger,
		comparator: config.Comparator,
		isOpen:     false,
		timestamp:  config.Timestamp,
		indexCache: make(map[string]uint64),
	}

	// Get entries from the MemTable based on options
	var entries [][]byte
	if options.IncludeDeleted {
		// Use version-aware entries that include tombstones
		entries = memTable.GetEntriesWithMetadata()
	} else {
		// Use traditional entries without deleted records
		entries = memTable.GetEntries()
	}

	// Flag to track if we should clean up temporary files
	var cleanupTmp bool = true
	defer func() {
		if cleanupTmp {
			// Clean up temporary directory on failure
			os.RemoveAll(tmpDir)
		}
	}()

	if len(entries) == 0 {
		// Create empty SSTable files
		if err := sst.createEmptySSTable(); err != nil {
			return nil, fmt.Errorf("failed to create empty SSTable: %w", err)
		}
	} else {
		// Build the SSTable files
		if err := sst.buildFromEntries(entries); err != nil {
			return nil, fmt.Errorf("failed to build SSTable: %w", err)
		}
	}

	// Now we need to atomically move the temporary files to their final location

	// First, ensure all data is synced to disk
	if err := syncDirectory(tmpDir); err != nil {
		return nil, fmt.Errorf("failed to sync temporary directory: %w", err)
	}

	// Move files one by one
	if err := atomicMoveFile(tmpDataFile, dataFile); err != nil {
		return nil, fmt.Errorf("failed to move data file: %w", err)
	}

	if err := atomicMoveFile(tmpIndexFile, indexFile); err != nil {
		// Try to clean up the moved data file
		os.Remove(dataFile)
		return nil, fmt.Errorf("failed to move index file: %w", err)
	}

	if err := atomicMoveFile(tmpFilterFile, filterFile); err != nil {
		// Try to clean up the moved files
		os.Remove(dataFile)
		os.Remove(indexFile)
		return nil, fmt.Errorf("failed to move filter file: %w", err)
	}

	// Sync the parent directory to ensure file moves are durable
	if err := syncDirectory(config.Path); err != nil {
		return nil, fmt.Errorf("failed to sync SSTable directory: %w", err)
	}

	// Update SSTable with final file paths
	sst.dataFile = dataFile
	sst.indexFile = indexFile
	sst.filterFile = filterFile

	// Don't clean up the temporary directory, as we've successfully moved all files
	cleanupTmp = false

	// Safe to remove the temp directory now
	os.RemoveAll(tmpDir)

	sst.logger.Info("Created SSTable with ID %d, %d keys, %d bytes",
		config.ID, sst.keyCount, sst.dataSize)

	return sst, nil
}

// CreateSSTable creates a new SSTable from a MemTable using an atomic operation
// to ensure that crashes during creation don't leave partial SSTables.
// This version does not include tombstones for deleted entries.
func CreateSSTable(config SSTableConfig, memTable MemTableInterface) (*SSTable, error) {
	return CreateSSTableWithOptions(config, memTable, DefaultCreateSSTOptions())
}

// atomicMoveFile atomically moves a file from src to dst
func atomicMoveFile(src, dst string) error {
	// Rename is atomic on most filesystems
	return os.Rename(src, dst)
}

// syncDirectory syncs a directory to ensure all file operations are durable
func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()

	// Sync the directory to ensure all operations are durable
	return f.Sync()
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
// With versioned records, this now checks tombstone status
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
	offset, version, err := sst.findKeyInIndex(key)
	if err != nil {
		return nil, err
	}

	// Read value and tombstone status from data file at offset
	value, isDeleted, valueVersion, err := sst.readValueAtOffset(offset)
	if err != nil {
		return nil, err
	}

	// If it's a tombstone (deleted record), return not found error
	if isDeleted {
		return nil, ErrKeyNotFoundInSSTable
	}

	// Verify version in data file matches what's in index
	if version != valueVersion {
		return nil, fmt.Errorf("version mismatch: index has %d, data has %d", version, valueVersion)
	}

	return value, nil
}

// Contains checks if a key exists in the SSTable
// With versioned records, this checks if the key exists and is not a tombstone
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
	offset, _, err := sst.findKeyInIndex(key)
	if err == ErrKeyNotFoundInSSTable {
		return false, nil
	} else if err != nil {
		return false, err
	}

	// Check if it's a tombstone
	_, isDeleted, _, err := sst.readValueAtOffset(offset)
	if err != nil {
		return false, err
	}

	// If it's a tombstone, consider it as not existing
	if isDeleted {
		return false, nil
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
	// We support two formats:
	// 1. Regular format: key, value pairs (len % 2 == 0)
	// 2. Versioned format: key, value, version, isDeleted (len % 4 == 0)
	if len(entries) == 0 || (len(entries)%2 != 0 && len(entries)%4 != 0) {
		return errors.New("invalid entries: must be non-empty and contain valid entry format")
	}

	// Determine if we're using versioned records
	isVersioned := len(entries)%4 == 0 && len(entries) > 0

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

	// Create empty filter file
	filterFile, err := os.Create(sst.filterFile)
	if err != nil {
		return fmt.Errorf("failed to create filter file: %w", err)
	}
	defer filterFile.Close()

	// Create buffered writers
	dataWriter := bufio.NewWriter(dataFile)
	indexWriter := bufio.NewWriter(indexFile)

	// Reserve space for header in data file
	headerSize := 14 + 8 // Magic(4) + Version(2) + KeyCount(4) + MinKeyLen(2) + MaxKeyLen(2) + Timestamp(8)

	// Get min and max keys from entries
	minKey := entries[0]
	maxKey := entries[0]

	if isVersioned {
		// Versioned format: keys at positions 0, 4, 8, etc.
		for i := 0; i < len(entries); i += 4 {
			key := entries[i]
			if sst.comparator(key, minKey) < 0 {
				minKey = key
			}
			if sst.comparator(key, maxKey) > 0 {
				maxKey = key
			}
		}
	} else {
		// Regular format: keys at positions 0, 2, 4, etc.
		for i := 0; i < len(entries); i += 2 {
			key := entries[i]
			if sst.comparator(key, minKey) < 0 {
				minKey = key
			}
			if sst.comparator(key, maxKey) > 0 {
				maxKey = key
			}
		}
	}

	// Adjust header size to include min and max keys
	headerSize += len(minKey) + len(maxKey)
	dataOffset := uint64(headerSize)

	// Write magic and version
	binary.Write(dataWriter, binary.LittleEndian, SSTableMagic)
	binary.Write(dataWriter, binary.LittleEndian, SSTableVersion)

	// Get number of key-value pairs
	var keyCount uint32
	if isVersioned {
		keyCount = uint32(len(entries) / 4)
	} else {
		keyCount = uint32(len(entries) / 2)
	}

	// Write key count
	binary.Write(dataWriter, binary.LittleEndian, keyCount)

	// Write min key length and max key length
	binary.Write(dataWriter, binary.LittleEndian, uint16(len(minKey)))
	binary.Write(dataWriter, binary.LittleEndian, uint16(len(maxKey)))

	// Write timestamp
	binary.Write(dataWriter, binary.LittleEndian, sst.timestamp)

	// Write min key and max key
	dataWriter.Write(minKey)
	dataWriter.Write(maxKey)

	// Write key-value pairs to data file and build index
	if isVersioned {
		// Versioned format
		for i := 0; i < len(entries); i += 4 {
			key := entries[i]
			value := entries[i+1]
			version := entries[i+2]
			isDeleted := entries[i+3]

			// If it's a tombstone and we're filtering, skip it
			if isDeleted[0] != 0 {
				// Skip this tombstone if we didn't request them
				continue
			}

			// Write index entry: key length, key, data offset, version
			binary.Write(indexWriter, binary.LittleEndian, uint16(len(key)))
			indexWriter.Write(key)
			binary.Write(indexWriter, binary.LittleEndian, dataOffset)
			indexWriter.Write(version) // 8 bytes for version

			// Write data entry: key length, key, value length, value, version, isDeleted flag
			binary.Write(dataWriter, binary.LittleEndian, uint16(len(key)))
			dataWriter.Write(key)
			binary.Write(dataWriter, binary.LittleEndian, uint32(len(value)))
			dataWriter.Write(value)
			dataWriter.Write(version)   // 8 bytes for version
			dataWriter.Write(isDeleted) // 1 byte for isDeleted flag

			// Update offset for the next entry
			dataOffset += uint64(2) + uint64(len(key)) + uint64(4) + uint64(len(value)) + uint64(8) + uint64(1)
		}
	} else {
		// Standard format
		for i := 0; i < len(entries); i += 2 {
			key := entries[i]
			value := entries[i+1]

			// Default values for version and isDeleted
			versionBytes := make([]byte, 8)
			isDeletedByte := []byte{0} // Not deleted

			// Write index entry: key length, key, data offset, dummy version
			binary.Write(indexWriter, binary.LittleEndian, uint16(len(key)))
			indexWriter.Write(key)
			binary.Write(indexWriter, binary.LittleEndian, dataOffset)
			indexWriter.Write(versionBytes) // 8 bytes for version (zeros)

			// Write data entry: key length, key, value length, value, dummy version, no deletion
			binary.Write(dataWriter, binary.LittleEndian, uint16(len(key)))
			dataWriter.Write(key)
			binary.Write(dataWriter, binary.LittleEndian, uint32(len(value)))
			dataWriter.Write(value)
			dataWriter.Write(versionBytes)  // 8 bytes for version (zeros)
			dataWriter.Write(isDeletedByte) // 1 byte for isDeleted flag (not deleted)

			// Update offset for the next entry
			dataOffset += uint64(2) + uint64(len(key)) + uint64(4) + uint64(len(value)) + uint64(8) + uint64(1)
		}
	}

	// Flush writers to ensure all data is written
	if err := indexWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush index writer: %w", err)
	}

	if err := dataWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush data writer: %w", err)
	}

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

	// Create empty filter file
	filterFile, err := os.Create(sst.filterFile)
	if err != nil {
		return fmt.Errorf("failed to create filter file: %w", err)
	}
	defer filterFile.Close()

	// Write header to data file
	binary.Write(dataFile, binary.LittleEndian, SSTableMagic)
	binary.Write(dataFile, binary.LittleEndian, SSTableVersion)
	binary.Write(dataFile, binary.LittleEndian, uint32(0))     // Key count
	binary.Write(dataFile, binary.LittleEndian, uint16(0))     // Min key length
	binary.Write(dataFile, binary.LittleEndian, uint16(0))     // Max key length
	binary.Write(dataFile, binary.LittleEndian, sst.timestamp) // Timestamp

	// Update SSTable metadata
	sst.keyCount = 0
	sst.dataSize = 22 // Size of header (14 + 8 for timestamp)
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

	// Read timestamp
	var timestamp int64
	if err := binary.Read(dataFile, binary.LittleEndian, &timestamp); err != nil {
		return fmt.Errorf("failed to read timestamp: %w", err)
	}
	sst.timestamp = timestamp

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
// and the version number of the entry
func (sst *SSTable) findKeyInIndex(key []byte) (uint64, uint64, error) {
	// We'll disable the cache for now since it doesn't include version info
	// In a real implementation, we'd update the cache structure to include version
	/*
		keyStr := string(key)
		if offset, ok := sst.indexCache[keyStr]; ok {
			return offset, 0, nil
		}
	*/

	// Open index file
	indexFile, err := os.Open(sst.indexFile)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open index file: %w", err)
	}
	defer indexFile.Close()

	// Read the entire index into memory for binary search
	indexData, err := io.ReadAll(indexFile)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read index file: %w", err)
	}

	// If the index is empty or just has a header, there are no entries
	if len(indexData) <= 2 {
		return 0, 0, ErrKeyNotFoundInSSTable
	}

	// Implement a true binary search through the index entries
	// Now each entry includes version (8 bytes)
	var entrySize int // size of a single index entry = 2 bytes (key length) + key + 8 bytes (offset) + 8 bytes (version)
	var entries []int // positions of index entries in the file
	var found bool
	var offset uint64
	var version uint64

	// First pass: build a list of entry positions
	pos := 0
	for pos < len(indexData) {
		entries = append(entries, pos)

		// Make sure there's enough data for key length
		if pos+2 > len(indexData) {
			break
		}

		keyLen := binary.LittleEndian.Uint16(indexData[pos:])

		// Calculate entry size and move to next entry - now includes 8 bytes for version
		entrySize = 2 + int(keyLen) + 8 + 8
		pos += entrySize
	}

	// No entries found
	if len(entries) == 0 {
		return 0, 0, ErrKeyNotFoundInSSTable
	}

	// Binary search through the entries
	left, right := 0, len(entries)-1

	for left <= right {
		mid := left + (right-left)/2
		pos = entries[mid]

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

		// Make sure there's enough data for the version
		if pos+8 > len(indexData) {
			break
		}

		indexVersion := binary.LittleEndian.Uint64(indexData[pos:])

		// Compare keys
		cmp := sst.comparator(key, indexKey)
		if cmp == 0 {
			// Key found
			offset = indexOffset
			version = indexVersion
			found = true

			// Disabled caching for now
			// sst.indexCache[string(key)] = offset

			break
		} else if cmp < 0 {
			// Search left half
			right = mid - 1
		} else {
			// Search right half
			left = mid + 1
		}
	}

	if !found {
		return 0, 0, ErrKeyNotFoundInSSTable
	}

	return offset, version, nil
}

// readValueAtOffset reads a value from the data file at the specified offset
// Now also returns the tombstone flag and version information
func (sst *SSTable) readValueAtOffset(offset uint64) ([]byte, bool, uint64, error) {
	// Open data file
	dataFile, err := os.Open(sst.dataFile)
	if err != nil {
		return nil, false, 0, fmt.Errorf("failed to open data file: %w", err)
	}
	defer dataFile.Close()

	// Get file size to make sure we don't read past the end
	fileInfo, err := dataFile.Stat()
	if err != nil {
		return nil, false, 0, fmt.Errorf("failed to get file info: %w", err)
	}

	fileSize := fileInfo.Size()
	if int64(offset) >= fileSize {
		return nil, false, 0, fmt.Errorf("offset %d is beyond file size %d", offset, fileSize)
	}

	// Seek to the offset
	if _, err := dataFile.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, false, 0, fmt.Errorf("failed to seek to offset %d: %w", offset, err)
	}

	// Read key length
	var keyLen uint16
	if err := binary.Read(dataFile, binary.LittleEndian, &keyLen); err != nil {
		return nil, false, 0, fmt.Errorf("failed to read key length: %w", err)
	}

	// Verify key length is reasonable (sanity check)
	if keyLen > 1024 { // Assume keys are not larger than 1KB
		return nil, false, 0, fmt.Errorf("key length %d appears invalid", keyLen)
	}

	// Validate key length
	if int64(offset)+int64(2)+int64(keyLen) >= fileSize {
		return nil, false, 0, fmt.Errorf("key length %d would read past end of file", keyLen)
	}

	// Skip key
	if _, err := dataFile.Seek(int64(keyLen), io.SeekCurrent); err != nil {
		return nil, false, 0, fmt.Errorf("failed to skip key: %w", err)
	}

	// Read value length
	var valueLen uint32
	if err := binary.Read(dataFile, binary.LittleEndian, &valueLen); err != nil {
		return nil, false, 0, fmt.Errorf("failed to read value length: %w", err)
	}

	// Verify value length is reasonable (sanity check)
	if valueLen > 10*1024*1024 { // 10MB max
		return nil, false, 0, fmt.Errorf("value length %d appears invalid (exceeds max size)", valueLen)
	}

	// Validate value length
	currentPos, err := dataFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, false, 0, fmt.Errorf("failed to get current position: %w", err)
	}

	if currentPos+int64(valueLen) > fileSize {
		return nil, false, 0, fmt.Errorf("value length %d would read past end of file (at pos %d of size %d)",
			valueLen, currentPos, fileSize)
	}

	// Read value
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(dataFile, value); err != nil {
		return nil, false, 0, fmt.Errorf("failed to read value: %w", err)
	}

	// Read version (8 bytes)
	versionBytes := make([]byte, 8)
	if _, err := io.ReadFull(dataFile, versionBytes); err != nil {
		return nil, false, 0, fmt.Errorf("failed to read version: %w", err)
	}
	version := binary.LittleEndian.Uint64(versionBytes)

	// Read isDeleted flag (1 byte)
	isDeletedByte := make([]byte, 1)
	if _, err := io.ReadFull(dataFile, isDeletedByte); err != nil {
		return nil, false, 0, fmt.Errorf("failed to read isDeleted flag: %w", err)
	}

	isDeleted := isDeletedByte[0] != 0

	return value, isDeleted, version, nil
}

// IteratorOptions defines options for creating an SSTable iterator
type IteratorOptions struct {
	// IncludeTombstones determines whether deleted entries (tombstones) are included
	IncludeTombstones bool
}

// DefaultIteratorOptions returns default options for creating an SSTable iterator
func DefaultIteratorOptions() IteratorOptions {
	return IteratorOptions{
		IncludeTombstones: false, // By default, don't include tombstones
	}
}

// Iterator returns an iterator for the SSTable with default options
func (sst *SSTable) Iterator() (*SSTableIterator, error) {
	sst.logger.Debug("Creating iterator for SSTable %d (keyCount: %d, size: %d bytes)",
		sst.id, sst.keyCount, sst.dataSize)
	return sst.IteratorWithOptions(DefaultIteratorOptions())
}

// IteratorWithOptions returns an iterator for the SSTable with specified options
func (sst *SSTable) IteratorWithOptions(options IteratorOptions) (*SSTableIterator, error) {
	// Check if SSTable is open
	if !sst.isOpen {
		return nil, errors.New("SSTable is not open")
	}

	// Create and initialize an iterator
	iter := &SSTableIterator{
		sst:               sst,
		position:          0,
		valid:             false,
		isDeleted:         false,
		version:           0,
		includeTombstones: options.IncludeTombstones,
	}

	// Open the data file
	dataFile, err := os.Open(sst.dataFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	// Skip the header
	headerSize := 22 // Magic(4) + Version(2) + KeyCount(4) + MinKeyLen(2) + MaxKeyLen(2) + Timestamp(8)
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
	sst               *SSTable
	dataFile          *os.File
	position          uint64
	key               []byte
	value             []byte
	valid             bool
	version           uint64 // Version number for versioned records
	isDeleted         bool   // Deletion flag for tombstones
	includeTombstones bool   // Whether to include tombstones during iteration
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

// Version returns the version number of the current record
func (iter *SSTableIterator) Version() uint64 {
	if !iter.valid {
		return 0
	}
	return iter.version
}

// IsDeleted returns whether the current record is a tombstone (deleted)
func (iter *SSTableIterator) IsDeleted() bool {
	if !iter.valid {
		return false
	}
	return iter.isDeleted
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

	// Read version (8 bytes)
	versionBytes := make([]byte, 8)
	if _, err := io.ReadFull(iter.dataFile, versionBytes); err != nil {
		iter.valid = false
		return fmt.Errorf("failed to read version: %w", err)
	}

	// Read isDeleted flag (1 byte)
	isDeletedByte := make([]byte, 1)
	if _, err := io.ReadFull(iter.dataFile, isDeletedByte); err != nil {
		iter.valid = false
		return fmt.Errorf("failed to read isDeleted flag: %w", err)
	}

	isDeleted := isDeletedByte[0] != 0

	// Update iterator state with version and deletion info
	iter.version = binary.LittleEndian.Uint64(versionBytes)
	iter.isDeleted = isDeleted

	// Skip tombstones unless specifically requested
	// If includeTombstones is false, skip deleted records
	if isDeleted && iter.includeTombstones == false {
		// Skip this entry and move to the next
		iter.position += uint64(2) + uint64(keyLen) + uint64(4) + uint64(valueLen) + uint64(8) + uint64(1)
		return iter.Next()
	}

	iter.key = key
	iter.value = value
	iter.valid = true
	// iter.version and iter.isDeleted are already set above
	iter.position += uint64(2) + uint64(keyLen) + uint64(4) + uint64(valueLen) + uint64(8) + uint64(1)

	return nil
}

// Seek positions the iterator at the first key that is >= the specified key
func (iter *SSTableIterator) Seek(key []byte) error {
	// Check if key is within range of this SSTable
	if iter.sst.minKey != nil && iter.sst.comparator(key, iter.sst.minKey) < 0 {
		// Key is less than min key, position at start
		return iter.SeekToFirst()
	}

	if iter.sst.maxKey != nil && iter.sst.comparator(key, iter.sst.maxKey) > 0 {
		// Key is greater than max key, invalidate iterator
		iter.valid = false
		return nil
	}

	// Find key in index
	offset, _, err := iter.sst.findKeyInIndex(key)
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
	headerSize := 22 // Magic(4) + Version(2) + KeyCount(4) + MinKeyLen(2) + MaxKeyLen(2) + Timestamp(8)
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
