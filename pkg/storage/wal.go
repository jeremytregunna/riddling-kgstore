package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// WAL errors
var (
	ErrWALCorrupted     = errors.New("WAL is corrupted")
	ErrWALClosed        = errors.New("WAL is closed")
	ErrInvalidWALRecord = errors.New("invalid WAL record")
)

// WAL record type
type RecordType byte

const (
	RecordPut    RecordType = 1
	RecordDelete RecordType = 2
)

// WAL header constants
const (
	WALMagic   uint32 = 0x57414C4C // "WALL"
	WALVersion uint16 = 1
)

// WALRecord represents a single record in the WAL
type WALRecord struct {
	Type      RecordType
	Key       []byte
	Value     []byte
	Timestamp int64
}

// WAL implements a Write-Ahead Log for durability
type WAL struct {
	mu          sync.Mutex
	file        *os.File
	writer      *bufio.Writer
	path        string
	isOpen      bool
	syncOnWrite bool
	logger      model.Logger
}

// WALConfig holds configuration options for the WAL
type WALConfig struct {
	Path        string       // Path to the WAL file
	SyncOnWrite bool         // Whether to sync to disk after each write
	Logger      model.Logger // Logger for WAL operations
}

// NewWAL creates a new WAL at the given path
func NewWAL(config WALConfig) (*WAL, error) {
	if config.Logger == nil {
		config.Logger = model.DefaultLoggerInstance
	}

	// Ensure the directory exists
	dir := filepath.Dir(config.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Create or open the WAL file
	file, err := os.OpenFile(config.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	wal := &WAL{
		file:        file,
		writer:      bufio.NewWriter(file),
		path:        config.Path,
		isOpen:      true,
		syncOnWrite: config.SyncOnWrite,
		logger:      config.Logger,
	}

	// If the file is new, write the header
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	if fileInfo.Size() == 0 {
		if err := wal.writeHeader(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to write WAL header: %w", err)
		}
	} else {
		// Verify the header
		if err := wal.verifyHeader(); err != nil {
			file.Close()
			return nil, fmt.Errorf("invalid WAL header: %w", err)
		}
	}

	wal.logger.Info("Opened WAL at %s", config.Path)
	return wal, nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.isOpen {
		return nil
	}

	w.isOpen = false

	// Flush any buffered data
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	w.logger.Info("Closed WAL at %s", w.path)
	return nil
}

// RecordPut records a key-value pair in the WAL
func (w *WAL) RecordPut(key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.isOpen {
		return ErrWALClosed
	}

	record := WALRecord{
		Type:      RecordPut,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}

	if err := w.writeRecord(record); err != nil {
		return fmt.Errorf("failed to write put record: %w", err)
	}

	w.logger.Debug("Recorded PUT operation for key of size %d", len(key))
	return nil
}

// RecordDelete records a key deletion in the WAL
func (w *WAL) RecordDelete(key []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.isOpen {
		return ErrWALClosed
	}

	record := WALRecord{
		Type:      RecordDelete,
		Key:       key,
		Value:     nil,
		Timestamp: time.Now().UnixNano(),
	}

	if err := w.writeRecord(record); err != nil {
		return fmt.Errorf("failed to write delete record: %w", err)
	}

	w.logger.Debug("Recorded DELETE operation for key of size %d", len(key))
	return nil
}

// Sync flushes the WAL to disk
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.isOpen {
		return ErrWALClosed
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL buffer: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL to disk: %w", err)
	}

	w.logger.Debug("Synced WAL to disk")
	return nil
}

// Replay replays the WAL records and applies them to the given MemTable
func (w *WAL) Replay(memTable *MemTable) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.isOpen {
		return ErrWALClosed
	}

	// Ensure the WAL is flushed to disk
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL before replay: %w", err)
	}

	// Seek to the beginning of the file, after the header
	if _, err := w.file.Seek(6, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to WAL data: %w", err)
	}

	reader := bufio.NewReader(w.file)
	recordCount := 0
	applyCount := 0

	for {
		record, err := w.readRecord(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			w.logger.Warn("Error reading WAL record: %v", err)
			continue
		}

		recordCount++

		// Apply the record to the MemTable
		switch record.Type {
		case RecordPut:
			if err := memTable.Put(record.Key, record.Value); err != nil {
				w.logger.Warn("Failed to apply PUT record: %v", err)
				continue
			}
		case RecordDelete:
			if err := memTable.Delete(record.Key); err != nil {
				w.logger.Warn("Failed to apply DELETE record: %v", err)
				continue
			}
		default:
			w.logger.Warn("Unknown record type: %d", record.Type)
			continue
		}

		applyCount++
	}

	// Seek back to the end of the file for future writes
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to end of WAL: %w", err)
	}

	w.logger.Info("Replayed %d of %d records from WAL", applyCount, recordCount)
	return nil
}

// Truncate truncates the WAL file, removing all records
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.isOpen {
		return ErrWALClosed
	}

	// Close the current file
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	// Reopen the file, truncating it
	file, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to truncate WAL file: %w", err)
	}

	w.file = file
	w.writer = bufio.NewWriter(file)

	// Write a new header
	if err := w.writeHeader(); err != nil {
		return fmt.Errorf("failed to write WAL header: %w", err)
	}

	w.logger.Info("Truncated WAL at %s", w.path)
	return nil
}

// writeHeader writes the WAL header to the file
func (w *WAL) writeHeader() error {
	// Write magic number and version
	if err := binary.Write(w.writer, binary.LittleEndian, WALMagic); err != nil {
		return err
	}

	if err := binary.Write(w.writer, binary.LittleEndian, WALVersion); err != nil {
		return err
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}

	if w.syncOnWrite {
		if err := w.file.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// verifyHeader verifies the WAL header
func (w *WAL) verifyHeader() error {
	// Seek to the beginning of the file
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Read magic number
	var magic uint32
	if err := binary.Read(w.file, binary.LittleEndian, &magic); err != nil {
		return err
	}

	if magic != WALMagic {
		return ErrWALCorrupted
	}

	// Read version
	var version uint16
	if err := binary.Read(w.file, binary.LittleEndian, &version); err != nil {
		return err
	}

	if version != WALVersion {
		return ErrWALCorrupted
	}

	// Seek back to the end of the file for future writes
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	return nil
}

// writeRecord writes a record to the WAL
func (w *WAL) writeRecord(record WALRecord) error {
	// Calculate total record size
	totalSize := 1 + // Type
		8 + // Timestamp
		4 + // Key length
		len(record.Key)

	if record.Type == RecordPut {
		totalSize += 4 + len(record.Value) // Value length + value
	}

	// Write record header (CRC + length)
	// First calculate CRC
	crc := crc32.NewIEEE()
	crc.Write([]byte{byte(record.Type)})
	binary.Write(crc, binary.LittleEndian, record.Timestamp)
	binary.Write(crc, binary.LittleEndian, uint32(len(record.Key)))
	crc.Write(record.Key)

	if record.Type == RecordPut {
		binary.Write(crc, binary.LittleEndian, uint32(len(record.Value)))
		crc.Write(record.Value)
	}

	crcValue := crc.Sum32()

	// Write CRC
	if err := binary.Write(w.writer, binary.LittleEndian, crcValue); err != nil {
		return err
	}

	// Write record size
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(totalSize)); err != nil {
		return err
	}

	// Write record type
	if err := w.writer.WriteByte(byte(record.Type)); err != nil {
		return err
	}

	// Write timestamp
	if err := binary.Write(w.writer, binary.LittleEndian, record.Timestamp); err != nil {
		return err
	}

	// Write key length and key
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(record.Key))); err != nil {
		return err
	}
	if _, err := w.writer.Write(record.Key); err != nil {
		return err
	}

	// Write value length and value if it's a PUT
	if record.Type == RecordPut {
		if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(record.Value))); err != nil {
			return err
		}
		if _, err := w.writer.Write(record.Value); err != nil {
			return err
		}
	}

	// Flush to the operating system's buffer
	if err := w.writer.Flush(); err != nil {
		return err
	}

	// Sync to disk if syncOnWrite is enabled
	if w.syncOnWrite {
		if err := w.file.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// readRecord reads a record from the WAL
func (w *WAL) readRecord(reader *bufio.Reader) (WALRecord, error) {
	var record WALRecord

	// Read CRC
	var crcValue uint32
	if err := binary.Read(reader, binary.LittleEndian, &crcValue); err != nil {
		return record, err
	}

	// Read record size
	var recordSize uint32
	if err := binary.Read(reader, binary.LittleEndian, &recordSize); err != nil {
		return record, err
	}

	// Read record type
	recordTypeByte, err := reader.ReadByte()
	if err != nil {
		return record, err
	}
	record.Type = RecordType(recordTypeByte)

	// Validate record type
	if record.Type != RecordPut && record.Type != RecordDelete {
		return record, ErrInvalidWALRecord
	}

	// Read timestamp
	if err := binary.Read(reader, binary.LittleEndian, &record.Timestamp); err != nil {
		return record, err
	}

	// Read key length
	var keyLength uint32
	if err := binary.Read(reader, binary.LittleEndian, &keyLength); err != nil {
		return record, err
	}

	// Read key
	record.Key = make([]byte, keyLength)
	if _, err := io.ReadFull(reader, record.Key); err != nil {
		return record, err
	}

	// Read value if it's a PUT
	if record.Type == RecordPut {
		// Read value length
		var valueLength uint32
		if err := binary.Read(reader, binary.LittleEndian, &valueLength); err != nil {
			return record, err
		}

		// Read value
		record.Value = make([]byte, valueLength)
		if _, err := io.ReadFull(reader, record.Value); err != nil {
			return record, err
		}
	}

	// Verify CRC
	crc := crc32.NewIEEE()
	crc.Write([]byte{byte(record.Type)})
	binary.Write(crc, binary.LittleEndian, record.Timestamp)
	binary.Write(crc, binary.LittleEndian, keyLength)
	crc.Write(record.Key)

	if record.Type == RecordPut {
		binary.Write(crc, binary.LittleEndian, uint32(len(record.Value)))
		crc.Write(record.Value)
	}

	if crc.Sum32() != crcValue {
		return record, ErrWALCorrupted
	}

	return record, nil
}

// IsOpen returns whether the WAL is open
func (w *WAL) IsOpen() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.isOpen
}

// Path returns the path to the WAL file
func (w *WAL) Path() string {
	return w.path
}
