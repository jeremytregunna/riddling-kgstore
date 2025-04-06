package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// EnhancedReplayToInterface replays WAL records to any MemTableInterface implementation
func EnhancedReplayToInterface(wal *WAL, memTable MemTableInterface, options ReplayOptions) (ReplayStats, error) {
	// Prepare statistics tracking
	stats := ReplayStats{
		StartTime:       time.Now(),
		RecordCount:     0,
		AppliedCount:    0,
		CorruptedCount:  0,
		TxBeginCount:    0,
		TxCommitCount:   0,
		TxRollbackCount: 0,
		SkippedTxCount:  0,
	}

	// Open the WAL file for reading
	file, err := os.Open(wal.Path())
	if err != nil {
		return stats, fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	// Create a reader
	reader := bufio.NewReader(file)

	// Verify the WAL header
	var magic uint32
	if err := binary.Read(reader, binary.LittleEndian, &magic); err != nil {
		return stats, fmt.Errorf("failed to read WAL magic: %w", err)
	}
	if magic != WALMagic {
		return stats, ErrWALCorrupted
	}

	// Read WAL version
	var version uint16
	if err := binary.Read(reader, binary.LittleEndian, &version); err != nil {
		return stats, fmt.Errorf("failed to read WAL version: %w", err)
	}

	// Handle different WAL versions
	if version == 1 {
		// Version 1 doesn't store transaction ID
		wal.logger.Info("WAL version 1 detected")
	} else if version == 2 {
		// Version 2 stores the next transaction ID
		var nextTxID uint64
		if err := binary.Read(reader, binary.LittleEndian, &nextTxID); err != nil {
			return stats, fmt.Errorf("failed to read next transaction ID: %w", err)
		}

		// Update next transaction ID if higher
		if nextTxID > wal.nextTxID {
			wal.nextTxID = nextTxID
			wal.logger.Info("Restored transaction ID counter to %d from WAL header", nextTxID)
		}
	} else {
		return stats, fmt.Errorf("unsupported WAL version: %d", version)
	}

	// Track transactions
	activeTxs := make(map[uint64][]WALRecord)
	committedTxs := make(map[uint64]bool)
	rolledBackTxs := make(map[uint64]bool)

	// Read and process records
	for {
		record, err := wal.readRecord(reader)
		if err != nil {
			if err == io.EOF {
				// Reached end of file
				break
			}

			stats.CorruptedCount++
			if errors.Is(err, ErrWALCorrupted) || errors.Is(err, ErrInvalidWALRecord) {
				wal.logger.Warn("Corrupted WAL record detected at position %d: %v", stats.RecordCount+1, err)
				if options.StrictMode {
					return stats, fmt.Errorf("WAL corruption detected at position %d in strict mode: %w", stats.RecordCount+1, err)
				}
				// In lenient mode, we try to continue
				continue
			}

			// Other errors are fatal
			// Add more detail about where in the WAL we encountered the error
			return stats, fmt.Errorf("error reading WAL record at position %d: %w", stats.RecordCount+1, err)
		}

		stats.RecordCount++

		// Process the record based on its type
		switch record.Type {
		case RecordTxBegin:
			stats.TxBeginCount++
			activeTxs[record.TxID] = make([]WALRecord, 0)

		case RecordTxCommit:
			stats.TxCommitCount++
			committedTxs[record.TxID] = true

			// Apply transaction records immediately if they're part of the atomic transaction
			if txRecords, ok := activeTxs[record.TxID]; ok && options.AtomicTxOnly {
				for _, txRecord := range txRecords {
					applyRecordToInterface(memTable, txRecord)
					stats.AppliedCount++
				}
				delete(activeTxs, record.TxID)
			}

		case RecordTxRollback:
			stats.TxRollbackCount++
			rolledBackTxs[record.TxID] = true
			delete(activeTxs, record.TxID)
			stats.SkippedTxCount++

		case RecordPut, RecordDelete:
			if record.TxID == 0 {
				// Non-transactional records get applied immediately
				applyRecordToInterface(memTable, record)
				stats.AppliedCount++
			} else if options.AtomicTxOnly {
				// Store record for later application when transaction commits
				if _, rolledBack := rolledBackTxs[record.TxID]; rolledBack {
					// Skip records from rolled back transactions
					continue
				}

				if _, committed := committedTxs[record.TxID]; committed {
					// Apply immediately for already committed transactions
					applyRecordToInterface(memTable, record)
					stats.AppliedCount++
				} else {
					// Store for later application
					activeTxs[record.TxID] = append(activeTxs[record.TxID], record)
				}
			} else {
				// Non-atomic mode: apply all records
				if _, rolledBack := rolledBackTxs[record.TxID]; !rolledBack {
					applyRecordToInterface(memTable, record)
					stats.AppliedCount++
				}
			}
		}
	}

	// Process incomplete transactions
	for txID, records := range activeTxs {
		if options.AtomicTxOnly {
			// Skip incomplete transactions in atomic mode
			stats.SkippedTxCount++
			wal.logger.Warn("Incomplete transaction %d skipped (%d operations)", txID, len(records))
			continue
		}

		// In non-atomic mode, apply records from incomplete transactions
		for _, record := range records {
			applyRecordToInterface(memTable, record)
			stats.AppliedCount++
		}
	}

	stats.EndTime = time.Now()
	stats.Duration = stats.EndTime.Sub(stats.StartTime)

	return stats, nil
}

// applyRecordToInterface applies a WAL record to any MemTableInterface implementation
func applyRecordToInterface(memTable MemTableInterface, record WALRecord) {
	switch record.Type {
	case RecordPut:
		memTable.Put(record.Key, record.Value)
	case RecordDelete:
		memTable.Delete(record.Key)
	}
}
