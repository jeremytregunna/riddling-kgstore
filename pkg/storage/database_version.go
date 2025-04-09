package storage

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// DatabaseVersion uniquely identifies a specific state of the database
type DatabaseVersion struct {
	Timestamp       time.Time // When this version was created
	MemTableID      uint64    // Unique identifier for the current MemTable
	SSTablesVersion string    // Hash representing the current set of SSTables
	Generation      uint64    // Database generation identifier (timestamp-based)
}

// NewDatabaseVersion creates a new database version identifier
func NewDatabaseVersion(memTable *MemTable, sstables []*SSTable, generation uint64) DatabaseVersion {
	// Create a unique signature for the current SSTables set
	var sstablesHash string
	if len(sstables) > 0 {
		// Create a simple hash of SSTable IDs
		sstableIDs := make([]string, len(sstables))
		for i, table := range sstables {
			sstableIDs[i] = fmt.Sprintf("%d", table.ID())
		}
		sstablesHash = fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(sstableIDs, ","))))[:16]
	} else {
		sstablesHash = "empty"
	}

	// Use the memtable pointer address as its unique identifier
	memTableID := uint64(reflect.ValueOf(memTable).Pointer())

	return DatabaseVersion{
		Timestamp:       time.Now(),
		MemTableID:      memTableID,
		SSTablesVersion: sstablesHash,
		Generation:      generation,
	}
}

// Equals checks if two database versions represent the same database state
func (v DatabaseVersion) Equals(other DatabaseVersion) bool {
	return v.MemTableID == other.MemTableID && 
		v.SSTablesVersion == other.SSTablesVersion &&
		v.Generation == other.Generation
}

// String returns a string representation of the database version
func (v DatabaseVersion) String() string {
	var shortSS string
	if len(v.SSTablesVersion) >= 6 {
		shortSS = v.SSTablesVersion[:6]
	} else {
		shortSS = v.SSTablesVersion
	}
	return fmt.Sprintf("DBVer[Gen:%d,Mem:%d,SS:%s,Time:%s]", 
		v.Generation, v.MemTableID, shortSS, v.Timestamp.Format(time.RFC3339))
}

// Age returns the time elapsed since this database version was created
func (v DatabaseVersion) Age() time.Duration {
	return time.Since(v.Timestamp)
}

// IsStale checks if the database version is older than the provided duration
func (v DatabaseVersion) IsStale(threshold time.Duration) bool {
	return v.Age() > threshold
}