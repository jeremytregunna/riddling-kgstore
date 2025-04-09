package storage

import (
	"strings"
	"testing"
	"time"
)

func TestDatabaseVersionEquals(t *testing.T) {
	// Create test version
	ver1 := DatabaseVersion{
		Timestamp:       time.Now(),
		MemTableID:      12345,
		SSTablesVersion: "abcdef1234567890",
		Generation:      123456789,
	}

	// Same version should equal itself
	if !ver1.Equals(ver1) {
		t.Error("DatabaseVersion should equal itself")
	}

	// Create a version with just a different timestamp
	ver2 := DatabaseVersion{
		Timestamp:       time.Now().Add(time.Hour), // Different timestamp
		MemTableID:      12345,                     // Same MemTableID
		SSTablesVersion: "abcdef1234567890",        // Same SSTables
		Generation:      123456789,                 // Same generation
	}

	// Versions should still be equal despite different timestamps
	if !ver1.Equals(ver2) {
		t.Error("DatabaseVersions with different timestamps but same structure should be equal")
	}

	// Create a version with a different MemTableID
	ver3 := DatabaseVersion{
		Timestamp:       ver1.Timestamp,
		MemTableID:      99999, // Different MemTableID
		SSTablesVersion: "abcdef1234567890",
		Generation:      123456789,
	}

	// Versions should not be equal
	if ver1.Equals(ver3) {
		t.Error("DatabaseVersions with different MemTableIDs should not be equal")
	}

	// Create a version with a different SSTables version
	ver4 := DatabaseVersion{
		Timestamp:       ver1.Timestamp,
		MemTableID:      12345,
		SSTablesVersion: "ffffffffffffffff", // Different SSTables
		Generation:      123456789,
	}

	// Versions should not be equal
	if ver1.Equals(ver4) {
		t.Error("DatabaseVersions with different SSTablesVersions should not be equal")
	}

	// Create a version with a different generation
	ver5 := DatabaseVersion{
		Timestamp:       ver1.Timestamp,
		MemTableID:      12345,
		SSTablesVersion: "abcdef1234567890",
		Generation:      987654321, // Different generation
	}

	// Versions should not be equal
	if ver1.Equals(ver5) {
		t.Error("DatabaseVersions with different Generations should not be equal")
	}
}

func TestDatabaseVersionString(t *testing.T) {
	ver := DatabaseVersion{
		Timestamp:       time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		MemTableID:      12345,
		SSTablesVersion: "abcdef1234567890",
		Generation:      123456789,
	}

	str := ver.String()
	
	// Check that the string representation contains all important parts
	if len(str) == 0 {
		t.Error("String representation should not be empty")
	}
	
	// Should contain the generation number
	if !contains(str, "123456789") {
		t.Error("String representation should contain generation number")
	}
	
	// Should contain the memtable ID
	if !contains(str, "12345") {
		t.Error("String representation should contain MemTableID")
	}
	
	// Should contain SSTables version prefix
	if !contains(str, "abcdef") {
		t.Error("String representation should contain SSTablesVersion")
	}
	
	// Should contain timestamp in some format
	if !contains(str, "2023") {
		t.Error("String representation should contain timestamp")
	}
}

func TestDatabaseVersionStaleCheck(t *testing.T) {
	// Create a version 2 hours ago
	oldVer := DatabaseVersion{
		Timestamp:       time.Now().Add(-2 * time.Hour),
		MemTableID:      12345,
		SSTablesVersion: "abcdef1234567890",
		Generation:      123456789,
	}

	// Create a recent version
	newVer := DatabaseVersion{
		Timestamp:       time.Now(),
		MemTableID:      12345,
		SSTablesVersion: "abcdef1234567890",
		Generation:      123456789,
	}

	// Old version should be stale with 1 hour threshold
	if !oldVer.IsStale(time.Hour) {
		t.Error("Old version should be considered stale")
	}

	// New version should not be stale
	if newVer.IsStale(time.Hour) {
		t.Error("New version should not be considered stale")
	}
	
	// Old version should have a positive age
	if oldVer.Age() <= 0 {
		t.Error("Old version should have positive age")
	}
}

func TestNewDatabaseVersion(t *testing.T) {
	// Create a mock MemTable
	memTable := NewMemTable(MemTableConfig{
		MaxSize:    1024 * 1024,
		Comparator: DefaultComparator,
	})

	// Create some mock SSTables
	var sstables []*SSTable
	
	// Create the version with empty SSTables
	emptyVer := NewDatabaseVersion(memTable, sstables, 123456789)
	
	// Check timestamp is recent
	if time.Since(emptyVer.Timestamp) > time.Minute {
		t.Error("Version timestamp should be recent")
	}
	
	// Check MemTableID is set
	if emptyVer.MemTableID == 0 {
		t.Error("MemTableID should not be zero")
	}
	
	// Empty SSTables should have the "empty" marker
	if emptyVer.SSTablesVersion != "empty" {
		t.Error("Empty SSTables should have 'empty' version marker")
	}
	
	// Check generation is passed through
	if emptyVer.Generation != 123456789 {
		t.Error("Generation should be set correctly")
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}