package storage

import (
	"bytes"
	"testing"

	"git.canoozie.net/riddling/kgstore/pkg/model"
)

// TestVersionedCompaction demonstrates how versioned records prevent newer
// records from being overwritten during compaction
func TestVersionedCompaction(t *testing.T) {
	// Create temporary directories for this test
	tempDir := t.TempDir()
	sstableDir := tempDir + "/sstables"

	// Create MemTable with a key-value pair representing an "old" record (version 1)
	oldMemTable := NewMemTable(DefaultConfig())
	key := []byte("test-key")
	oldValue := []byte("old-value")
	
	// Use explicit version for demonstration
	err := oldMemTable.PutWithVersion(key, oldValue, 1)
	if err != nil {
		t.Fatalf("Failed to put entry in old MemTable: %v", err)
	}
	
	// Create the "old" SSTable (level 1)
	oldSSTableConfig := SSTableConfig{
		ID:         1,
		Path:       sstableDir,
		Logger:     model.DefaultLoggerInstance,
		Comparator: DefaultComparator,
	}
	
	oldSSTable, err := CreateSSTable(oldSSTableConfig, oldMemTable)
	if err != nil {
		t.Fatalf("Failed to create old SSTable: %v", err)
	}
	defer oldSSTable.Close()
	
	// Create MemTable with the same key but a newer value (version 2)
	newMemTable := NewMemTable(DefaultConfig())
	newValue := []byte("new-value")
	
	// Use higher version for demonstration
	err = newMemTable.PutWithVersion(key, newValue, 2)
	if err != nil {
		t.Fatalf("Failed to put entry in new MemTable: %v", err)
	}
	
	// Create the "new" SSTable (level 0)
	newSSTableConfig := SSTableConfig{
		ID:         2,
		Path:       sstableDir,
		Logger:     model.DefaultLoggerInstance,
		Comparator: DefaultComparator,
	}
	
	newSSTable, err := CreateSSTable(newSSTableConfig, newMemTable)
	if err != nil {
		t.Fatalf("Failed to create new SSTable: %v", err)
	}
	defer newSSTable.Close()
	
	// Now simulate compaction by merging the two SSTables
	// First, create a merged MemTable
	mergedMemTable := NewMemTable(DefaultConfig())
	
	// A naive compaction would merge without considering versions:
	// 1. Read from "old" SSTable
	oldIter, err := oldSSTable.Iterator()
	if err != nil {
		t.Fatalf("Failed to create old iterator: %v", err)
	}
	defer oldIter.Close()
	
	// 2. Read from "new" SSTable
	newIter, err := newSSTable.Iterator()
	if err != nil {
		t.Fatalf("Failed to create new iterator: %v", err)
	}
	defer newIter.Close()
	
	// Our improved approach uses version-aware compaction:
	// 1. Create iterators that include version information
	versionedNewIter, err := newSSTable.IteratorWithOptions(IteratorOptions{IncludeTombstones: true})
	if err != nil {
		t.Fatalf("Failed to create versioned new iterator: %v", err)
	}
	defer versionedNewIter.Close()
	
	versionedOldIter, err := oldSSTable.IteratorWithOptions(IteratorOptions{IncludeTombstones: true})
	if err != nil {
		t.Fatalf("Failed to create versioned old iterator: %v", err)
	}
	defer versionedOldIter.Close()
	
	// 2. Gather all keys and their versioned values
	keyVersions := make(map[string]struct {
		value     []byte
		version   uint64
		isDeleted bool
	})
	
	// Process entries from "new" SSTable (level 0)
	for versionedNewIter.Valid() {
		key := string(versionedNewIter.Key())
		currentVersion := versionedNewIter.Version()
		
		// If we haven't seen this key before, or if this version is higher than what we've seen
		existingEntry, exists := keyVersions[key]
		if !exists || currentVersion > existingEntry.version {
			keyVersions[key] = struct {
				value     []byte
				version   uint64
				isDeleted bool
			}{
				value:     versionedNewIter.Value(),
				version:   currentVersion,
				isDeleted: versionedNewIter.IsDeleted(),
			}
		}
		
		if err := versionedNewIter.Next(); err != nil {
			t.Fatalf("Failed to advance new iterator: %v", err)
		}
	}
	
	// Process entries from "old" SSTable (level 1)
	for versionedOldIter.Valid() {
		key := string(versionedOldIter.Key())
		currentVersion := versionedOldIter.Version()
		
		// If we haven't seen this key before, or if this version is higher than what we've seen
		existingEntry, exists := keyVersions[key]
		if !exists || currentVersion > existingEntry.version {
			keyVersions[key] = struct {
				value     []byte
				version   uint64
				isDeleted bool
			}{
				value:     versionedOldIter.Value(),
				version:   currentVersion,
				isDeleted: versionedOldIter.IsDeleted(),
			}
		}
		
		if err := versionedOldIter.Next(); err != nil {
			t.Fatalf("Failed to advance old iterator: %v", err)
		}
	}
	
	// 3. Add the entries with the highest version to the merged MemTable
	for keyStr, entry := range keyVersions {
		keyBytes := []byte(keyStr)
		if entry.isDeleted {
			// For tombstones, use Delete with version
			if err := mergedMemTable.DeleteWithVersion(keyBytes, entry.version); err != nil {
				t.Fatalf("Failed to delete entry in merged MemTable: %v", err)
			}
		} else {
			// For regular entries, use Put with version
			if err := mergedMemTable.PutWithVersion(keyBytes, entry.value, entry.version); err != nil {
				t.Fatalf("Failed to put entry in merged MemTable: %v", err)
			}
		}
	}
	
	// 4. Verify the merged MemTable has the correct (newer) value
	mergedValue, err := mergedMemTable.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key from merged MemTable: %v", err)
	}
	
	if !bytes.Equal(mergedValue, newValue) {
		t.Fatalf("Merged MemTable has incorrect value: expected %s, got %s", 
			newValue, mergedValue)
	}
	
	t.Logf("Successfully preserved newer value during version-aware compaction")
}