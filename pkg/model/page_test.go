package model

import (
	"bytes"
	"testing"
)

func TestNewPage(t *testing.T) {
	id := uint64(42)
	size := 4096

	page := NewPage(id, size)

	if page.ID != id {
		t.Errorf("Expected page ID to be %d, got %d", id, page.ID)
	}

	if len(page.Data) != size {
		t.Errorf("Expected page data size to be %d, got %d", size, len(page.Data))
	}

	if page.Flags != PageFlagNone {
		t.Errorf("Expected page flags to be %d, got %d", PageFlagNone, page.Flags)
	}

	// Check that the data buffer is zeroed
	zeroBuffer := make([]byte, size)
	if !bytes.Equal(page.Data, zeroBuffer) {
		t.Error("Expected page data to be initialized to zeros")
	}
}

func TestPageFlags(t *testing.T) {
	page := NewPage(1, 1024)

	// Test setting individual flags
	page.SetFlag(PageFlagDirty)
	if !page.HasFlag(PageFlagDirty) {
		t.Error("Expected PageFlagDirty to be set")
	}
	if page.HasFlag(PageFlagPinned) {
		t.Error("Expected PageFlagPinned to not be set")
	}

	page.SetFlag(PageFlagPinned)
	if !page.HasFlag(PageFlagPinned) {
		t.Error("Expected PageFlagPinned to be set")
	}

	// Test clearing flags
	page.ClearFlag(PageFlagDirty)
	if page.HasFlag(PageFlagDirty) {
		t.Error("Expected PageFlagDirty to be cleared")
	}
	if !page.HasFlag(PageFlagPinned) {
		t.Error("Expected PageFlagPinned to still be set")
	}

	// Test convenience methods
	page.MarkDirty()
	if !page.IsDirty() {
		t.Error("Expected page to be marked dirty")
	}

	page.ClearDirty()
	if page.IsDirty() {
		t.Error("Expected page to no longer be dirty")
	}

	if !page.IsPinned() {
		t.Error("Expected page to still be pinned")
	}

	if page.IsIndexed() {
		t.Error("Expected page to not be indexed")
	}

	page.SetFlag(PageFlagIndexed)
	if !page.IsIndexed() {
		t.Error("Expected page to be indexed")
	}
}
