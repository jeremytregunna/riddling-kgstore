package model

import (
	"testing"
)

func TestNewPageAllocator(t *testing.T) {
	// Test with default values
	allocator := NewPageAllocator(0, 0)
	if allocator.pageSize != DefaultPageSize {
		t.Errorf("Expected page size to be %d, got %d", DefaultPageSize, allocator.pageSize)
	}
	if allocator.maxPages != DefaultMaxPages {
		t.Errorf("Expected max pages to be %d, got %d", DefaultMaxPages, allocator.maxPages)
	}

	// Test with custom values
	customPageSize := 8192
	customMaxPages := uint64(500)
	allocator = NewPageAllocator(customPageSize, customMaxPages)
	if allocator.pageSize != customPageSize {
		t.Errorf("Expected page size to be %d, got %d", customPageSize, allocator.pageSize)
	}
	if allocator.maxPages != customMaxPages {
		t.Errorf("Expected max pages to be %d, got %d", customMaxPages, allocator.maxPages)
	}

	// Verify initial state
	if allocator.nextPageID != 1 {
		t.Errorf("Expected next page ID to be 1, got %d", allocator.nextPageID)
	}
	if len(allocator.allocatedMap) != 0 {
		t.Errorf("Expected allocated map to be empty, got %d entries", len(allocator.allocatedMap))
	}
	if len(allocator.freeList) != 0 {
		t.Errorf("Expected free list to be empty, got %d entries", len(allocator.freeList))
	}
	if len(allocator.pages) != 0 {
		t.Errorf("Expected pages map to be empty, got %d entries", len(allocator.pages))
	}
}

func TestAllocatePage(t *testing.T) {
	allocator := NewPageAllocator(DefaultPageSize, DefaultMaxPages)

	// Allocate a page
	page1, err := allocator.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}
	if page1 == nil {
		t.Fatal("Expected page to be non-nil")
	}
	if page1.ID != 1 {
		t.Errorf("Expected page ID to be 1, got %d", page1.ID)
	}
	if len(page1.Data) != DefaultPageSize {
		t.Errorf("Expected page data size to be %d, got %d", DefaultPageSize, len(page1.Data))
	}
	if !allocator.IsAllocated(page1.ID) {
		t.Errorf("Expected page %d to be allocated", page1.ID)
	}

	// Allocate another page
	page2, err := allocator.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate second page: %v", err)
	}
	if page2.ID != 2 {
		t.Errorf("Expected second page ID to be 2, got %d", page2.ID)
	}
	if !allocator.IsAllocated(page2.ID) {
		t.Errorf("Expected page %d to be allocated", page2.ID)
	}

	// Verify allocated page count
	if allocator.AllocatedPageCount() != 2 {
		t.Errorf("Expected allocated page count to be 2, got %d", allocator.AllocatedPageCount())
	}
}

func TestDeallocatePage(t *testing.T) {
	allocator := NewPageAllocator(DefaultPageSize, DefaultMaxPages)

	// Allocate a page
	page, err := allocator.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}
	pageID := page.ID

	// Deallocate the page
	err = allocator.DeallocatePage(pageID)
	if err != nil {
		t.Fatalf("Failed to deallocate page: %v", err)
	}
	if allocator.IsAllocated(pageID) {
		t.Errorf("Expected page %d to be deallocated", pageID)
	}

	// Verify the page is in the free list
	if allocator.FreePageCount() != 1 {
		t.Errorf("Expected free page count to be 1, got %d", allocator.FreePageCount())
	}

	// Try to deallocate a non-allocated page
	err = allocator.DeallocatePage(999)
	if err != ErrPageNotAllocated {
		t.Errorf("Expected ErrPageNotAllocated, got %v", err)
	}

	// Allocate a new page and verify it reuses the deallocated ID
	newPage, err := allocator.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate new page: %v", err)
	}
	if newPage.ID != pageID {
		t.Errorf("Expected new page ID to be %d (reused), got %d", pageID, newPage.ID)
	}
	if allocator.FreePageCount() != 0 {
		t.Errorf("Expected free page count to be 0, got %d", allocator.FreePageCount())
	}
}

func TestGetPage(t *testing.T) {
	allocator := NewPageAllocator(DefaultPageSize, DefaultMaxPages)

	// Allocate a page
	originalPage, err := allocator.AllocatePage()
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}
	pageID := originalPage.ID

	// Get the page
	retrievedPage, err := allocator.GetPage(pageID)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}
	if retrievedPage.ID != pageID {
		t.Errorf("Expected retrieved page ID to be %d, got %d", pageID, retrievedPage.ID)
	}

	// Verify it's the same page object (cached)
	if retrievedPage != originalPage {
		t.Error("Expected retrieved page to be the same object as original page")
	}

	// Try to get a non-allocated page
	_, err = allocator.GetPage(999)
	if err != ErrPageNotAllocated {
		t.Errorf("Expected ErrPageNotAllocated, got %v", err)
	}
}

func TestPageCachingAndFlush(t *testing.T) {
	allocator := NewPageAllocator(DefaultPageSize, DefaultMaxPages)

	// Allocate a few pages
	page1, _ := allocator.AllocatePage()
	page2, _ := allocator.AllocatePage()
	page3, _ := allocator.AllocatePage()

	// Mark some pages as dirty
	page1.MarkDirty()
	page3.MarkDirty()

	// Check dirty pages
	dirtyPages := allocator.FlushDirtyPages()
	if len(dirtyPages) != 2 {
		t.Errorf("Expected 2 dirty pages, got %d", len(dirtyPages))
	}

	// Verify dirty pages have correct IDs
	foundPage1 := false
	foundPage3 := false
	for _, page := range dirtyPages {
		if page.ID == page1.ID {
			foundPage1 = true
		}
		if page.ID == page3.ID {
			foundPage3 = true
		}
	}
	if !foundPage1 {
		t.Errorf("Expected page %d to be in dirty pages list", page1.ID)
	}
	if !foundPage3 {
		t.Errorf("Expected page %d to be in dirty pages list", page3.ID)
	}

	// Clear the cache
	allocator.ClearCache()

	// Verify pages are no longer cached but still allocated
	if len(allocator.pages) != 0 {
		t.Errorf("Expected pages map to be empty after clearing cache, got %d entries", len(allocator.pages))
	}
	if allocator.AllocatedPageCount() != 3 {
		t.Errorf("Expected allocated page count to still be 3, got %d", allocator.AllocatedPageCount())
	}
	if !allocator.IsAllocated(page1.ID) {
		t.Errorf("Expected page %d to still be allocated after clearing cache", page1.ID)
	}
	if !allocator.IsAllocated(page2.ID) {
		t.Errorf("Expected page %d to still be allocated after clearing cache", page2.ID)
	}
	if !allocator.IsAllocated(page3.ID) {
		t.Errorf("Expected page %d to still be allocated after clearing cache", page3.ID)
	}
}

func TestReset(t *testing.T) {
	allocator := NewPageAllocator(DefaultPageSize, DefaultMaxPages)

	// Allocate a few pages
	allocator.AllocatePage()
	allocator.AllocatePage()
	allocator.AllocatePage()

	// Deallocate a page to add to free list
	allocator.DeallocatePage(2)

	// Reset the allocator
	allocator.Reset()

	// Verify state after reset
	if allocator.AllocatedPageCount() != 0 {
		t.Errorf("Expected allocated page count to be 0 after reset, got %d", allocator.AllocatedPageCount())
	}
	if allocator.FreePageCount() != 0 {
		t.Errorf("Expected free page count to be 0 after reset, got %d", allocator.FreePageCount())
	}
	if len(allocator.pages) != 0 {
		t.Errorf("Expected pages map to be empty after reset, got %d entries", len(allocator.pages))
	}
	if allocator.nextPageID != 1 {
		t.Errorf("Expected next page ID to be 1 after reset, got %d", allocator.nextPageID)
	}

	// Allocate a new page and verify ID starts from 1 again
	page, _ := allocator.AllocatePage()
	if page.ID != 1 {
		t.Errorf("Expected page ID to be 1 after reset, got %d", page.ID)
	}
}

func TestMaxPageLimit(t *testing.T) {
	// Create an allocator with a very small limit
	maxPages := uint64(3)
	allocator := NewPageAllocator(DefaultPageSize, maxPages)

	// Allocate up to the limit
	for i := uint64(0); i < maxPages; i++ {
		_, err := allocator.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page %d: %v", i+1, err)
		}
	}

	// Try to allocate one more page (should fail)
	_, err := allocator.AllocatePage()
	if err != ErrNoFreePages {
		t.Errorf("Expected ErrNoFreePages when exceeding max pages, got %v", err)
	}

	// Deallocate a page
	allocator.DeallocatePage(1)

	// Now allocation should work again
	_, err = allocator.AllocatePage()
	if err != nil {
		t.Errorf("Failed to allocate page after deallocation: %v", err)
	}
}

func TestAllocatorAccessors(t *testing.T) {
	pageSize := 8192
	maxPages := uint64(1000)
	allocator := NewPageAllocator(pageSize, maxPages)

	// Test PageSize
	if allocator.PageSize() != pageSize {
		t.Errorf("Expected PageSize() to return %d, got %d", pageSize, allocator.PageSize())
	}

	// Test MaxPageCount
	if allocator.MaxPageCount() != maxPages {
		t.Errorf("Expected MaxPageCount() to return %d, got %d", maxPages, allocator.MaxPageCount())
	}
}

func TestCustomLogger(t *testing.T) {
	allocator := NewPageAllocator(DefaultPageSize, DefaultMaxPages)

	// Create a custom logger
	customLogger := NewNoOpLogger()

	// Set the custom logger
	allocator.SetLogger(customLogger)

	// Perform some operations that would log
	allocator.AllocatePage()
	allocator.DeallocatePage(1)

	// No assertions needed since NoOpLogger discards all messages
	// This just verifies that setting a custom logger doesn't cause any issues
}