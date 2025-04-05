package model

import (
	"errors"
	"sync"
)

const (
	// DefaultPageSize is the default size of a page in bytes
	DefaultPageSize = 4096

	// DefaultMaxPages is the default maximum number of pages that can be allocated
	DefaultMaxPages = 1000000
)

var (
	// ErrNoFreePages is returned when there are no more free pages available
	ErrNoFreePages = errors.New("no free pages available")

	// ErrInvalidPageID is returned when an invalid page ID is provided
	ErrInvalidPageID = errors.New("invalid page ID")

	// ErrPageNotAllocated is returned when trying to access a page that is not allocated
	ErrPageNotAllocated = errors.New("page not allocated")
)

// PageAllocator manages the allocation and deallocation of pages
type PageAllocator struct {
	pageSize     int              // Size of each page in bytes
	maxPages     uint64           // Maximum number of pages that can be allocated
	allocatedMap map[uint64]bool  // Map of allocated page IDs
	freeList     []uint64         // List of available page IDs
	nextPageID   uint64           // Next page ID to allocate if free list is empty
	pages        map[uint64]*Page // Cache of allocated pages
	mu           sync.RWMutex     // Mutex for thread safety
	logger       Logger           // Logger for allocation/deallocation events
}

// NewPageAllocator creates a new PageAllocator with specified page size and maximum pages
func NewPageAllocator(pageSize int, maxPages uint64) *PageAllocator {
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	if maxPages == 0 {
		maxPages = DefaultMaxPages
	}

	return &PageAllocator{
		pageSize:     pageSize,
		maxPages:     maxPages,
		allocatedMap: make(map[uint64]bool),
		freeList:     make([]uint64, 0),
		nextPageID:   1, // Start with page ID 1 (0 is reserved)
		pages:        make(map[uint64]*Page),
		logger:       DefaultLoggerInstance,
	}
}

// SetLogger sets the logger for the page allocator
func (pa *PageAllocator) SetLogger(logger Logger) {
	pa.mu.Lock()
	defer pa.mu.Unlock()
	pa.logger = logger
}

// AllocatePage allocates a new page and returns its ID
func (pa *PageAllocator) AllocatePage() (*Page, error) {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	var pageID uint64

	// Check if we've reached the maximum number of pages
	if uint64(len(pa.allocatedMap)) >= pa.maxPages {
		pa.logger.Error("Cannot allocate page: maximum number of pages (%d) reached", pa.maxPages)
		return nil, ErrNoFreePages
	}

	// Try to reuse a page ID from the free list
	if len(pa.freeList) > 0 {
		pageID = pa.freeList[0]
		pa.freeList = pa.freeList[1:]
	} else {
		// Allocate a new page ID
		pageID = pa.nextPageID
		pa.nextPageID++
	}

	// Mark the page as allocated
	pa.allocatedMap[pageID] = true

	// Create a new page
	page := NewPage(pageID, pa.pageSize)

	// Cache the page
	pa.pages[pageID] = page

	pa.logger.Debug("Allocated page with ID %d", pageID)
	return page, nil
}

// DeallocatePage deallocates a page with the given ID
func (pa *PageAllocator) DeallocatePage(pageID uint64) error {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	// Check if the page is allocated
	if !pa.allocatedMap[pageID] {
		pa.logger.Warn("Cannot deallocate page: page with ID %d is not allocated", pageID)
		return ErrPageNotAllocated
	}

	// Remove the page from the allocated map
	delete(pa.allocatedMap, pageID)

	// Remove the page from the cache
	delete(pa.pages, pageID)

	// Add the page ID to the free list for reuse
	pa.freeList = append(pa.freeList, pageID)

	pa.logger.Debug("Deallocated page with ID %d", pageID)
	return nil
}

// GetPage returns a page with the given ID
func (pa *PageAllocator) GetPage(pageID uint64) (*Page, error) {
	pa.mu.RLock()
	defer pa.mu.RUnlock()

	// Check if the page is allocated
	if !pa.allocatedMap[pageID] {
		return nil, ErrPageNotAllocated
	}

	// Return the page from the cache
	page := pa.pages[pageID]
	return page, nil
}

// IsAllocated returns true if the page with the given ID is allocated
func (pa *PageAllocator) IsAllocated(pageID uint64) bool {
	pa.mu.RLock()
	defer pa.mu.RUnlock()
	return pa.allocatedMap[pageID]
}

// AllocatedPageCount returns the number of currently allocated pages
func (pa *PageAllocator) AllocatedPageCount() int {
	pa.mu.RLock()
	defer pa.mu.RUnlock()
	return len(pa.allocatedMap)
}

// FreePageCount returns the number of pages in the free list
func (pa *PageAllocator) FreePageCount() int {
	pa.mu.RLock()
	defer pa.mu.RUnlock()
	return len(pa.freeList)
}

// MaxPageCount returns the maximum number of pages
func (pa *PageAllocator) MaxPageCount() uint64 {
	return pa.maxPages
}

// PageSize returns the size of each page in bytes
func (pa *PageAllocator) PageSize() int {
	return pa.pageSize
}

// ClearCache releases all cached pages
func (pa *PageAllocator) ClearCache() {
	pa.mu.Lock()
	defer pa.mu.Unlock()
	pa.pages = make(map[uint64]*Page)
	pa.logger.Info("Cleared page cache")
}

// FlushDirtyPages returns a list of all dirty pages in the cache
func (pa *PageAllocator) FlushDirtyPages() []*Page {
	pa.mu.RLock()
	defer pa.mu.RUnlock()

	dirtyPages := make([]*Page, 0)
	for _, page := range pa.pages {
		if page.IsDirty() {
			dirtyPages = append(dirtyPages, page)
		}
	}

	return dirtyPages
}

// Reset resets the page allocator to its initial state
func (pa *PageAllocator) Reset() {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	pa.allocatedMap = make(map[uint64]bool)
	pa.freeList = make([]uint64, 0)
	pa.nextPageID = 1
	pa.pages = make(map[uint64]*Page)

	pa.logger.Info("Reset page allocator")
}
