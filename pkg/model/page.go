package model

// PageFlag represents various status flags for a page
type PageFlag uint8

const (
	PageFlagNone    PageFlag = 0
	PageFlagDirty   PageFlag = 1 << 0 // Page has been modified in memory but not written to disk
	PageFlagPinned  PageFlag = 1 << 1 // Page is pinned in memory and cannot be evicted
	PageFlagIndexed PageFlag = 1 << 2 // Page belongs to an index structure
)

// Page represents a fixed-size block of data in the database
type Page struct {
	ID    uint64   // Unique identifier for the page
	Data  []byte   // Raw data stored in the page
	Flags PageFlag // Status flags for the page
}

// NewPage creates a new Page with the given ID and an empty data buffer of the specified size
func NewPage(id uint64, size int) *Page {
	return &Page{
		ID:    id,
		Data:  make([]byte, size),
		Flags: PageFlagNone,
	}
}

// SetFlag sets the specified flag on the page
func (p *Page) SetFlag(flag PageFlag) {
	p.Flags |= flag
}

// ClearFlag clears the specified flag on the page
func (p *Page) ClearFlag(flag PageFlag) {
	p.Flags &= ^flag
}

// HasFlag checks if the page has the specified flag set
func (p *Page) HasFlag(flag PageFlag) bool {
	return (p.Flags & flag) != 0
}

// IsDirty returns true if the page is marked as dirty
func (p *Page) IsDirty() bool {
	return p.HasFlag(PageFlagDirty)
}

// IsPinned returns true if the page is pinned in memory
func (p *Page) IsPinned() bool {
	return p.HasFlag(PageFlagPinned)
}

// IsIndexed returns true if the page belongs to an index structure
func (p *Page) IsIndexed() bool {
	return p.HasFlag(PageFlagIndexed)
}

// MarkDirty marks the page as dirty (modified but not saved)
func (p *Page) MarkDirty() {
	p.SetFlag(PageFlagDirty)
}

// ClearDirty clears the dirty flag (after the page has been saved)
func (p *Page) ClearDirty() {
	p.ClearFlag(PageFlagDirty)
}
