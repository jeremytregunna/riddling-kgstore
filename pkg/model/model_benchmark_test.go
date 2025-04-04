package model

import (
	"testing"
)

// BenchmarkNodeCreation benchmarks Node creation
func BenchmarkNodeCreation(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewNode(uint64(i), "Person")
	}
}

// BenchmarkNodeWithProperties benchmarks Node creation with properties
func BenchmarkNodeWithProperties(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node := NewNode(uint64(i), "Person")
		node.AddProperty("name", "John Doe")
		node.AddProperty("age", "30")
		node.AddProperty("city", "New York")
	}
}

// BenchmarkEdgeCreation benchmarks Edge creation
func BenchmarkEdgeCreation(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewEdge(uint64(i), uint64(i+1), "KNOWS")
	}
}

// BenchmarkPropertyCreation benchmarks Property creation
func BenchmarkPropertyCreation(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewProperty("name", "John Doe")
	}
}

// BenchmarkPageCreation benchmarks Page creation
func BenchmarkPageCreation(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewPage(uint64(i), 4096)
	}
}

// BenchmarkPageAllocator benchmarks PageAllocator operations
func BenchmarkPageAllocator(b *testing.B) {
	allocator := NewPageAllocator(4096, 1000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page, _ := allocator.AllocatePage()
		allocator.DeallocatePage(page.ID)
	}
}

// BenchmarkNodeSerialization benchmarks Node serialization
func BenchmarkNodeSerialization(b *testing.B) {
	node := NewNode(42, "Person")
	node.AddProperty("name", "John Doe")
	node.AddProperty("age", "30")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = SerializeNode(node)
	}
}

// BenchmarkNodeDeserialization benchmarks Node deserialization
func BenchmarkNodeDeserialization(b *testing.B) {
	node := NewNode(42, "Person")
	node.AddProperty("name", "John Doe")
	node.AddProperty("age", "30")
	
	data, _ := SerializeNode(node)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DeserializeNode(data)
	}
}

// BenchmarkWriteEntityToPage benchmarks writing an entity to a page
func BenchmarkWriteEntityToPage(b *testing.B) {
	page := NewPage(1, 4096)
	node := NewNode(42, "Person")
	node.AddProperty("name", "John Doe")
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page.ClearDirty() // Reset dirty flag
		_, _ = WriteEntityToPage(page, node, 0)
	}
}

// BenchmarkReadEntityFromPage benchmarks reading an entity from a page
func BenchmarkReadEntityFromPage(b *testing.B) {
	page := NewPage(1, 4096)
	node := NewNode(42, "Person")
	node.AddProperty("name", "John Doe")
	
	WriteEntityToPage(page, node, 0)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = ReadEntityFromPage(page, 0)
	}
}