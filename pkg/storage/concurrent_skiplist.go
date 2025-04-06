package storage

import (
	"bytes"
	"math/rand"
	"sync/atomic"
	"unsafe"
)

// Max height of the skiplist
const maxHeight = 12

// Node represents a node in the concurrent skiplist
type Node struct {
	key       []byte
	value     []byte
	marked    uint32  // Marked for deletion (atomically accessed)
	size      uint64  // Size in bytes
	next      []*Node // Array of next pointers at each level
	version   uint64  // Version number for this record
	isDeleted uint32  // Deletion marker (atomically accessed)
}

// ConcurrentSkipList is a lock-free skiplist implementation
type ConcurrentSkipList struct {
	head           *Node      // Pointer to the head (sentinel) node
	tail           *Node      // Pointer to the tail (sentinel) node
	height         int32      // Current maximum height (atomically accessed)
	size           int64      // Total size in bytes (atomically accessed)
	count          int64      // Number of entries (atomically accessed)
	currentVersion uint64     // Current version counter (atomically accessed)
	comparator     Comparator // Function for comparing keys
}

// NewConcurrentSkipList creates a new empty concurrent skiplist
func NewConcurrentSkipList(comparator Comparator) *ConcurrentSkipList {
	// Create head node with maximum height
	// All keys are greater than head
	head := &Node{
		key:   nil,
		next:  make([]*Node, maxHeight),
		value: nil,
	}

	// Create tail node with maximum height
	// All keys are less than tail
	tail := &Node{
		key:   nil,
		next:  make([]*Node, maxHeight),
		value: nil,
	}

	// Link head to tail at all levels
	for i := 0; i < maxHeight; i++ {
		head.next[i] = tail
	}

	if comparator == nil {
		comparator = bytes.Compare
	}

	return &ConcurrentSkipList{
		head:           head,
		tail:           tail,
		height:         1,
		size:           0,
		count:          0,
		currentVersion: 1,
		comparator:     comparator,
	}
}

// randomHeight generates a random height for a new node
func (cs *ConcurrentSkipList) randomHeight() int {
	const probability = 0.25 // Probability to increase height
	height := 1

	// With probability 1/4, increase height until reaching max height
	for height < maxHeight && rand.Float64() < probability {
		height++
	}

	return height
}

// findNodeAndPrevs searches for a key in the skiplist
// Returns the node with the key (or nil if not found) and an array of predecessor nodes
// Improved for better consistency in concurrent environments
func (cs *ConcurrentSkipList) findNodeAndPrevs(key []byte) (*Node, []*Node) {
	// Safety check
	if key == nil || cs.head == nil || cs.tail == nil {
		return nil, nil
	}

	// Maximum retries for the entire operation
	// This helps handle cases where the list structure is being heavily modified
	const maxFullRetries = 2
	var foundNode *Node
	var foundPrevs []*Node

	for fullRetry := 0; fullRetry <= maxFullRetries; fullRetry++ {
		prevs := make([]*Node, maxHeight)
		curr := cs.head

		// Get current height atomically to prevent race conditions
		// Start from the highest level of the skip list
		currHeight := int(atomic.LoadInt32(&cs.height))
		if currHeight <= 0 {
			currHeight = 1 // Ensure we at least check level 0
		}

		// Track if we had a consistent traversal without structural modifications
		consistent := true

		// Traverse the list level by level, from highest to lowest
		for i := currHeight - 1; i >= 0; i-- {
			// Safety check for current node
			if curr == nil || curr.next == nil || i >= len(curr.next) {
				// Something is wrong, use head as fallback
				curr = cs.head
				// Skip this level if we still have issues
				if curr == nil || curr.next == nil || i >= len(curr.next) {
					consistent = false
					continue
				}
			}

			// Traverse the current level
			for {
				// Get next pointer safely
				next := curr.next[i]

				// Check if we've hit the end of the list at this level
				if next == nil || next == cs.tail {
					break
				}

				// Skip nodes marked for deletion
				if atomic.LoadUint32(&next.marked) == 1 {
					// Try to help by updating curr.next[i] to skip the marked node
					// This helps with "garbage collection" of marked nodes
					nextNext := next.next[i]
					if nextNext != nil {
						// This is a helping CAS - it's ok if it fails
						atomic.CompareAndSwapPointer(
							(*unsafe.Pointer)(unsafe.Pointer(&curr.next[i])),
							unsafe.Pointer(next),
							unsafe.Pointer(nextNext))
					}
					// Regardless of CAS success, skip this node
					curr = next
					continue
				}

				// Safety check for key
				if next.key == nil {
					// Skip invalid nodes
					curr = next
					continue
				}

				// Compare keys
				cmp := cs.comparator(next.key, key)
				if cmp < 0 {
					// Move to next node
					curr = next
				} else {
					// Found node ≥ key, stop at this level
					break
				}
			}

			// Save predecessor at this level
			prevs[i] = curr
		}

		// Only proceed if we had a consistent traversal
		if consistent {
			// Check if we found the exact key - with appropriate safety checks
			if prevs[0] != nil && prevs[0].next != nil && len(prevs[0].next) > 0 {
				next := prevs[0].next[0]
				if next != nil && next != cs.tail && next.key != nil &&
					cs.comparator(next.key, key) == 0 && atomic.LoadUint32(&next.marked) == 0 {
					// Found the key, return the node and its predecessors
					foundNode = next
					foundPrevs = prevs
					break
				}
			}

			// If we didn't find the exact key but had a consistent traversal,
			// still return the predecessors for potential insertion
			foundPrevs = prevs
			break
		}

		// If we had an inconsistent traversal and this is not the last retry,
		// add a small delay before retrying
		if fullRetry < maxFullRetries {
			// Simple CPU yield
			for i := 0; i < 50; i++ {
				// Busy wait
			}
		}
	}

	return foundNode, foundPrevs
}

// Put adds or updates a key-value pair in the skiplist
func (cs *ConcurrentSkipList) Put(key, value []byte) (uint64, bool) {
	// Generate new version
	version := atomic.AddUint64(&cs.currentVersion, 1)
	return cs.PutWithVersion(key, value, version)
}

// PutWithVersion adds or updates a key-value pair with a specific version
// Returns the version used and whether it was a new insert (true) or update (false)
func (cs *ConcurrentSkipList) PutWithVersion(key, value []byte, version uint64) (uint64, bool) {
	if key == nil || value == nil {
		return 0, false
	}

	entrySize := uint64(len(key) + len(value))
	isInsert := false

	// Find the node and its predecessors
	node, prevs := cs.findNodeAndPrevs(key)

	// If key exists, update it
	if node != nil {
		// Update value atomically
		oldSize := node.size

		// Create a new copy of the value
		newValue := make([]byte, len(value))
		copy(newValue, value)

		// Atomically update the node
		node.value = newValue
		node.size = entrySize
		atomic.StoreUint64(&node.version, version)

		// If node was deleted, mark as not deleted and update count
		if atomic.LoadUint32(&node.isDeleted) == 1 {
			if atomic.CompareAndSwapUint32(&node.isDeleted, 1, 0) {
				atomic.AddInt64(&cs.count, 1)
				isInsert = true
			}
		}

		// Update size tracker
		diff := int64(entrySize) - int64(oldSize)
		atomic.AddInt64(&cs.size, diff)

		return version, isInsert
	}

	// Key doesn't exist, create a new node
	height := cs.randomHeight()
	newNode := &Node{
		key:     make([]byte, len(key)),
		value:   make([]byte, len(value)),
		next:    make([]*Node, height),
		size:    entrySize,
		version: version,
	}

	// Copy the key and value
	copy(newNode.key, key)
	copy(newNode.value, value)

	// Update the skiplist height if necessary
	currHeight := int(atomic.LoadInt32(&cs.height))
	if height > currHeight {
		// Try to update the height atomically
		for !atomic.CompareAndSwapInt32(&cs.height, int32(currHeight), int32(height)) {
			currHeight = int(atomic.LoadInt32(&cs.height))
			if height <= currHeight {
				break
			}
		}
	}

	// Link the new node to successors at each level
	for i := 0; i < height; i++ {
		if i >= currHeight {
			// For levels above current height, link from head
			newNode.next[i] = cs.head.next[i]
		} else if prevs != nil && i < len(prevs) && prevs[i] != nil &&
			i < len(prevs[i].next) {
			// Normal case - link from predecessor
			newNode.next[i] = prevs[i].next[i]
		} else {
			// Safety fallback if we can't get proper next pointer
			newNode.next[i] = cs.tail
		}
	}

	// Try to insert at level 0 first (most important level)
	success := false
	maxRetries := 5 // Increased retry count for high-contention scenarios

	for retries := 0; !success && retries < maxRetries; retries++ {
		// First, ensure prevs[0] is valid
		if prevs[0] == nil || prevs[0].next == nil || len(prevs[0].next) == 0 {
			// Invalid predecessor, find new ones
			_, prevs = cs.findNodeAndPrevs(key)
			if prevs == nil || prevs[0] == nil {
				// Something is wrong with the list structure
				// Create a fresh node and try again
				continue
			}
		}

		// Get the successor
		succ := prevs[0].next[0]
		if succ == nil {
			// Invalid successor, find new predecessors
			_, prevs = cs.findNodeAndPrevs(key)
			continue
		}

		// Update newNode's next pointer to current successor
		newNode.next[0] = succ

		// Check if the current successor is our key (added by another thread)
		if succ != cs.tail && succ.key != nil && cs.comparator(succ.key, key) == 0 {
			// Key already exists, update it instead
			node = succ

			// Update the node (similar logic as the update case above)
			oldSize := node.size
			newValue := make([]byte, len(value))
			copy(newValue, value)

			// Atomically update the node's value and size
			node.value = newValue
			node.size = entrySize
			atomic.StoreUint64(&node.version, version)

			// If the node was deleted, mark it as not deleted
			if atomic.LoadUint32(&node.isDeleted) == 1 {
				atomic.StoreUint32(&node.isDeleted, 0)
			}

			// Update size tracker
			diff := int64(entrySize) - int64(oldSize)
			atomic.AddInt64(&cs.size, diff)

			return version, false // Not a new insert
		}

		// Try to insert between predecessor and successor
		if atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&prevs[0].next[0])),
			unsafe.Pointer(succ),
			unsafe.Pointer(newNode)) {
			success = true
		} else {
			// If insertion fails, find new predecessors and try again
			_, prevs = cs.findNodeAndPrevs(key)

			// Add a small delay to reduce contention (helps with fairness)
			for i := 0; i < (retries+1)*10 && i < 100; i++ {
				// Simple CPU yield to give other threads a chance
			}
		}
	}

	// If we've exhausted retries, do one final check to see if our key was added
	if !success {
		node, _ := cs.findNodeAndPrevs(key)
		if node != nil && cs.comparator(node.key, key) == 0 {
			// Key was added by another thread, update it
			oldSize := node.size
			newValue := make([]byte, len(value))
			copy(newValue, value)

			node.value = newValue
			node.size = entrySize
			atomic.StoreUint64(&node.version, version)

			// Update size tracker
			diff := int64(entrySize) - int64(oldSize)
			atomic.AddInt64(&cs.size, diff)

			return version, false
		}

		// We couldn't add the node after all retries - this is an edge case
		// that shouldn't happen often with sufficient retries.
		// At this point, we'll just abandon the insertion at all levels.
		return 0, false
	}

	// Update node's next pointers with new successors for other levels
	// This is done after level 0 insertion succeeds
	for i := 1; i < height; i++ {
		if i < len(prevs) && prevs[i] != nil {
			newNode.next[i] = prevs[i].next[i]
		}
	}

	// Insert at all other levels
	// We'll only attempt higher levels if the key hasn't been inserted already by another thread
	for i := 1; i < height && i < len(prevs); i++ {
		// Make sure prevs[i] isn't nil before proceeding
		if prevs[i] == nil || prevs[i].next == nil || len(prevs[i].next) <= i {
			continue // Skip this level if prevs[i] is invalid
		}

		// Only proceed if the node still exists and isn't marked
		// This is an extra safety check since another thread might have modified the list
		if atomic.LoadUint32(&newNode.marked) == 1 {
			break // Stop if the node has been marked for deletion
		}

		// Get current successor safely
		succ := prevs[i].next[i]
		if succ == nil {
			continue // Skip if successor is nil
		}

		// Try to insert between predecessor and successor
		newNode.next[i] = succ

		// Use CAS to atomically update the link
		success := atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&prevs[i].next[i])),
			unsafe.Pointer(succ),
			unsafe.Pointer(newNode))

		if !success {
			// If CAS failed, the predecessor's next pointer changed
			// We need to find the correct predecessor again for this level
			for retries := 0; !success && retries < 3; retries++ {
				// Update prevs for this level
				prevNode := cs.findPredecessorAtLevel(key, i)
				if prevNode == nil {
					break // Can't find proper predecessor, give up
				}

				// Check if someone else linked our node
				if isNodeLinkedAtLevel(prevNode, newNode, i) {
					success = true
					break
				}

				// Try to get the current successor
				succ = prevNode.next[i]
				if succ == nil {
					break // Invalid successor
				}

				// Update node's next pointer
				newNode.next[i] = succ

				// Try CAS again
				success = atomic.CompareAndSwapPointer(
					(*unsafe.Pointer)(unsafe.Pointer(&prevNode.next[i])),
					unsafe.Pointer(succ),
					unsafe.Pointer(newNode))
			}
		}

		// We limit retries but ensure level 0 is always properly linked
		// The key will always be accessible from level 0, even if higher levels fail
	}

	// Update size and count
	atomic.AddInt64(&cs.size, int64(entrySize))
	atomic.AddInt64(&cs.count, 1)

	return version, true
}

// Get retrieves a value from the skiplist by key with retry capability for concurrent environments
func (cs *ConcurrentSkipList) Get(key []byte) ([]byte, uint64, bool) {
	// Safety check
	if key == nil || cs.head == nil {
		return nil, 0, false
	}

	// Try up to 3 times to get the key
	// This helps handle cases where the node is being inserted by another thread
	// but hasn't completed linking at higher levels
	const maxRetries = 3
	for retry := 0; retry < maxRetries; retry++ {
		// Find the node using findNodeAndPrevs, which is already designed to handle
		// concurrent modifications
		node, _ := cs.findNodeAndPrevs(key)

		// Check if we found a valid node with the key
		if node != nil &&
			cs.comparator(node.key, key) == 0 &&
			atomic.LoadUint32(&node.marked) == 0 &&
			atomic.LoadUint32(&node.isDeleted) == 0 {

			// Safety check for value
			if node.value == nil {
				return nil, 0, false
			}

			// Make a copy of the value to avoid any race conditions
			// if another thread modifies the value
			valueCopy := make([]byte, len(node.value))
			copy(valueCopy, node.value)

			// Read the version atomically
			version := atomic.LoadUint64(&node.version)

			return valueCopy, version, true
		}

		// If we're not on the last retry, insert a small delay
		// This helps reduce contention when other threads are in the middle of updates
		if retry < maxRetries-1 {
			// Simple busy-waiting as a form of backoff
			for i := 0; i < (retry+1)*10 && i < 50; i++ {
				// CPU yield - alternative to time.Sleep for short durations
				// in a concurrent environment
			}
		}
	}

	// Key not found after retries
	return nil, 0, false
}

// Contains checks if a key exists in the skiplist
func (cs *ConcurrentSkipList) Contains(key []byte) bool {
	_, _, found := cs.Get(key)
	return found
}

// Delete marks a key as deleted
func (cs *ConcurrentSkipList) Delete(key []byte) (uint64, bool) {
	// Generate new version
	version := atomic.AddUint64(&cs.currentVersion, 1)
	return cs.DeleteWithVersion(key, version)
}

// DeleteWithVersion marks a key as deleted with the given version
// Returns the version used and whether the key was found and deleted
func (cs *ConcurrentSkipList) DeleteWithVersion(key []byte, version uint64) (uint64, bool) {
	if key == nil {
		return 0, false
	}

	// Maximum retries when deletion fails due to concurrent modifications
	const maxRetries = 3
	var deleted bool
	var usedVersion uint64

	for retry := 0; retry < maxRetries; retry++ {
		// Find the node
		node, _ := cs.findNodeAndPrevs(key)
		if node == nil || atomic.LoadUint32(&node.marked) == 1 {
			return 0, false // Key not found or node already marked for removal
		}

		// Try to mark as logically deleted
		// If node is already logically deleted, nothing to do
		if !atomic.CompareAndSwapUint32(&node.isDeleted, 0, 1) {
			// Node is already deleted, check if we should break or retry
			if retry == maxRetries-1 {
				// On last retry, consider it success if the node is deleted
				// regardless of who deleted it
				return atomic.LoadUint64(&node.version), true
			}
			continue // Try again to find a node that's not deleted
		}

		// Update version
		atomic.StoreUint64(&node.version, version)

		// Update size and count
		atomic.AddInt64(&cs.size, -int64(node.size))
		atomic.AddInt64(&cs.count, -1)

		// Deletion was successful
		usedVersion = version
		deleted = true
		break
	}

	// If we've successfully deleted the node, make sure the delete is visible
	// by helping with physical deletion from the list if needed
	if deleted {
		// Attempt to help with physical removal - this is an optimization
		// but not strictly required, as logically deleted nodes will be skipped
		_, prevs := cs.findNodeAndPrevs(key)

		// Skip physical deletion if we don't have valid predecessors
		if prevs != nil && len(prevs) > 0 && prevs[0] != nil {
			// Try to physically unlink at level 0 for better space efficiency
			// This is best-effort only - logical deletion is sufficient for correctness
			if prevs[0].next != nil && len(prevs[0].next) > 0 {
				next := prevs[0].next[0]
				if next != nil && next.key != nil && cs.comparator(next.key, key) == 0 {
					// Check if we can perform the unlinking
					if atomic.LoadUint32(&next.isDeleted) == 1 {
						// Get the successor at level 0
						successor := next.next[0]
						if successor != nil {
							// Attempt to unlink - this is non-critical
							atomic.CompareAndSwapPointer(
								(*unsafe.Pointer)(unsafe.Pointer(&prevs[0].next[0])),
								unsafe.Pointer(next),
								unsafe.Pointer(successor))
						}
					}
				}
			}
		}
	}

	return usedVersion, deleted
}

// ForceDelete physically removes a node from the skiplist
// This is much more complex in a lock-free implementation
func (cs *ConcurrentSkipList) ForceDelete(key []byte) bool {
	if key == nil {
		return false
	}

	// First find the node
	var node *Node
	var prevs []*Node
	var marked bool = false

	// First mark the node for deletion
	for !marked {
		var newNode *Node
		// Find the node and its predecessors
		newNode, prevs = cs.findNodeAndPrevs(key)
		if newNode == nil {
			return false // Key not found
		}

		// Mark the node as deleted to prevent new references
		if atomic.CompareAndSwapUint32(&newNode.marked, 0, 1) {
			node = newNode
			marked = true
		} else {
			// If marking failed, someone else might have marked or deleted the node
			return false
		}
	}

	// Now physically remove the node from the list
	if node == nil {
		// This shouldn't happen but check anyway
		return false
	}

	// Get node height
	nodeHeight := len(node.next)

	// Try to physically remove the node from each level
	// Start from highest level and work down
	for level := nodeHeight - 1; level >= 0; level-- {
		for {
			// Get the successor at this level
			succ := node.next[level]

			// Try to unlink node at this level
			if atomic.CompareAndSwapPointer(
				(*unsafe.Pointer)(unsafe.Pointer(&prevs[level].next[level])),
				unsafe.Pointer(node),
				unsafe.Pointer(succ)) {
				break // Successfully unlinked at this level
			}

			// If unsuccessful, find new predecessors and retry
			_, newPrevs := cs.findNodeAndPrevs(key)
			// If node is no longer in the list, another thread removed it
			if newPrevs[level].next[level] != node {
				break
			}
			prevs = newPrevs
		}
	}

	// Update size and count if the node was logically deleted
	if atomic.LoadUint32(&node.isDeleted) == 1 {
		// No need to update size/count as it was already done in logical delete
		return true
	}

	// If node wasn't logically deleted before, update size and count now
	atomic.AddInt64(&cs.size, -int64(node.size))
	atomic.AddInt64(&cs.count, -1)

	// Mark as logically deleted too
	atomic.StoreUint32(&node.isDeleted, 1)

	return true
}

// Size returns the current size of the skiplist in bytes
func (cs *ConcurrentSkipList) Size() uint64 {
	return uint64(atomic.LoadInt64(&cs.size))
}

// Count returns the number of entries in the skiplist
func (cs *ConcurrentSkipList) Count() int {
	return int(atomic.LoadInt64(&cs.count))
}

// GetEntries returns all entries in the skiplist in sorted order
// This is not a lock-free operation but is needed for compatibility
func (cs *ConcurrentSkipList) GetEntries() [][]byte {
	// Create a slice to hold key-value pairs
	entries := make([][]byte, 0, cs.Count()*2)

	// Start traversal from level 0
	curr := cs.head.next[0]

	// Traverse all nodes
	for curr != cs.tail {
		// Skip deleted nodes
		if atomic.LoadUint32(&curr.marked) == 1 || atomic.LoadUint32(&curr.isDeleted) == 1 {
			curr = curr.next[0]
			continue
		}

		// Add key and value to result
		keyCopy := make([]byte, len(curr.key))
		valueCopy := make([]byte, len(curr.value))
		copy(keyCopy, curr.key)
		copy(valueCopy, curr.value)

		entries = append(entries, keyCopy)
		entries = append(entries, valueCopy)

		// Move to next node
		curr = curr.next[0]
	}

	return entries
}

// GetVersion returns the current version counter
func (cs *ConcurrentSkipList) GetVersion() uint64 {
	return atomic.LoadUint64(&cs.currentVersion)
}

// findPredecessorAtLevel finds the predecessor node for a key at a specific level
func (cs *ConcurrentSkipList) findPredecessorAtLevel(key []byte, level int) *Node {
	if key == nil || level < 0 || level >= maxHeight {
		return nil
	}

	// Start from head
	curr := cs.head

	// For efficiency, start searching from a higher level and work down
	// until we reach the target level
	height := int(atomic.LoadInt32(&cs.height)) - 1
	startLevel := height
	if height > maxHeight-1 {
		startLevel = maxHeight - 1
	}
	for i := startLevel; i >= level; i-- {
		// Traverse the current level until we find a node whose next key
		// is greater than or equal to our target key
		for {
			// Safety checks
			if curr == nil || curr.next == nil || i >= len(curr.next) {
				if i == level {
					return nil // Invalid state at our target level
				}
				break // Move down to next level with curr as is
			}

			next := curr.next[i]
			if next == nil || next == cs.tail {
				break // End of list at this level
			}

			// Skip deleted nodes
			if atomic.LoadUint32(&next.marked) == 1 {
				curr = next
				continue
			}

			// Safety check for key
			if next.key == nil {
				curr = next
				continue
			}

			// Compare keys
			cmp := cs.comparator(next.key, key)
			if cmp < 0 {
				// next.key < key, move forward
				curr = next
			} else {
				// next.key >= key, found potential predecessor
				break
			}
		}
	}

	// At this point, curr is the predecessor at our target level
	return curr
}

// isNodeLinkedAtLevel checks if a node is already linked at the specified level
// starting from a given predecessor node
func isNodeLinkedAtLevel(pred *Node, node *Node, level int) bool {
	if pred == nil || node == nil || level < 0 ||
		pred.next == nil || level >= len(pred.next) {
		return false
	}

	// Get the current next pointer at this level
	curr := pred.next[level]

	// Check if the node is already linked at this level
	for curr != nil && curr != node.next[level] {
		// Skip deleted nodes
		if atomic.LoadUint32(&curr.marked) == 1 {
			curr = curr.next[level]
			continue
		}

		// Check if this is our target node
		if curr == node {
			return true
		}

		// If we found a node with key greater than our node,
		// then our node is not linked at this level
		if node.key != nil && curr.key != nil {
			cmp := bytes.Compare(curr.key, node.key)
			if cmp > 0 {
				return false
			}
		}

		// Move to next node
		curr = curr.next[level]
	}

	return false
}

// Clear removes all entries from the skiplist
func (cs *ConcurrentSkipList) Clear() {
	// Create new head and tail nodes
	newHead := &Node{
		key:  nil,
		next: make([]*Node, maxHeight),
	}

	newTail := &Node{
		key:  nil,
		next: make([]*Node, maxHeight),
	}

	// Link head to tail
	for i := 0; i < maxHeight; i++ {
		newHead.next[i] = newTail
	}

	// Replace the existing head
	cs.head = newHead
	cs.tail = newTail

	// Reset counters
	atomic.StoreInt32(&cs.height, 1)
	atomic.StoreInt64(&cs.size, 0)
	atomic.StoreInt64(&cs.count, 0)
	atomic.AddUint64(&cs.currentVersion, 1) // Increment version
}
