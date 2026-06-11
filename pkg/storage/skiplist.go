package storage

import (
	"math/rand"
	"sync"
)

const (
	maxLevel = 16

	probability = 0.5
)

// skipNode is a single node in the skip list. Each node has forward pointers
// at multiple levels — level 0 is the "ground floor" linked list that contains
// every element, and higher levels skip over nodes for faster traversal.
type skipNode struct {
	key     string
	value   []byte
	forward []*skipNode
}

// SkipList is a probabilistic sorted data structure. It's basically a stack
// of linked lists where higher levels skip over more nodes, giving us O(log n)
// search by "dropping down" levels as we go.
type SkipList struct {
	head  *skipNode
	level int
	size  int
	mu    sync.RWMutex
}

// NewSkipList creates an empty skip list ready for inserts.
func NewSkipList() *SkipList {
	head := &skipNode{
		forward: make([]*skipNode, maxLevel),
	}
	return &SkipList{
		head:  head,
		level: 0,
	}
}

// randomLevel picks how many levels a new node should participate in.
// Each level has a 50% chance of being included — so most nodes are only
// on level 0, ~50% are on level 1, ~25% on level 2, etc. This gives us
// the O(log n) property without any explicit balancing.
func randomLevel() int {
	lvl := 1
	for lvl < maxLevel && rand.Float64() < probability {
		lvl++
	}
	return lvl
}

// Insert adds or updates a key-value pair in the skip list.
//
// If the key already exists, we just overwrite the value. This is important
// for the memtable — a PUT followed by another PUT on the same key should
// only keep the latest value.
func (sl *SkipList) Insert(key string, value []byte) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*skipNode, maxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	if current != nil && current.key == key {

		current.value = value
		return
	}

	newLevel := randomLevel()

	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	newNode := &skipNode{
		key:     key,
		value:   value,
		forward: make([]*skipNode, newLevel),
	}

	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.size++
}

// Get looks up a key and returns its value. The second return value indicates
// whether the key was found. A nil value with found=true means the key exists
// but was set to nil (which we use for tombstones in delete operations).
func (sl *SkipList) Get(key string) ([]byte, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
	}

	current = current.forward[0]

	if current != nil && current.key == key {
		return current.value, true
	}
	return nil, false
}

// Delete marks a key as deleted by inserting a tombstone (nil value).
//
// We don't actually remove the node from the skip list. Instead, we write
// a nil value which the storage engine interprets as "this key was deleted."
// The actual removal happens later during SSTable compaction.
//
// Why tombstones instead of real deletion?
// Because we need to propagate deletes to older SSTables. If we just removed
// the key from the memtable, an older SSTable might still have the key and
// we'd "resurrect" it on the next read. The tombstone ensures the delete
// is visible across all levels of the LSM tree.
func (sl *SkipList) Delete(key string) {
	sl.Insert(key, nil)
}

// SkipListIterator provides sorted iteration over the skip list.
// We use this when flushing the memtable to an SSTable — we need all
// entries in sorted order.
type SkipListIterator struct {
	current *skipNode
}

// Iterator returns a new iterator positioned before the first element.
// Call Next() to advance to each element.
func (sl *SkipList) Iterator() *SkipListIterator {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	return &SkipListIterator{
		current: sl.head,
	}
}

// Next advances the iterator to the next entry. Returns false when there
// are no more entries.
func (it *SkipListIterator) Next() bool {
	if it.current == nil || it.current.forward[0] == nil {
		return false
	}
	it.current = it.current.forward[0]
	return true
}

// Key returns the current entry's key. Only valid after a successful Next().
func (it *SkipListIterator) Key() string {
	return it.current.key
}

// Value returns the current entry's value. A nil value means this is a
// tombstone (deletion marker).
func (it *SkipListIterator) Value() []byte {
	return it.current.value
}

// Size returns the number of entries in the skip list.
func (sl *SkipList) Size() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.size
}
