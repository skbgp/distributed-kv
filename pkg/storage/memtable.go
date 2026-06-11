package storage

import (
	"sync"
)

const (
	defaultMemTableSize = 4 * 1024 * 1024
)

// MemTable wraps a skip list with size tracking.
type MemTable struct {
	sl          *SkipList
	currentSize int
	maxSize     int
	mu          sync.RWMutex
}

// NewMemTable creates a memtable that will signal a flush when it grows
// past maxSize bytes. If maxSize is 0, we use the default (4MB).
func NewMemTable(maxSize int) *MemTable {
	if maxSize <= 0 {
		maxSize = defaultMemTableSize
	}
	return &MemTable{
		sl:      NewSkipList(),
		maxSize: maxSize,
	}
}

// Put inserts or updates a key-value pair.
// Returns true if the memtable has exceeded its size limit and should be flushed.
//
// Note: the caller (storage engine) is responsible for writing to the WAL
// BEFORE calling this. We don't touch the WAL here because the memtable
// doesn't own it — the engine does.
func (m *MemTable) Put(key string, value []byte) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if oldVal, found := m.sl.Get(key); found {
		m.currentSize -= len(key) + len(oldVal)
	}

	m.sl.Insert(key, value)
	m.currentSize += len(key) + len(value)

	return m.currentSize >= m.maxSize
}

// Get retrieves a value by key.
// Returns (value, true) if found, (nil, false) if not present.
// A (nil, true) means the key was explicitly deleted (tombstone).
func (m *MemTable) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Get(key)
}

// Delete marks a key as deleted by writing a tombstone.
// Returns true if the memtable should be flushed (same as Put).
func (m *MemTable) Delete(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sl.Insert(key, nil)
	m.currentSize += len(key)

	return m.currentSize >= m.maxSize
}

// Iterator returns a sorted iterator over all entries in the memtable.
// Used during flush to write entries to an SSTable in sorted order.
func (m *MemTable) Iterator() *SkipListIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Iterator()
}

// Size returns the number of entries (including tombstones).
func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Size()
}

// ApproximateBytes returns the rough memory usage in bytes.
// This isn't exact — we don't count skip list node overhead, forward pointers,
// etc. But it's good enough for deciding when to flush.
func (m *MemTable) ApproximateBytes() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentSize
}
