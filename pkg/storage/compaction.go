package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxLevel0Tables = 4

	level1MaxSize  = 10 * 1024 * 1024
	sizeMultiplier = 10
)

// CompactionManager tracks SSTables across levels and runs compaction
// when levels get too full.
type CompactionManager struct {
	levels     map[int][]*SSTable
	dataDir    string
	mu         sync.RWMutex
	nextFileID atomic.Int64
	running    atomic.Bool
}

// NewCompactionManager creates a manager for the given data directory.
func NewCompactionManager(dataDir string) *CompactionManager {
	cm := &CompactionManager{
		levels:  make(map[int][]*SSTable),
		dataDir: dataDir,
	}
	cm.nextFileID.Store(time.Now().UnixMicro())
	return cm
}

// AddSSTable registers a new SSTable at the specified level.
// Called after a memtable flush (level 0) or after compaction produces
// new SSTables at a higher level.
func (cm *CompactionManager) AddSSTable(sst *SSTable) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.levels[sst.Level()] = append(cm.levels[sst.Level()], sst)
}

// NeedsCompaction checks if any level has too many SSTables.
// The engine calls this after every flush to decide whether to kick
// off a background compaction.
func (cm *CompactionManager) NeedsCompaction() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if len(cm.levels[0]) >= maxLevel0Tables {
		return true
	}

	return false
}

// RunCompaction merges SSTables from level 0 into level 1.
//
// This is the simplified version — in production systems you'd also
// compact level N into level N+1, handle concurrent reads during
// compaction, etc. But L0→L1 is where the magic happens and covers
// the most important interview talking points.
//
// The algorithm:
//  1. Pick all L0 SSTables (they might overlap)
//  2. Find all L1 SSTables that overlap with the L0 key range
//  3. K-way merge all of them, keeping only the newest version of each key
//  4. Write the merged result as new L1 SSTables
//  5. Delete the old SSTables
func (cm *CompactionManager) RunCompaction() error {
	if !cm.running.CompareAndSwap(false, true) {
		return nil
	}
	defer cm.running.Store(false)

	cm.mu.Lock()
	l0Tables := cm.levels[0]
	if len(l0Tables) == 0 {
		cm.mu.Unlock()
		return nil
	}

	l1Tables := cm.levels[1]
	cm.mu.Unlock()

	var allEntries []SSTableEntry

	for _, sst := range l0Tables {
		entries, err := sst.AllEntries()
		if err != nil {
			return fmt.Errorf("read L0 SSTable %s: %w", sst.Path(), err)
		}
		allEntries = append(allEntries, entries...)
	}

	for _, sst := range l1Tables {
		entries, err := sst.AllEntries()
		if err != nil {
			return fmt.Errorf("read L1 SSTable %s: %w", sst.Path(), err)
		}
		allEntries = append(allEntries, entries...)
	}

	sort.SliceStable(allEntries, func(i, j int) bool {
		return allEntries[i].Key < allEntries[j].Key
	})

	deduplicated := deduplicateEntries(allEntries)

	newPath := cm.newSSTablePath()
	newSST, err := WriteSSTable(newPath, deduplicated, 1)
	if err != nil {
		return fmt.Errorf("write compacted SSTable: %w", err)
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	for _, sst := range l0Tables {
		os.Remove(sst.Path())
	}
	cm.levels[0] = nil

	for _, sst := range l1Tables {
		os.Remove(sst.Path())
	}
	cm.levels[1] = []*SSTable{newSST}

	return nil
}

// GetSSTables returns all SSTables in read order: Level 0 first (newest),
// then Level 1, then Level 2, etc. Within each level, newer tables come
// first. This ordering ensures that we always find the most recent version
// of a key first.
func (cm *CompactionManager) GetSSTables() []*SSTable {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var result []*SSTable

	var levels []int
	for lvl := range cm.levels {
		levels = append(levels, lvl)
	}
	sort.Ints(levels)

	for _, lvl := range levels {
		tables := cm.levels[lvl]

		for i := len(tables) - 1; i >= 0; i-- {
			result = append(result, tables[i])
		}
	}

	return result
}

// LoadExistingSSTables scans the data directory for existing SSTable files
// and loads them. Called on startup to recover state.
func (cm *CompactionManager) LoadExistingSSTables() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	files, err := filepath.Glob(filepath.Join(cm.dataDir, "*.sst"))
	if err != nil {
		return fmt.Errorf("glob SSTables: %w", err)
	}

	for _, path := range files {

		sst, err := OpenSSTable(path, 1)
		if err != nil {
			return fmt.Errorf("open SSTable %s: %w", path, err)
		}
		cm.levels[sst.Level()] = append(cm.levels[sst.Level()], sst)
	}

	return nil
}

// newSSTablePath generates a unique filename for a new SSTable.
func (cm *CompactionManager) newSSTablePath() string {
	id := cm.nextFileID.Add(1)
	return filepath.Join(cm.dataDir, fmt.Sprintf("%020d.sst", id))
}

// NewSSTablePath is the exported version — used by the engine when flushing.
func (cm *CompactionManager) NewSSTablePath() string {
	return cm.newSSTablePath()
}

// deduplicateEntries keeps only the first occurrence of each key.
// The input must be sorted by key. Since we put newer entries first
// during the merge phase, "first" means "newest."
func deduplicateEntries(entries []SSTableEntry) []SSTableEntry {
	if len(entries) == 0 {
		return entries
	}

	result := make([]SSTableEntry, 0, len(entries))
	result = append(result, entries[0])

	for i := 1; i < len(entries); i++ {
		if entries[i].Key != entries[i-1].Key {

			result = append(result, entries[i])
		}

	}

	final := make([]SSTableEntry, 0, len(result))
	for _, e := range result {
		if e.Value != nil {
			final = append(final, e)
		}
	}
	return final
}
