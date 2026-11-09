package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shubham/distributed-kv/pkg/persist"
)

const (
	maxLevel0Tables = 4
	manifestName    = "sstables.json"
)

type tableRef struct {
	Name  string
	Level int
}

// CompactionManager publishes a durable table set and keeps its input files
// available until any lookup using them has finished.
type CompactionManager struct {
	tables     []*SSTable // newest first, with level 0 ahead of level 1
	dataDir    string
	mu         sync.RWMutex
	nextFileID atomic.Int64
	publishErr error
}

func NewCompactionManager(dataDir string) *CompactionManager {
	cm := &CompactionManager{dataDir: dataDir}
	cm.nextFileID.Store(time.Now().UnixMicro())
	return cm
}

func (cm *CompactionManager) AddSSTable(sst *SSTable) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	tables := append([]*SSTable{sst}, cm.tables...)
	sort.SliceStable(tables, func(i, j int) bool {
		return tables[i].Level() < tables[j].Level()
	})
	return cm.publish(tables)
}

func (cm *CompactionManager) publish(tables []*SSTable) error {
	if cm.publishErr != nil {
		return cm.publishErr
	}
	refs := make([]tableRef, 0, len(tables))
	for _, table := range tables {
		refs = append(refs, tableRef{filepath.Base(table.Path()), table.Level()})
	}
	data, err := json.Marshal(refs)
	if err == nil {
		err = persist.Replace(filepath.Join(cm.dataDir, manifestName), data)
	}
	if err != nil {
		// Rename may already have succeeded. Keep both sets of files and
		// require reopening before trying to publish anything else.
		cm.publishErr = fmt.Errorf("publish SSTable manifest: %w", err)
		return cm.publishErr
	}
	cm.tables = tables
	return nil
}

func (cm *CompactionManager) NeedsCompaction() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return len(cm.tables) >= maxLevel0Tables && cm.tables[maxLevel0Tables-1].Level() == 0
}

// RunCompaction merges every visible table, keeping the most recent value or
// deletion. Publish the new table set before unlinking any inputs.
func (cm *CompactionManager) RunCompaction() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if len(cm.tables) == 0 || cm.tables[0].Level() != 0 {
		return nil
	}
	inputs := cm.tables
	var entries []SSTableEntry
	for _, table := range inputs {
		run, err := table.AllEntries()
		if err != nil {
			return err
		}
		entries = append(entries, run...)
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	output := entries[:0]
	for _, entry := range entries {
		if len(output) == 0 || output[len(output)-1].Key != entry.Key {
			output = append(output, entry)
		}
	}

	table, err := WriteSSTable(cm.NewSSTablePath(), output, 1)
	if err != nil {
		return err
	}
	if err := cm.publish([]*SSTable{table}); err != nil {
		return err
	}
	for _, input := range inputs {
		if err := os.Remove(input.Path()); err != nil {
			log.Printf("[compaction] remove retired table: %v", err)
		}
	}
	return nil
}

func (cm *CompactionManager) get(key string) ([]byte, bool, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	for _, table := range cm.tables {
		value, found, err := table.Get(key)
		if err != nil || found {
			return value, found, err
		}
	}
	return nil, false, nil
}

// GetSSTables is a snapshot for statistics. Reads use get, which holds the
// manager read lock until the disk lookup is finished.
func (cm *CompactionManager) GetSSTables() []*SSTable {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return append([]*SSTable(nil), cm.tables...)
}

func (cm *CompactionManager) LoadExistingSSTables() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	files, err := filepath.Glob(filepath.Join(cm.dataDir, "*.sst"))
	if err != nil {
		return err
	}
	for _, path := range files {
		id, err := strconv.ParseInt(strings.TrimSuffix(filepath.Base(path), ".sst"), 10, 64)
		if err == nil && id > cm.nextFileID.Load() {
			cm.nextFileID.Store(id)
		}
	}

	var refs []tableRef
	data, err := os.ReadFile(filepath.Join(cm.dataDir, manifestName))
	legacy := errors.Is(err, os.ErrNotExist)
	switch {
	case legacy:
		// Older versions recover all table files in filename order.
		for i := len(files) - 1; i >= 0; i-- {
			refs = append(refs, tableRef{filepath.Base(files[i]), 1})
		}
	case err != nil:
		return err
	default:
		if err := json.Unmarshal(data, &refs); err != nil {
			return fmt.Errorf("read SSTable manifest: %w", err)
		}
	}

	var tables []*SSTable
	for _, ref := range refs {
		if filepath.Base(ref.Name) != ref.Name || !strings.HasSuffix(ref.Name, ".sst") {
			return fmt.Errorf("invalid SSTable manifest entry %q", ref.Name)
		}
		table, err := OpenSSTable(filepath.Join(cm.dataDir, ref.Name), ref.Level)
		if err != nil {
			return err
		}
		tables = append(tables, table)
	}
	if legacy {
		return cm.publish(tables)
	}
	cm.tables = tables
	return nil
}

func (cm *CompactionManager) NewSSTablePath() string {
	return filepath.Join(cm.dataDir, fmt.Sprintf("%020d.sst", cm.nextFileID.Add(1)))
}
