package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Engine ties together the memtable, WAL, SSTables, and compaction
// into a single coherent storage system.
type Engine struct {
	dataDir    string
	memtable   *MemTable
	immutable  *MemTable
	wal        *WAL
	compaction *CompactionManager
	flushCh    chan struct{}
	closeCh    chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex
}

// NewEngine creates and initializes a storage engine at the given directory.
// If the directory already has data from a previous run, we recover by
// replaying the WAL and loading existing SSTables.
func NewEngine(dataDir string) (*Engine, error) {

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	walPath := filepath.Join(dataDir, "wal.log")
	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	cm := NewCompactionManager(dataDir)
	if err := cm.LoadExistingSSTables(); err != nil {
		wal.Close()
		return nil, fmt.Errorf("load SSTables: %w", err)
	}

	engine := &Engine{
		dataDir:    dataDir,
		memtable:   NewMemTable(0),
		wal:        wal,
		compaction: cm,
		flushCh:    make(chan struct{}, 1),
		closeCh:    make(chan struct{}),
	}

	if err := engine.recoverFromWAL(); err != nil {
		wal.Close()
		return nil, fmt.Errorf("WAL recovery: %w", err)
	}

	engine.wg.Add(1)
	go engine.backgroundFlusher()

	return engine, nil
}

// Put stores a key-value pair. The write goes to the WAL first for
// durability, then to the in-memory memtable for fast reads.
func (e *Engine) Put(key string, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	record := WALRecord{Op: OpPut, Key: key, Value: value}
	if err := e.wal.Write(record); err != nil {
		return fmt.Errorf("WAL write: %w", err)
	}

	e.mu.Lock()
	shouldFlush := e.memtable.Put(key, value)
	e.mu.Unlock()

	if shouldFlush {
		e.triggerFlush()
	}

	return nil
}

// Get retrieves the value for a key. It searches in order:
//  1. Active memtable (most recent writes)
//  2. Immutable memtable (data being flushed)
//  3. SSTables from newest to oldest
//
// Returns (value, true) if found, (nil, false) if not present.
// Returns (nil, true) if the key exists but was deleted (tombstone).
func (e *Engine) Get(key string) ([]byte, bool, error) {
	e.mu.RLock()

	if val, found := e.memtable.Get(key); found {
		e.mu.RUnlock()
		return val, true, nil
	}

	if e.immutable != nil {
		if val, found := e.immutable.Get(key); found {
			e.mu.RUnlock()
			return val, true, nil
		}
	}
	e.mu.RUnlock()

	for _, sst := range e.compaction.GetSSTables() {
		val, found, err := sst.Get(key)
		if err != nil {
			return nil, false, fmt.Errorf("SSTable read: %w", err)
		}
		if found {
			return val, true, nil
		}
	}

	return nil, false, nil
}

// Delete marks a key as deleted by writing a tombstone.
// The actual data removal happens during compaction.
func (e *Engine) Delete(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	record := WALRecord{Op: OpDelete, Key: key}
	if err := e.wal.Write(record); err != nil {
		return fmt.Errorf("WAL write: %w", err)
	}

	e.mu.Lock()
	shouldFlush := e.memtable.Delete(key)
	e.mu.Unlock()

	if shouldFlush {
		e.triggerFlush()
	}

	return nil
}

// Close shuts down the engine gracefully. Flushes the current memtable
// to disk so no data is lost, then stops background goroutines.
func (e *Engine) Close() error {

	close(e.closeCh)

	e.mu.Lock()
	if e.memtable.Size() > 0 {
		e.flushMemTable(e.memtable)
	}
	e.mu.Unlock()

	e.wg.Wait()

	return e.wal.Close()
}

// triggerFlush swaps the active memtable with a fresh one and signals
// the background flusher to write the old one to disk.
func (e *Engine) triggerFlush() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.immutable != nil {
		log.Println("[engine] flush already in progress, flushing synchronously")
		e.flushMemTable(e.immutable)
		e.immutable = nil
	}

	e.immutable = e.memtable
	e.memtable = NewMemTable(0)

	select {
	case e.flushCh <- struct{}{}:
	default:

	}
}

// backgroundFlusher runs in a goroutine, waiting for flush signals.
// When triggered, it writes the immutable memtable to an SSTable and
// then checks if compaction is needed.
func (e *Engine) backgroundFlusher() {
	defer e.wg.Done()

	for {
		select {
		case <-e.flushCh:
			e.mu.RLock()
			imm := e.immutable
			e.mu.RUnlock()

			if imm == nil {
				continue
			}

			e.flushMemTable(imm)

			if err := e.wal.Reset(); err != nil {
				log.Printf("[engine] WAL reset failed: %v\n", err)
			}

			e.mu.Lock()
			e.immutable = nil
			e.mu.Unlock()

			if e.compaction.NeedsCompaction() {
				log.Println("[engine] triggering compaction")
				if err := e.compaction.RunCompaction(); err != nil {
					log.Printf("[engine] compaction error: %v\n", err)
				}
			}

		case <-e.closeCh:
			return
		}
	}
}

// flushMemTable writes all entries from a memtable to a new SSTable.
func (e *Engine) flushMemTable(mt *MemTable) {

	var entries []SSTableEntry
	iter := mt.Iterator()
	for iter.Next() {
		entries = append(entries, SSTableEntry{
			Key:   iter.Key(),
			Value: iter.Value(),
		})
	}

	if len(entries) == 0 {
		return
	}

	path := e.compaction.NewSSTablePath()
	sst, err := WriteSSTable(path, entries, 0)
	if err != nil {
		log.Printf("[engine] flush failed: %v\n", err)
		return
	}

	e.compaction.AddSSTable(sst)
	log.Printf("[engine] flushed %d entries to %s\n", len(entries), path)
}

// recoverFromWAL replays the write-ahead log to reconstruct the memtable
// after a crash or clean restart.
func (e *Engine) recoverFromWAL() error {
	count := 0
	err := e.wal.Replay(func(rec WALRecord) {
		switch rec.Op {
		case OpPut:
			e.memtable.Put(rec.Key, rec.Value)
		case OpDelete:
			e.memtable.Delete(rec.Key)
		}
		count++
	})

	if count > 0 {
		log.Printf("[engine] recovered %d records from WAL\n", count)
	}
	return err
}

// Stats returns some basic metrics about the storage engine.
// Useful for the dashboard / monitoring.
type EngineStats struct {
	MemTableEntries     int
	MemTableBytes       int
	ImmutableEntries    int
	SSTableCount        int
	TotalSSTableEntries int
}

func (e *Engine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := EngineStats{
		MemTableEntries: e.memtable.Size(),
		MemTableBytes:   e.memtable.ApproximateBytes(),
	}

	if e.immutable != nil {
		stats.ImmutableEntries = e.immutable.Size()
	}

	tables := e.compaction.GetSSTables()
	stats.SSTableCount = len(tables)
	for _, t := range tables {
		stats.TotalSSTableEntries += t.EntryCount()
	}

	return stats
}
