package storage

import (
	"fmt"
	"log"
	"os"
	"sync"
)

// Engine ties together the memtable, WAL, SSTables, and compaction
// into a single coherent storage system.
type Engine struct {
	dataDir   string
	memtable  *MemTable
	immutable *MemTable

	// immutableSegID is the WAL segment that became active when `immutable`
	// was swapped out. Every record belonging to `immutable` therefore lives
	// in a segment with a smaller ID, and those segments — and only those —
	// can be retired once its SSTable is durable.
	immutableSegID uint64

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

	wal, err := OpenWAL(dataDir)
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
	return e.write(WALRecord{Op: OpPut, Key: key, Value: value})
}

// Delete marks a key as deleted by writing a tombstone.
// The actual data removal happens during compaction.
func (e *Engine) Delete(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}
	return e.write(WALRecord{Op: OpDelete, Key: key})
}

// write is the single durable write path for both puts and deletes.
//
// The engine lock is held across the WAL append AND the memtable insert.
// That looks heavy-handed — it means a reader waits on the fsync — but it is
// what makes the WAL segment boundary meaningful: if the lock were released
// between the two steps, a rotation could slip in and the record would end up
// in a segment that gets retired while its data sits in the *new* memtable.
// That exact interleaving is what used to lose writes during a flush.
//
// The finer-grained fix is to have the WAL return the segment it wrote to and
// reconcile afterwards; that is more machinery than this engine needs, since
// the fsync dominates the critical section either way.
func (e *Engine) write(record WALRecord) error {
	e.mu.Lock()

	if err := e.wal.Write(record); err != nil {
		e.mu.Unlock()
		return fmt.Errorf("WAL write: %w", err)
	}

	var shouldFlush bool
	switch record.Op {
	case OpPut:
		shouldFlush = e.memtable.Put(record.Key, record.Value)
	case OpDelete:
		shouldFlush = e.memtable.Delete(record.Key)
	}

	if shouldFlush {
		if err := e.rotateAndSwapLocked(); err != nil {
			e.mu.Unlock()
			return fmt.Errorf("rotate WAL for flush: %w", err)
		}
	}
	e.mu.Unlock()

	if shouldFlush {
		select {
		case e.flushCh <- struct{}{}:
		default:
		}
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

// Close shuts down the engine gracefully. Stops the background flusher,
// writes whatever is still in memory to disk, and only then retires the
// WAL segments covering it.
func (e *Engine) Close() error {

	close(e.closeCh)
	e.wg.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()

	// A flush may have been signalled but not picked up before shutdown.
	if e.immutable != nil {
		if err := e.flushMemTable(e.immutable); err != nil {
			e.wal.Close()
			return fmt.Errorf("flush pending memtable on close: %w", err)
		}
		e.immutable = nil
	}

	if e.memtable.Size() > 0 {
		if err := e.flushMemTable(e.memtable); err != nil {
			e.wal.Close()
			return fmt.Errorf("flush memtable on close: %w", err)
		}
		e.memtable = NewMemTable(0)

		// Everything in memory is now durable in SSTables, so the log can be
		// rolled forward and the old segments dropped.
		newID, err := e.wal.Rotate()
		if err != nil {
			e.wal.Close()
			return fmt.Errorf("rotate WAL on close: %w", err)
		}
		if err := e.wal.RemoveSegmentsBefore(newID); err != nil {
			log.Printf("[engine] could not retire WAL segments on close: %v\n", err)
		}
	}

	return e.wal.Close()
}

// rotateAndSwapLocked starts a new WAL segment and makes the current memtable
// immutable, so the background flusher can write it out. The caller must hold
// e.mu — the rotation and the swap have to be a single atomic step.
func (e *Engine) rotateAndSwapLocked() error {

	// A previous flush hasn't been picked up yet. Write it out inline rather
	// than dropping it on the floor, and retire its segments first.
	if e.immutable != nil {
		log.Println("[engine] flush already in progress, flushing synchronously")
		if err := e.flushMemTable(e.immutable); err != nil {
			return err
		}
		if err := e.wal.RemoveSegmentsBefore(e.immutableSegID); err != nil {
			log.Printf("[engine] could not retire WAL segments: %v\n", err)
		}
		e.immutable = nil
	}

	newID, err := e.wal.Rotate()
	if err != nil {
		return err
	}

	e.immutable = e.memtable
	e.immutableSegID = newID
	e.memtable = NewMemTable(0)

	return nil
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
			segID := e.immutableSegID
			e.mu.RUnlock()

			if imm == nil {
				continue
			}

			// Order matters: the SSTable must be durable before the WAL
			// segments backing it are removed. If the flush fails we keep
			// the segments, so the data is still recoverable on restart.
			if err := e.flushMemTable(imm); err != nil {
				log.Printf("[engine] flush failed, keeping WAL segments: %v\n", err)
				continue
			}

			if err := e.wal.RemoveSegmentsBefore(segID); err != nil {
				log.Printf("[engine] could not retire WAL segments: %v\n", err)
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
//
// It returns an error rather than only logging one, because the caller uses
// success as the signal that the WAL segments behind this data are safe to
// delete. Swallowing the error here would drop the records from both places.
func (e *Engine) flushMemTable(mt *MemTable) error {

	var entries []SSTableEntry
	iter := mt.Iterator()
	for iter.Next() {
		entries = append(entries, SSTableEntry{
			Key:   iter.Key(),
			Value: iter.Value(),
		})
	}

	if len(entries) == 0 {
		return nil
	}

	path := e.compaction.NewSSTablePath()
	sst, err := WriteSSTable(path, entries, 0)
	if err != nil {
		return fmt.Errorf("write SSTable %s: %w", path, err)
	}

	e.compaction.AddSSTable(sst)
	log.Printf("[engine] flushed %d entries to %s\n", len(entries), path)
	return nil
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
	WALSegments         int
}

func (e *Engine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := EngineStats{
		MemTableEntries: e.memtable.Size(),
		MemTableBytes:   e.memtable.ApproximateBytes(),
		WALSegments:     e.wal.SegmentCount(),
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
