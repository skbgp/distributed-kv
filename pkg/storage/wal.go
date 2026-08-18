package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// OpType identifies what kind of operation this WAL record represents.
// We only have two: Put and Delete. A Delete is stored as a Put with a
// special marker — but at the WAL level we track them separately for
// clarity during replay.
type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// WAL file naming. The log is split into numbered segments so that a
// memtable flush can retire exactly the records it made durable, instead
// of truncating the whole log and taking newer writes down with it.
const (
	segmentPrefix = "wal-"
	segmentSuffix = ".log"

	// legacyWALName is the single-file layout used before segmentation.
	// Data directories written by the old code are migrated on open.
	legacyWALName = "wal.log"
)

// WALRecord is a single entry in the write-ahead log.
type WALRecord struct {
	Op    OpType
	Key   string
	Value []byte
}

// WAL is a segmented write-ahead log.
//
// Records are appended to the newest segment. When the engine flushes a
// memtable it rotates to a fresh segment first, so every record belonging to
// the memtable being flushed is confined to older segments — which can be
// deleted once the SSTable is durable. Writes that arrive during the flush
// land in the new segment and survive.
//
// The previous single-file version truncated the entire log after a flush,
// which silently discarded any write that arrived between the memtable swap
// and the truncation.
type WAL struct {
	dir string

	mu       sync.Mutex
	active   *os.File
	activeID uint64
}

// segmentName builds the filename for a segment ID.
func segmentName(id uint64) string {
	return fmt.Sprintf("%s%06d%s", segmentPrefix, id, segmentSuffix)
}

// parseSegmentID extracts the numeric ID from a segment filename.
func parseSegmentID(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	digits := name[len(segmentPrefix) : len(name)-len(segmentSuffix)]
	id, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// listSegmentIDs returns every segment ID in the directory, ascending.
func listSegmentIDs(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read WAL dir %s: %w", dir, err)
	}

	var ids []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if id, ok := parseSegmentID(e.Name()); ok {
			ids = append(ids, id)
		}
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// syncDir fsyncs a directory so that file creations and deletions inside it
// survive a crash. Creating or unlinking a file is only durable once the
// directory entry itself has been flushed.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// OpenWAL opens the segmented log in dir, creating it if necessary.
// Writes continue on the highest-numbered existing segment.
func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create WAL dir %s: %w", dir, err)
	}

	w := &WAL{dir: dir}

	if err := w.migrateLegacyLog(); err != nil {
		return nil, err
	}

	ids, err := listSegmentIDs(dir)
	if err != nil {
		return nil, err
	}

	// Fresh directory: start at segment 1.
	if len(ids) == 0 {
		if err := w.openSegment(1); err != nil {
			return nil, err
		}
		return w, nil
	}

	if err := w.openSegment(ids[len(ids)-1]); err != nil {
		return nil, err
	}
	return w, nil
}

// migrateLegacyLog renames a pre-segmentation wal.log into segment 1 so that
// data directories written by the old single-file layout still recover.
func (w *WAL) migrateLegacyLog() error {
	legacy := filepath.Join(w.dir, legacyWALName)
	if _, err := os.Stat(legacy); err != nil {
		return nil // no legacy log, nothing to do
	}

	ids, err := listSegmentIDs(w.dir)
	if err != nil {
		return err
	}
	if len(ids) > 0 {
		// Already segmented; the legacy file is stale leftover.
		return nil
	}

	target := filepath.Join(w.dir, segmentName(1))
	if err := os.Rename(legacy, target); err != nil {
		return fmt.Errorf("migrate legacy WAL: %w", err)
	}
	log.Printf("[wal] migrated %s to %s\n", legacyWALName, segmentName(1))
	return syncDir(w.dir)
}

// openSegment makes the given segment the active one, creating it if needed.
func (w *WAL) openSegment(id uint64) error {
	path := filepath.Join(w.dir, segmentName(id))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open WAL segment %s: %w", path, err)
	}
	w.active = file
	w.activeID = id
	return nil
}

// Write appends a single record to the active segment and fsyncs.
//
// The fsync is what makes the write durable — without it the OS may still be
// holding the bytes in page cache when the machine dies. It is also the main
// cost of every write; production systems amortize it by batching several
// records into one fsync (group commit).
func (w *WAL) Write(record WALRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := encodeRecord(record)

	if _, err := w.active.Write(data); err != nil {
		return fmt.Errorf("WAL write: %w", err)
	}

	if err := w.active.Sync(); err != nil {
		return fmt.Errorf("WAL fsync: %w", err)
	}

	return nil
}

// Rotate closes the active segment and starts a new one, returning the ID of
// the new active segment. Every record written before this call lives in a
// segment with a strictly smaller ID.
//
// The engine calls this while holding its own lock, at the same moment it
// swaps the memtable, so the boundary between "records in the memtable being
// flushed" and "records written after" is exact.
func (w *WAL) Rotate() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.active.Sync(); err != nil {
		return 0, fmt.Errorf("fsync before rotate: %w", err)
	}
	if err := w.active.Close(); err != nil {
		return 0, fmt.Errorf("close segment before rotate: %w", err)
	}

	newID := w.activeID + 1
	if err := w.openSegment(newID); err != nil {
		return 0, err
	}

	// Make the new segment's directory entry durable before anything relies
	// on it holding writes.
	if err := syncDir(w.dir); err != nil {
		return 0, fmt.Errorf("fsync WAL dir after rotate: %w", err)
	}

	return newID, nil
}

// RemoveSegmentsBefore deletes every segment with an ID lower than the given
// one. The engine calls this only after the corresponding SSTable is durable,
// so the records being dropped are already safe on disk.
func (w *WAL) RemoveSegmentsBefore(id uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	ids, err := listSegmentIDs(w.dir)
	if err != nil {
		return err
	}

	removed := 0
	for _, sid := range ids {
		if sid >= id || sid == w.activeID {
			continue
		}
		path := filepath.Join(w.dir, segmentName(sid))
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove WAL segment %s: %w", path, err)
		}
		removed++
	}

	if removed == 0 {
		return nil
	}

	log.Printf("[wal] retired %d segment(s) below %d\n", removed, id)
	return syncDir(w.dir)
}

// Replay reads every segment in order and calls handler for each valid
// record. Called on startup to rebuild the memtable.
//
// A corrupt or partially written record is treated as the end of the log:
// that is the normal signature of a crash during an append. The damaged tail
// is truncated so future writes start from a clean offset, and replay stops —
// records after a hole cannot be trusted to be in order.
func (w *WAL) Replay(handler func(WALRecord)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	ids, err := listSegmentIDs(w.dir)
	if err != nil {
		return err
	}

	for _, id := range ids {
		clean, err := w.replaySegment(id, handler)
		if err != nil {
			return err
		}
		if !clean {
			break
		}
	}

	return nil
}

// replaySegment replays one segment, reporting whether it was read all the
// way to a clean EOF.
func (w *WAL) replaySegment(id uint64, handler func(WALRecord)) (bool, error) {
	path := filepath.Join(w.dir, segmentName(id))

	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, fmt.Errorf("open WAL segment %s: %w", path, err)
	}
	defer file.Close()

	var validPos int64

	for {
		record, err := decodeRecord(file)
		if err == io.EOF {
			return true, nil
		}
		if err != nil {
			log.Printf("[wal] corrupt record in %s at offset %d, truncating tail: %v\n",
				segmentName(id), validPos, err)

			if truncErr := file.Truncate(validPos); truncErr != nil {
				log.Printf("[wal] truncate failed: %v\n", truncErr)
			}
			if syncErr := file.Sync(); syncErr != nil {
				log.Printf("[wal] fsync after truncate failed: %v\n", syncErr)
			}
			return false, nil
		}

		handler(record)
		validPos, _ = file.Seek(0, io.SeekCurrent)
	}
}

// SegmentCount reports how many segments currently exist. Used by tests and
// the status endpoint to show that retired segments really are cleaned up.
func (w *WAL) SegmentCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	ids, err := listSegmentIDs(w.dir)
	if err != nil {
		return 0
	}
	return len(ids)
}

// ActiveSegment returns the ID of the segment currently being appended to.
func (w *WAL) ActiveSegment() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.activeID
}

// Close flushes and closes the active segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.active == nil {
		return nil
	}
	if err := w.active.Sync(); err != nil {
		w.active.Close()
		return fmt.Errorf("fsync on close: %w", err)
	}
	return w.active.Close()
}

func encodeRecord(rec WALRecord) []byte {
	keyBytes := []byte(rec.Key)
	valBytes := rec.Value
	if valBytes == nil {
		valBytes = []byte{}
	}

	payloadSize := 1 + 4 + len(keyBytes) + 4 + len(valBytes)

	buf := make([]byte, 4+4+payloadSize)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(4+payloadSize))

	offset := 8

	buf[offset] = byte(rec.Op)
	offset++

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(keyBytes)))
	offset += 4
	copy(buf[offset:], keyBytes)
	offset += len(keyBytes)

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(valBytes)))
	offset += 4
	copy(buf[offset:], valBytes)

	checksum := crc32.ChecksumIEEE(buf[8:])
	binary.LittleEndian.PutUint32(buf[4:8], checksum)

	return buf
}

func decodeRecord(r io.Reader) (WALRecord, error) {

	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return WALRecord{}, err
	}
	totalLen := binary.LittleEndian.Uint32(lenBuf)

	if totalLen > 64*1024*1024 {
		return WALRecord{}, fmt.Errorf("record too large (%d bytes), likely corruption", totalLen)
	}
	if totalLen < 4 {
		return WALRecord{}, fmt.Errorf("record too small (%d bytes), likely corruption", totalLen)
	}

	data := make([]byte, totalLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return WALRecord{}, err
	}

	storedCRC := binary.LittleEndian.Uint32(data[0:4])
	actualCRC := crc32.ChecksumIEEE(data[4:])
	if storedCRC != actualCRC {
		return WALRecord{}, fmt.Errorf("CRC mismatch: stored=%d actual=%d", storedCRC, actualCRC)
	}

	offset := 4

	op := OpType(data[offset])
	offset++

	if offset+4 > len(data) {
		return WALRecord{}, fmt.Errorf("truncated key length")
	}
	keyLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	if offset+int(keyLen) > len(data) {
		return WALRecord{}, fmt.Errorf("truncated key")
	}
	key := string(data[offset : offset+int(keyLen)])
	offset += int(keyLen)

	if offset+4 > len(data) {
		return WALRecord{}, fmt.Errorf("truncated value length")
	}
	valLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	if offset+int(valLen) > len(data) {
		return WALRecord{}, fmt.Errorf("truncated value")
	}

	var value []byte
	if valLen > 0 {
		value = make([]byte, valLen)
		copy(value, data[offset:offset+int(valLen)])
	}

	return WALRecord{
		Op:    op,
		Key:   key,
		Value: value,
	}, nil
}
