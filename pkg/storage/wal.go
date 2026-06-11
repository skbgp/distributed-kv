package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
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

// WALRecord is a single entry in the write-ahead log.
type WALRecord struct {
	Op    OpType
	Key   string
	Value []byte
}

// WAL handles writing operation records to disk and replaying them on recovery.
type WAL struct {
	file *os.File
	mu   sync.Mutex
	path string
}

// OpenWAL opens an existing WAL file or creates a new one.
// The file is opened in append mode — we never overwrite old records.
func OpenWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL at %s: %w", path, err)
	}

	return &WAL{
		file: file,
		path: path,
	}, nil
}

// Write appends a single record to the WAL and fsyncs.
//
// The fsync is critical — without it, the OS might buffer the write and
// we'd lose data on crash. This is the main performance cost of durability.
// In production systems you might batch multiple writes before fsyncing
// (group commit), but for correctness we fsync every record.
func (w *WAL) Write(record WALRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := encodeRecord(record)

	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("WAL write failed: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("WAL fsync failed: %w", err)
	}

	return nil
}

// Replay reads all records from the WAL and calls the handler function
// for each valid record. This is called on startup to rebuild the memtable.
//
// If we encounter a corrupted record (bad CRC or partial write), we treat
// it as the end of the valid log. This handles the common case where the
// server crashed mid-write — the last record is incomplete but everything
// before it is fine. We truncate the file to the last valid position so
// future writes start from a clean state.
func (w *WAL) Replay(handler func(WALRecord)) error {

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("WAL seek failed: %w", err)
	}

	var validPos int64

	for {
		record, err := decodeRecord(w.file)
		if err == io.EOF {

			break
		}
		if err != nil {

			log.Printf("[wal] hit corrupt record at offset %d, truncating tail: %v\n",
				validPos, err)
			if truncErr := w.file.Truncate(validPos); truncErr != nil {
				log.Printf("[wal] truncate failed: %v\n", truncErr)
			}
			w.file.Seek(0, io.SeekEnd)
			break
		}
		handler(record)

		validPos, _ = w.file.Seek(0, io.SeekCurrent)
	}

	return nil
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// Reset truncates the WAL file. Called after the memtable is successfully
// flushed to an SSTable — at that point the WAL records are no longer needed
// because the data is safely on disk in the SSTable.
func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("WAL truncate failed: %w", err)
	}
	_, err := w.file.Seek(0, io.SeekStart)
	return err
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

	keyLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	key := string(data[offset : offset+int(keyLen)])
	offset += int(keyLen)

	valLen := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
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
