package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

const (
	indexInterval = 16

	bitsPerKey = 10
)

// SSTableEntry is a single key-value pair stored in the SSTable.
// Value is nil for tombstones (deleted keys).
type SSTableEntry struct {
	Key   string
	Value []byte
}

// indexEntry maps a key to its byte offset in the data block.
type indexEntry struct {
	Key    string
	Offset int64
}

// footer sits at the end of the file and tells us where to find the
// index block and bloom filter.
type footer struct {
	IndexOffset  int64
	IndexSize    int64
	BloomOffset  int64
	BloomSize    int64
	BloomNumHash uint32
	NumEntries   uint32
}

const footerSize = 8 + 8 + 8 + 8 + 4 + 4

// SSTable represents an on-disk sorted string table.
// Once created, the data file is never modified.
type SSTable struct {
	path    string
	index   []indexEntry
	bloom   *BloomFilter
	entries int
	level   int
	mu      sync.RWMutex
}

// WriteSSTable creates a new SSTable file from a sorted sequence of entries.
// The entries MUST be in sorted order by key — the caller (memtable flush or
// compaction) is responsible for this.
//
// Returns the SSTable handle (with index and bloom loaded) or an error.
func WriteSSTable(path string, entries []SSTableEntry, level int) (*SSTable, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create SSTable %s: %w", path, err)
	}
	defer file.Close()

	var sparseIndex []indexEntry
	bloom := NewBloomFilter(len(entries), bitsPerKey)

	for i, entry := range entries {
		offset, _ := file.Seek(0, io.SeekCurrent)

		if i%indexInterval == 0 {
			sparseIndex = append(sparseIndex, indexEntry{
				Key:    entry.Key,
				Offset: offset,
			})
		}

		bloom.Add([]byte(entry.Key))

		if err := writeEntry(file, entry); err != nil {
			return nil, fmt.Errorf("write entry %q: %w", entry.Key, err)
		}
	}

	indexOffset, _ := file.Seek(0, io.SeekCurrent)
	indexSize, err := writeIndex(file, sparseIndex)
	if err != nil {
		return nil, fmt.Errorf("write index: %w", err)
	}

	bloomOffset, _ := file.Seek(0, io.SeekCurrent)
	bloomData := bloom.Bytes()
	if _, err := file.Write(bloomData); err != nil {
		return nil, fmt.Errorf("write bloom filter: %w", err)
	}

	ft := footer{
		IndexOffset:  indexOffset,
		IndexSize:    indexSize,
		BloomOffset:  bloomOffset,
		BloomSize:    int64(len(bloomData)),
		BloomNumHash: bloom.NumHash(),
		NumEntries:   uint32(len(entries)),
	}
	if err := writeFooter(file, ft); err != nil {
		return nil, fmt.Errorf("write footer: %w", err)
	}

	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("fsync SSTable: %w", err)
	}

	return &SSTable{
		path:    path,
		index:   sparseIndex,
		bloom:   bloom,
		entries: len(entries),
		level:   level,
	}, nil
}

// OpenSSTable reads an existing SSTable file from disk, loading the index
// and bloom filter into memory. The data block stays on disk and is read
// on demand during lookups.
func OpenSSTable(path string, level int) (*SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open SSTable %s: %w", path, err)
	}
	defer file.Close()

	ft, err := readFooter(file)
	if err != nil {
		return nil, fmt.Errorf("read footer: %w", err)
	}

	sparseIndex, err := readIndex(file, ft.IndexOffset, ft.IndexSize)
	if err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}

	bloomData := make([]byte, ft.BloomSize)
	if _, err := file.ReadAt(bloomData, ft.BloomOffset); err != nil {
		return nil, fmt.Errorf("read bloom: %w", err)
	}
	bloom := LoadBloomFilter(bloomData, ft.BloomNumHash)

	return &SSTable{
		path:    path,
		index:   sparseIndex,
		bloom:   bloom,
		entries: int(ft.NumEntries),
		level:   level,
	}, nil
}

// Get looks up a key in this SSTable.
// Returns (value, true) if found, (nil, false) if not present.
// Returns (nil, true) if the key was found but is a tombstone.
//
// The lookup flow:
//  1. Check bloom filter → if NO, return immediately (no disk read!)
//  2. Binary search the sparse index to find the right region
//  3. Linear scan within that region to find the exact key
func (sst *SSTable) Get(key string) ([]byte, bool, error) {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	if !sst.bloom.MayContain([]byte(key)) {
		return nil, false, nil
	}

	regionIdx := sort.Search(len(sst.index), func(i int) bool {
		return sst.index[i].Key > key
	}) - 1

	if regionIdx < 0 {
		regionIdx = 0
	}

	file, err := os.Open(sst.path)
	if err != nil {
		return nil, false, fmt.Errorf("open for read: %w", err)
	}
	defer file.Close()

	startOffset := sst.index[regionIdx].Offset
	if _, err := file.Seek(startOffset, io.SeekStart); err != nil {
		return nil, false, fmt.Errorf("seek to offset: %w", err)
	}

	var endOffset int64
	if regionIdx+1 < len(sst.index) {
		endOffset = sst.index[regionIdx+1].Offset
	} else {

		endOffset = sst.index[regionIdx].Offset + int64(sst.entries)*256
	}

	for {
		currentPos, _ := file.Seek(0, io.SeekCurrent)
		if currentPos >= endOffset && regionIdx+1 < len(sst.index) {
			break
		}

		entry, err := readEntry(file)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, false, fmt.Errorf("read entry: %w", err)
		}

		if entry.Key == key {

			return entry.Value, true, nil
		}

		if entry.Key > key {
			break
		}
	}

	return nil, false, nil
}

// AllEntries reads all entries from the SSTable in sorted order.
// Used during compaction when we need to merge multiple SSTables.
func (sst *SSTable) AllEntries() ([]SSTableEntry, error) {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	file, err := os.Open(sst.path)
	if err != nil {
		return nil, fmt.Errorf("open for scan: %w", err)
	}
	defer file.Close()

	var entries []SSTableEntry
	for i := 0; i < sst.entries; i++ {
		entry, err := readEntry(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read entry %d: %w", i, err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// Path returns the file path of this SSTable.
func (sst *SSTable) Path() string {
	return sst.path
}

// Level returns the compaction level of this SSTable.
func (sst *SSTable) Level() int {
	return sst.level
}

// EntryCount returns the number of entries in this SSTable.
func (sst *SSTable) EntryCount() int {
	return sst.entries
}

// tombstoneMarker is written as the value length for deleted keys.
// We use MaxUint32 because no real value should be 4GB.
const tombstoneMarker = ^uint32(0)

func writeEntry(w io.Writer, entry SSTableEntry) error {
	keyBytes := []byte(entry.Key)

	if err := binary.Write(w, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
		return err
	}
	if _, err := w.Write(keyBytes); err != nil {
		return err
	}

	if entry.Value == nil {
		return binary.Write(w, binary.LittleEndian, tombstoneMarker)
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(entry.Value))); err != nil {
		return err
	}
	_, err := w.Write(entry.Value)
	return err
}

func readEntry(r io.Reader) (SSTableEntry, error) {
	var keyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return SSTableEntry{}, err
	}

	keyBuf := make([]byte, keyLen)
	if _, err := io.ReadFull(r, keyBuf); err != nil {
		return SSTableEntry{}, err
	}

	var valLen uint32
	if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
		return SSTableEntry{}, err
	}

	var value []byte
	if valLen == tombstoneMarker {
		value = nil
	} else {
		value = make([]byte, valLen)
		if _, err := io.ReadFull(r, value); err != nil {
			return SSTableEntry{}, err
		}
	}

	return SSTableEntry{
		Key:   string(keyBuf),
		Value: value,
	}, nil
}

func writeIndex(w io.Writer, idx []indexEntry) (int64, error) {
	var buf bytes.Buffer

	binary.Write(&buf, binary.LittleEndian, uint32(len(idx)))

	for _, entry := range idx {
		keyBytes := []byte(entry.Key)
		binary.Write(&buf, binary.LittleEndian, uint32(len(keyBytes)))
		buf.Write(keyBytes)
		binary.Write(&buf, binary.LittleEndian, entry.Offset)
	}

	data := buf.Bytes()
	_, err := w.Write(data)
	return int64(len(data)), err
}

func readIndex(r io.ReaderAt, offset int64, size int64) ([]indexEntry, error) {
	data := make([]byte, size)
	if _, err := r.ReadAt(data, offset); err != nil {
		return nil, err
	}

	reader := bytes.NewReader(data)

	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	entries := make([]indexEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBuf); err != nil {
			return nil, err
		}
		var off int64
		if err := binary.Read(reader, binary.LittleEndian, &off); err != nil {
			return nil, err
		}
		entries = append(entries, indexEntry{Key: string(keyBuf), Offset: off})
	}

	return entries, nil
}

func writeFooter(w io.Writer, ft footer) error {
	if err := binary.Write(w, binary.LittleEndian, ft.IndexOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, ft.IndexSize); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, ft.BloomOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, ft.BloomSize); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, ft.BloomNumHash); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, ft.NumEntries)
}

func readFooter(r io.ReadSeeker) (footer, error) {

	if _, err := r.Seek(-int64(footerSize), io.SeekEnd); err != nil {
		return footer{}, err
	}

	var ft footer
	if err := binary.Read(r, binary.LittleEndian, &ft.IndexOffset); err != nil {
		return footer{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &ft.IndexSize); err != nil {
		return footer{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &ft.BloomOffset); err != nil {
		return footer{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &ft.BloomSize); err != nil {
		return footer{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &ft.BloomNumHash); err != nil {
		return footer{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &ft.NumEntries); err != nil {
		return footer{}, err
	}
	return ft, nil
}
