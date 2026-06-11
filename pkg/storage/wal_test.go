package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_WriteAndReplay(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test")
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.wal")
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	// Write some records
	records := []WALRecord{
		{Op: OpPut, Key: "k1", Value: []byte("v1")},
		{Op: OpPut, Key: "k2", Value: []byte("v2")},
		{Op: OpDelete, Key: "k1"},
	}

	for _, r := range records {
		if err := wal.Write(r); err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}
	wal.Close()

	// Reopen and replay
	wal2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("failed to open WAL for replay: %v", err)
	}
	defer wal2.Close()

	var replayed []WALRecord
	err = wal2.Replay(func(r WALRecord) {
		replayed = append(replayed, r)
	})

	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if len(replayed) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(replayed))
	}

	for i, r := range records {
		if replayed[i].Key != r.Key || !bytes.Equal(replayed[i].Value, r.Value) {
			t.Errorf("mismatch at index %d", i)
		}
	}
}
