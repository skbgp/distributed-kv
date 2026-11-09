package storage

import (
	"fmt"
	"testing"
)

func TestReopenLegacySSTablePreservesBloomModulus(t *testing.T) {
	for _, size := range []int{7, 21, 512} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			cm := NewCompactionManager(t.TempDir())
			var entries []SSTableEntry
			for i := 0; i < size; i++ {
				entries = append(entries, SSTableEntry{fmt.Sprintf("key-%03d", i), []byte("value")})
			}
			path := cm.NewSSTablePath()
			if _, err := WriteSSTable(path, entries, 0); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenSSTable(path, 1)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				value, found, err := reopened.Get(entry.Key)
				if err != nil || !found || string(value) != "value" {
					t.Fatalf("read %s = %q, %t, %v", entry.Key, value, found, err)
				}
			}
		})
	}
}

func TestCompactionPreservesNewestValuesAndDeletionsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cm := NewCompactionManager(dir)
	old := []SSTableEntry{{"deleted", []byte("old")}, {"updated", []byte("old")}}
	batches := [][]SSTableEntry{
		old,
		{{"deleted", nil}, {"updated", []byte("new")}},
		{{"another", []byte("value")}},
		{{"last", []byte("value")}},
	}
	for _, entries := range batches {
		table, err := WriteSSTable(cm.NewSSTablePath(), entries, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := cm.AddSSTable(table); err != nil {
			t.Fatal(err)
		}
	}
	if err := cm.RunCompaction(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after writing a file but before publishing it.
	if _, err := WriteSSTable(cm.NewSSTablePath(), old, 0); err != nil {
		t.Fatal(err)
	}
	reopened := NewCompactionManager(dir)
	if err := reopened.LoadExistingSSTables(); err != nil {
		t.Fatal(err)
	}
	for _, manager := range []*CompactionManager{cm, reopened} {
		value, found, err := manager.get("deleted")
		if err != nil || !found || value != nil {
			t.Fatalf("deleted: %v %t %v", value, found, err)
		}
		value, found, err = manager.get("updated")
		if err != nil || !found || string(value) != "new" {
			t.Fatalf("updated: %q %t %v", value, found, err)
		}
		if got := len(manager.GetSSTables()); got != 1 {
			t.Fatalf("visible tables = %d", got)
		}
	}
}

func TestWALDistinguishesEmptyPutFromDeletion(t *testing.T) {
	path := t.TempDir()
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Write(WALRecord{Op: OpPut, Key: "empty", Value: []byte{}}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Write(WALRecord{Op: OpDelete, Key: "deleted"}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	value, found, err := engine.Get("empty")
	if err != nil || !found || value == nil || len(value) != 0 {
		t.Fatalf("empty: %v %t %v", value, found, err)
	}
	value, found, err = engine.Get("deleted")
	if err != nil || !found || value != nil {
		t.Fatalf("deleted: %v %t %v", value, found, err)
	}
}
