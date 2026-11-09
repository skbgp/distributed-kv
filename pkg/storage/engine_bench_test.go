package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkEngine_Put(b *testing.B) {
	engine, err := NewEngine(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%08d", i)
		val := []byte(fmt.Sprintf("value-%08d", i))
		if err := engine.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEngine_Get(b *testing.B) {
	b.Run("MemTable", func(b *testing.B) {
		engine, err := NewEngine(b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		defer engine.Close()
		if err := engine.Put("target", []byte("value")); err != nil {
			b.Fatal(err)
		}
		benchmarkGet(b, engine)
	})
	b.Run("SSTable", func(b *testing.B) {
		dir := b.TempDir()
		entries := make([]SSTableEntry, 0, 512)
		for i := 0; i < 511; i++ {
			entries = append(entries, SSTableEntry{fmt.Sprintf("key-%08d", i), []byte("value")})
		}
		entries = append(entries, SSTableEntry{"target", []byte("value")})
		if _, err := WriteSSTable(filepath.Join(dir, "00000000000000000001.sst"), entries, 0); err != nil {
			b.Fatal(err)
		}
		engine, err := NewEngine(dir)
		if err != nil {
			b.Fatal(err)
		}
		defer engine.Close()
		benchmarkGet(b, engine)
	})
}

func benchmarkGet(b *testing.B, engine *Engine) {
	b.Helper()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, found, err := engine.Get("target")
		if err != nil || !found || string(value) != "value" {
			b.Fatalf("unexpected lookup: %q, %t, %v", value, found, err)
		}
	}
}
