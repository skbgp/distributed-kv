package storage

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkEngine_Put(b *testing.B) {
	dir, _ := os.MkdirTemp("", "dkv-bench-put-*")
	defer os.RemoveAll(dir)

	engine, err := NewEngine(dir)
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
	dir, _ := os.MkdirTemp("", "dkv-bench-get-*")
	defer os.RemoveAll(dir)

	engine, err := NewEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	// Pre-populate with 5,000 keys to ensure some flushes to SSTables
	numKeys := 5000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%08d", i)
		val := []byte(fmt.Sprintf("value-%08d", i))
		engine.Put(key, val)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%08d", i%numKeys)
		_, _, err := engine.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}
