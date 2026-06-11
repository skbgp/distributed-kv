package storage

import (
	"testing"
)

func TestBloomFilter_Basics(t *testing.T) {
	bf := NewBloomFilter(100, 3)

	key1 := []byte("hello")
	key2 := []byte("world")
	key3 := []byte("missing")

	bf.Add(key1)
	bf.Add(key2)

	if !bf.MayContain(key1) {
		t.Errorf("expected Bloom filter to contain %q", key1)
	}
	if !bf.MayContain(key2) {
		t.Errorf("expected Bloom filter to contain %q", key2)
	}

	if bf.MayContain(key3) {
		t.Errorf("did not expect Bloom filter to contain %q", key3)
	}
}
