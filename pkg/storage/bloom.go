package storage

import (
	"hash/fnv"
	"math"
)

// BloomFilter is a probabilistic set membership structure.
type BloomFilter struct {
	bits    []byte
	numBits uint32
	numHash uint32
}

// NewBloomFilter creates a Bloom filter sized for the expected number of keys.
// bitsPerKey controls the space/accuracy tradeoff:
//   - 10 bits/key → ~1% false positive rate (good default)
//   - 15 bits/key → ~0.1% false positive rate
//   - 5 bits/key  → ~10% false positive rate (too high for most uses)
func NewBloomFilter(expectedKeys int, bitsPerKey int) *BloomFilter {
	if expectedKeys <= 0 {
		expectedKeys = 1
	}
	if bitsPerKey <= 0 {
		bitsPerKey = 10
	}

	numBits := uint32(expectedKeys * bitsPerKey)

	if numBits < 64 {
		numBits = 64
	}

	numHash := uint32(float64(bitsPerKey) * 0.693)
	if numHash < 1 {
		numHash = 1
	}
	if numHash > 30 {
		numHash = 30
	}

	return &BloomFilter{
		bits:    make([]byte, (numBits+7)/8),
		numBits: numBits,
		numHash: numHash,
	}
}

// Add inserts a key into the Bloom filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := bf.baseHashes(key)

	for i := uint32(0); i < bf.numHash; i++ {

		pos := (h1 + i*h2) % bf.numBits
		bf.bits[pos/8] |= 1 << (pos % 8)
	}
}

// MayContain checks if a key might be in the set.
// Returns false → definitely not present.
// Returns true  → probably present (small chance of false positive).
func (bf *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := bf.baseHashes(key)

	for i := uint32(0); i < bf.numHash; i++ {
		pos := (h1 + i*h2) % bf.numBits
		if bf.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Bytes returns the raw filter data for serialization (when writing to SSTable).
func (bf *BloomFilter) Bytes() []byte {
	return bf.bits
}

// NumHash returns the number of hash functions (needed for deserialization).
func (bf *BloomFilter) NumHash() uint32 {
	return bf.numHash
}

// LoadBloomFilter reconstructs a Bloom filter from serialized data.
// Used when reading an SSTable from disk.
func LoadBloomFilter(data []byte, numHash uint32) *BloomFilter {
	return &BloomFilter{
		bits:    data,
		numBits: uint32(len(data)) * 8,
		numHash: numHash,
	}
}

// baseHashes computes the two FNV-based hashes that we combine to generate
// all k hash functions. We split a 64-bit FNV hash into two 32-bit halves.
func (bf *BloomFilter) baseHashes(key []byte) (uint32, uint32) {
	hasher := fnv.New64a()
	hasher.Write(key)
	sum := hasher.Sum64()

	h1 := uint32(sum)
	h2 := uint32(sum >> 32)

	if h2%2 == 0 {
		h2++
	}

	return h1, h2
}

// EstimateFalsePositiveRate returns the theoretical false positive rate
// for the current filter configuration. Useful for benchmarking.
//
// Formula: (1 - e^(-k*n/m))^k
// where k = numHash, n = number of inserted keys, m = numBits
func (bf *BloomFilter) EstimateFalsePositiveRate(numKeys int) float64 {
	k := float64(bf.numHash)
	n := float64(numKeys)
	m := float64(bf.numBits)

	return math.Pow(1.0-math.Exp(-k*n/m), k)
}
