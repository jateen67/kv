package internal

import (
	"hash"
	"math"
	"os"

	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	file       *os.File
	bitSetSize uint64
	bitSet     []bool
	hashes     []hash.Hash64
}

const p = 0.01 // False positive probability

func NewBloomFilter(bloomFile *os.File) *BloomFilter {
	return &BloomFilter{file: bloomFile}
}

func (bf *BloomFilter) InitBloomFilterAttrs(numElements uint32) {
	bf.calculatebitSetSize(numElements)
	bf.initBitArray()
}

func (bf *BloomFilter) calculatebitSetSize(numElements uint32) {
	// proven math formulas to calculate optimal bloom filter params
	bf.bitSetSize = uint64(math.Ceil(-1 * float64(numElements) * math.Log(p) / math.Pow(math.Log(2), 2)))
	hashCount := uint64(math.Ceil((float64(bf.bitSetSize) / float64(numElements)) * math.Log(2)))
	bf.hashes = getHashes(hashCount)
}

func (bf *BloomFilter) initBitArray() {
	bf.bitSet = make([]bool, bf.bitSetSize)
}

func (bf *BloomFilter) Add(key string) {
	// hash the key n times, and store it into the bits array
	for _, hash := range bf.hashes {
		hash.Reset()
		hash.Write([]byte(key))
		hashValue := hash.Sum64() % bf.bitSetSize
		bf.bitSet[hashValue] = true
	}
}

func (bf *BloomFilter) MightContain(key string) bool {
	// ! Bloom filter is probabilistic, so there's a chance to get false positives
	for _, hasher := range bf.hashes {
		hasher.Reset()
		hasher.Write([]byte(key))
		hashValue := hasher.Sum64() % bf.bitSetSize
		if !bf.bitSet[hashValue] {
			return false
		}
	}
	return true
}

func getHashes(k uint64) []hash.Hash64 {
	hashers := make([]hash.Hash64, k)
	for i := 0; uint64(i) < k; i++ {
		hashers[i] = murmur3.New64WithSeed(uint32(i))
	}
	return hashers
}
