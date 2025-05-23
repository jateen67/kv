package internal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"

	"github.com/jateen67/kv/utils"
)

const (
	DATA_FILE_EXTENSION      string = ".data"
	INDEX_FILE_EXTENSION     string = ".index"
	BLOOM_FILE_EXTENSION     string = ".bloom"
	SPARSE_INDEX_SAMPLE_SIZE int    = 1000
)

var ssTableCounter uint32

type SSTable struct {
	dataFile    *os.File
	indexFile   *os.File
	bloomFilter *BloomFilter
	sstCounter  uint32
	minKey      string
	maxKey      string
	totalSize   uint32
	sparseKeys  []sparseIndex
}

func InitSSTableOnDisk(directory string, entries *[]Record) *SSTable {
	atomic.AddUint32(&ssTableCounter, 1)
	table := &SSTable{
		sstCounter: ssTableCounter,
	}
	table.initTableFiles(directory)
	writeEntriesToSST(entries, table)
	return table
}

func (sst *SSTable) initTableFiles(directory string) error {
	// Create "storage" folder with read-write-execute for owner & group, read-only for others
	if err := os.MkdirAll("../storage", 0755); err != nil {
		return err
	}

	dataFile, err := os.Create(getNextSstFilename(directory, sst.sstCounter) + DATA_FILE_EXTENSION)
	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}
	indexFile, err := os.Create(getNextSstFilename(directory, sst.sstCounter) + INDEX_FILE_EXTENSION)
	if err != nil {
		dataFile.Close()
		return fmt.Errorf("failed to create index file: %w", err)
	}
	bloomFile, err := os.Create(getNextSstFilename(directory, sst.sstCounter) + BLOOM_FILE_EXTENSION)
	if err != nil {
		dataFile.Close()
		indexFile.Close()
		return fmt.Errorf("failed to create bloom filter file: %w", err)
	}

	sst.dataFile, sst.indexFile = dataFile, indexFile
	sst.bloomFilter = NewBloomFilter(bloomFile)

	return nil
}

func getNextSstFilename(directory string, c uint32) string {
	return fmt.Sprintf("../%s/sst_%d", directory, c)
}

type sparseIndex struct {
	keySize    uint32
	key        string
	byteOffset uint32
}

func writeEntriesToSST(entries *[]Record, table *SSTable) {
	buf := new(bytes.Buffer)
	var byteOffsetCounter uint32

	// Keep track of min, max for searching in the case our desired key is outside these bounds
	table.minKey = (*entries)[0].Key
	table.maxKey = (*entries)[len(*entries)-1].Key

	// * every 1000th key will be put into the sparse index
	for i := range *entries {
		table.totalSize += (*entries)[i].TotalSize
		if i%SPARSE_INDEX_SAMPLE_SIZE == 0 {
			table.sparseKeys = append(table.sparseKeys, sparseIndex{
				keySize:    (*entries)[i].Header.KeySize,
				key:        (*entries)[i].Key,
				byteOffset: byteOffsetCounter,
			})
		}
		byteOffsetCounter += (*entries)[i].TotalSize
		(*entries)[i].EncodeKV(buf)
	}
	// after encoding each entry, dump into the SSTable
	if err := writeToFile(buf.Bytes(), table.dataFile); err != nil {
		fmt.Println("write to sst err:", err)
	}

	// Set up sparse index
	populateSparseIndexFile(&table.sparseKeys, table.indexFile)
	// Set up + populate bloom filter
	table.bloomFilter.InitBloomFilterAttrs(uint32(len(*entries)))
	populateBloomFilter(entries, table.bloomFilter)
}

func populateSparseIndexFile(indices *[]sparseIndex, indexFile *os.File) {
	// encode and write to index file
	buf := new(bytes.Buffer)
	for i := range *indices {
		binary.Write(buf, binary.LittleEndian, (*indices)[i].keySize)
		buf.WriteString((*indices)[i].key)
		binary.Write(buf, binary.LittleEndian, (*indices)[i].byteOffset)
	}

	if err := writeToFile(buf.Bytes(), indexFile); err != nil {
		fmt.Println("write to indexfile err:", err)
	}
}

func populateBloomFilter(entries *[]Record, bloomFilter *BloomFilter) {
	for i := range *entries {
		bloomFilter.Add((*entries)[i].Key)
	}

	bfBytes := make([]byte, bloomFilter.bitSetSize)
	for i, b := range bloomFilter.bitSet {
		if b {
			bfBytes[i] = 1
		} else {
			bfBytes[i] = 0
		}
	}

	if err := writeToFile(bfBytes, bloomFilter.file); err != nil {
		fmt.Println("write to bloomfile err:", err)
	}
}

func writeToFile(data []byte, file *os.File) error {
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return nil
}

func (sst *SSTable) Get(key string) (string, error) {
	if key < sst.minKey || key > sst.maxKey {
		return "<!>", utils.ErrKeyNotWithinTable
	}
	if !sst.bloomFilter.MightContain(key) {
		return "", utils.ErrKeyNotWithinTable
	}

	// Get sparse index and move to offset
	currOffset := sst.sparseKeys[sst.getCandidateByteOffsetIndex(key)].byteOffset
	if _, err := sst.dataFile.Seek(int64(currOffset), 0); err != nil {
		return "", err
	}

	var keyFound = false
	for !keyFound {
		// set up entry for the header
		currEntry := make([]byte, 17)
		_, err := io.ReadFull(sst.dataFile, currEntry)
		if errors.Is(err, io.EOF) {
			fmt.Println("LOG: END OF FILE")
			return "", err
		}

		h := &Header{}
		h.decodeHeader(currEntry)

		// move the cursor so we can read the rest of the record
		currOffset += headerSize
		sst.dataFile.Seek(int64(currOffset), 0)
		// set up []byte for the rest of the record
		currRecord := make([]byte, h.KeySize+h.ValueSize)
		if _, err := io.ReadFull(sst.dataFile, currRecord); err != nil {
			fmt.Println("LOG: READFULL ERR:", err)
			return "", err
		}
		// append both []byte together in order to decode as a whole
		currEntry = append(currEntry, currRecord...) // full size of the record
		r := &Record{}
		r.DecodeKV(currEntry)

		if r.Key == key {
			fmt.Printf("LOG: FOUND KEY %s -> %s\n", key, r.Value)
			keyFound = true
			return r.Value, nil
		} else if r.Key > key {
			// return early
			// this works b/c since our data is sorted, if the curr key is > target key,
			// ..then the key is not in this table
			return "", utils.ErrKeyNotFound
		} else {
			// else, keep iterating & looking
			currOffset += r.Header.KeySize + r.Header.ValueSize
			sst.dataFile.Seek(int64(currOffset), 0)
		}
	}

	return "", utils.ErrKeyNotWithinTable
}

func (sst *SSTable) getCandidateByteOffsetIndex(targetKey string) int {
	low := 0
	high := len(sst.sparseKeys) - 1
	for low <= high {
		mid := (low + high) / 2
		cmp := strings.Compare(targetKey, sst.sparseKeys[mid].key)
		if cmp > 0 { // targetKey > sparseKeys[mid]
			low = mid + 1
		} else if cmp < 0 { // targetKey < sparseKeys[mid]
			high = mid - 1
		} else {
			return mid
		}
	}
	return low - 1
}
