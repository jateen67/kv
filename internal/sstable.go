package internal

import (
	"bufio"
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
	nodeId      string
	dataFile    *os.File
	indexFile   *os.File
	bloomFilter *BloomFilter
	sstCounter  uint32
	minKey      string
	maxKey      string
	totalSize   uint32
	sparseKeys  []sparseIndex
}

func InitSSTableOnDisk(nodeId string, directory string, entries []Record) (*SSTable, error) {
	atomic.AddUint32(&ssTableCounter, 1)
	table := &SSTable{
		nodeId:     nodeId,
		sstCounter: ssTableCounter,
	}
	err := table.initTableFiles(directory)
	if err != nil {
		return nil, err
	}

	err2 := writeEntriesToSST(entries, table)
	if err2 != nil {
		return nil, err2
	}
	return table, nil
}

func (sst *SSTable) initTableFiles(directory string) error {
	// Create "storage" folder with read-write-execute for owner & group, read-only for others
	if err := os.MkdirAll("../storage", 0755); err != nil {
		return err
	}

	dataFile, err := os.Create(getNextSstFilename(sst.nodeId, directory, sst.sstCounter) + DATA_FILE_EXTENSION)
	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}
	indexFile, err := os.Create(getNextSstFilename(sst.nodeId, directory, sst.sstCounter) + INDEX_FILE_EXTENSION)
	if err != nil {
		return errors.Join(
			dataFile.Close(),
			fmt.Errorf("failed to create index file: %w", err),
		)
	}
	bloomFile, err := os.Create(getNextSstFilename(sst.nodeId, directory, sst.sstCounter) + BLOOM_FILE_EXTENSION)
	if err != nil {
		return errors.Join(
			dataFile.Close(),
			indexFile.Close(),
			fmt.Errorf("failed to create bloom filter file: %w", err),
		)
	}

	sst.dataFile, sst.indexFile = dataFile, indexFile
	sst.bloomFilter = NewBloomFilter(bloomFile)

	return nil
}

func getNextSstFilename(nodeId string, directory string, c uint32) string {
	return fmt.Sprintf("../%s/%s_sst_%d", directory, nodeId, c)
}

type sparseIndex struct {
	keySize    uint32
	key        string
	byteOffset uint32
}

func writeEntriesToSST(entries []Record, table *SSTable) error {
	buf := new(bytes.Buffer)
	var byteOffsetCounter uint32

	// Keep track of min, max for searching in the case our desired key is outside these bounds
	table.minKey = entries[0].Key
	table.maxKey = entries[len(entries)-1].Key

	// * every 1000th key will be put into the sparse index
	for i := range entries {
		table.totalSize += entries[i].TotalSize
		if i%SPARSE_INDEX_SAMPLE_SIZE == 0 {
			table.sparseKeys = append(table.sparseKeys, sparseIndex{
				keySize:    entries[i].Header.KeySize,
				key:        entries[i].Key,
				byteOffset: byteOffsetCounter,
			})
		}
		byteOffsetCounter += entries[i].TotalSize
		err := entries[i].EncodeKV(buf)
		if err != nil {
			return err
		}
	}
	// after encoding each entry, dump into the SSTable
	if err := writeToFile(buf.Bytes(), table.dataFile); err != nil {
		fmt.Println("write to sst err:", err)
	}

	// Set up sparse index
	err := populateSparseIndexFile(table.sparseKeys, table.indexFile)
	if err != nil {
		return fmt.Errorf("populate sparse index: %w", err)
	}

	// Set up + populate bloom filter
	table.bloomFilter.InitBloomFilterAttrs(uint32(len(entries)))
	err = populateBloomFilter(entries, table.bloomFilter)
	if err != nil {
		return fmt.Errorf("populate bloom filter: %w", err)
	}

	return nil
}

func populateSparseIndexFile(indices []sparseIndex, indexFile *os.File) error {
	// encode and write to index file
	buf := new(bytes.Buffer)
	for i := range indices {
		err := binary.Write(buf, binary.LittleEndian, indices[i].keySize)
		if err != nil {
			return err
		}

		buf.WriteString(indices[i].key)
		err2 := binary.Write(buf, binary.LittleEndian, indices[i].byteOffset)
		if err2 != nil {
			return err2
		}
	}

	if err := writeToFile(buf.Bytes(), indexFile); err != nil {
		return fmt.Errorf("write to indexfile err: %w", err)
	}

	return nil
}

func populateBloomFilter(entries []Record, bloomFilter *BloomFilter) error {
	for i := range entries {
		err := bloomFilter.Add(entries[i].Key)
		if err != nil {
			return fmt.Errorf("bloom filter add key %q: %w", entries[i].Key, err)
		}
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
		return fmt.Errorf("write bloom filter file: %w", err)
	}

	return nil
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

	// seek to best candidate offset from the sparse index
	startOffset := int64(sst.sparseKeys[sst.getCandidateByteOffsetIndex(key)].byteOffset)
	if _, err := sst.dataFile.Seek(startOffset, io.SeekStart); err != nil {
		return "", fmt.Errorf("seek to sparse index offset: %w", err)
	}

	// Use a buffered reader from the seek point to avoid syscalls per read due to io.ReadFull on the raw *os.File
	reader := bufio.NewReader(sst.dataFile)

	headerBuf := make([]byte, headerSize)
	for {
		// read header
		_, err := io.ReadFull(reader, headerBuf)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return "", utils.ErrKeyNotFound
			}
			return "", fmt.Errorf("read header: %w", err)
		}

		h := &Header{}
		if err := h.decodeHeader(headerBuf); err != nil {
			return "", fmt.Errorf("decode header: %w", err)
		}

		// read in the key-value after the header (cursor naturally moves)
		kvBuf := make([]byte, h.KeySize+h.ValueSize)
		if _, err := io.ReadFull(reader, kvBuf); err != nil {
			return "", fmt.Errorf("read key-value: %w", err)
		}

		// append header and kv together to decode as a whole
		r := &Record{}
		if err := r.DecodeKV(append(headerBuf, kvBuf...)); err != nil {
			return "", fmt.Errorf("decode record: %w", err)
		}

		if r.Key == key {
			fmt.Printf("LOG: FOUND KEY %s -> %s\n", key, r.Value)
			return r.Value, nil
		} else if r.Key > key {
			// return early
			// this works b/c since our data is sorted, if the curr key is > target key,
			// ..then the key is not in this table
			return "", utils.ErrKeyNotFound
		}
		// else continue if r.Key < key
	}
}

// looks through sparse indexes and see which byte offset to start from when scanning SSTable
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

	if low == 0 {
		return 0
	}

	return low - 1
}

func (sst *SSTable) Close() error {
	return errors.Join(
		sst.dataFile.Close(),
		sst.indexFile.Close(),
		sst.bloomFilter.file.Close(),
	)
}
