package internal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/jateen67/kv/utils"
)

const (
	DATA_FILE_EXTENSION      string = ".data"
	INDEX_FILE_EXTENSION     string = ".index"
	SPARSE_INDEX_SAMPLE_SIZE int    = 100
)

var ssTableCounter uint32

type SSTable struct {
	dataFile   *os.File
	indexFile  *os.File
	sstCounter uint32
	minKey     string
	maxKey     string
	sparseKeys []sparseIndex
}

func InitSSTableOnDisk(directory string, entries []Record) (*SSTable, error) {
	atomic.AddUint32(&ssTableCounter, 1)
	table := &SSTable{
		sstCounter: ssTableCounter,
	}
	err := table.initTableFiles(directory)
	if err != nil {
		return nil, err
	}
	err = writeEntriesToSST(entries, table)
	return table, err
}

func (sst *SSTable) initTableFiles(directory string) error {
	// Create "storage" folder with read-write-execute for owner & group, read-only for others
	if err := os.MkdirAll("../storage", 0755); err != nil {
		return err
	}

	dataFile, _ := os.Create(getNextSstFilename(directory, sst.sstCounter) + DATA_FILE_EXTENSION)
	indexFile, err := os.Create(getNextSstFilename(directory, sst.sstCounter) + INDEX_FILE_EXTENSION)
	if err != nil {
		return err
	}

	sst.dataFile, sst.indexFile = dataFile, indexFile
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

func writeEntriesToSST(entries []Record, table *SSTable) error {
	buf := new(bytes.Buffer)
	var byteOffsetCounter uint32

	// Keep track of min, max for searching in the case our desired key is outside these bounds
	table.minKey = entries[0].Key
	table.maxKey = entries[len(entries)-1].Key

	// * every 100th key will be put into the sparse index
	for i := range entries {
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
		return err
	}
	err := populateSparseIndexFile(table.sparseKeys, table.indexFile)
	return err
}

func populateSparseIndexFile(indices []sparseIndex, indexFile *os.File) error {
	// encode and write to index file
	buf := new(bytes.Buffer)
	for i := range indices {
		binary.Write(buf, binary.LittleEndian, &indices[i].keySize)
		buf.WriteString(indices[i].key)
		binary.Write(buf, binary.LittleEndian, &indices[i].byteOffset)
	}

	if err := writeToFile(buf.Bytes(), indexFile); err != nil {
		return err
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

	// Get sparse index and move to offset
	currOffset := sst.getCandidateByteOffset(key)
	if _, err := sst.dataFile.Seek(int64(currOffset), 0); err != nil {
		return "", err
	}

	var keyFound = false
	var eofErr error
	for keyFound == false || eofErr == nil {
		// set up entry for the header
		currEntry := make([]byte, 17)
		_, err := io.ReadFull(sst.dataFile, currEntry)
		if errors.Is(err, io.EOF) {
			eofErr = err
			fmt.Println("LOG: END OF FILE")
			return "EOF", err
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
			fmt.Println("LOG: SEARCH OVEREXTENSION, RETURNING AS KEY NOT FOUND.")
			// return early
			// this works b/c since our data is sorted, if the curr key is > target key,
			// ..then the key is not in this table
			return "<!>", utils.ErrKeyNotFound
		}

		// else, keep iterating & looking
		currOffset += r.Header.KeySize + r.Header.ValueSize
		sst.dataFile.Seek(int64(currOffset), 0)
	}

	return "<!>", utils.ErrKeyNotFound
}

func (sst *SSTable) getCandidateByteOffset(target string) uint32 {
	low := 0
	high := len(sst.sparseKeys) - 1

	for low < high {
		mid := (low + high) / 2
		if target < sst.sparseKeys[mid].key {
			high = mid - 1
		} else if target > sst.sparseKeys[mid].key {
			low = mid + 1
		} else {
			return sst.sparseKeys[mid].byteOffset
		}
	}
	return sst.sparseKeys[low].byteOffset
}
