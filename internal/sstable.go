package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"
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
}

func InitSSTableOnDisk(directory string, entries []Record) error {
	atomic.AddUint32(&ssTableCounter, 1)
	table := &SSTable{
		sstCounter: ssTableCounter,
	}
	err := table.initTableFiles(directory)
	if err != nil {
		return err
	}
	err = writeEntriesToSST(entries, table)
	return err
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
	var sparseKeys []sparseIndex
	var byteOffsetCounter uint32

	// Keep track of min, max for searching in the case our desired key is outside these bounds
	table.minKey = entries[0].Key
	table.maxKey = entries[len(entries)-1].Key

	// * every 100th key will be put into the sparse index
	for i := range entries {
		if i%SPARSE_INDEX_SAMPLE_SIZE == 0 {
			sparseKeys = append(sparseKeys, sparseIndex{
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
	err := populateSparseIndex(sparseKeys, table.indexFile)
	return err
}

func populateSparseIndex(indices []sparseIndex, indexFile *os.File) error {
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
