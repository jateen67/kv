package internal

import (
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
)

const (
	DATA_FILE_EXTENSION  string = ".data"
	INDEX_FILE_EXTENSION string = ".index"
)

var ssTableCounter uint32

type SSTable struct {
	dataFile   *os.File
	indexFile  *os.File
	sstCounter uint32
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
	err = writeEntriesToSST(entries, table.dataFile)
	return err
}

func (sst *SSTable) initTableFiles(directory string) error {
	// Create "storage" folder with read-write-execute for owner & group, read-only for others
	if err := os.MkdirAll("../storage", 0755); err != nil {
		return err
	}

	dataFile, _ := os.Create(sst.getNextSstFilename(directory) + DATA_FILE_EXTENSION)
	indexFile, err := os.Create(sst.getNextSstFilename(directory) + INDEX_FILE_EXTENSION)
	if err != nil {
		return err
	}

	sst.dataFile, sst.indexFile = dataFile, indexFile
	return nil
}

func (sst *SSTable) getNextSstFilename(directory string) string {
	return fmt.Sprintf("../%s/sst_%d", directory, sst.sstCounter)
}

func writeEntriesToSST(entries []Record, dataFile *os.File) error {
	buf := new(bytes.Buffer)
	for i := range entries {
		err := entries[i].EncodeKV(buf)
		if err != nil {
			return err
		}
	}
	// after encoding each entry, dump into the SSTable
	if err := writeToFile(buf.Bytes(), dataFile); err != nil {
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
