package internal

import (
	"bytes"
	"os"
)

const (
	DATA_FILE_EXTENSION  string = ".data"
	INDEX_FILE_EXTENSION string = ".index"
)

type SSTable struct {
	dataFile  *os.File
	indexFile *os.File
}

func NewSSTable(filename string) *SSTable {
	dataFile, indexFile := initializeFromDisk(filename)
	return &SSTable{
		dataFile:  dataFile,
		indexFile: indexFile,
	}
}

func initializeFromDisk(filename string) (*os.File, *os.File) {
	dataFile, err := os.Create(filename + DATA_FILE_EXTENSION)
	indexFile, err := os.Create(filename + INDEX_FILE_EXTENSION)
	if err != nil {
		panic(err)
	}
	return dataFile, indexFile
}

func (sst *SSTable) writeEntriesToSST(entries []Record) error {
	buf := new(bytes.Buffer)
	for i := range entries {
		err := entries[i].EncodeKV(buf)
		if err != nil {
			return err
		}
	}
	// after encoding each entry, dump into the SSTable
	if err := writeToFile(buf.Bytes(), sst.dataFile); err != nil {
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
