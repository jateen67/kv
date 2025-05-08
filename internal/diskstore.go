package internal

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"
)

/*
notes:
ok so a bitcask on disk is just a directory (our databse server),
with multiple files inside it
	-> 1 active file, 0 or more inactive files

ok so how do we actually create the bitcask?
	-> single file on disk called the "main database server"
	-> this file will contain 1 or more data files (active/inactive)

within each data file:
	-> data format is: tstamp | ksz | value_sz | key | val
	-> a data file is nothing more than a linear sequence of the above entries

*note: the active data file will automatically close once it reaches a certain size threshold

*/

const SEEK = 0

type DiskStore struct {
	file          *os.File
	writePosition int
	keyDir        map[string]KeyEntry // recall: KeyEntry has the position of the byte offset in the file where the value exists
}

func fileExists(fileName string) bool {
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	ds := &DiskStore{keyDir: make(map[string]KeyEntry)}
	if fileExists(fileName) {
		ds.initKeyDir(fileName)
	}
	// we open the file in following modes:
	//	os.O_APPEND - writes are append only.
	// 	os.O_RDWR - can read and write to the file
	// 	os.O_CREATE - creates the file if it does not exist
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.file = file
	return ds, nil
}

func (ds *DiskStore) Get(key string) string {
	kEntry, ok := ds.keyDir[key]
	if !ok {
		return ""
	}
	// move the current pointer to the right offset
	ds.file.Seek(int64(kEntry.position), SEEK)
	data := make([]byte, kEntry.totalSize)
	// TODO: handle errors
	_, err := io.ReadFull(ds.file, data)
	if err != nil {
		panic("read error")
	}
	_, _, value := decodeKV(data)
	return value
}

func (ds *DiskStore) Set(key string, value string) {
	_, ok := ds.keyDir[key]
	if ok {
		fmt.Println("key already set")
		return
	}
	// The steps to save a KV to disk is simple:
	// 1. Encode the KV into bytes
	// 2. Write the bytes to disk by appending to the file
	// 3. Update KeyDir with the KeyEntry of this key
	timestamp := uint32(time.Now().Unix())
	size, data := encodeKV(timestamp, key, value)
	// file consistency is hard (comp310)
	if _, err := ds.file.Write(data); err != nil {
		panic(err)
	}
	// ensure our writes are actually persisted to the disk
	if err := ds.file.Sync(); err != nil {
		panic(err)
	}
	ds.keyDir[key] = NewKeyEntry(timestamp, uint32(ds.writePosition), uint32(size))
	ds.writePosition += size
}

func (ds *DiskStore) initKeyDir(existingFile string) error {
	// we will initialise the keyDir by reading the contents of the file, record by
	// record. As we read each record, we will also update our keyDir with the
	// corresponding KeyEntry
	//
	// NOTE: this method is a blocking one, if the DB size is yuge then it will take
	// a lot of time to startup
	file, _ := os.Open(existingFile)
	defer file.Close()
	for {
		header := make([]byte, headerSize)
		_, err := io.ReadFull(file, header)
		if err == io.EOF {
			break
		}
		// TODO: handle errors
		if err != nil {
			break
		}
		timestamp, keySize, valueSize := decodeHeader(header)
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		_, err = io.ReadFull(file, key)
		// TODO: handle errors
		if err != nil {
			break
		}
		_, err = io.ReadFull(file, value)
		// TODO: handle errors
		if err != nil {
			break
		}
		totalSize := headerSize + keySize + valueSize
		ds.keyDir[string(key)] = NewKeyEntry(timestamp, uint32(ds.writePosition), totalSize)
		ds.writePosition += int(totalSize)
	}
	return nil
}
