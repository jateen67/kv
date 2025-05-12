package internal

import (
	"bytes"
	"errors"
	"os"
	"time"

	"github.com/jateen67/kv/utils"
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

this is DISK storage, so this will all be stored in SSD/HDD, therefore being persistent
*/

type DiskStore struct {
	serverFile    *os.File
	writePosition int
	memtable      *Memtable
	wal           *os.File
	levels        [][]SSTable
}

type Operation int

const (
	PUT Operation = iota
	GET
	DELETE
)

func fileExists(fileName string) bool {
	if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}

func NewDiskStore() (*DiskStore, error) {
	ds := &DiskStore{memtable: NewMemtable()}
	logFile, err := os.OpenFile("wal.log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.wal = logFile
	return ds, err
}

func (ds *DiskStore) Set(key string, value string) error {
	// Pprevent writes occurring while memtable is locked and flushing to disk
	if ds.memtable.locked {
		return utils.ErrMemtableLocked
	}
	if len(key) == 0 {
		return errors.New("set() error: key empty")
	}
	if len(value) == 0 {
		return errors.New("set() error: value empty")
	}

	header := Header{
		CheckSum:  0,
		Tombstone: 0,
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
	}
	record := Record{
		Header:    header,
		Key:       key,
		Value:     value,
		TotalSize: headerSize + header.KeySize + header.ValueSize,
	}
	record.Header.CheckSum = record.CalculateChecksum()

	ds.memtable.Set(key, record)

	buf := new(bytes.Buffer)
	buf.WriteByte(byte(PUT))
	if err := record.EncodeKV(buf); err != nil {
		return utils.ErrEncodingKVFailed
	}

	err := ds.writeToFile(buf.Bytes(), ds.wal)
	return err
}

func (ds *DiskStore) Get(key string) (string, error) {
	record, err := ds.memtable.Get(key)
	// if not found in memtable search in sstable
	if err == nil {
		return record.Value, nil
	} else if !errors.Is(err, utils.ErrKeyNotFound) {
		return "<!>", err
	}

	for i := range ds.levels[0] {
		value, err := ds.levels[0][i].Get(key)
		if errors.Is(err, utils.ErrKeyNotWithinTable) {
			continue
		}
		return value, err
	}

	return "<!not_found>", utils.ErrKeyNotFound
}

// TODO: rework to add rbtree and sstable
// func (ds *DiskStore) Delete(key string) error {
// 	// key note: this is an APPEND-ONLY db, so it wouldn't make sense to
// 	// overwrite existing data and place a tombstone value there
// 	// thus we have to write a semi-copy of the record w/ the tombstone val activated
// 	_, ok := ds.keyDir[key]
// 	if !ok {
// 		return errors.New("delete() error: key not found")
// 	}

// 	tempVal := ""
// 	header := Header{
// 		CheckSum:  0,
// 		Tombstone: 1,
// 		TimeStamp: uint32(time.Now().Unix()),
// 		KeySize:   uint32(len(key)),
// 		ValueSize: uint32(len(tempVal)),
// 	}

// 	record := Record{
// 		Header:    header,
// 		Key:       key,
// 		Value:     tempVal,
// 		TotalSize: headerSize + header.KeySize + header.ValueSize,
// 	}
// 	record.Header.CheckSum = record.CalculateChecksum()

// 	buf := new(bytes.Buffer)
// 	if err := record.EncodeKV(buf); err != nil {
// 		return errors.New("delete() error: could not encode record")
// 	}
// 	ds.writeToFile(buf.Bytes())

// 	delete(ds.keyDir, key)
// 	return nil
// }

func (ds *DiskStore) Close() bool {
	ds.serverFile.Sync()
	if err := ds.serverFile.Close(); err != nil {
		return false
	}
	return true
}

func (ds *DiskStore) writeToFile(data []byte, file *os.File) error {
	if _, err := file.Write(data); err != nil {
		return err
	}
	// file consistency very complex (comp310)
	if err := file.Sync(); err != nil {
		return err
	}
	return nil
}

var counter int = 0

func (ds *DiskStore) FlushMemtable() {
	if ds.memtable.totalSize >= 800 {
		counter++
		sstable, err := ds.memtable.Flush("storage")
		if err != nil {
			panic(err)
		}

		if len(ds.levels) == 0 {
			ds.levels = append(ds.levels, []SSTable{*sstable})
		} else {
			ds.levels[0] = append(ds.levels[0], *sstable)
		}
	}
}
