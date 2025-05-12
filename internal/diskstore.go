package internal

import (
	"bytes"
	"errors"
	"os"
	"time"

	"github.com/jateen67/kv/utils"
)

type DiskStore struct {
	memtable *Memtable
	wal      *os.File
	levels   [][]SSTable
}

type Operation int

const (
	PUT Operation = iota
	GET
	DELETE
)

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

	for i := len(ds.levels[0]) - 1; i >= 0; i-- {
		value, err := ds.levels[0][i].Get(key)
		if errors.Is(err, utils.ErrKeyNotWithinTable) {
			continue
		}
		return value, err
	}

	return "<!not_found>", utils.ErrKeyNotFound
}

func (ds *DiskStore) Delete(key string) error {
	return nil
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
	if ds.memtable.totalSize >= 6500 {
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
