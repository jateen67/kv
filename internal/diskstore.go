package internal

import (
	"bytes"
	"errors"
	"os"
	"time"

	"github.com/jateen67/kv/utils"
)

type DiskStore struct {
	memtable           *Memtable
	wal                *os.File
	bucketManager      *BucketManager
	immutableMemtables []Memtable
}

type Operation int

const (
	SET Operation = iota
	GET
	DELETE
)

const FlushSizeThreshold = 3_000

func NewDiskStore() (*DiskStore, error) {
	ds := &DiskStore{memtable: NewMemtable(), bucketManager: InitBucketManager()}
	logFile, err := os.OpenFile("../log/wal.log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.wal = logFile
	return ds, err
}

func (ds *DiskStore) Get(key string) (string, error) {
	// log 'GET' operation first
	ds.appendOperationToWAL(GET, Record{Key: key})

	record, err := ds.memtable.Get(key)
	// if not found in memtable search in sstable
	if err == nil {
		return record.Value, nil
	} else if !errors.Is(err, utils.ErrKeyNotFound) {
		return "<!>", err
	}

	return ds.bucketManager.RetrieveKey(key)
}

func (ds *DiskStore) Set(key string, value string) error {
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

	ds.appendOperationToWAL(SET, record)

	// Automatically flush when memtable reaches certain threshold
	if ds.memtable.totalSize >= FlushSizeThreshold {
		ds.immutableMemtables = append(ds.immutableMemtables, deepCopyMemtable(*ds.memtable))
		ds.memtable.clear()
		ds.FlushMemtable()
	}
	return nil
}

func (ds *DiskStore) Delete(key string) error {
	// appending a new entry but with a tombstone value and empty key
	value := ""
	header := Header{
		Tombstone: 1,
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
	}
	deletionRecord := Record{
		Header:    header,
		Key:       key,
		Value:     value,
		TotalSize: headerSize + header.KeySize + header.ValueSize,
	}
	deletionRecord.CalculateChecksum()

	ds.memtable.Set(key, deletionRecord)

	ds.appendOperationToWAL(DELETE, deletionRecord)

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

func (ds *DiskStore) FlushMemtable() {
	for i := range ds.immutableMemtables {
		sstable, err := ds.immutableMemtables[i].Flush("storage")
		if err != nil {
			panic(err)
		}

		ds.bucketManager.InsertTable(sstable)
		ds.immutableMemtables = ds.immutableMemtables[:i] // basically removing a "queued" memtable since its flushed
	}
}

func deepCopyMemtable(memtable Memtable) Memtable {
	deepCopy := NewMemtable()
	deepCopy.totalSize = memtable.totalSize

	keys := memtable.data.Keys()
	values := memtable.data.Values()
	for i := range keys {
		deepCopy.data.Put(keys[i], values[i])
	}

	return *deepCopy
}

func (ds *DiskStore) appendOperationToWAL(op Operation, record Record) error {
	buf := new(bytes.Buffer)
	// Store operation as only 1 byte (only WAL entries will have this extra byte)
	buf.WriteByte(byte(op))

	// encode the entire key, value entry
	if encodeErr := record.EncodeKV(buf); encodeErr != nil {
		return utils.ErrEncodingKVFailed
	}

	// store in WAL
	return writeToFile(buf.Bytes(), ds.wal)
}
