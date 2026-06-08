package internal

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jateen67/kv/proto"
	"github.com/jateen67/kv/utils"
)

type DiskStore struct {
	mu                 sync.Mutex
	memtable           *Memtable
	wal                *writeAheadLog
	bucketManager      *BucketManager
	immutableMemtables []Memtable
}

type Operation int

const (
	SET Operation = iota
	GET
	DELETE
)

const FlushSizeThreshold = 1024 * 1024 * 256

// NewCluster starts up a cluster of N nodes (stores), internally calls the newStore method per node
func NewCluster(numOfNodes uint32) *Cluster {
	cluster := Cluster{}
	cluster.initNodes(numOfNodes)
	return &cluster
}

// newStore starts up a single-node KV store
func newStore(nodeId string) (*DiskStore, error) {
	ds := &DiskStore{memtable: NewMemtable(nodeId), bucketManager: InitBucketManager()}
	err := os.MkdirAll("log", 0755)
	if err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(fmt.Sprintf("../log/wal-%d.log", nodeId), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.wal = &writeAheadLog{file: logFile}
	return ds, err
}

func (ds *DiskStore) PutRecordFromGRPC(record *proto.Record) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	rec := convertProtoRecordToStoreRecord(record)
	ds.memtable.Set(rec.Key, rec)

	if err := ds.wal.appendWALOperation(SET, rec); err != nil {
		return fmt.Errorf("append to WAL: %w", err)
	}

	if ds.memtable.totalSize >= FlushSizeThreshold {
		ds.immutableMemtables = append(ds.immutableMemtables, *ds.memtable)
		ds.memtable = NewMemtable(ds.memtable.nodeId)
		if err := ds.FlushMemtable(); err != nil {
			return fmt.Errorf("flush memtable: %w", err)
		}
	}

	return nil
}

func (ds *DiskStore) Get(key string) (string, error) {
	if ds == nil {
		return "<!>", fmt.Errorf("disk store is not initialized")
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

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
	if ds == nil {
		return fmt.Errorf("disk store is not initialized")
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.memtable == nil {
		return fmt.Errorf("memtable is not initialized")
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
	record := &Record{
		Header:    header,
		Key:       key,
		Value:     value,
		TotalSize: headerSize + header.KeySize + header.ValueSize,
	}
	cs, err := record.CalculateChecksum()
	if err != nil {
		return err
	}

	record.Header.CheckSum = cs

	ds.memtable.Set(key, record)
	// Batch WAL appends to improve performance, constant disk writes are too expensive
	err = ds.wal.appendWALOperation(SET, record)
	if err != nil {
		return err
	}

	// Automatically flush when memtable reaches certain threshold
	if ds.memtable.totalSize >= FlushSizeThreshold {
		// store shallow copy of the memtable's struct values
		ds.immutableMemtables = append(ds.immutableMemtables, *ds.memtable)
		ds.memtable = NewMemtable(ds.memtable.nodeId)
		if err := ds.FlushMemtable(); err != nil {
			return fmt.Errorf("flush memtable: %w", err)
		}
	}
	return nil
}

func (ds *DiskStore) Delete(key string) error {
	if ds == nil {
		return fmt.Errorf("disk store is not initialized")
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

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
	var err error
	deletionRecord.Header.CheckSum, err = deletionRecord.CalculateChecksum()
	if err != nil {
		return err
	}

	ds.memtable.Set(key, &deletionRecord)
	err = ds.wal.appendWALOperation(DELETE, &deletionRecord)
	if err != nil {
		return err
	}

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

func (ds *DiskStore) LengthOfMemtable() {
	fmt.Println(len(ds.memtable.data.Keys()))
}

func (ds *DiskStore) FlushMemtable() error {
	for i := range ds.immutableMemtables {
		sstable, err := ds.immutableMemtables[i].Flush("storage")
		if err != nil {
			ds.immutableMemtables = ds.immutableMemtables[i:]
			return fmt.Errorf("flush memtable at index %d: %w", i, err)
		}

		err = ds.bucketManager.InsertTable(sstable)
		if err != nil {
			// Retain remaining memtables upon error so they can be still be flushed later
			ds.immutableMemtables = ds.immutableMemtables[i:]
			return fmt.Errorf("flush memtable at index %d: %w", i, err)
		}
	}

	// By this point all memtables were successfully flushed, so clear the slice
	ds.immutableMemtables = ds.immutableMemtables[:0]
	return nil
}

func deepCopyMemtable(memtable *Memtable) *Memtable {
	deepCopy := NewMemtable(memtable.nodeId)
	deepCopy.totalSize = memtable.totalSize

	keys := memtable.data.Keys()
	values := memtable.data.Values()
	for i := range keys {
		deepCopy.data.Put(keys[i], values[i])
	}

	return deepCopy
}

func (ds *DiskStore) Close() error {
	//TODO implement
	return errors.Join(
		ds.wal.file.Close(),
	)
}
