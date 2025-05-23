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
func newStore(nodeNum uint32) (*DiskStore, error) {
	ds := &DiskStore{memtable: NewMemtable(), bucketManager: InitBucketManager()}
	err := os.MkdirAll("log", 0755)
	if err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(fmt.Sprintf("../log/wal-%d.log", nodeNum), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.wal = &writeAheadLog{file: logFile}
	return ds, err
}

func (ds *DiskStore) PutRecordFromGRPC(record *proto.Record) {
	rec := convertProtoRecordToStoreRecord(record)
	ds.memtable.Set(&record.Key, rec)
	fmt.Printf("stored proto record with key = %s into memtable", rec.Key)
}

func (ds *DiskStore) Get(key string) (string, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	// log 'GET' operation first
	ds.wal.appendWALOperation(GET, &Record{Key: key})

	record, err := ds.memtable.Get(&key)
	// if not found in memtable search in sstable
	if err == nil {
		return record.Value, nil
	} else if !errors.Is(err, utils.ErrKeyNotFound) {
		return "<!>", err
	}

	return ds.bucketManager.RetrieveKey(&key)
}

func (ds *DiskStore) Set(key *string, value *string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if len(*key) == 0 {
		return errors.New("set() error: key empty")
	}
	if len(*value) == 0 {
		return errors.New("set() error: value empty")
	}

	header := Header{
		CheckSum:  0,
		Tombstone: 0,
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(*key)),
		ValueSize: uint32(len(*value)),
	}
	record := &Record{
		Header:    header,
		Key:       *key,
		Value:     *value,
		TotalSize: headerSize + header.KeySize + header.ValueSize,
	}
	record.Header.CheckSum = record.CalculateChecksum()

	ds.memtable.Set(key, record)
	// Batch WAL appends to improve performance, constant disk writes are too expensive
	ds.wal.appendWALOperation(SET, record)
	// Automatically flush when memtable reaches certain threshold
	if ds.memtable.totalSize >= FlushSizeThreshold {
		ds.immutableMemtables = append(ds.immutableMemtables, *deepCopyMemtable(ds.memtable))
		ds.memtable.clear()
		ds.FlushMemtable()
	}
	return nil
}

func (ds *DiskStore) Delete(key string) error {
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
	deletionRecord.CalculateChecksum()

	ds.memtable.Set(&key, &deletionRecord)
	ds.wal.appendWALOperation(DELETE, &deletionRecord)

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

func (ds *DiskStore) FlushMemtable() {
	for i := range ds.immutableMemtables {
		sstable := ds.immutableMemtables[i].Flush("storage")
		ds.bucketManager.InsertTable(sstable)
		ds.immutableMemtables = ds.immutableMemtables[:i] // basically removing a "queued" memtable since its flushed
	}
}

func deepCopyMemtable(memtable *Memtable) *Memtable {
	deepCopy := NewMemtable()
	deepCopy.totalSize = memtable.totalSize

	keys := memtable.data.Keys()
	values := memtable.data.Values()
	for i := range keys {
		deepCopy.data.Put(keys[i], values[i])
	}

	return deepCopy
}

func (ds *DiskStore) Close() bool {
	//TODO implement
	return true
}
