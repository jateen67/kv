package internal

import (
	"fmt"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/jateen67/kv/utils"
)

/*
Red-Black tree as memtable -- will replace original hash table
*/

type Memtable struct {
	nodeId    string
	data      *rbt.Tree
	totalSize uint32
}

func NewMemtable(nodeId string) *Memtable {
	return &Memtable{
		nodeId,
		rbt.NewWithStringComparator(),
		0,
	}
}

func (m *Memtable) Get(key string) (*Record, error) {
	val, found := m.data.Get(key)
	if !found {
		return nil, utils.ErrKeyNotFound
	}
	return val.(*Record), nil
}

func (m *Memtable) Set(key string, value *Record) {
	m.data.Put(key, value)
	// TODO: duplicate keys could inflate size
	m.totalSize += value.TotalSize
}

func (m *Memtable) GetAllKVPairs() map[string]*Record {
	kvPairs := make(map[string]*Record, m.data.Size())

	iter := m.data.Iterator()
	for iter.Next() {
		kvPairs[iter.Key().(string)] = iter.Value().(*Record)
	}

	return kvPairs
}

func (m *Memtable) Flush(dir string) (*SSTable, error) {
	sortedEntries := m.returnAllRecordsInSortedOrder()
	table, err := InitSSTableOnDisk(m.nodeId, dir, sortedEntries)
	if err != nil {
		return nil, fmt.Errorf("flush memtable to disk: %w", err)
	}

	return table, nil
}

func (m *Memtable) returnAllRecordsInSortedOrder() []*Record {
	records := make([]*Record, 0, m.data.Size())
	it := m.data.Iterator()
	for it.Next() {
		records = append(records, it.Value().(*Record))
	}

	return records
}

func castToRecordSlice(interfaceSlice *[]any) []Record {
	recordSlice := make([]Record, len(*interfaceSlice))
	for i, iface := range *interfaceSlice {
		record, ok := iface.(Record)
		if !ok {
			fmt.Errorf("element %d is not a Record", i)
		}
		recordSlice[i] = record
	}
	return recordSlice
}

func (m *Memtable) clear() {
	m.data.Clear()
	m.totalSize = 0
}
