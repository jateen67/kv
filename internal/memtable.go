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
	data      *rbt.Tree
	totalSize uint32
}

func NewMemtable() *Memtable {
	return &Memtable{
		rbt.NewWithStringComparator(),
		0,
	}
}

func (m *Memtable) Get(key *string) (Record, error) {
	val, found := m.data.Get(*key)
	if !found {
		return Record{}, utils.ErrKeyNotFound
	}
	return val.(Record), nil
}

var recCounter int = 0

func (m *Memtable) Set(key *string, value *Record) {
	m.data.Put(*key, *value)
	recCounter++
	m.totalSize += value.TotalSize
}

func (m *Memtable) GetAllKVPairs() map[string]Record {
	kvPairs := make(map[string]Record)

	for _, k := range m.data.Keys() {
		val, _ := m.data.Get(k)
		kvPairs[k.(string)] = val.(Record)
	}

	return kvPairs
}

func (m *Memtable) Flush(dir string) *SSTable {
	sortedEntries := m.returnAllRecordsInSortedOrder()
	return InitSSTableOnDisk(dir, castToRecordSlice(&sortedEntries))
}

func (m *Memtable) returnAllRecordsInSortedOrder() []any {
	return inorderRBT(m.data.Root, make([]any, 0))
}

func castToRecordSlice(interfaceSlice *[]any) *[]Record {
	recordSlice := make([]Record, len(*interfaceSlice))
	for i, iface := range *interfaceSlice {
		record, ok := iface.(Record)
		if !ok {
			fmt.Errorf("element %d is not a Record", i)
		}
		recordSlice[i] = record
	}
	return &recordSlice
}

func inorderRBT(node *rbt.Node, data []interface{}) []interface{} {
	if node != nil {
		data = inorderRBT(node.Left, data)
		data = append(data, node.Value)
		data = inorderRBT(node.Right, data)
	}
	return data
}

func (m *Memtable) clear() {
	m.data.Clear()
	m.totalSize = 0
}
