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
	locked    bool
	totalSize uint32
}

func NewMemtable() *Memtable {
	return &Memtable{
		rbt.NewWithStringComparator(),
		false,
		0,
	}
}

func (m *Memtable) Get(key string) (Record, error) {
	val, found := m.data.Get(key)
	if !found {
		return Record{}, utils.ErrKeyNotFound
	}
	return val.(Record), nil
}

var recCounter int = 0

func (m *Memtable) Set(key string, value Record) {
	m.data.Put(key, value)
	recCounter++
	m.totalSize += value.TotalSize
}

func (m *Memtable) Flush(dir string) (*SSTable, error) {
	m.locked = true
	sortedEntries := m.returnAllRecordsInSortedOrder()
	table, err := InitSSTableOnDisk(dir, castToRecordSlice(sortedEntries))
	if err != nil {
		return nil, err
	}
	m.clear()
	return table, nil
}

func (m *Memtable) returnAllRecordsInSortedOrder() []any {
	data := inorderRBT(m.data.Root, make([]any, 0))
	return data
}

func castToRecordSlice(interfaceSlice []any) []Record {
	recordSlice := make([]Record, len(interfaceSlice))
	for i, iface := range interfaceSlice {
		record, ok := iface.(Record)
		if !ok {
			fmt.Errorf("element %d is not a Record", i)
		}
		recordSlice[i] = record
	}
	return recordSlice
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
	m.locked = false
}
