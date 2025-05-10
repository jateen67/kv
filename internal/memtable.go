package internal

/*
Red-Black tree as memtable -- will replace original hash table
*/

type Memtable struct {
	data      RedBlackTree
	totalSize uint32
}

func NewMemtable() *Memtable {
	return &Memtable{RedBlackTree{root: nil}, 0}
}

func (m *Memtable) Get(key string) (Record, error) {
	return m.data.Find(key)
}

func (m *Memtable) Set(key string, value Record) {
	m.data.Insert(key, value)
	m.totalSize += value.TotalSize
}

func (m *Memtable) Flush(filename string) error {
	sortedEntries := m.data.ReturnAllRecordsInSortedOrder()
	table := NewSSTable(filename)
	err := table.writeEntriesToSST(sortedEntries)
	if err != nil {
		return err
	}
	m.clear()
	return nil
}

func (m *Memtable) clear() {
	m.data.root = nil
	m.totalSize = 0
}
