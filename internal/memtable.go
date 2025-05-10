package internal

/*
Red-Black tree as memtable -- will replace original hash table
*/

type Memtable struct {
	data      RedBlackTree
	locked    bool
	totalSize uint32
}

func NewMemtable() *Memtable {
	return &Memtable{RedBlackTree{root: nil}, false, 0}
}

func (m *Memtable) Get(key string) (Record, error) {
	return m.data.Find(key)
}

func (m *Memtable) Set(key string, value Record) {
	m.data.Insert(key, value)
	m.totalSize += value.TotalSize
}

func (m *Memtable) Flush(dir string) error {
	m.locked = true
	sortedEntries := m.data.ReturnAllRecordsInSortedOrder()
	err := InitSSTableOnDisk(dir, sortedEntries)
	if err != nil {
		return err
	}
	m.clear()
	return nil
}

func (m *Memtable) clear() {
	m.data.root = nil
	m.totalSize = 0
	m.locked = false
}
