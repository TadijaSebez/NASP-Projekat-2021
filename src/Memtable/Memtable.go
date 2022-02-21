package Memtable

import (
	"Config"
	"SSTable"
	"SkipList"
)

type Memtable struct {
	skipList        *SkipList.SkipList
	currentCapacity uint64
	maxCapacity     uint64
}

func New(sl *SkipList.SkipList, config Config.Config) *Memtable {
	return &Memtable{
		skipList:    sl,
		maxCapacity: config.MemtableSize,
	}
}

func (m *Memtable) Insert(key string, value []byte, tombstone bool) bool {
	m.skipList.Insert(key, value, tombstone)
	if m.skipList.GetSize() > m.maxCapacity {
		m.Flush()
		return true
	} else {
		return false
	}
}

func (m *Memtable) Get(key string) (bool, []byte) {
	found, value := m.skipList.Get(key)
	return found, value
}

func (m *Memtable) Flush() {
	var recArray = m.skipList.GetArray() // get data

	SSTable.CreateSSTable(recArray)
}
