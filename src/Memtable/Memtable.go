package Memtable

import (
	"Config"
	"SSTable"
	"SkipList"
	"time"
	"unsafe"
)

type Memtable struct {
	skipList        *SkipList.SkipList
	currentCapacity uint64
	maxCapacity     uint64
}

func New(sl *SkipList.SkipList, config Config.Config) *Memtable {
	return &Memtable{
		skipList:        sl,
		currentCapacity: 0,
		maxCapacity:     config.MemtableSize,
	}
}

func (m *Memtable) Insert(key string, value []byte) {
	elemSize := uint64(unsafe.Sizeof(key) + unsafe.Sizeof(value))
	if m.currentCapacity+elemSize > m.maxCapacity {
		m.Flush(key, value)
	} else {
		m.currentCapacity += elemSize
		m.skipList.Insert(key, value)
	}
}

func (m *Memtable) Delete(key string) {
	m.skipList.Delete(key)
}

func (m *Memtable) Flush(key string, value []byte) []SSTable.Record {
	var recArray = m.skipList.GetArray() // get data

	m.currentCapacity = uint64(unsafe.Sizeof(key) + unsafe.Sizeof(value)) // change skip list
	m.skipList = SkipList.New(&SkipList.SkipListNode{
		Key:       key,
		Value:     value,
		Tombstone: false,
		Timestamp: time.Now().Unix(),
		Next:      make([]*SkipList.SkipListNode, SkipList.MaxHeight)})
	
	return recArray // return data
}
