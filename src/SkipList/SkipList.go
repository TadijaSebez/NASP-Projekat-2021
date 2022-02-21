package SkipList

import (
	"SSTable"
	"fmt"
	"hash/crc32"
	"math/rand"
	"time"
)

const MaxHeight = 32

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type SkipList struct {
	height int
	size   uint64
	head   *SkipListNode
}

type SkipListNode struct {
	Key       string
	Value     []byte
	Tombstone bool
	Timestamp int64
	prev      *SkipListNode
	Next      []*SkipListNode
}

func New(head *SkipListNode) *SkipList {
	return &SkipList{
		height: 0,
		size:   0,
		head:   head,
	}
}

func (s *SkipList) GetSize() uint64 {
	return s.size
}

func (s *SkipList) IsKeyFree(key string) bool {
	current := s.head
	for i := s.height; i >= 0; i-- {
		for ; current.Next[i] != nil; current = current.Next[i] {
			next := current.Next[i]
			if next.Key > key {
				break
			}
		}
		if current.Key == key {
			return false
		}
	}
	return true
}

func (s *SkipList) Insert(key string, value []byte, tombstone bool) {
	keyFree := s.IsKeyFree(key)

	level := 0
	for ; rand.Int31n(2) == 1; level++ {
		if level > s.height {
			s.height = level
			break
		}
	}
	node := &SkipListNode{
		Key:       key,
		Value:     value,
		Tombstone: tombstone,
		Timestamp: time.Now().Unix(),
		Next:      make([]*SkipListNode, level+1),
	}
	current := s.head
	for i := s.height; i >= 0; i-- {
		for ; current.Next[i] != nil; current = current.Next[i] {
			next := current.Next[i]
			if next.Key > key {
				break
			}
		}
		if i > level {
			continue
		}
		if current.Key == key {
			current.Key = key
			current.Value = value
			current.Tombstone = tombstone
			current.Timestamp = time.Now().Unix()
			return
		}
		if keyFree {
			node.Next[i] = current.Next[i]
			current.Next[i] = node
			node.prev = current
		}
	}
	s.size++
}

func (s *SkipList) Get(key string) (bool, []byte) {
	current := s.head
	for i := s.height; i >= 0; i-- {
		for ; current.Next[i] != nil; current = current.Next[i] {
			next := current.Next[i]
			if next.Key > key {
				break
			}
		}
		if current.Key == key {
			if current.Tombstone {
				return false, nil
			} else {
				return true, current.Value
			}
		}
	}
	return false, nil
}

func (s *SkipList) Draw() {
	ranks := make(map[string]int)
	for i, node := 0, s.head.Next[0]; node != nil; node = node.Next[0] {
		ranks[node.Key] = i
		i++
	}

	for level := s.height; level >= 0; level-- {
		if s.head.Next[level] == nil {
			continue
		}
		for i, node := 0, s.head.Next[level]; node != nil; node = node.Next[level] {
			rank := ranks[node.Key]
			for j := 0; j < rank-i; j++ {
				print("--")
			}
			print(node.Key, "-")
			i = rank + 1
		}
		println("\n")
	}
	fmt.Println("")
}

func (s *SkipList) GetArray() []SSTable.Record {
	var recArray []SSTable.Record
	for node := s.head.Next[0]; node != nil; node = node.Next[0] {
		rec := SSTable.Record{
			Crc:       CRC32(node.Value),
			Key:       node.Key,
			Value:     node.Value,
			Tombstone: node.Tombstone,
			Timestamp: uint64(node.Timestamp),
		}
		recArray = append(recArray, rec)

	}
	return recArray
}

func (s *SkipList) PrintNodeByKey(key string) {
	current := s.head
	for i := s.height; i >= 0; i-- {
		for ; current.Next[i] != nil; current = current.Next[i] {
			next := current.Next[i]
			if next.Key > key {
				break
			}
		}
		if current.Key == key {
			println("Key:", current.Key)
			println("Value:", current.Value)
			println("Tombstone:", current.Tombstone)
			println("Timestamp:", current.Timestamp)
		}
	}
}
