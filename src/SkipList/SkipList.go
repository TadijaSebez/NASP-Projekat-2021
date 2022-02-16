package SkipList

import (
	"fmt"
	"math/rand"
	"time"
)

const maxHeight = 32

type SkipList struct {
	height int
	head   *SkipListNode
}

type SkipListNode struct {
	key       string
	value     []byte
	tombstone bool
	timestamp int64
	prev      *SkipListNode
	next      []*SkipListNode
}

func New(head *SkipListNode) *SkipList {
	return &SkipList{
		height: 0,
		head:   head,
	}
}

func (s *SkipList) IsKeyFree(key string) bool {
	current := s.head
	for i := s.height; i >= 0; i-- {
		for ; current.next[i] != nil; current = current.next[i] {
			next := current.next[i]
			if next.key > key {
				break
			}
		}
		if current.key == key {
			return false
		}
	}
	return true
}

func (s *SkipList) Insert(key string, value []byte) {
	keyFree := s.IsKeyFree(key)

	level := 0
	for ; rand.Int31n(2) == 1; level++ {
		if level > s.height {
			s.height = level
			break
		}
	}
	node := &SkipListNode{
		key:       key,
		value:     value,
		tombstone: false,
		timestamp: time.Now().Unix(),
		next:      make([]*SkipListNode, level+1),
	}
	current := s.head
	for i := s.height; i >= 0; i-- {
		for ; current.next[i] != nil; current = current.next[i] {
			next := current.next[i]
			if next.key > key {
				break
			}
		}
		if i > level {
			continue
		}
		if current.key == key {
			current.key = key
			current.value = value
			current.tombstone = false
			current.timestamp = time.Now().Unix()
			return
		}
		if keyFree {
			node.next[i] = current.next[i]
			current.next[i] = node
			node.prev = current
		}
	}
}

func (s *SkipList) Delete(key string) {
	current := s.head
	for i := s.height; i >= 0; i-- {
		for ; current.next[i] != nil; current = current.next[i] {
			next := current.next[i]
			if next.key > key {
				break
			}
		}
		if current.key == key {
			current.tombstone = true
			current.timestamp = time.Now().Unix()
			return
		}
	}
}

func (s *SkipList) Draw() {
	ranks := make(map[string]int)
	for i, node := 0, s.head.next[0]; node != nil; node = node.next[0] {
		ranks[node.key] = i
		i++
	}

	for level := s.height; level >= 0; level-- {
		if s.head.next[level] == nil {
			continue
		}
		for i, node := 0, s.head.next[level]; node != nil; node = node.next[level] {
			rank := ranks[node.key]
			for j := 0; j < rank-i; j++ {
				print("--")
			}
			print(node.key, "-")
			i = rank + 1
		}
		println("\n")
	}
	fmt.Println("")
}

func (s *SkipList) Read() {
	for node := s.head; node != nil; node = node.next[0] {
		print(node.key, "-")
	}
}

func (s *SkipList) PrintNodeByKey(key string) {
	current := s.head
	for i := s.height; i >= 0; i-- {
		for ; current.next[i] != nil; current = current.next[i] {
			next := current.next[i]
			if next.key > key {
				break
			}
		}
		if current.key == key {
			println("Key:", current.key)
			println("Value:", current.value)
			println("Tombstone:", current.tombstone)
			println("Timestamp:", current.timestamp)
		}
	}
}

func main() {
	// change "32" to anything else to see
	rand.Seed(8)
	head := SkipListNode{
		key:       "",
		value:     nil,
		tombstone: false,
		timestamp: 0,
		next:      make([]*SkipListNode, maxHeight),
	}

	sl := New(&head)
	sl.Insert("a", []byte("aaa"))
	sl.Insert("d", []byte("dddd"))
	sl.Insert("c", []byte("cccccc"))
	sl.Insert("b", []byte("cccccc"))
	sl.Insert("d", []byte("cccccc"))
	sl.Insert("d", []byte("cccccc"))
	sl.Insert("d", []byte("cccccc"))
	sl.Insert("d", []byte("cccccc"))
	sl.Insert("d", []byte("cccccc"))
	sl.Insert("e", []byte("cccccc"))
	sl.Insert("f", []byte("cccccc"))
	sl.Insert("g", []byte("cccccc"))
	sl.Insert("h", []byte("cccccc"))
	sl.Insert("j", []byte("cccccc"))
	sl.Insert("k", []byte("cccccc"))
	sl.Insert("l", []byte("cccccc"))
	sl.Insert("m", []byte("cccccc"))
	sl.Insert("n", []byte("cccccc"))
	sl.Insert("o", []byte("cccccc"))
	sl.Insert("o", []byte("cccccc"))
	sl.Insert("o", []byte("cccccc"))
	sl.Insert("o", []byte("cccccc"))
	sl.Insert("o", []byte("cccccc"))
	sl.Insert("p", []byte("cccccc"))
	sl.Insert("r", []byte("cccccc"))
	sl.Insert("s", []byte("cccccc"))
	sl.Insert("t", []byte("cccccc"))
	sl.Delete("m")
	sl.Draw()
	//println("Key free:", sl.IsKeyFree("a"))

	sl.PrintNodeByKey("e")

}
