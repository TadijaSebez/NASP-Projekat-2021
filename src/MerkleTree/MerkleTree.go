package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"

	_ "strconv"
	"strings"
)

type Hash [20]byte

type MerkleRoot struct {
	root *Node
}

type Node struct {
	left  Hashovano
	right Hashovano
}

func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

func hash(data []byte) Hash {
	return sha1.Sum(data)
}

type Hashovano interface {
	hash() Hash
}

type Blok string

func (b Blok) hash() Hash {
	return hash([]byte(b)[:])
}

func (n Node) hash() Hash {
	var l, r [sha1.Size]byte
	l = n.right.hash()
	r = n.right.hash()
	return hash(append(l[:], r[:]...))
}

type PrazanBlok struct {
}

func (_ PrazanBlok) hash() Hash {
	return [20]byte{}
}

func KreiranjeStabla(delovi []Hashovano) []Hashovano {
	var nodes []Hashovano
	var i int
	for i = 0; i < len(delovi); i += 2 {
		if i+1 < len(delovi) {
			nodes = append(nodes, Node{left: delovi[i], right: delovi[i+1]})
		} else {
			nodes = append(nodes, Node{left: delovi[i], right: PrazanBlok{}})
		}
	}
	if len(nodes) == 1 {
		return nodes
	} else {
		return KreiranjeStabla(nodes)
	}
}

func printTree(node Node) string {
	s := ""
	s += printNode(node, 0)
	err := ioutil.WriteFile("MerkleTree.txt", []byte(s), 0666)
	if err != nil {
		log.Fatal(err)
	}
	return s
}

func printNode(node Node, level int) string {

	s := ""
	fmt.Printf("(%d) %s %s\n", level, strings.Repeat(" ", level), node.hash())
	if l, ok := node.left.(Node); ok {
		printNode(l, level+1)
	} else if l, ok := node.left.(Blok); ok {
		s += fmt.Sprintf("(%d) %s %s (data: %s)\n", level+1, strings.Repeat(" ", level+1), l.hash(), l)
	}
	if r, ok := node.right.(Node); ok {
		printNode(r, level+1)
	} else if r, ok := node.right.(Blok); ok {
		s += fmt.Sprintf("(%d) %s %s (data: %s)\n", level+1, strings.Repeat(" ", level+1), r.hash(), r)
	}

	return s
}

func main() {
	s := printTree(KreiranjeStabla([]Hashovano{Blok("a"), Blok("b"), Blok("c"), Blok("d"), Blok("def"), Blok("f")})[0].(Node))
	fmt.Printf("%s", s)
}
