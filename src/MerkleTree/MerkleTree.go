package MerkleTree

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"

	_ "strconv"
	"strings"
)

type Hash [20]byte

type MerkleRoot struct {
	Root *Node
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
	Hash() Hash
}

type Blok []byte

func (b Blok) Hash() Hash {
	return hash(b[:])
}

func (n Node) Hash() Hash {
	var l, r [sha1.Size]byte
	l = n.right.Hash()
	r = n.right.Hash()
	return hash(append(l[:], r[:]...))
}

type PrazanBlok struct {
}

func (_ PrazanBlok) Hash() Hash {
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

func CreateMerkleTree(data [][]byte) MerkleRoot {
	merkleTreeInput := make([]Hashovano, 0)
	for _, bytes := range data {
		merkleTreeInput = append(merkleTreeInput, Hashovano(Blok(bytes)))
	}
	node := KreiranjeStabla(merkleTreeInput)[0].(Node)
	return MerkleRoot{Root: &node}
}

func SerializeTree(node Node, fn string) string {
	s := ""
	s += printNode(node, 0)
	err := ioutil.WriteFile(fn, []byte(s), 0666)
	if err != nil {
		log.Fatal(err)
	}
	return s
}

func printNode(node Node, level int) string {

	s := ""
	s += fmt.Sprintf("(%d) %s %s\n", level, strings.Repeat(" ", level), node.Hash())
	if l, ok := node.left.(Node); ok {
		s += printNode(l, level+1)
	} else if l, ok := node.left.(Blok); ok {
		s += fmt.Sprintf("(%d) %s %s\n", level+1, strings.Repeat(" ", level+1), l.Hash())
	}
	if r, ok := node.right.(Node); ok {
		s += printNode(r, level+1)
	} else if r, ok := node.right.(Blok); ok {
		s += fmt.Sprintf("(%d) %s %s\n", level+1, strings.Repeat(" ", level+1), r.Hash())
	}

	return s
}
