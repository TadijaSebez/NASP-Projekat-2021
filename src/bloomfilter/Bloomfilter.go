package Bloomfilter

import (
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"log"
	"math"
	"os"
	"time"
)

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func CreateHashFunctions(k uint,t uint) ([]hash.Hash32, uint) {
	h := []hash.Hash32{}
	if t == 0 {
		t = uint(time.Now().Unix())
	}
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(t+i)))
	}
	return h, t
}

type BloomFilter struct {
	M, K, T uint
	H []hash.Hash32
	BitSet []byte
}
func Constructor(expectedElements int, falsePositiveRate float64) BloomFilter{
	b := BloomFilter{}
	b.M = CalculateM(expectedElements, falsePositiveRate)
	b.K = CalculateK(expectedElements, b.M)
	b.H, b.T = CreateHashFunctions(b.K, 0)
	b.BitSet = make([]byte, b.M, b.M)
	return b
}
func (b *BloomFilter) Add(element string){
	for _, h := range b.H{
		h.Reset()
		_, err := h.Write([]byte(element))
		if err != nil {
			log.Fatal(err)
		}
		index := h.Sum32() % uint32(b.M)
		b.BitSet[index] = 1
	}
}
func (b *BloomFilter) Search(element string) bool {
	for _, h := range b.H{
		h.Reset()
		_, err := h.Write([]byte(element))
		if err != nil {
			log.Fatal(err)
		}
		index := h.Sum32() % uint32(b.M)
		if b.BitSet[index] == 0 {
			return false
		}

	}
	return true
}
func (b *BloomFilter) Serialize(path string) {
	b.H = nil
	file, err := os.Create(path)
	if err != nil{
		fmt.Println(err)
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(b)
	if err != nil{
		fmt.Println(err)
	}
	err = file.Close()
	if err != nil{
		fmt.Println(err)
	}
}
func DeserializeBloomFilter(path string) BloomFilter{
	file, err := os.OpenFile(path, os.O_RDWR, 0777)
	if err != nil{
		fmt.Println(err)
	}
	b := BloomFilter{}
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&b)
	if err != nil{
		fmt.Println(err)
	}
	b.H, _ = CreateHashFunctions(b.K, b.T)
	err = file.Close()
	if err != nil{
		fmt.Println(err)
	}
	return b
}
