module main

go 1.17

require WAL v0.0.0

require Config v0.0.0

require Memtable v0.0.0

require SkipList v0.0.0

require SSTable v0.0.0

require Bloomfilter v0.0.0 // indirect

require LRUCache v0.0.0

require LSMTree v0.0.0

require HyperLogLog v0.0.0

require MerkleTree v0.0.0

require TokenBucket v0.0.0

require CountMinSkatch v0.0.0

require (
	github.com/edsrzf/mmap-go v1.0.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	golang.org/x/sys v0.0.0-20211117180635-dee7805ff2e1 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace WAL => ./WAL

replace Config => ./Config

replace Memtable => ./Memtable

replace SkipList => ./SkipList

replace SSTable => ./SSTable

replace Bloomfilter => ./bloomfilter

replace LRUCache => ./LRUCache

replace LSMTree => ./LSMTree

replace HyperLogLog => ./HyperLogLog

replace MerkleTree => ./MerkleTree

replace TokenBucket => ./TokenBucket

replace CountMinSkatch => ./CountMinSkatch
