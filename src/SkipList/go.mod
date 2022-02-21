module SkipList

go 1.17

require SSTable v0.0.0

require Bloomfilter v0.0.0

require Config v0.0.0

require MerkleTree v0.0.0

require github.com/spaolacci/murmur3 v1.1.0 // indirect

replace SSTable => ../SSTable

replace Bloomfilter => ../bloomfilter

replace Config => ../Config

replace MerkleTree => ../MerkleTree
