module SkipList

go 1.17

require SSTable v0.0.0-00010101000000-000000000000

require github.com/spaolacci/murmur3 v1.1.0 // indirect

replace Config => ../Config

replace Bloomfilter => ../Bloomfilter

replace SSTable => ../SSTable
