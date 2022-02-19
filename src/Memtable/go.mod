module Memtable

go 1.17

require Config v0.0.0

require SkipList v0.0.0

require gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect

replace SkipList => ../SkipList

replace Config => ../Config

replace Bloomfilter => ../Bloomfilter

replace SSTable => ../SSTable
