module LSMTree

go 1.17

require Config v0.0.0

require Bloomfilter v0.0.0

require SSTable v0.0.0

require (
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace Config => ../Config

replace Bloomfilter => ../bloomfilter

replace SSTable => ../SSTable
