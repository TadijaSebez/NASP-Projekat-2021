module main

go 1.17

require SSTable v0.0.0

require Config v0.0.0

require Bloomfilter v0.0.0 // indirect

require (
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace SSTable => ../../src/SSTable

replace Config => ../../src/Config

replace Bloomfilter => ../../src/Bloomfilter
