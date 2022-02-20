package main

import (
	"Config"
	"LSMTree"
	"SSTable"
	"fmt"
)

func main() {
	config := Config.Config{}
	config.LSMLevel = 4
	LSMTree.Compact(config)
	data := make([]SSTable.Record, 0)
	data = append(data, SSTable.Record{0, 0, false, "A", make([]byte, 0)})
	SSTable.CreateSSTable(data)
	record, found := SSTable.SearchSSTables("A", config)
	if found {
		fmt.Printf("Found key %s\n", record.Key)
	} else {
		fmt.Printf("No key\n")
	}
	record, found = SSTable.SearchSSTables("B", config)
	if found {
		fmt.Printf("Found key %s\n", record.Key)
	} else {
		fmt.Printf("No key\n")
	}
}
