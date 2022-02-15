package main

import (
	"HyperLogLog"
	"fmt"
	"math/rand"
)

func main() {
	arr := []int{5, 10, 15, 200, 1000, 10000, 100000, 10000000}
	for _, n := range arr {
		hll := HyperLogLog.Create(4)
		for i := 0; i < n; i++ {
			hll.Add(rand.Uint32())
		}
		fmt.Println("Real size:", n, ", estimated size:", hll.GetCardinality())
		bytes := hll.ToBytes()
		hll = HyperLogLog.FromBytes(bytes)
		for i := 0; i < n; i++ {
			hll.Add(rand.Uint32())
		}
		fmt.Println("Real size:", 2*n, ", estimated size:", hll.GetCardinality())
	}
}
