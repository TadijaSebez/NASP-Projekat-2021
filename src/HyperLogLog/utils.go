package HyperLogLog

import (
	"github.com/spaolacci/murmur3"
	"log"
)

func CountTrailingZeros(value uint32, precision int) uint8 {
	for i := 0; i < 32-precision; i++ {
		if ((value >> i) & 1) == 1 {
			return uint8(i)
		}
	}
	return uint8(32 - precision)
}

func CreateHash(key string) uint32 {
	hashFunc := murmur3.New32WithSeed(4325)
	hashFunc.Reset()
	_, err := hashFunc.Write([]byte(key))
	if err != nil {
		log.Fatal(err)
	}
	return hashFunc.Sum32()
}
