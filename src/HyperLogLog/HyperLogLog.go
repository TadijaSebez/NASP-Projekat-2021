package HyperLogLog

import (
	"encoding/binary"
	"math"
)

type HyperLogLog struct {
	precision int
	size      int
	data      []uint8
}

func Create(precision int) *HyperLogLog {
	hll := new(HyperLogLog)
	hll.precision = precision
	hll.size = 1 << precision
	hll.data = make([]uint8, hll.size)
	return hll
}

func (hll *HyperLogLog) Add(hash uint32) {
	bucket := hash >> (32 - hll.precision)
	mask := (uint32(1) << (32 - hll.precision)) - 1
	trailingZeros := CountTrailingZeros(hash&mask, hll.precision)
	if hll.data[bucket] < trailingZeros {
		hll.data[bucket] = trailingZeros
	}
}

func (hll *HyperLogLog) AddKey(key string) {
	hash := CreateHash(key)
	hll.Add(hash)
}

func (hll *HyperLogLog) GetCardinality() uint64 {
	constant := 0.79402
	invSum := float64(0)
	for _, r := range hll.data {
		invSum += math.Pow(float64(2), -float64(r))
	}
	harmonicMean := float64(hll.size) / invSum
	cardinality := constant * float64(hll.size) * harmonicMean
	return uint64(cardinality)
}

func (hll *HyperLogLog) ToBytes() []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(bytes, uint32(hll.precision))
	binary.LittleEndian.PutUint32(bytes[4:], uint32(hll.size))
	bytes = append(bytes, hll.data...)
	return bytes
}

func FromBytes(bytes []byte) *HyperLogLog {
	hll := new(HyperLogLog)
	hll.precision = int(binary.LittleEndian.Uint32(bytes[:4]))
	hll.size = int(binary.LittleEndian.Uint32(bytes[4:8]))
	hll.data = bytes[8:]
	return hll
}
