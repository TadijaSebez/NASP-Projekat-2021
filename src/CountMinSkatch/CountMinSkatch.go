package CountMinSkatch

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
)

type Sketch struct {
	numRows   uint32
	numCols   uint32
	sk        [][]uint32
	rowCounts []uint32
}

func NewSketch(w, d int) *Sketch {
	if d < 1 || w < 1 {
		panic("Dimensions must be positive")
	}

	s := &Sketch{}

	s.numRows = uint32(d)
	s.numCols = uint32(w)

	s.sk = make([][]uint32, d)
	for i := 0; i < d; i++ {
		s.sk[i] = make([]uint32, w)
	}

	s.rowCounts = make([]uint32, d)

	return s
}

func (s *Sketch) ToBytes() []byte {
	numBytes := 8 + s.numRows*s.numCols*4 + s.numRows*4
	bytes := make([]byte, numBytes)
	binary.LittleEndian.PutUint32(bytes[0:4], s.numRows)
	binary.LittleEndian.PutUint32(bytes[4:8], s.numCols)
	offset := 8
	for i := uint32(0); i < s.numRows; i++ {
		for j := uint32(0); j < s.numCols; j++ {
			binary.LittleEndian.PutUint32(bytes[offset:offset+4], s.sk[i][j])
			offset += 4
		}
	}
	for i := uint32(0); i < s.numRows; i++ {
		binary.LittleEndian.PutUint32(bytes[offset:offset+4], s.rowCounts[i])
		offset += 4
	}
	return bytes
}

func FromBytes(bytes []byte) *Sketch {
	s := &Sketch{}

	s.numRows = binary.LittleEndian.Uint32(bytes[0:4])
	s.numCols = binary.LittleEndian.Uint32(bytes[4:8])

	offset := 8
	s.sk = make([][]uint32, s.numRows)
	for i := uint32(0); i < s.numRows; i++ {
		s.sk[i] = make([]uint32, s.numCols)
		for j := uint32(0); j < s.numCols; j++ {
			s.sk[i][j] = binary.LittleEndian.Uint32(bytes[offset : offset+4])
			offset += 4
		}
	}

	s.rowCounts = make([]uint32, s.numRows)
	for i := uint32(0); i < s.numRows; i++ {
		s.rowCounts[i] = binary.LittleEndian.Uint32(bytes[offset : offset+4])
		offset += 4
	}

	return s
}

func (s Sketch) String() string {
	return fmt.Sprintf("{Sketch %dx%d}", len(s.sk[0]), len(s.sk))
}

func hashn(s string) (h1, h2 uint32) {

	fnv1a := fnv.New32a()
	fnv1a.Write([]byte(s))
	h1 = fnv1a.Sum32()

	h2 = uint32(0)
	for _, c := range s {
		h2 += uint32(c)
		h2 += h2 << 10
		h2 ^= h2 >> 6
	}
	h2 += (h2 << 3)
	h2 ^= (h2 >> 11)
	h2 += (h2 << 15)

	return h1, h2
}

func (s *Sketch) Reset() {

	for _, w := range s.sk {
		for i := range w {
			w[i] = 0
		}
	}

	for i := range s.rowCounts {
		s.rowCounts[i] = 0
	}
}

func (s *Sketch) Add(h string, count uint32) (val uint32) {
	w := len(s.sk[0])
	d := len(s.sk)
	val = math.MaxUint32
	h1, h2 := hashn(h)
	for i := 0; i < d; i++ {
		pos := (h1 + uint32(i)*h2) % uint32(w)
		s.rowCounts[i] += count
		v := s.sk[i][pos] + count
		s.sk[i][pos] = v
		if v < val {
			val = v
		}
	}
	return val
}

func (s *Sketch) Del(h string, count uint32) (val uint32) {
	w := len(s.sk[0])
	d := len(s.sk)
	val = math.MaxUint32
	h1, h2 := hashn(h)
	for i := 0; i < d; i++ {
		pos := (h1 + uint32(i)*h2) % uint32(w)
		s.rowCounts[i] -= count
		v := s.sk[i][pos] - count
		if v > s.sk[i][pos] {
			v = 0
		}
		s.sk[i][pos] = v
		if v < val {
			val = v
		}
	}
	return val
}

func (s *Sketch) Increment(h string) (val uint32) {
	return s.Add(h, 1)
}

func (s *Sketch) ConservativeIncrement(h string) (val uint32) {
	return s.ConservativeAdd(h, 1)
}

func (s *Sketch) ConservativeAdd(h string, count uint32) (val uint32) {
	w := len(s.sk[0])
	d := len(s.sk)
	h1, h2 := hashn(h)
	val = math.MaxUint32
	for i := 0; i < d; i++ {
		pos := (h1 + uint32(i)*h2) % uint32(w)

		v := s.sk[i][pos]
		if v < val {
			val = v
		}
	}

	val += count

	for i := 0; i < d; i++ {
		pos := (h1 + uint32(i)*h2) % uint32(w)
		v := s.sk[i][pos]
		if v < val {
			s.rowCounts[i] += (val - s.sk[i][pos])
			s.sk[i][pos] = val
		}
	}
	return val
}

func (s Sketch) Count(h string) uint32 {
	min := uint32(math.MaxUint32)
	w := len(s.sk[0])
	d := len(s.sk)

	h1, h2 := hashn(h)
	for i := 0; i < d; i++ {
		pos := (h1 + uint32(i)*h2) % uint32(w)

		v := s.sk[i][pos]
		if v < min {
			min = v
		}
	}
	return min
}

func (s Sketch) Values(h string) []uint32 {
	w := len(s.sk[0])
	d := len(s.sk)

	vals := make([]uint32, d)

	h1, h2 := hashn(h)
	for i := 0; i < d; i++ {
		pos := (h1 + uint32(i)*h2) % uint32(w)

		vals[i] = s.sk[i][pos]
	}

	return vals
}

func (s Sketch) CountMeanMin(h string) uint32 {
	min := uint32(math.MaxUint32)
	w := len(s.sk[0])
	d := len(s.sk)
	residues := make([]float64, d)
	h1, h2 := hashn(h)
	for i := 0; i < d; i++ {
		pos := (h1 + uint32(i)*h2) % uint32(w)
		v := s.sk[i][pos]
		noise := float64(s.rowCounts[i]-s.sk[i][pos]) / float64(w-1)
		residues[i] = float64(v) - noise
		if residues[i] < 0 {
			residues[i] = 0
		}
		if v < min {
			min = v
		}
	}

	sort.Float64s(residues)
	var median uint32
	if d%2 == 1 {
		median = uint32(residues[(d+1)/2])
	} else {
		x := uint32(residues[d/2])
		y := uint32(residues[d/2+1])
		median = (x & y) + (x^y)/2
	}

	if min < median {
		return min
	}

	return median
}

func (s *Sketch) Merge(from *Sketch) {
	if len(s.sk) != len(from.sk) || len(s.sk[0]) != len(from.sk[0]) {
		panic("Can't merge different sketches with different dimensions")
	}

	for i, l := range from.sk {
		for j, v := range l {
			s.sk[i][j] += v
		}
	}
}

func (s *Sketch) Clone() *Sketch {

	w := len(s.sk[0])
	d := len(s.sk)

	clone := NewSketch(w, d)

	for i, l := range s.sk {
		copy(clone.sk[i], l)
	}

	copy(clone.rowCounts, s.rowCounts)

	return clone
}

func (s *Sketch) Compress() {
	w := len(s.sk[0])

	if w&(w-1) != 0 {
		panic("width must be a power of two")
	}

	neww := w / 2

	for i, l := range s.sk {

		row := make([]uint32, neww)
		for j := 0; j < neww; j++ {
			row[j] = l[j] + l[j+neww]
		}
		s.sk[i] = row
	}
}
