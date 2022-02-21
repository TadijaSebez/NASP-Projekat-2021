package SSTable

import (
	"Bloomfilter"
	"Config"
	"MerkleTree"
	"os"
)

type SSTable struct {
	Data         *os.File
	Index        *os.File
	IndexSummary *os.File
	BloomFilter  Bloomfilter.BloomFilter
	MerkleTree   MerkleTree.MerkleRoot
}

func CloseSSTable(sst SSTable) {
	sst.Data.Close()
	sst.Index.Close()
	sst.IndexSummary.Close()
}

func CreateSSTable(data []Record) {
	var res = SSTable{}

	index := 0
	for Exists(GetBaseName(index, 1) + "data") {
		index++
	}

	baseName := GetBaseName(index, 1)
	res.Data = CreateFile(baseName + "data")
	res.Index = CreateFile(baseName + "index")
	res.IndexSummary = CreateFile(baseName + "indexSummary")
	res.BloomFilter = Bloomfilter.Constructor(len(data), 0.01)

	minKey := data[0].Key
	maxKey := data[len(data)-1].Key
	WriteKey(res.IndexSummary, minKey)
	WriteKey(res.IndexSummary, maxKey)

	for _, d := range data {
		offset := FileSize(res.Data)
		res.Data.Write(d.ToBytes())

		indexOffset := FileSize(res.Index)
		WriteKey(res.Index, d.Key)
		WriteInt64(res.Index, offset)

		WriteKey(res.IndexSummary, d.Key)
		WriteInt64(res.IndexSummary, indexOffset)

		res.BloomFilter.Add(d.Key)
	}

	res.BloomFilter.Serialize(baseName + "filter")

	bytes := make([][]byte, 0)
	for _, d := range data {
		bytes = append(bytes, d.ToBytes())
	}
	res.MerkleTree = MerkleTree.CreateMerkleTree(bytes)
	MerkleTree.SerializeTree(*res.MerkleTree.Root, baseName+"metadata.txt")

	CloseSSTable(res)
}

func OpenSSTable(index int, lsmLevel int) SSTable {
	sst := SSTable{}
	baseName := GetBaseName(index, lsmLevel)
	sst.Data = OpenFile(baseName + "data")
	sst.Index = OpenFile(baseName + "index")
	sst.IndexSummary = OpenFile(baseName + "indexSummary")
	sst.BloomFilter = Bloomfilter.DeserializeBloomFilter(baseName + "filter")
	return sst
}

func DeleteSSTable(index int, lsmLevel int) {
	baseName := GetBaseName(index, lsmLevel)
	DeleteFile(baseName + "data")
	DeleteFile(baseName + "index")
	DeleteFile(baseName + "indexSummary")
	DeleteFile(baseName + "filter")
	DeleteFile(baseName + "metadata.txt")
}

func Find(index int, lsmLevel int, key string) (record Record, found bool) {
	table := OpenSSTable(index, lsmLevel)
	defer CloseSSTable(table)

	if !table.BloomFilter.Search(key) {
		found = false
		return
	}

	minKey := ReadKey(table.IndexSummary)
	maxKey := ReadKey(table.IndexSummary)
	if minKey > key || maxKey < key {
		found = false
		return
	}

	for {
		key2 := ReadKey(table.IndexSummary)
		if key2 == "" {
			break
		}
		offset, _ := ReadInt64(table.IndexSummary)
		if key < key2 {
			found = false
			return
		}
		if key == key2 {
			table.Index.Seek(int64(offset), 0)
			ReadKey(table.Index)
			dataOffset, _ := ReadInt64(table.Index)

			table.Data.Seek(int64(dataOffset), 0)
			record = ReadRecord(table.Data)
			found = true
			return
		}
	}

	found = false
	return
}

func SearchSSTables(key string, config Config.Config) (Record, bool) {
	maxLsmLevel := int(config.LSMLevel)
	answer := Record{}
	foundAnswer := false
	for lsmLevel := 1; lsmLevel <= maxLsmLevel; lsmLevel++ {
		for index := 0; Exists(GetBaseName(index, lsmLevel) + "data"); index++ {
			record, found := Find(index, lsmLevel, key)
			if found {
				if !foundAnswer || record.Timestamp > answer.Timestamp {
					answer = record
					foundAnswer = true
				}
			}
		}
	}
	return answer, foundAnswer
}
