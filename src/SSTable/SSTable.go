package SSTable

import (
	"Bloomfilter"
	"Config"
	"os"
)

type SSTable struct {
	Data         *os.File
	Index        *os.File
	IndexSummary *os.File
	BloomFilter  Bloomfilter.BloomFilter
	// MerkleTree ?
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

	// res.MerkleTree = ...

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
	// DeleteFile(baseName + "metadata")
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
		if key > key2 {
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

func Merge(lsmLevel int, sstIndices []int) {
	numSsts := len(sstIndices)
	ssts := make([]SSTable, 0)
	records := make([]Record, 0)
	for i := 0; i < numSsts; i++ {
		sst := OpenSSTable(sstIndices[i], lsmLevel)
		defer CloseSSTable(sst)
		ssts = append(ssts, sst)
		record := ReadRecord(sst.Data)
		records = append(records, record)
	}

	res := SSTable{}

	index := 0
	for Exists(GetBaseName(index, lsmLevel+1) + "data") {
		index++
	}

	baseName := GetBaseName(index, lsmLevel+1)
	res.Data = CreateFile(baseName + "data")
	res.Index = CreateFile(baseName + "index")
	res.IndexSummary = CreateFile(baseName + "indexSummary")

	minKey := ""
	maxKey := ""
	for _, sst := range ssts {
		curMinKey := ReadKey(sst.IndexSummary)
		curMaxKey := ReadKey(sst.IndexSummary)

		if len(minKey) == 0 || minKey > curMinKey {
			minKey = curMinKey
		}

		if len(maxKey) == 0 || maxKey < curMaxKey {
			maxKey = curMaxKey
		}
	}

	WriteKey(res.IndexSummary, minKey)
	WriteKey(res.IndexSummary, maxKey)

	numRecords := 0

	for {
		minKey := ""
		maxTimestampRecord := Record{}
		for _, record := range records {
			if len(record.Key) != 0 {
				if len(minKey) == 0 || minKey > record.Key {
					minKey = record.Key
					maxTimestampRecord = record
				} else if minKey == record.Key {
					if maxTimestampRecord.Timestamp < record.Timestamp {
						maxTimestampRecord = record
					}
				}
			}
		}

		if len(minKey) == 0 {
			break
		}

		numRecords++

		offset := FileSize(res.Data)
		res.Data.Write(maxTimestampRecord.ToBytes())

		indexOffset := FileSize(res.Index)
		WriteKey(res.Index, maxTimestampRecord.Key)
		WriteInt64(res.Index, offset)

		WriteKey(res.IndexSummary, maxTimestampRecord.Key)
		WriteInt64(res.IndexSummary, indexOffset)

		for i, record := range records {
			if len(record.Key) != 0 {
				if minKey == record.Key {
					records[i] = ReadRecord(ssts[i].Data)
				}
			}
		}
	}

	res.BloomFilter = Bloomfilter.Constructor(numRecords, 0.01)
	res.Data.Seek(0, 0)
	for {
		record := ReadRecord(res.Data)
		if len(record.Key) == 0 {
			break
		}
		res.BloomFilter.Add(record.Key)
	}
	res.BloomFilter.Serialize(baseName + "filter")

	// res.MerkleTree = ...
	CloseSSTable(res)
}

func Compact(config Config.Config) {
	maxLsmLevel := int(config.LSMLevel)
	for lsmLevel := 1; lsmLevel < maxLsmLevel; lsmLevel++ {
		sstCount := 0
		for Exists(GetBaseName(sstCount, lsmLevel) + "data") {
			sstCount++
		}

		for sstCount >= 4 {
			Merge(lsmLevel, []int{sstCount - 4, sstCount - 3, sstCount - 2, sstCount - 1})
			for i := 1; i <= 4; i++ {
				DeleteSSTable(sstCount-i, lsmLevel)
			}
			sstCount -= 4
		}
	}
}
