package LSMTree

import (
	"Bloomfilter"
	"Config"
	"SSTable"
)

func Merge(lsmLevel int, sstIndices []int) {
	numSsts := len(sstIndices)
	ssts := make([]SSTable.SSTable, 0)
	records := make([]SSTable.Record, 0)
	for i := 0; i < numSsts; i++ {
		sst := SSTable.OpenSSTable(sstIndices[i], lsmLevel)
		defer SSTable.CloseSSTable(sst)
		ssts = append(ssts, sst)
		record := SSTable.ReadRecord(sst.Data)
		records = append(records, record)
	}

	res := SSTable.SSTable{}

	index := 0
	for SSTable.Exists(SSTable.GetBaseName(index, lsmLevel+1) + "data") {
		index++
	}

	baseName := SSTable.GetBaseName(index, lsmLevel+1)
	res.Data = SSTable.CreateFile(baseName + "data")
	res.Index = SSTable.CreateFile(baseName + "index")
	res.IndexSummary = SSTable.CreateFile(baseName + "indexSummary")

	minKey := ""
	maxKey := ""
	for _, sst := range ssts {
		curMinKey := SSTable.ReadKey(sst.IndexSummary)
		curMaxKey := SSTable.ReadKey(sst.IndexSummary)

		if len(minKey) == 0 || minKey > curMinKey {
			minKey = curMinKey
		}

		if len(maxKey) == 0 || maxKey < curMaxKey {
			maxKey = curMaxKey
		}
	}

	SSTable.WriteKey(res.IndexSummary, minKey)
	SSTable.WriteKey(res.IndexSummary, maxKey)

	numRecords := 0

	for {
		minKey := ""
		maxTimestampRecord := SSTable.Record{}
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

		offset := SSTable.FileSize(res.Data)
		res.Data.Write(maxTimestampRecord.ToBytes())

		indexOffset := SSTable.FileSize(res.Index)
		SSTable.WriteKey(res.Index, maxTimestampRecord.Key)
		SSTable.WriteInt64(res.Index, offset)

		SSTable.WriteKey(res.IndexSummary, maxTimestampRecord.Key)
		SSTable.WriteInt64(res.IndexSummary, indexOffset)

		for i, record := range records {
			if len(record.Key) != 0 {
				if minKey == record.Key {
					records[i] = SSTable.ReadRecord(ssts[i].Data)
				}
			}
		}
	}

	res.BloomFilter = Bloomfilter.Constructor(numRecords, 0.01)
	res.Data.Seek(0, 0)
	for {
		record := SSTable.ReadRecord(res.Data)
		if len(record.Key) == 0 {
			break
		}
		res.BloomFilter.Add(record.Key)
	}
	res.BloomFilter.Serialize(baseName + "filter")

	// res.MerkleTree = ...
	SSTable.CloseSSTable(res)
}

func Compact(config Config.Config) {
	maxLsmLevel := int(config.LSMLevel)
	for lsmLevel := 1; lsmLevel < maxLsmLevel; lsmLevel++ {
		sstCount := 0
		for SSTable.Exists(SSTable.GetBaseName(sstCount, lsmLevel) + "data") {
			sstCount++
		}

		for sstCount >= 4 {
			Merge(lsmLevel, []int{sstCount - 4, sstCount - 3, sstCount - 2, sstCount - 1})
			for i := 1; i <= 4; i++ {
				SSTable.DeleteSSTable(sstCount-i, lsmLevel)
			}
			sstCount -= 4
		}
	}
}
