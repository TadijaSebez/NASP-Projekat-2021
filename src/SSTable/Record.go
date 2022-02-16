package SSTable

type Record struct {
	Crc       uint32
	Timestamp uint64
	Tombstone bool
	Key       string
	Value     []byte
}

func (r Record) ToBytes() []byte {
	res := make([]byte, 0)
	res = PushInt32(res, r.Crc)
	res = PushInt64(res, r.Timestamp)
	res = PushBool(res, r.Tombstone)
	res = PushKey(res, r.Key)
	res = PushValue(res, r.Value)
	return res
}
