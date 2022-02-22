package main

import (
	"Config"
	"CountMinSkatch"
	"HyperLogLog"
	"LRUCache"
	"LSMTree"
	"Memtable"
	"SSTable"
	"SkipList"
	"TokenBucket"
	"WAL"
	"fmt"
	"os"
	"strconv"
)

type KVEngine struct {
	config   Config.Config
	wal      *WAL.WAL
	memtable *Memtable.Memtable
	cache    *LRUCache.LRUCache
}

func (engine *KVEngine) Put(key string, value []byte) bool {
	succeded := engine.wal.Insert(key, value, "i")
	if !succeded {
		return false
	}
	createdSSTable := engine.memtable.Insert(key, value, false)
	if createdSSTable {
		engine.memtable = CreateMemtable(engine.config)
		engine.wal.DeleteSegments()
	}
	return true
}

func (engine *KVEngine) Get(key string) []byte {
	found, value := engine.memtable.Get(key)
	if found {
		engine.cache.Put(key, value)
		return value
	}
	found, value = engine.cache.Get(key)
	if found {
		return value
	}
	record, found := SSTable.SearchSSTables(key, engine.config)
	if found {
		value = record.Value
		if record.Tombstone {
			value = nil
		}
		engine.cache.Put(key, value)
		return value
	}
	engine.cache.Put(key, nil)
	return nil
}

func (engine *KVEngine) Delete(key string) bool {
	succeded := engine.wal.Insert(key, make([]byte, 0), "d")
	if !succeded {
		return false
	}
	createdSSTable := engine.memtable.Insert(key, make([]byte, 0), true)
	if createdSSTable {
		engine.wal.DeleteSegments()
	}
	return true
}

func (engine *KVEngine) RestoreFromWAL() {
	data := engine.wal.Read()
	for key, value := range data {
		engine.memtable.Insert(key, value, false)
	}
}

func (engine *KVEngine) Compact() {
	LSMTree.Compact(engine.config)
}

func CreateMemtable(config Config.Config) *Memtable.Memtable {
	head := SkipList.SkipListNode{
		Key:       "",
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
		Next:      make([]*SkipList.SkipListNode, SkipList.MaxHeight),
	}
	sl := SkipList.New(&head)
	memtable := Memtable.New(sl, config)
	return memtable
}

func CreateKVEngine() *KVEngine {
	engine := &KVEngine{}
	config := Config.Config{}
	config.Innit()
	engine.config = config
	wal := &WAL.WAL{SegmentSize: int64(config.WalSize)}
	os.MkdirAll("./data/WAL", 0777)
	wal.Innit()
	engine.wal = wal
	head := SkipList.SkipListNode{
		Key:       "",
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
		Next:      make([]*SkipList.SkipListNode, SkipList.MaxHeight),
	}
	sl := SkipList.New(&head)
	memtable := Memtable.New(sl, config)
	engine.memtable = memtable
	cache := LRUCache.Constructor(int(config.CacheElements))
	engine.cache = &cache
	return engine
}

type Engine struct {
	kvEngine *KVEngine
	username string
}

func (engine *Engine) PutHLL(hllKey string, key string) {
	bytes := engine.Get("__HYPER_LOG_LOG_" + hllKey)
	var hll *HyperLogLog.HyperLogLog
	if bytes == nil {
		hll = HyperLogLog.Create(4)
	} else {
		hll = HyperLogLog.FromBytes(bytes)
	}
	hll.AddKey(key)
	bytes = hll.ToBytes()
	engine.Put("__HYPER_LOG_LOG_"+hllKey, bytes)
}

func (engine *Engine) GetHLL(hllKey string) *HyperLogLog.HyperLogLog {
	bytes := engine.Get("__HYPER_LOG_LOG_" + hllKey)
	var hll *HyperLogLog.HyperLogLog
	if bytes == nil {
		hll = nil
	} else {
		hll = HyperLogLog.FromBytes(bytes)
	}
	return hll
}

func (engine *Engine) DeleteHLL(hllKey string) bool {
	return engine.kvEngine.Delete("__HYPER_LOG_LOG_" + hllKey)
}

func (engine *Engine) PutCMS(cmsKey string, key string) {
	bytes := engine.Get("__COUNT_MIN_SKETCH_" + cmsKey)
	var cms *CountMinSkatch.Sketch
	if bytes == nil {
		cms = CountMinSkatch.NewSketch(40, 40)
	} else {
		cms = CountMinSkatch.FromBytes(bytes)
	}
	cms.Add(key, 1)
	bytes = cms.ToBytes()
	engine.Put("__COUNT_MIN_SKETCH_"+cmsKey, bytes)
}

func (engine *Engine) GetCMS(cmsKey string) *CountMinSkatch.Sketch {
	bytes := engine.Get("__COUNT_MIN_SKETCH_" + cmsKey)
	var cms *CountMinSkatch.Sketch
	if bytes == nil {
		cms = nil
	} else {
		cms = CountMinSkatch.FromBytes(bytes)
	}
	return cms
}

func (engine *Engine) DeleteCMS(cmsKey string) bool {
	return engine.kvEngine.Delete(cmsKey)
}

func (engine *Engine) CheckTokenBucket() bool {
	bytes := engine.kvEngine.Get("__TOKEN_BUCKET_" + engine.username)
	var tb *TokenBucket.TokenBucket
	if bytes == nil {
		tb = TokenBucket.CreateTokenBucket(engine.kvEngine.config)
	} else {
		tb = TokenBucket.FromBytes(bytes)
	}
	if tb.HasMoreTokens() {
		tb.RemoveToken()
		engine.kvEngine.Put("__TOKEN_BUCKET_"+engine.username, tb.ToBytes())
		return true
	} else {
		return false
	}
}

func (engine *Engine) Put(key string, value []byte) bool {
	if !engine.CheckTokenBucket() {
		return false
	}
	return engine.kvEngine.Put(key, value)
}

func (engine *Engine) Get(key string) []byte {
	if !engine.CheckTokenBucket() {
		return nil
	}
	return engine.kvEngine.Get(key)
}

func (engine *Engine) Delete(key string) bool {
	if !engine.CheckTokenBucket() {
		return false
	}
	return engine.kvEngine.Delete(key)
}

func (engine *Engine) Flush() {
	engine.kvEngine.memtable.Flush()
	engine.kvEngine.wal.DeleteSegments()
}

func CreateEngine(username string) *Engine {
	return &Engine{kvEngine: CreateKVEngine(), username: username}
}

func testKVEngine() {
	engine := CreateKVEngine()
	for i := 0; i < 100; i++ {
		key := "kljuc_" + strconv.Itoa(i)
		engine.Put(key, []byte{1, 2, 3, 4, 5})
	}
	fmt.Println(engine.Get("kljuc_14"))
	fmt.Println(engine.Get("kljuc_98"))
	engine.memtable = CreateMemtable(engine.config)
	engine.RestoreFromWAL()
	for i := 0; i < 100; i++ {
		key := "novi_kljuc_" + strconv.Itoa(i)
		engine.Put(key, []byte{0, 34, 1})
	}
	fmt.Println(engine.Get("kljuc_99"))
	fmt.Println(engine.Get("kljuc_99"))
	engine.Compact()
	fmt.Println(engine.Get("novi_kljuc_50"))
}

func testHLL() {
	engine := CreateEngine("user")
	engine.kvEngine.RestoreFromWAL()
	hllVal := HyperLogLog.Create(4)
	for i := 0; i < 100; i++ {
		key := "kljuc_" + strconv.Itoa(i)
		engine.PutHLL("myHLL", key)
		hllVal.AddKey(key)
		//hll := engine.GetHLL("myHLL")
		//fmt.Println(hll.GetCardinality())
	}
	hll := engine.GetHLL("myHLL")
	//fmt.Println(hll)
	fmt.Println(hll.GetCardinality())
	fmt.Println(hllVal.GetCardinality())
	engine.Flush()
}

func testCMS() {
	engine := CreateEngine("user2")
	cmsVal := CountMinSkatch.NewSketch(40, 40)
	for i := 0; i < 100; i++ {
		key := "kljuc_" + strconv.Itoa(i%10)
		engine.PutCMS("myCMS", key)
		cmsVal.Add(key, 1)
	}
	cms := engine.GetCMS("myCMS")
	fmt.Println(cms.Count("kljuc_5"))
	fmt.Println(cmsVal.Count("kljuc_5"))
	engine.Flush()
}

func main() {
	//testKVEngine()
	//testHLL()
	//testCMS()
	engine := CreateEngine("user3")
	engine.kvEngine.RestoreFromWAL()
	fmt.Println(engine.Get("A"))
	engine.Put("A", []byte{1, 2})
	fmt.Println(engine.Get("A"))
}
