package WAL

import (
"encoding/binary"
"github.com/edsrzf/mmap-go"
"hash/crc32"
"io/ioutil"
"log"
"os"
"strconv"
"time"
)


func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}


type WAL struct {
	segmentSize int64
	filesSlice []string
}


func (w *WAL) innit() {
	files, err := ioutil.ReadDir("./files")
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		w.filesSlice = append(w.filesSlice, f.Name())
	}
	if len(w.filesSlice) == 0{
		file, err2 := os.Create("./files/wal_1.log")
		if err2 != nil{
			panic(err)
		}
		file.Close()
		w.filesSlice = append(w.filesSlice, "wal_1.log" )
	}
}


func (w *WAL) insert(key string, value []byte, tmbStn string) bool{
	activeFile := w.filesSlice[len(w.filesSlice)-1]
	r, err := os.Stat("./files/" + activeFile)
	if err!= nil{
		panic(err)
	}
	if r.Size() > w.segmentSize{
		activeFile ="wal_"+ strconv.Itoa(len(w.filesSlice) + 1) + ".log"
		file, err2 := os.Create("./files/" + activeFile)
		if err2 != nil{
			panic(err)
		}
		file.Close()
		w.filesSlice = append(w.filesSlice, activeFile)
	}

	var keySize  = uint64(len([]byte(key)))
	var valueSize  = uint64(len(value))
	var now = time.Now()
	var timestamp = now.Unix()
	var crc = CRC32(value)

	fileBytes := make([]byte, 37)
	binary.BigEndian.PutUint32(fileBytes[:], crc)
	binary.BigEndian.PutUint64(fileBytes[4:], uint64(timestamp))
	var tombStone = []byte{0}
	if tmbStn == "d"{
		tombStone = []byte{1}
	}
	fileBytes[20] = tombStone[0]

	binary.BigEndian.PutUint64(fileBytes[21:], keySize)
	binary.BigEndian.PutUint64(fileBytes[29:], valueSize)
	fileBytes = append(fileBytes, []byte(key)...)
	fileBytes = append(fileBytes, value...)
	file, err := os.OpenFile("./files/"+ activeFile, os.O_RDWR, 0777)
	defer file.Close()
	if err != nil{
		panic(err.Error())
	}
	writeWall(file, fileBytes)
	return true
}


func writeWall(file *os.File, data []byte) error {
	currentLen, err := fileLen(file)
	if err != nil {
		return err
	}
	if err != nil { return err }
	mmapf, err := mmap.MapRegion(file, int(currentLen)+len(data), mmap.RDWR, 0, 0)
	if err != nil {
		return err
	}
	defer mmapf.Unmap()
	copy(mmapf[currentLen:], data)
	mmapf.Flush()
	return nil
}


func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}


func (w *WAL) read() map[string][]byte {
	retMap := make(map[string][]byte)
	for _, activeFil := range w.filesSlice {
		file, err := os.OpenFile("./files/" + activeFil, os.O_RDWR, 0777)
		defer file.Close()
		for {
			if err != nil {
				panic(err.Error())
			}
			crc := make([]byte, 4)
			file.Read(crc)

			c := binary.BigEndian.Uint32(crc)
			if c == 0 {
				break
			}
			tajm := make([]byte, 16)
			file.Read(tajm)

			tmbStone := make([]byte, 1)
			file.Read(tmbStone)

			keySiz := make([]byte, 8)
			file.Read(keySiz)
			n := binary.BigEndian.Uint64(keySiz)

			valueSiz := make([]byte, 8)
			file.Read(valueSiz)
			m := binary.BigEndian.Uint64(valueSiz)

			key := make([]byte, n)
			file.Read(key)
			sKey := string(key)

			value := make([]byte, m)
			file.Read(value)
			if (CRC32(value)) != c {
				continue
			}
			if tmbStone[0] == 0 {
				retMap[sKey] = value
			} else if tmbStone[0] == 1 {
				delete(retMap, sKey)
			}
		}
	}
	return retMap
}


func(w *WAL) deleteSegments(){
	lastSegment := w.filesSlice[len(w.filesSlice)-1]
	for _, seg := range w.filesSlice {
		if seg != lastSegment {
			err := os.Remove("./files/" + seg)

			if err != nil {
				panic(err)
			}
		}
	}
	err := os.Rename("./files/" + lastSegment, "./files/wal_1.log" )
	if err != nil {
		panic(err)
	}
	w.filesSlice = nil
	w.filesSlice = append(w.filesSlice, "wal_1.log")
}
