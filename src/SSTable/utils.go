package SSTable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func GetBaseName(index int, lsmLevel int) string {
	return fmt.Sprintf("data/SSTable/LSMLevel%02d_SSTable%06d_", lsmLevel, index)
}

func OpenFile(fn string) *os.File {
	fp, err := os.Open(fn)
	check(err)
	return fp
}

func CreateFile(fn string) *os.File {
	err := os.MkdirAll(filepath.Dir(fn), 0777)
	fp, err := os.Create(fn)
	check(err)
	return fp
}

func DeleteFile(fn string) {
	err := os.Remove(fn)
	check(err)
}

func FileSize(fp *os.File) uint64 {
	fi, err := fp.Stat()
	check(err)
	return uint64(fi.Size())
}

func Exists(fn string) bool {
	_, err := os.Stat(fn)
	return err == nil
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func PushInt32(buffer []byte, value uint32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, value)
	return append(buffer, bytes...)
}

func PushInt64(buffer []byte, value uint64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, value)
	return append(buffer, bytes...)
}

func WriteInt64(fp *os.File, value uint64) {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, value)
	fp.Write(bytes)
}

func ReadInt64(fp *os.File) (uint64, error) {
	bytes := make([]byte, 8)
	_, err := fp.Read(bytes)
	if err == io.EOF {
		return 0, err
	} else {
		check(err)
	}
	return binary.LittleEndian.Uint64(bytes), nil
}

func ReadInt32(fp *os.File) (uint32, error) {
	bytes := make([]byte, 4)
	_, err := fp.Read(bytes)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(bytes), nil
}

func ReadBool(fp *os.File) bool {
	bytes := make([]byte, 1)
	_, err := fp.Read(bytes)
	check(err)
	return bytes[0] == 1
}

func PushBool(buffer []byte, value bool) []byte {
	if value {
		return append(buffer, 1)
	}
	return append(buffer, 0)
}

func PushString(buffer []byte, value string) []byte {
	bytes := []byte(value)
	return append(buffer, bytes...)
}

func PushKey(buffer []byte, key string) []byte {
	bytes := make([]byte, 0)
	bytes = PushInt64(bytes, uint64(len(key)))
	bytes = PushString(bytes, key)
	return append(buffer, bytes...)
}

func WriteKey(fp *os.File, key string) {
	bytes := make([]byte, 0)
	bytes = PushKey(bytes, key)
	fp.Write(bytes)
}

func ReadKey(fp *os.File) string {
	len, err := ReadInt64(fp)
	if err == io.EOF {
		return ""
	}
	bytes := make([]byte, len)
	_, err = fp.Read(bytes)
	check(err)
	return string(bytes)
}

func ReadValue(fp *os.File) []byte {
	len, err := ReadInt64(fp)
	check(err)
	bytes := make([]byte, len)
	_, err = fp.Read(bytes)
	check(err)
	return bytes
}

func PushValue(buffer []byte, value []byte) []byte {
	bytes := make([]byte, 0)
	bytes = PushInt64(bytes, uint64(len(value)))
	bytes = append(bytes, value...)
	return append(buffer, bytes...)
}

func ReadRecord(fp *os.File) Record {
	res := Record{}
	var err error
	res.Crc, err = ReadInt32(fp)
	if err != nil {
		return Record{}
	}
	res.Timestamp, _ = ReadInt64(fp)
	res.Tombstone = ReadBool(fp)
	res.Key = ReadKey(fp)
	res.Value = ReadValue(fp)
	return res
}
