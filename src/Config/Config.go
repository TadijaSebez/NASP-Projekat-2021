package Config

import (
	"errors"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"os"
)

type Config struct {
	WalSize       uint64 `yaml:"wal_size"`
	MemtableSize  uint64 `yaml:"memtable_size"`
	LSMLevel      uint8  `yaml:"LSM_level"`
	CacheElements uint8  `yaml:"cache_elements"`
	BucketReset   uint8  `yaml:"bucket_reset"`
	TokenNumber   uint8  `yaml:"token_number"`
}

func (config *Config) Innit() {
	if _, err := os.Stat("config.yaml"); errors.Is(err, os.ErrNotExist) {
		config.WalSize = 200
		config.MemtableSize = 1000
		config.LSMLevel = 3
		config.CacheElements = 20
		config.BucketReset = 5
		config.TokenNumber = 4
	} else {
		configData, err := ioutil.ReadFile("config.yaml")
		if err != nil {
			log.Fatal(err)
		}
		yaml.Unmarshal(configData, &config)
		if config.WalSize == 0 {
			config.WalSize = 200
		}
		if config.MemtableSize == 0 {
			config.MemtableSize = 1000
		}
		if config.LSMLevel == 0 {
			config.LSMLevel = 5
		}
		if config.CacheElements == 0 {
			config.CacheElements = 20
		}
		if config.BucketReset == 0 {
			config.BucketReset = 2
		}
		if config.TokenNumber == 0 {
			config.TokenNumber = 4
		}
	}
}
