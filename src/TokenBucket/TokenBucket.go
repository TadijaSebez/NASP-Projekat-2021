package TokenBucket

import (
	"Config"
	"encoding/binary"
	"time"
)

type TokenBucket struct {
	maxTokens     uint8
	tokensLeft    uint8
	resetInterval int64
	nextResetTime int64
}

func CreateTokenBucket(config Config.Config) *TokenBucket {
	res := &TokenBucket{}
	res.maxTokens = config.TokenNumber
	res.tokensLeft = res.maxTokens
	res.resetInterval = int64(config.BucketReset)
	currentTime := time.Now().Unix()
	res.nextResetTime = currentTime + res.resetInterval
	return res
}

func (b *TokenBucket) CheckForReset() {
	currentTime := time.Now().Unix()
	if currentTime > b.nextResetTime {
		b.tokensLeft = b.maxTokens
		b.nextResetTime = currentTime + b.resetInterval
	}
}

func (b *TokenBucket) HasMoreTokens() bool {
	b.CheckForReset()
	return b.tokensLeft > 0
}

func (b *TokenBucket) GetTokensLeft() uint8 {
	return b.tokensLeft
}

func (b *TokenBucket) RemoveToken() {
	b.tokensLeft--
}

func (b *TokenBucket) ToBytes() []byte {
	bytes := make([]byte, 18)
	bytes[0] = b.maxTokens
	bytes[1] = b.tokensLeft
	binary.LittleEndian.PutUint64(bytes[2:], uint64(b.resetInterval))
	binary.LittleEndian.PutUint64(bytes[10:], uint64(b.nextResetTime))
	return bytes
}

func FromBytes(bytes []byte) *TokenBucket {
	res := &TokenBucket{}
	res.maxTokens = bytes[0]
	res.tokensLeft = bytes[1]
	res.resetInterval = int64(binary.LittleEndian.Uint64(bytes[2:10]))
	res.nextResetTime = int64(binary.LittleEndian.Uint64(bytes[10:18]))
	return res
}
