package TokenBucket

import (
	"time"
)

type Token struct{}

type TokenBucket struct {
	Kolicina int
	b        chan Token
	t        *time.Ticker
	s        chan Token
}

func NewTokenBucket(kolicina int) TokenBucket {
	b := make(chan Token, kolicina)
	return TokenBucket{
		Kolicina: kolicina,
		b:        b,
	}
}

func (tb *TokenBucket) Stop() {
	tb.s <- Token{}
	<-tb.s

	tb.drain()
}

func (tb *TokenBucket) IsEmpty() bool {
	select {
	case <-tb.b:
		return false
	default:
		return true
	}
}

func (tb *TokenBucket) fill() {
	for i := 0; i < tb.Kolicina; i++ {
		select {
		case tb.b <- Token{}:
		default:
		}
	}
}

func (tb *TokenBucket) drain() {
	for i := 0; i < tb.Kolicina; i++ {
		select {
		case <-tb.b:
		default:
		}
	}
}

func (tb *TokenBucket) Start() {
	tb.t = time.NewTicker(time.Second)
	tb.s = make(chan Token)

	tb.fill()

	go func() {
		defer close(tb.s)
		for {
			select {
			case <-tb.t.C:
				tb.fill()
			case <-tb.s:
				tb.t.Stop()
				return
			}
		}
	}()
}


