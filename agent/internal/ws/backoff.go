package ws

import (
	"math/rand/v2"
	"time"
)

type Backoff struct {
	Base time.Duration
	Max  time.Duration
	rand func(time.Duration) time.Duration
}

func NewBackoff(base, max time.Duration) Backoff {
	return Backoff{
		Base: base,
		Max:  max,
		rand: func(max time.Duration) time.Duration {
			if max <= 0 {
				return 0
			}
			return time.Duration(rand.Int64N(int64(max)))
		},
	}
}

func (b *Backoff) Delay(attempt int) time.Duration {
	if b.Base <= 0 {
		b.Base = time.Second
	}
	if b.Max <= 0 {
		b.Max = time.Minute
	}
	if b.rand == nil {
		b.rand = func(max time.Duration) time.Duration {
			if max <= 0 {
				return 0
			}
			return time.Duration(rand.Int64N(int64(max)))
		}
	}
	if attempt < 0 {
		attempt = 0
	}

	delay := b.Base
	for i := 0; i < attempt; i++ {
		if delay >= b.Max/2 {
			delay = b.Max
			break
		}
		next := delay * 2
		if next < delay || next >= b.Max {
			delay = b.Max
			break
		}
		delay = next
	}
	jitter := b.rand(delay / 2)
	delay = delay/2 + jitter
	if delay > b.Max {
		return b.Max
	}
	return delay
}
