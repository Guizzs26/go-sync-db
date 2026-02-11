package infra

import (
	"math/rand/v2"
	"sync"
	"time"
)

type Backoff struct {
	minDelay   time.Duration
	maxDelay   time.Duration
	multiplier float64
	current    time.Duration
	attempts   int
	mu         sync.Mutex
}

func NewBackoff(min, max time.Duration, mult float64) *Backoff {
	return &Backoff{
		minDelay:   min,
		maxDelay:   max,
		multiplier: mult,
		current:    min,
	}
}

func (b *Backoff) Next() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.attempts++

	jitterFactor := rand.Float64()*0.4 - 0.2
	jitter := time.Duration(jitterFactor * float64(b.current))
	wait := b.current + jitter

	if wait < b.minDelay {
		wait = b.minDelay
	}

	b.current = time.Duration(float64(b.current) * b.multiplier)
	if b.current > b.maxDelay {
		b.current = b.maxDelay
	}

	return wait
}

func (b *Backoff) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.current = b.minDelay
	b.attempts = 0
}

func (b *Backoff) Attempts() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.attempts
}
