package backoff

import (
	"context"
	"time"
)

// SleepContext blocks for delay or until ctx is done. It returns false when the
// context is cancelled before the delay elapses.
func SleepContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// ExponentialDelay returns a bounded exponential backoff delay.
func ExponentialDelay(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	if max <= 0 {
		max = 2 * time.Second
	}
	if base > max {
		return max
	}
	if attempt < 0 {
		attempt = 0
	}
	delay := base
	for i := 0; i < attempt; i++ {
		next := delay * 2
		if next <= 0 || next > max {
			return max
		}
		delay = next
	}
	if delay > max {
		return max
	}
	return delay
}
