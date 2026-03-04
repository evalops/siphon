package poller

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v5"
	"golang.org/x/time/rate"
)

type PollFn func(context.Context) error

type Poller struct {
	Provider    string
	Interval    time.Duration
	RateLimiter *rate.Limiter
	Backoff     backoff.BackOff
	Run         PollFn
}

func (p *Poller) Start(ctx context.Context) {
	if p == nil || p.Run == nil {
		return
	}
	if p.Interval <= 0 {
		p.Interval = time.Minute
	}
	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.RateLimiter != nil {
				if err := p.RateLimiter.Wait(ctx); err != nil {
					continue
				}
			}
			if err := p.Run(ctx); err != nil {
				if p.Backoff == nil {
					continue
				}
				wait := p.Backoff.NextBackOff()
				if wait > 0 {
					t := time.NewTimer(wait)
					select {
					case <-ctx.Done():
						t.Stop()
						return
					case <-t.C:
					}
				}
			} else if p.Backoff != nil {
				p.Backoff.Reset()
			}
		}
	}
}
