package reliablewriter

import (
	"context"
	"math/rand"
	"time"
)

type Backoff struct {
	currentDelay time.Duration
	multiplier   float64
	maxDelay     time.Duration
}

func NewBackoff(startDelay time.Duration, multiplier float64, maxDelay time.Duration) *Backoff {
	return &Backoff{
		currentDelay: startDelay,
		multiplier:   multiplier,
		maxDelay:     maxDelay,
	}
}

func (b *Backoff) Wait(ctx context.Context) error {
	sleepDuration := b.currentDelay + time.Duration(float64(b.currentDelay)*(rand.Float64()-0.5)/5)
	t := time.NewTimer(sleepDuration)
	defer t.Stop()

	select {
	case <-t.C:
		// done
	case <-ctx.Done():
		return ctx.Err()
	}

	b.currentDelay = min(b.maxDelay, time.Duration(float64(b.currentDelay)*b.multiplier))
	return nil
}
