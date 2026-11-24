package routersvc

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Backoff of a failing step.
const (
	minBackoff = 100 * time.Millisecond
	maxBackoff = 5 * time.Second
	// minInterval is the shortest pause of a loop. A model-time interval
	// divided by a large speed factor never makes a loop spin.
	minInterval = 5 * time.Millisecond
	// stepTimeout bounds one step of a loop, so a step that hangs counts
	// as a failure.
	stepTimeout = time.Minute
)

// timer is the time source of the loops: the machine clock in the service,
// a fake one in tests.
type timer interface {
	Now() time.Time
	// After returns a channel that receives once d has passed.
	After(d time.Duration) <-chan time.Time
}

// realTimer is the machine clock.
type realTimer struct{}

func (realTimer) Now() time.Time                         { return time.Now() }
func (realTimer) After(d time.Duration) <-chan time.Time { return time.After(d) }

// modelInterval turns an interval in model time into machine time: the
// interval divided by the speed factor, and not below minInterval.
func modelInterval(model time.Duration, speedFactor float64) time.Duration {
	d := time.Duration(float64(model) / speedFactor)
	return max(d, minInterval)
}

// runner runs steps with retries and the outage limit.
type runner struct {
	timer       timer
	outageLimit time.Duration
	logger      *slog.Logger
	// failed counts each failed step by loop name. It may be nil.
	failed func(loop string)
}

// retry calls fn until it succeeds or ctx ends. Between two failures it
// waits with a backoff that doubles up to maxBackoff. When fn has failed
// without a break for outageLimit, retry returns the last error: the
// process then stops, and the restart policy starts it again (as in the
// other services). It returns ctx.Err() when ctx ends.
func (r *runner) retry(ctx context.Context, name string, fn func(context.Context) error) error {
	var since time.Time
	backoff := minBackoff
	for {
		sctx, cancel := context.WithTimeout(ctx, stepTimeout)
		err := fn(sctx)
		cancel()
		if err == nil {
			if !since.IsZero() {
				r.logger.Info("loop recovered", "loop", name, "failing_for", r.timer.Now().Sub(since).String())
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.failed != nil {
			r.failed(name)
		}
		now := r.timer.Now()
		if since.IsZero() {
			since = now
		}
		if failing := now.Sub(since); failing >= r.outageLimit {
			return fmt.Errorf("%s: failing for %s, longer than the outage limit %s: %w",
				name, failing.Round(time.Millisecond), r.outageLimit, err)
		}
		r.logger.Warn("loop step failed, retrying", "loop", name, "backoff", backoff.String(), "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.timer.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// loopSpec is one background loop.
type loopSpec struct {
	name string
	// interval is the pause after a step that reported no more work.
	interval time.Duration
	// kick wakes the loop before the interval ends. It may be nil.
	kick <-chan struct{}
	// step does one round of work. more reports that work is left, so the
	// next round starts at once.
	step func(ctx context.Context) (more bool, err error)
}

// loop runs l until ctx ends, and returns nil then. A failing step is
// retried by retry; an error is returned only when the outage limit is
// passed.
func (r *runner) loop(ctx context.Context, l loopSpec) error {
	for {
		var more bool
		err := r.retry(ctx, l.name, func(c context.Context) error {
			m, err := l.step(c)
			more = m
			return err
		})
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		if more {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-r.timer.After(l.interval):
		case <-l.kick:
		}
	}
}

// kicker wakes a set of loops. A kick that finds a loop already woken is
// dropped, so a burst of kicks costs one extra round at most.
type kicker struct {
	chans []chan struct{}
}

// add returns a new channel for one loop.
func (k *kicker) add() <-chan struct{} {
	c := make(chan struct{}, 1)
	k.chans = append(k.chans, c)
	return c
}

// kick wakes every loop. It never blocks. The set of channels must not
// change after the loops start.
func (k *kicker) kick() {
	for _, c := range k.chans {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}
