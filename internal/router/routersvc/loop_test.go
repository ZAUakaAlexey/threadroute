package routersvc

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeTimer moves only when a loop waits: After advances the clock by the
// wait and fires at once. It records every wait.
type fakeTimer struct {
	mu    sync.Mutex
	now   time.Time
	waits []time.Duration
}

func newFakeTimer() *fakeTimer {
	return &fakeTimer{now: time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)}
}

func (f *fakeTimer) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeTimer) After(d time.Duration) <-chan time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	f.waits = append(f.waits, d)
	c := make(chan time.Time, 1)
	c <- f.now
	return c
}

func (f *fakeTimer) Waits() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Duration(nil), f.waits...)
}

func newTestRunner(tm timer, limit time.Duration) (*runner, *map[string]int) {
	failed := map[string]int{}
	var mu sync.Mutex
	return &runner{
		timer: tm, outageLimit: limit, logger: slog.New(slog.DiscardHandler),
		failed: func(l string) {
			mu.Lock()
			failed[l]++
			mu.Unlock()
		},
	}, &failed
}

func TestRetryBackoffAndOutageLimit(t *testing.T) {
	tm := newFakeTimer()
	r, failed := newTestRunner(tm, 20*time.Second)
	calls := 0
	boom := errors.New("database down")
	err := r.retry(t.Context(), "apply", func(context.Context) error {
		calls++
		return boom
	})
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "outage limit") {
		t.Fatalf("retry() = %v, want the outage error wrapping the cause", err)
	}
	// Backoff 0.1 0.2 0.4 0.8 1.6 3.2 5 5 5 = 21.3s > 20s: the 10th failure
	// is past the limit.
	want := []time.Duration{100, 200, 400, 800, 1600, 3200, 5000, 5000, 5000}
	waits := tm.Waits()
	if len(waits) != len(want) {
		t.Fatalf("waits %v, want %d of them", waits, len(want))
	}
	for i, w := range want {
		if waits[i] != w*time.Millisecond {
			t.Errorf("wait %d = %s, want %s", i, waits[i], w*time.Millisecond)
		}
	}
	if calls != 10 || (*failed)["apply"] != 10 {
		t.Errorf("calls %d, failures counted %d, want 10", calls, (*failed)["apply"])
	}
}

func TestRetryRecovers(t *testing.T) {
	tm := newFakeTimer()
	r, _ := newTestRunner(tm, time.Minute)
	calls := 0
	err := r.retry(t.Context(), "apply", func(context.Context) error {
		calls++
		if calls < 4 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil || calls != 4 {
		t.Fatalf("retry() = %v after %d calls", err, calls)
	}
}

// TestLoopResetsOutageAfterSuccess checks that the failing time counts from
// the first failure after the last success.
func TestLoopResetsOutageAfterSuccess(t *testing.T) {
	tm := newFakeTimer()
	r, _ := newTestRunner(tm, 3*time.Second)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	step := 0
	err := r.loop(ctx, loopSpec{name: "expire", interval: time.Second, step: func(context.Context) (bool, error) {
		step++
		switch {
		case step > 40:
			cancel()
			return false, nil
		case step%3 == 0:
			return false, nil
		default:
			// Two failures in a row cost 0.1s + 0.2s of backoff, far below
			// the limit, so the loop never gives up.
			return false, errors.New("flaky")
		}
	}})
	if err != nil {
		t.Fatalf("loop() = %v, want nil after cancel", err)
	}
}

func TestLoopMoreAndInterval(t *testing.T) {
	tm := newFakeTimer()
	r, _ := newTestRunner(tm, time.Minute)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	step := 0
	err := r.loop(ctx, loopSpec{name: "assign", interval: 250 * time.Millisecond, step: func(context.Context) (bool, error) {
		step++
		if step == 6 {
			cancel()
		}
		// Work is left on odd steps.
		return step%2 == 1, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	// Steps 2 and 4 had no more work and waited; 1, 3 and 5 went on at once.
	waits := tm.Waits()
	if len(waits) != 2 || waits[0] != 250*time.Millisecond || waits[1] != 250*time.Millisecond {
		t.Errorf("waits %v, want two of 250ms", waits)
	}
}

func TestLoopReturnsOutageError(t *testing.T) {
	tm := newFakeTimer()
	r, _ := newTestRunner(tm, 2*time.Second)
	err := r.loop(t.Context(), loopSpec{name: "publish", interval: time.Second, step: func(context.Context) (bool, error) {
		return false, errors.New("kafka down")
	}})
	if err == nil || !strings.Contains(err.Error(), "publish") || !strings.Contains(err.Error(), "kafka down") {
		t.Fatalf("loop() = %v, want the outage error", err)
	}
}

// TestLoopKick checks that a kick ends the wait before the interval.
func TestLoopKick(t *testing.T) {
	r := &runner{timer: realTimer{}, outageLimit: time.Minute, logger: slog.New(slog.DiscardHandler)}
	var k kicker
	wake := k.add()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	steps := make(chan struct{}, 10)
	done := make(chan error, 1)
	go func() {
		done <- r.loop(ctx, loopSpec{name: "assign", interval: time.Hour, kick: wake, step: func(context.Context) (bool, error) {
			steps <- struct{}{}
			return false, nil
		}})
	}()
	<-steps
	k.kick()
	k.kick() // dropped: the loop is already woken
	select {
	case <-steps:
	case <-time.After(5 * time.Second):
		t.Fatal("kick did not wake the loop")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestModelInterval(t *testing.T) {
	tests := []struct {
		model time.Duration
		speed float64
		want  time.Duration
	}{
		{time.Second, 1, time.Second},
		{time.Second, 50, 20 * time.Millisecond},
		{10 * time.Second, 0.5, 20 * time.Second},
		{time.Second, 10000, minInterval},
	}
	for _, tt := range tests {
		if got := modelInterval(tt.model, tt.speed); got != tt.want {
			t.Errorf("modelInterval(%s, %v) = %s, want %s", tt.model, tt.speed, got, tt.want)
		}
	}
}

func TestRetryStopsOnCancel(t *testing.T) {
	tm := newFakeTimer()
	r, _ := newTestRunner(tm, time.Hour)
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	err := r.retry(ctx, "apply", func(context.Context) error {
		calls++
		if calls == 3 {
			cancel()
		}
		return errors.New("down")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retry() = %v, want context.Canceled", err)
	}
}
