package rpcpolicy

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The real clock's precise sleep lands on its deadline: the fault injector's latency
// is service time the matched model adds exactly, and a plain timer ran ~0.6 ms long
// on slimfly (hotel K16: a 29 ms injection slept 29.63 ms at the median).
func TestRealClockSleepPreciseLandsOnTheDeadline(t *testing.T) {
	clock := newRealClock()
	ctx := context.Background()
	for _, d := range []time.Duration{time.Millisecond, 1740 * time.Microsecond, 5 * time.Millisecond} {
		var over []time.Duration
		for i := 0; i < 25; i++ {
			start := time.Now()
			if err := clock.SleepPrecise(ctx, d); err != nil {
				t.Fatalf("SleepPrecise(%v): %v", d, err)
			}
			elapsed := time.Since(start)
			if elapsed < d {
				t.Fatalf("SleepPrecise(%v) returned after %v, before its deadline", d, elapsed)
			}
			over = append(over, elapsed-d)
		}
		sort.Slice(over, func(i, j int) bool { return over[i] < over[j] })
		// Generous for a loaded test host; the plain timer's median overshoot on
		// slimfly was 630 us.
		if median := over[len(over)/2]; median > 200*time.Microsecond {
			t.Fatalf("SleepPrecise(%v): median overshoot %v, want <= 200us", d, median)
		}
	}
}

// Cancellation ends it in the timer phase and in the spin phase, as it ends Sleep.
func TestRealClockSleepPreciseHonoursCancellation(t *testing.T) {
	clock := newRealClock()
	for _, tc := range []struct {
		name     string
		d, after time.Duration
	}{
		{"in the timer phase", 50 * time.Millisecond, 5 * time.Millisecond},
		{"in the spin phase", injectionSpin, 500 * time.Microsecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(tc.after, cancel)
			start := time.Now()
			err := clock.SleepPrecise(ctx, tc.d)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("SleepPrecise returned %v, want context.Canceled", err)
			}
			if elapsed := time.Since(start); elapsed >= tc.d {
				t.Fatalf("cancelled after %v but returned only after %v (d = %v)", tc.after, elapsed, tc.d)
			}
		})
	}
}

func TestRealClockSleepPreciseOfNothingReturnsTheContextsState(t *testing.T) {
	clock := newRealClock()
	if err := clock.SleepPrecise(context.Background(), 0); err != nil {
		t.Fatalf("SleepPrecise(0) = %v, want nil", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := clock.SleepPrecise(ctx, -time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Fatalf("SleepPrecise(<0) on a cancelled context = %v, want context.Canceled", err)
	}
}

// spyClock is the real clock, counting which sleep the policy layer asked for.
type spyClock struct {
	realClock
	mu             sync.Mutex
	plain, precise int
}

func (c *spyClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.plain++
	c.mu.Unlock()
	return c.realClock.Sleep(ctx, d)
}

func (c *spyClock) SleepPrecise(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.precise++
	c.mu.Unlock()
	return c.realClock.SleepPrecise(ctx, d)
}

// The injector spends its latency through SleepPrecise when the clock offers one, and
// the served record's injected_ms is the armed latency, not the timer's overshoot.
func TestTheInjectorSpendsItsLatencyPrecisely(t *testing.T) {
	epochMs, _ := epochNow()
	clock := &spyClock{realClock: newRealClock()}
	s := newTestStateOn(t, clock, t.TempDir(), "svc-test",
		"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 2\n  queue_capacity: 4\n",
		scheduleOf(FaultRule{Method: testMethod, StartS: 0, EndS: 3600, AddLatencyMS: 1.74}),
		epochMs, func() float64 { return 1.0 })

	for i := 0; i < 5; i++ {
		_, err := s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			return "ok", nil
		})
		require.NoError(t, err)
	}
	clock.mu.Lock()
	precise, plain := clock.precise, clock.plain
	clock.mu.Unlock()
	assert.Equal(t, 5, precise, "every injection goes through SleepPrecise")
	assert.Equal(t, 0, plain, "no injection falls back to the plain timer")

	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 5)
	for _, rec := range recs {
		injected, _ := rec["injected_ms"].(float64)
		assert.GreaterOrEqual(t, injected, 1.74)
		assert.Less(t, injected, 1.74+0.5, "the served attempt slept the armed latency, not a timer's overshoot")
	}
}
