package rpcpolicy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustFixed(t *testing.T, maxAttempts int, delay int64) *FixedBackoffRetryPolicy {
	t.Helper()
	p, err := NewFixedBackoffRetryPolicy(maxAttempts, delay)
	require.NoError(t, err)
	return p
}

func mustLeaky(t *testing.T, maxRequests, maxAttempts int, period int64) *LeakyRateLimiterPolicy {
	t.Helper()
	p, err := NewLeakyRateLimiterPolicy(maxRequests, maxAttempts, period)
	require.NoError(t, err)
	return p
}

func mustBursty(t *testing.T, maxRequests int, refillRate float64, period int64, underlying Policy) *BurstyRateLimiterPolicy {
	t.Helper()
	p, err := NewBurstyRateLimiterPolicy(maxRequests, refillRate, period, underlying)
	require.NoError(t, err)
	return p
}

func mustFixedWindow(t *testing.T, maxRequests int, period int64, underlying Policy) *FixedWindowBurstyLimiterPolicy {
	t.Helper()
	p, err := NewFixedWindowBurstyLimiterPolicy(maxRequests, period, underlying)
	require.NoError(t, err)
	return p
}

func TestLeakyRateLimiterMaxAttempts(t *testing.T) {
	p := mustLeaky(t, 10, 2, 100)
	assert.True(t, p.NextDelay(ctxAt(1, 0)).Allow)
	assert.False(t, p.NextDelay(ctxAt(2, 0)).Allow)
}

func TestLeakyRateLimiterSpacing(t *testing.T) {
	// period / max_requests = 25 ns between admitted requests
	p := mustLeaky(t, 4, 10, 100)
	assert.Equal(t, DelayDecision{true, 0}, p.NextDelay(ctxAt(1, 0)))
	assert.Equal(t, DelayDecision{true, 25}, p.NextDelay(ctxAt(2, 0)))
	assert.Equal(t, DelayDecision{true, 50}, p.NextDelay(ctxAt(3, 0)))
	// once wall-clock passes the reserved slot, no extra delay is added
	assert.Equal(t, DelayDecision{true, 0}, p.NextDelay(ctxAt(4, 1000)))
}

// Rounding the reserved slot every step drops the same remainder each time, so
// the achieved spacing becomes floor(period / max_requests) and the bucket runs
// permanently fast: 3 per 100 ns would admit a 4th at 99 ns.
func TestLeakyRateLimiterKeepsTheFractionalRemainder(t *testing.T) {
	p := mustLeaky(t, 3, 10, 100)
	var delays []int64
	for i := 1; i <= 4; i++ {
		delays = append(delays, p.NextDelay(ctxAt(i, 0)).Delay)
	}
	assert.Equal(t, []int64{0, 34, 67, 100}, delays)
	under := 0
	for _, d := range delays {
		if d < 100 {
			under++
		}
	}
	assert.Equal(t, p.MaxRequests, under)

	// The exact-half case rounded to 2 ns instead of 2.5 ns, a permanent 25%
	// rate inflation.
	halved := mustLeaky(t, 2, 10, 5)
	var hd []int64
	for i := 1; i <= 3; i++ {
		hd = append(hd, halved.NextDelay(ctxAt(i, 0)).Delay)
	}
	assert.Equal(t, []int64{0, 3, 5}, hd)
}

func TestLeakyRollbackReleasesTheReservedSlot(t *testing.T) {
	p := mustLeaky(t, 10, 10, secondNS)
	require.True(t, p.NextDelay(ctxAt(1, 0)).Allow)
	require.NotNil(t, p.nextAt)
	p.Rollback(ctxAt(1, 0))
	assert.Nil(t, p.nextAt, "_next_at advances once per ISSUED retry, never for a vetoed one")
	// A second rollback with nothing reserved is a no-op.
	p.Rollback(ctxAt(1, 0))
	assert.Nil(t, p.nextAt)
}

func TestBurstyRateLimiterRefillsByPeriod(t *testing.T) {
	p := mustBursty(t, 2, 2, 100, mustFixed(t, 10, 0))
	assert.Equal(t, DelayDecision{true, 0}, p.NextDelay(ctxAt(1, 0)))
	assert.Equal(t, DelayDecision{true, 0}, p.NextDelay(ctxAt(1, 0)))
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(1, 1)))
	assert.InDelta(t, 0.02, p.Tokens(), 1e-12)

	assert.Equal(t, DelayDecision{true, 0}, p.NextDelay(ctxAt(1, 50)))
	assert.InDelta(t, 0.0, p.Tokens(), 1e-12)
}

func TestBurstyRateLimiterRejectsInvalidConfig(t *testing.T) {
	_, err := NewBurstyRateLimiterPolicy(0, 1, 100, NewNoRetryPolicy())
	assert.ErrorContains(t, err, "max_requests")
	_, err = NewBurstyRateLimiterPolicy(1, 1, 0, NewNoRetryPolicy())
	assert.ErrorContains(t, err, "period")
	_, err = NewBurstyRateLimiterPolicy(1, -1, 100, NewNoRetryPolicy())
	assert.ErrorContains(t, err, "refill_rate")
}

func TestBurstyRateLimiterBurstsThenDenies(t *testing.T) {
	p := mustBursty(t, 3, 1, 100, mustFixed(t, 100, 0))
	for i := 0; i < 3; i++ { // burst capacity = 3
		assert.Equal(t, DelayDecision{true, 0}, p.NextDelay(ctxAt(1, 0)))
	}
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(1, 0)))
}

func TestBurstyRateLimiterRefillCapsAtCapacity(t *testing.T) {
	p := mustBursty(t, 2, 2, 100, mustFixed(t, 100, 0))
	p.NextDelay(ctxAt(1, 0)) // drain both tokens
	p.NextDelay(ctxAt(1, 0))
	// a long gap would over-refill, but tokens are capped at max_requests=2
	assert.Equal(t, DelayDecision{true, 0}, p.NextDelay(ctxAt(1, 10_000)))
	assert.InDelta(t, 1.0, p.Tokens(), 1e-12) // capped to 2, then consumed 1
}

func TestBurstyRateLimiterDoesNotConsumeWhenUnderlyingDenies(t *testing.T) {
	p := mustBursty(t, 2, 1, 100, mustFixed(t, 1, 0)) // underlying denies
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(1, 0)))
	assert.Equal(t, 2.0, p.Tokens())
}

func TestFixedWindowBurstyLimitsPerWindow(t *testing.T) {
	p := mustFixedWindow(t, 2, 100, mustFixed(t, 100, 0))
	// window [0, 100): 2 admitted, 3rd denied
	assert.Equal(t, DelayDecision{true, 0}, p.NextDelay(ctxAt(1, 0)))
	assert.Equal(t, DelayDecision{true, 0}, p.NextDelay(ctxAt(1, 10)))
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(1, 20)))
	// the next window resets the quota
	assert.Equal(t, DelayDecision{true, 0}, p.NextDelay(ctxAt(1, 100)))
}

func TestFixedWindowBurstyNextRefillTime(t *testing.T) {
	p := mustFixedWindow(t, 1, 100, NewNoRetryPolicy())
	assert.Equal(t, int64(100), p.NextRefillTime(20))
	assert.Equal(t, int64(200), p.NextRefillTime(150))
}

func TestFixedWindowRejectsInvalidConfig(t *testing.T) {
	_, err := NewFixedWindowBurstyLimiterPolicy(0, 100, NewNoRetryPolicy())
	assert.ErrorContains(t, err, "max_requests")
	_, err = NewFixedWindowBurstyLimiterPolicy(1, 0, NewNoRetryPolicy())
	assert.ErrorContains(t, err, "period")
}

func TestLeakyRejectsInvalidConfig(t *testing.T) {
	_, err := NewLeakyRateLimiterPolicy(0, 1, 100)
	assert.ErrorContains(t, err, "max_requests")
	_, err = NewLeakyRateLimiterPolicy(1, 0, 100)
	assert.ErrorContains(t, err, "max_attempts")
	_, err = NewLeakyRateLimiterPolicy(1, 1, 0)
	assert.ErrorContains(t, err, "period")
}

// A policy that charges in NextDelay must not be refunded late by default: the
// deferred path (a cancel arriving mid-backoff) can rewind a reservation that
// belongs to a different live root.
func TestDeferredRollbackIsOptIn(t *testing.T) {
	budget, err := NewRetryBudgetPolicy(0.1, 1, NewNoRetryPolicy())
	require.NoError(t, err)
	for _, p := range []Policy{
		NewNoRetryPolicy(),
		mustLeaky(t, 1, 2, 100),
		budget,
		mustBursty(t, 1, 1, 100, NewNoRetryPolicy()),
		mustFixedWindow(t, 1, 100, NewNoRetryPolicy()),
	} {
		assert.False(t, p.SupportsDeferredRollback())
	}
}

// A new side-effecting policy is skipped by the deferred path without opting in.
type concurrencyLimiterPolicy struct {
	basePolicy
	refunds int
}

func (p *concurrencyLimiterPolicy) NextDelay(RetryContext) DelayDecision {
	return DelayDecision{true, 0}
}
func (p *concurrencyLimiterPolicy) Rollback(RetryContext) { p.refunds++ }

func TestANewSideEffectingPolicyIsSkippedWithoutOptingIn(t *testing.T) {
	p := &concurrencyLimiterPolicy{}
	ctx := ctxAt(1, 0)
	rollbackPolicy(p, ctx, true)
	assert.Equal(t, 0, p.refunds)
	rollbackPolicy(p, ctx, false)
	assert.Equal(t, 1, p.refunds)
}
