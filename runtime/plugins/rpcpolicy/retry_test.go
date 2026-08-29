package rpcpolicy

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ported case for case from msim tests/test_retry_policies.py, with the same
// literal numbers: these are the equivalence gate between the interceptor and
// the simulator it is matched against.

func ctxAt(attempt int, now int64) RetryContext {
	return RetryContext{Attempt: attempt, Now: now}
}

func TestFixedBackoffMaxAttempts(t *testing.T) {
	p, err := NewFixedBackoffRetryPolicy(2, 10)
	require.NoError(t, err)
	assert.Equal(t, DelayDecision{true, 10}, p.NextDelay(ctxAt(1, 0)))
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(2, 0)))
}

func TestFixedBackoffReturnsConfiguredDelay(t *testing.T) {
	p, err := NewFixedBackoffRetryPolicy(3, 25)
	require.NoError(t, err)
	assert.Equal(t, DelayDecision{true, 25}, p.NextDelay(ctxAt(1, 0)))
	assert.Equal(t, DelayDecision{true, 25}, p.NextDelay(ctxAt(2, 0)))
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(3, 0)))
}

func TestNoRetryAlwaysDenies(t *testing.T) {
	p := NewNoRetryPolicy()
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(1, 0)))
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(5, 0)))
}

func TestExponentialBackoffDoublesAndCaps(t *testing.T) {
	p, err := NewExponentialBackoffRetryPolicy(5, 10, 50)
	require.NoError(t, err)
	assert.Equal(t, DelayDecision{true, 10}, p.NextDelay(ctxAt(1, 0))) // 10*2^0
	assert.Equal(t, DelayDecision{true, 20}, p.NextDelay(ctxAt(2, 0))) // 10*2^1
	assert.Equal(t, DelayDecision{true, 40}, p.NextDelay(ctxAt(3, 0))) // 10*2^2
	assert.Equal(t, DelayDecision{true, 50}, p.NextDelay(ctxAt(4, 0))) // 80 -> cap
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(5, 0))) // exhausted
}

func TestExponentialJitterFullModeBounds(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	p, err := NewExponentialBackoffWithJitterRetryPolicy(4, 100, 10_000, JitterFull, r.Float64)
	require.NoError(t, err)
	for attempt := 1; attempt < 4; attempt++ {
		exp := expBackoff(100, 10_000, attempt)
		d := p.NextDelay(ctxAt(attempt, 0))
		assert.True(t, d.Allow)
		assert.GreaterOrEqual(t, d.Delay, int64(0))
		assert.LessOrEqual(t, d.Delay, exp)
	}
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(4, 0)))
}

func TestExponentialJitterEqualModeBounds(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	p, err := NewExponentialBackoffWithJitterRetryPolicy(4, 100, 10_000, JitterEqual, r.Float64)
	require.NoError(t, err)
	for attempt := 1; attempt < 4; attempt++ {
		exp := expBackoff(100, 10_000, attempt)
		d := p.NextDelay(ctxAt(attempt, 0))
		assert.True(t, d.Allow)
		assert.GreaterOrEqual(t, d.Delay, exp/2) // EQUAL jitter floors at exp/2
		assert.LessOrEqual(t, d.Delay, exp)
	}
}

func TestExponentialJitterIsDeterministicUnderSeed(t *testing.T) {
	run := func() []DelayDecision {
		r := rand.New(rand.NewSource(42))
		p, err := NewExponentialBackoffWithJitterRetryPolicy(5, 100, 10_000, JitterFull, r.Float64)
		require.NoError(t, err)
		out := make([]DelayDecision, 0, 4)
		for i := 1; i < 5; i++ {
			out = append(out, p.NextDelay(ctxAt(i, 0)))
		}
		return out
	}
	assert.Equal(t, run(), run())
}

func TestExponentialJitterRejectsUnknownMode(t *testing.T) {
	_, err := NewExponentialBackoffWithJitterRetryPolicy(3, 100, 10_000, JitterMode(2), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "jitter mode")
}

func TestNegativeFixedBackoffRejected(t *testing.T) {
	_, err := NewFixedBackoffRetryPolicy(2, -1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delay")
}

func TestNonPositiveAttemptsRejected(t *testing.T) {
	_, err := NewFixedBackoffRetryPolicy(0, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_attempts")
}

func TestNegativeExponentialDelaysRejected(t *testing.T) {
	_, err := NewExponentialBackoffRetryPolicy(2, -5, 10)
	require.Error(t, err)
}

func TestNegativeJitterDelaysRejected(t *testing.T) {
	_, err := NewExponentialBackoffWithJitterRetryPolicy(2, -5, 10, JitterFull, nil)
	require.Error(t, err)
}

// expBackoff saturates rather than overflowing, where msim promotes to a
// bignum before taking the min. Both answer max_delay.
func TestExpBackoffSaturatesInsteadOfOverflowing(t *testing.T) {
	assert.Equal(t, int64(50), expBackoff(10, 50, 200))
	assert.Equal(t, int64(7), expBackoff(9, 7, 1)) // initial above the cap
}

// The base policy's defaults are msim's RetryPolicy defaults.
func TestBasePolicyDefaults(t *testing.T) {
	p := NewNoRetryPolicy()
	assert.True(t, p.AllowRequest(0))
	assert.Nil(t, p.Underlying())
	assert.False(t, p.SupportsDeferredRollback())
	p.OnRequestStart(0)
	p.Rollback(ctxAt(1, 0))
}
