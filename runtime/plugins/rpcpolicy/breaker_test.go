package rpcpolicy

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const secondNS = int64(1_000_000_000)

func mustCountBreaker(t *testing.T, failure, success [2]int, halfOpenDelay int64, underlying Policy) *CountBasedCircuitBreakerPolicy {
	t.Helper()
	p, err := NewCountBasedCircuitBreakerPolicy(failure, success, halfOpenDelay, underlying)
	require.NoError(t, err)
	return p
}

func mustTimeBreaker(t *testing.T, failureRate, successRate float64, minRequests int, window, halfOpenDelay int64, halfOpenMin int, underlying Policy) *TimeBasedCircuitBreakerPolicy {
	t.Helper()
	p, err := NewTimeBasedCircuitBreakerPolicy(failureRate, successRate, minRequests, window, halfOpenDelay, halfOpenMin, underlying)
	require.NoError(t, err)
	return p
}

func TestCountCircuitBreakerLifecycle(t *testing.T) {
	inner, err := NewFixedBackoffRetryPolicy(10, 5)
	require.NoError(t, err)
	p := mustCountBreaker(t, [2]int{2, 2}, [2]int{2, 2}, 100, inner)

	// closed: delegates to the underlying policy
	assert.Equal(t, CBClosed, p.GetState())
	assert.Equal(t, DelayDecision{true, 5}, p.NextDelay(ctxAt(1, 0)))

	// filling the failure window opens the circuit
	p.AddResult(false, 0)
	assert.Equal(t, CBClosed, p.GetState())
	p.AddResult(false, 1)
	assert.Equal(t, CBOpen, p.GetState())

	// open: denies retries and blocks requests before the half-open delay
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(1, 1)))
	assert.False(t, p.AllowRequest(50))

	// after the half-open delay it flips to half-open and admits a probe
	assert.True(t, p.AllowRequest(101))
	assert.Equal(t, CBHalfOpen, p.GetState())

	// a full batch of successful probes closes the circuit again
	p.AddResult(true, 101)
	assert.Equal(t, CBHalfOpen, p.GetState())
	p.AddResult(true, 102)
	assert.Equal(t, CBClosed, p.GetState())
}

func TestCountCircuitBreakerFailedProbeReopens(t *testing.T) {
	p := mustCountBreaker(t, [2]int{1, 1}, [2]int{2, 2}, 10, NewNoRetryPolicy())
	p.AddResult(false, 0) // window maxlen 1 -> opens immediately
	assert.Equal(t, CBOpen, p.GetState())
	assert.True(t, p.AllowRequest(20)) // -> half-open
	p.AddResult(true, 20)
	p.AddResult(false, 21) // 1 success out of 2 -> reopen
	assert.Equal(t, CBOpen, p.GetState())
}

func TestTimeCircuitBreakerMinRequestsGating(t *testing.T) {
	p := mustTimeBreaker(t, 0.5, 0.5, 3, 1000, 100, 1, NewNoRetryPolicy())
	// 100% failure rate but too few samples -> stays closed
	p.AddResult(false, 0)
	p.AddResult(false, 1)
	assert.Equal(t, CBClosed, p.GetState())
	// crossing min_requests with rate over threshold opens it
	p.AddResult(false, 2)
	assert.Equal(t, CBOpen, p.GetState())
}

func TestTimeCircuitBreakerEvictsStaleResults(t *testing.T) {
	p := mustTimeBreaker(t, 0.5, 0.5, 2, 100, 10, 1, NewNoRetryPolicy())
	p.AddResult(false, 0)
	// the gap evicts the first result, so only one sample remains -> closed
	p.AddResult(false, 200)
	assert.Equal(t, CBClosed, p.GetState())
}

func TestTimeCircuitBreakerRecovery(t *testing.T) {
	p := mustTimeBreaker(t, 0.5, 0.5, 2, 10_000, 50, 1, NewNoRetryPolicy())
	p.AddResult(false, 0)
	p.AddResult(false, 1)
	assert.Equal(t, CBOpen, p.GetState())
	assert.False(t, p.AllowRequest(10))
	assert.True(t, p.AllowRequest(51))
	assert.Equal(t, CBHalfOpen, p.GetState())
	p.AddResult(true, 51)
	p.AddResult(true, 52)
	assert.Equal(t, CBClosed, p.GetState())
}

func TestTimeCircuitBreakerResetsWindowOnClose(t *testing.T) {
	p := mustTimeBreaker(t, 0.5, 0.5, 2, 10_000, 50, 1, NewNoRetryPolicy())
	p.AddResult(false, 0)
	p.AddResult(false, 1)
	assert.True(t, p.AllowRequest(51))
	p.AddResult(true, 51)
	p.AddResult(true, 52)
	assert.Equal(t, CBClosed, p.GetState())
	// The half-open probes must not seed the closed window: two fresh failures
	// alone (100% rate over min_requests) re-open the circuit.
	p.AddResult(false, 53)
	p.AddResult(false, 54)
	assert.Equal(t, CBOpen, p.GetState())
}

// The incremental failure counter must agree with a full rescan of the window
// at every step; the O(n^2) rescan is what it replaced.
func TestTimeBreakerIncrementalCountersMatchFullRescan(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	inner, err := NewFixedBackoffRetryPolicy(3, 0)
	require.NoError(t, err)
	p := mustTimeBreaker(t, 0.5, 0.6, 5, 2*secondNS, secondNS, 1, inner)
	now := int64(0)
	for i := 0; i < 3000; i++ {
		now += rng.Int63n(secondNS/100 + 1)
		p.AllowRequest(now)
		p.AddResult(rng.Float64() < 0.6, now)
		rescan := 0
		for _, r := range p.window {
			if !r.ok {
				rescan++
			}
		}
		require.Equal(t, rescan, p.failuresInWindow, "step %d", i)
		require.Equal(t, len(p.window), p.windowLen())
	}
}

func TestCountBreakerRejectsImpossibleRatios(t *testing.T) {
	build := func(failure, success [2]int, halfOpenDelay int64) error {
		_, err := NewCountBasedCircuitBreakerPolicy(failure, success, halfOpenDelay, NewNoRetryPolicy())
		return err
	}
	assert.ErrorContains(t, build([2]int{3, 2}, [2]int{1, 1}, 0), "failure_threshold_ratio")
	assert.ErrorContains(t, build([2]int{0, 0}, [2]int{1, 1}, 0), "failure_threshold_ratio")
	assert.ErrorContains(t, build([2]int{1, 1}, [2]int{-1, 2}, 0), "success_threshold_ratio")
	assert.ErrorContains(t, build([2]int{1, 1}, [2]int{1, 1}, -1), "half_open_delay")
	require.NoError(t, build([2]int{1, 1}, [2]int{1, 1}, 0))
}

func TestTimeBreakerRejectsInvalidConfig(t *testing.T) {
	build := func(failureRate, successRate float64, minRequests int, window, halfOpenDelay int64, halfOpenMin int) error {
		_, err := NewTimeBasedCircuitBreakerPolicy(failureRate, successRate, minRequests, window, halfOpenDelay, halfOpenMin, NewNoRetryPolicy())
		return err
	}
	// A non-positive window evicts every prior result, so the breaker could
	// never reach min_requests and silently never opened.
	assert.ErrorContains(t, build(0.5, 0.5, 2, -1, 10, 1), "window_duration")
	assert.ErrorContains(t, build(1.5, 0.5, 2, 1000, 10, 1), "failure_threshold_rate")
	assert.ErrorContains(t, build(0.5, -0.1, 2, 1000, 10, 1), "success_threshold_rate")
	assert.ErrorContains(t, build(0.5, 0.5, 0, 1000, 10, 1), "min_requests")
	assert.ErrorContains(t, build(0.5, 0.5, 2, 1000, 10, 0), "half_open_min_requests")
	assert.ErrorContains(t, build(0.5, 0.5, 2, 1000, -1, 1), "half_open_delay")
	require.NoError(t, build(0.5, 0.5, 2, 1000, 10, 1))
}

// A zero half_open_delay flips straight to half-open on the next request, and
// an opened-at that was never recorded does too.
func TestBreakerZeroHalfOpenDelayProbesImmediately(t *testing.T) {
	p := mustCountBreaker(t, [2]int{1, 1}, [2]int{1, 1}, 0, NewNoRetryPolicy())
	p.AddResult(false, 100)
	assert.Equal(t, CBOpen, p.GetState())
	assert.True(t, p.AllowRequest(100))
	assert.Equal(t, CBHalfOpen, p.GetState())
}

// AllowRequest must short-circuit: an outer layer's denial must not flip an
// inner breaker into its probe stage on a request that never ran.
func TestAllowRequestShortCircuitsAtTheFirstDenial(t *testing.T) {
	inner := mustCountBreaker(t, [2]int{1, 1}, [2]int{1, 1}, secondNS, NewNoRetryPolicy())
	outer := mustCountBreaker(t, [2]int{1, 1}, [2]int{1, 1}, secondNS, inner)
	inner.AddResult(false, 0)
	outer.AddResult(false, 0)
	require.Equal(t, CBOpen, inner.GetState())
	require.Equal(t, CBOpen, outer.GetState())

	assert.False(t, allowPolicyRequest(outer, 10))
	assert.Equal(t, CBOpen, inner.GetState(), "the inner breaker must not have been consulted")
}
