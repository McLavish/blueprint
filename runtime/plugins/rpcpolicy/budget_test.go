package rpcpolicy

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustBudget(t *testing.T, ratio float64, maxRetries int, underlying Policy) *RetryBudgetPolicy {
	t.Helper()
	p, err := NewRetryBudgetPolicy(ratio, maxRetries, underlying)
	require.NoError(t, err)
	return p
}

func TestRetryBudgetDepletesAndRefills(t *testing.T) {
	// budget_ratio 0.1 => deposit 10, retry_cost 100, max_tokens 200
	p := mustBudget(t, 0.1, 2, mustFixed(t, 100, 7))
	assert.Equal(t, 200.0, p.TokenBalance())
	assert.Equal(t, DelayDecision{true, 7}, p.NextDelay(ctxAt(1, 0)))  // 200 -> 100
	assert.Equal(t, DelayDecision{true, 7}, p.NextDelay(ctxAt(2, 0)))  // 100 -> 0
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(3, 0))) // depleted

	// Finagle semantics: the denominator is REQUESTS, so a request refills the
	// budget regardless of how it turns out.
	for i := 0; i < 10; i++ { // 10 requests * 10 deposit = 100 = one retry
		p.OnRequestStart(0)
	}
	assert.Equal(t, 100.0, p.TokenBalance())
	assert.Equal(t, DelayDecision{true, 7}, p.NextDelay(ctxAt(4, 0)))
}

func TestRetryBudgetDepositsRegardlessOfOutcome(t *testing.T) {
	p := mustBudget(t, 0.1, 2, mustFixed(t, 100, 7))
	for p.CanRetry() { // drain
		p.NextDelay(ctxAt(1, 0))
	}
	assert.Equal(t, 0.0, p.TokenBalance())
	// A request that will ultimately fail still deposits.
	for i := 0; i < 10; i++ {
		p.OnRequestStart(0)
	}
	assert.True(t, p.CanRetry())
}

func TestRetryBudgetDoesNotConsumeWhenUnderlyingDenies(t *testing.T) {
	p := mustBudget(t, 0.1, 2, mustFixed(t, 1, 7)) // always denies
	before := p.TokenBalance()
	assert.Equal(t, DelayDecision{false, 0}, p.NextDelay(ctxAt(1, 0)))
	assert.Equal(t, before, p.TokenBalance())
}

func drainedBudget(t *testing.T, ratio float64) *RetryBudgetPolicy {
	t.Helper()
	p := mustBudget(t, ratio, 1, mustFixed(t, 10, 0))
	for p.CanRetry() { // drain the initial burst
		p.NextDelay(ctxAt(1, 0))
	}
	return p
}

func TestOneThirdRatioEarnsARetryAfterThreeRequests(t *testing.T) {
	p := drainedBudget(t, 1.0/3.0)
	for i := 0; i < 3; i++ {
		p.OnRequestStart(0)
	}
	assert.True(t, p.CanRetry(), "float accumulation would have given 99.99999999999999")
}

func TestOneSeventhRatioEarnsARetryAfterSevenRequests(t *testing.T) {
	p := drainedBudget(t, 1.0/7.0)
	for i := 0; i < 6; i++ {
		p.OnRequestStart(0)
	}
	assert.False(t, p.CanRetry())
	p.OnRequestStart(0)
	assert.True(t, p.CanRetry())
}

func TestShippedRatiosUnchanged(t *testing.T) {
	for _, tc := range []struct {
		ratio  float64
		needed int
	}{{0.05, 20}, {0.1, 10}, {0.2, 5}} {
		p := drainedBudget(t, tc.ratio)
		for i := 0; i < tc.needed-1; i++ {
			p.OnRequestStart(0)
		}
		assert.False(t, p.CanRetry(), "ratio %v", tc.ratio)
		p.OnRequestStart(0)
		assert.True(t, p.CanRetry(), "ratio %v", tc.ratio)
	}
}

// A ratio below ~1/20000 used to rationalize to a zero deposit, so the budget
// never refilled: 1e-5, 1e-7 and 1e-9 all became "max_retries retries for the
// whole run", silently and identically.
func TestTinyRatioIsStillARatio(t *testing.T) {
	p := drainedBudget(t, 1e-5)
	assert.Equal(t, int64(1), p.Deposit())
	assert.Equal(t, int64(100_000), p.RetryCost())
	for i := 0; i < 99_999; i++ {
		p.OnRequestStart(0)
	}
	assert.False(t, p.CanRetry())
	p.OnRequestStart(0)
	assert.True(t, p.CanRetry())
}

func TestUnrepresentableRatioIsRejected(t *testing.T) {
	_, err := NewRetryBudgetPolicy(1e-12, 1, NewNoRetryPolicy())
	assert.ErrorContains(t, err, "budget_ratio")
}

func TestRetryBudgetRejectsInvalidConfig(t *testing.T) {
	_, err := NewRetryBudgetPolicy(0, 1, NewNoRetryPolicy())
	assert.ErrorContains(t, err, "budget_ratio")
	_, err = NewRetryBudgetPolicy(1.5, 1, NewNoRetryPolicy())
	assert.ErrorContains(t, err, "budget_ratio")
	_, err = NewRetryBudgetPolicy(0.5, 0, NewNoRetryPolicy())
	assert.ErrorContains(t, err, "max_retries")
}

func TestRetryBudgetRefundIsCappedAtTheCeiling(t *testing.T) {
	p := mustBudget(t, 0.1, 1, mustFixed(t, 10, 0))
	before := p.Tokens()
	p.Rollback(ctxAt(1, 0)) // nothing was charged; the cap must hold
	assert.Equal(t, before, p.Tokens())
}

// limitDenominator is a port of CPython's Fraction.limit_denominator; these are
// the values msim's budget arithmetic depends on.
func TestLimitDenominatorMatchesCPython(t *testing.T) {
	bound := big.NewInt(maxRatioDenominator)
	for _, tc := range []struct {
		in       float64
		num, den int64
	}{
		{1.0 / 3.0, 1, 3},
		{1.0 / 7.0, 1, 7},
		{0.05, 1, 20},
		{0.1, 1, 10},
		{0.2, 1, 5},
		{1e-5, 1, 100_000},
		{1.0, 1, 1},
		{0.5, 1, 2},
		{1e-12, 0, 1},
		{0.65, 13, 20},
	} {
		got := limitDenominator(new(big.Rat).SetFloat64(tc.in), bound)
		assert.Equal(t, big.NewRat(tc.num, tc.den).RatString(), got.RatString(), "input %v", tc.in)
	}
	// A fraction already inside the bound is returned unchanged.
	assert.Equal(t, "3/4", limitDenominator(big.NewRat(3, 4), bound).RatString())
	// Negative inputs keep their sign (never produced by a validated ratio, but
	// the port must still be the same function).
	assert.Equal(t, "-1/3", limitDenominator(new(big.Rat).SetFloat64(-1.0/3.0), bound).RatString())
}
