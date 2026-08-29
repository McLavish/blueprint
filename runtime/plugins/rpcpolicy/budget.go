package rpcpolicy

import (
	"fmt"
	"math"
	"math/big"
)

// RetryBudgetPolicy is msim's Finagle-style retry budget: retries capped at
// BudgetRatio of REQUESTS.
//
// One deposit per admitted root request (OnRequestStart), one withdrawal per
// retry (NextDelay), refunded if the client then declines to issue it
// (Rollback) -- the same deposit/tryWithdraw shape as Finagle's RetryBudget.
//
// Deliberate divergence from Finagle, inherited from msim: deposits never
// expire. Finagle ages them out on a TTL (default 60 s); this caps the balance
// at max_retries worth of tokens instead, which bounds the burst but not the
// staleness.
//
// The denominator is REQUESTS, not successes. Depositing only on success would
// enforce retries <= ratio x successes, which is strictly tighter during
// exactly the failure storms a budget exists to damp.
type RetryBudgetPolicy struct {
	throttled
	BudgetRatio float64
	MaxRetries  int

	// Integer token bucket in units of 1/denominator of the (rationalized)
	// ratio: one retry costs `retryCost` tokens, each request deposits
	// `deposit`. Exact arithmetic -- no float drift; limitDenominator recovers
	// intended ratios like 1/3 from their float representations.
	retryCost int64
	deposit   int64
	maxTokens int64
	tokens    int64
}

// maxRatioDenominator is msim's _MAX_RATIO_DENOMINATOR. Wide enough that every
// ratio a caller can plausibly mean survives it: at 10_000 anything below
// ~1/20000 rounded to 0/1, which silently turned the budget into a one-shot
// allowance of max_retries retries for the whole run.
const maxRatioDenominator = 1_000_000_000

// NewRetryBudgetPolicy validates exactly as msim's __post_init__.
func NewRetryBudgetPolicy(budgetRatio float64, maxRetries int, underlying Policy) (*RetryBudgetPolicy, error) {
	if math.IsNaN(budgetRatio) || math.IsInf(budgetRatio, 0) || !(budgetRatio > 0 && budgetRatio <= 1) {
		return nil, fmt.Errorf("budget_ratio must be in (0, 1], got %v", budgetRatio)
	}
	if maxRetries < 1 {
		return nil, fmt.Errorf("max_retries must be >= 1, got %d", maxRetries)
	}
	if underlying == nil {
		return nil, fmt.Errorf("retry budget requires an underlying strategy")
	}
	exact := new(big.Rat).SetFloat64(budgetRatio)
	if exact == nil {
		return nil, fmt.Errorf("budget_ratio must be a finite number, got %v", budgetRatio)
	}
	ratio := limitDenominator(exact, big.NewInt(maxRatioDenominator))
	if ratio.Num().Sign() == 0 {
		return nil, fmt.Errorf("budget_ratio %v is below the representable floor 1/%d: it would "+
			"rationalize to a zero deposit, so the budget would never refill", budgetRatio, maxRatioDenominator)
	}
	if !ratio.Num().IsInt64() || !ratio.Denom().IsInt64() {
		return nil, fmt.Errorf("budget_ratio %v does not fit the integer token bucket", budgetRatio)
	}
	p := &RetryBudgetPolicy{
		BudgetRatio: budgetRatio,
		MaxRetries:  maxRetries,
		retryCost:   ratio.Denom().Int64(),
		deposit:     ratio.Num().Int64(),
	}
	p.maxTokens = p.retryCost * int64(maxRetries)
	p.tokens = p.maxTokens // start with full budget
	p.underlying = underlying
	p.throttleAllow = func(int64) bool { return p.CanRetry() }
	p.charge = func() { p.tokens -= p.retryCost }
	// Capped balance: a refund after an intervening deposit clamps at the cap
	// and mints quota. Only the immediate (deadline-veto) refund is exact.
	p.refund = func() { p.tokens = minInt64(p.maxTokens, p.tokens+p.retryCost) }
	return p, nil
}

// OnRequestStart implements Policy: one deposit per admitted request,
// regardless of how it turns out.
func (p *RetryBudgetPolicy) OnRequestStart(int64) {
	p.tokens = minInt64(p.maxTokens, p.tokens+p.deposit)
}

// CanRetry reports whether the bucket holds a whole retry.
func (p *RetryBudgetPolicy) CanRetry() bool { return p.tokens >= p.retryCost }

// TokenBalance is msim's get_token_balance: 100 tokens == one retry.
func (p *RetryBudgetPolicy) TokenBalance() float64 {
	return float64(p.tokens) / float64(p.retryCost) * 100.0
}

// Tokens exposes the raw integer bucket (tests assert on it, as msim's do).
func (p *RetryBudgetPolicy) Tokens() int64 { return p.tokens }

// Deposit exposes the per-request deposit in bucket units.
func (p *RetryBudgetPolicy) Deposit() int64 { return p.deposit }

// RetryCost exposes the per-retry withdrawal in bucket units.
func (p *RetryBudgetPolicy) RetryCost() int64 { return p.retryCost }

// limitDenominator is a port of CPython's Fraction.limit_denominator: the
// closest rational to x whose denominator is at most maxDen.
//
// Ported rather than approximated because msim's budget arithmetic depends on
// it exactly: a ratio of 1/3 must come back as 1/3 and not as the 53-bit binary
// fraction 6004799503160661/18014398509481984, or a budget that should earn a
// retry every three requests never earns one.
func limitDenominator(x *big.Rat, maxDen *big.Int) *big.Rat {
	if maxDen.Sign() < 1 {
		panic("rpcpolicy: max_denominator must be at least 1")
	}
	if x.Denom().Cmp(maxDen) <= 0 {
		return new(big.Rat).Set(x)
	}
	// Sign is handled by working on |x| and restoring it at the end; CPython
	// keeps the sign inside the numerator, and Go's Int arithmetic below
	// assumes non-negative n.
	neg := x.Sign() < 0
	n := new(big.Int).Abs(x.Num())
	d := new(big.Int).Set(x.Denom())

	p0, q0 := big.NewInt(0), big.NewInt(1)
	p1, q1 := big.NewInt(1), big.NewInt(0)
	for {
		a := new(big.Int).Quo(n, d)
		q2 := new(big.Int).Add(q0, new(big.Int).Mul(a, q1))
		if q2.Cmp(maxDen) > 0 {
			break
		}
		p0, q0, p1, q1 = p1, q1, new(big.Int).Add(p0, new(big.Int).Mul(a, p1)), q2
		n, d = d, new(big.Int).Sub(n, new(big.Int).Mul(a, d))
	}

	k := new(big.Int).Quo(new(big.Int).Sub(maxDen, q0), q1)
	// Which of (p0+k*p1)/(q0+k*q1) and p1/q1 is closer to x? The distance
	// between them is 1/(q1*(q0+k*q1)); the distance from p1/q1 to x is
	// d/(q1*x.denom). So compare 2*d*(q0+k*q1) with x.denom.
	qk := new(big.Int).Add(q0, new(big.Int).Mul(k, q1))
	lhs := new(big.Int).Mul(big.NewInt(2), new(big.Int).Mul(d, qk))
	var num, den *big.Int
	if lhs.Cmp(x.Denom()) <= 0 {
		num, den = p1, q1
	} else {
		num, den = new(big.Int).Add(p0, new(big.Int).Mul(k, p1)), qk
	}
	out := new(big.Rat).SetFrac(num, den)
	if neg {
		out.Neg(out)
	}
	return out
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
