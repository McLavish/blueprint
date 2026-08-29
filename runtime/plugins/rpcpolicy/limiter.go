package rpcpolicy

import (
	"fmt"
	"math/big"
)

// The three rate limiters of msim/retry_policies.py.

// throttled is msim's _ThrottledRetryPolicy: a wrapping policy that spends one
// unit of quota per ISSUED retry. The charge/refund contract the client attempt
// loop relies on lives here, in one place, for every quota-carrying policy:
// charge only on the path that returns Allow=true (the client asks for a delay
// before it knows the global deadline will veto the retry), and refund exactly
// one unit when it then declines.
type throttled struct {
	underlying    Policy
	throttleAllow func(now int64) bool
	charge        func()
	refund        func()
}

// Underlying implements Policy.
func (t *throttled) Underlying() Policy { return t.underlying }

// AllowRequest implements Policy.
func (t *throttled) AllowRequest(int64) bool { return true }

// OnRequestStart implements Policy.
func (t *throttled) OnRequestStart(int64) {}

// SupportsDeferredRollback implements Policy.
func (t *throttled) SupportsDeferredRollback() bool { return false }

// NextDelay implements Policy.
func (t *throttled) NextDelay(ctx RetryContext) DelayDecision {
	if !t.throttleAllow(ctx.Now) {
		return DelayDecision{false, 0}
	}
	d := t.underlying.NextDelay(ctx)
	if !d.Allow {
		return DelayDecision{false, 0}
	}
	t.charge()
	return DelayDecision{true, d.Delay}
}

// Rollback implements Policy: refund a withdrawal for a retry the client then
// declined to issue.
func (t *throttled) Rollback(RetryContext) { t.refund() }

// LeakyRateLimiterPolicy is msim's leaky bucket as a QUEUE: it does not shed,
// it spaces. It REPLACES the retry block rather than wrapping one, which is
// why it has no underlying strategy.
type LeakyRateLimiterPolicy struct {
	basePolicy
	MaxRequests int
	MaxAttempts int
	Period      int64

	interval *big.Rat

	// Next free slot, kept as an EXACT rational instant in ns. Rounding it
	// every step drops the same remainder each time, so the achieved spacing
	// becomes floor(period / max_requests) and the bucket runs permanently
	// fast (3 per 100 ns spaced 33 ns apart admits four in [0, 100)). The
	// emitted delay is still an integer number of nanoseconds.
	nextAt     *big.Rat
	prevNextAt *big.Rat
	// slotReserved distinguishes "no reservation" from "reservation whose
	// previous value happened to be nil".
	slotReserved bool
}

// NewLeakyRateLimiterPolicy validates exactly as msim's __post_init__.
func NewLeakyRateLimiterPolicy(maxRequests, maxAttempts int, period int64) (*LeakyRateLimiterPolicy, error) {
	if maxRequests < 1 {
		return nil, fmt.Errorf("max_requests must be >= 1, got %d", maxRequests)
	}
	if maxAttempts < 1 {
		return nil, fmt.Errorf("max_attempts must be >= 1, got %d", maxAttempts)
	}
	if period <= 0 {
		return nil, fmt.Errorf("period must be > 0 ns, got %d", period)
	}
	return &LeakyRateLimiterPolicy{
		MaxRequests: maxRequests,
		MaxAttempts: maxAttempts,
		Period:      period,
		interval:    new(big.Rat).SetFrac64(period, int64(maxRequests)),
	}, nil
}

// ratCeilMinus returns ceil(x) - now, floored at zero.
func ratCeilMinus(x *big.Rat, now int64) int64 {
	num, den := x.Num(), x.Denom()
	q, r := new(big.Int).QuoRem(num, den, new(big.Int))
	// Go's Quo truncates toward zero; ceil adds one only for a positive
	// remainder.
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	d := q.Int64() - now
	if d < 0 {
		return 0
	}
	return d
}

// AddRequest reserves the next slot and returns the wait until it, as msim's
// add_request does.
func (p *LeakyRateLimiterPolicy) AddRequest(now int64) int64 {
	nowRat := new(big.Rat).SetInt64(now)
	startAt := nowRat
	if p.nextAt != nil && p.nextAt.Cmp(nowRat) > 0 {
		startAt = new(big.Rat).Set(p.nextAt)
	}
	p.prevNextAt = p.nextAt
	p.slotReserved = true
	p.nextAt = new(big.Rat).Add(startAt, p.interval)
	// Retries are issued on integer-ns events, so a slot lands on the first
	// instant at or after it -- never before, which would admit early.
	return ratCeilMinus(startAt, now)
}

// NextDelay implements Policy.
func (p *LeakyRateLimiterPolicy) NextDelay(ctx RetryContext) DelayDecision {
	if ctx.Attempt >= p.MaxAttempts {
		return DelayDecision{false, 0}
	}
	return DelayDecision{true, p.AddRequest(ctx.Now)}
}

// Rollback implements Policy: release the slot a vetoed retry reserved.
func (p *LeakyRateLimiterPolicy) Rollback(RetryContext) {
	if !p.slotReserved {
		return
	}
	p.nextAt = p.prevNextAt
	p.prevNextAt = nil
	p.slotReserved = false
}

// BurstyRateLimiterPolicy is msim's continuously refilling float token bucket.
type BurstyRateLimiterPolicy struct {
	throttled
	MaxRequests int
	RefillRate  float64
	Period      int64

	tokens        float64
	lastRefill    int64
	hasLastRefill bool
}

// NewBurstyRateLimiterPolicy validates exactly as msim's __post_init__.
func NewBurstyRateLimiterPolicy(maxRequests int, refillRate float64, period int64, underlying Policy) (*BurstyRateLimiterPolicy, error) {
	if maxRequests <= 0 {
		return nil, fmt.Errorf("max_requests must be > 0, got %d", maxRequests)
	}
	if refillRate < 0 {
		return nil, fmt.Errorf("refill_rate must be >= 0, got %v", refillRate)
	}
	if period <= 0 {
		return nil, fmt.Errorf("period must be > 0, got %d", period)
	}
	if underlying == nil {
		return nil, fmt.Errorf("bursty rate limiter requires an underlying strategy")
	}
	p := &BurstyRateLimiterPolicy{
		MaxRequests: maxRequests,
		RefillRate:  refillRate,
		Period:      period,
		tokens:      float64(maxRequests),
	}
	p.underlying = underlying
	p.throttleAllow = p.CanRetry
	p.charge = func() { p.tokens-- }
	// Capped, continuously refilling balance: a refund arriving after an
	// intervening refill would MINT quota, which is why deferred rollback is
	// off.
	p.refund = func() {
		p.tokens = minFloat(float64(p.MaxRequests), p.tokens+1.0)
	}
	return p, nil
}

func (p *BurstyRateLimiterPolicy) refillTokens(now int64) {
	if !p.hasLastRefill {
		p.lastRefill, p.hasLastRefill = now, true
		return
	}
	elapsed := now - p.lastRefill
	if elapsed < 0 {
		elapsed = 0
	}
	tokensToAdd := (float64(elapsed) * p.RefillRate) / float64(p.Period)
	p.tokens = minFloat(float64(p.MaxRequests), p.tokens+tokensToAdd)
	p.lastRefill = now
}

// CanRetry refills and reports whether a whole token is available.
func (p *BurstyRateLimiterPolicy) CanRetry(now int64) bool {
	p.refillTokens(now)
	return p.tokens >= 1.0
}

// Tokens exposes the live balance (tests assert on it, as msim's do).
func (p *BurstyRateLimiterPolicy) Tokens() float64 { return p.tokens }

// FixedWindowBurstyLimiterPolicy is msim's fixed-window integer token bucket.
type FixedWindowBurstyLimiterPolicy struct {
	throttled
	MaxRequests int
	Period      int64

	tokens         int
	windowStart    int64
	hasWindowStart bool
}

// NewFixedWindowBurstyLimiterPolicy validates exactly as msim's __post_init__.
func NewFixedWindowBurstyLimiterPolicy(maxRequests int, period int64, underlying Policy) (*FixedWindowBurstyLimiterPolicy, error) {
	if maxRequests < 1 {
		return nil, fmt.Errorf("max_requests must be >= 1, got %d", maxRequests)
	}
	if period <= 0 {
		return nil, fmt.Errorf("period must be > 0 ns, got %d", period)
	}
	if underlying == nil {
		return nil, fmt.Errorf("fixed-window rate limiter requires an underlying strategy")
	}
	p := &FixedWindowBurstyLimiterPolicy{
		MaxRequests: maxRequests,
		Period:      period,
		tokens:      maxRequests,
	}
	p.underlying = underlying
	p.throttleAllow = p.CanRetry
	p.charge = func() { p.tokens-- }
	// Refunds land in the window the charge was made in: the client vetoes at
	// the same instant, so no window rollover intervenes.
	p.refund = func() {
		p.tokens = minInt(p.MaxRequests, p.tokens+1)
	}
	return p, nil
}

func (p *FixedWindowBurstyLimiterPolicy) currentWindowStart(now int64) int64 {
	return floorDiv(now, p.Period) * p.Period
}

func (p *FixedWindowBurstyLimiterPolicy) refillTokens(now int64) {
	start := p.currentWindowStart(now)
	if !p.hasWindowStart || start > p.windowStart {
		p.windowStart, p.hasWindowStart = start, true
		p.tokens = p.MaxRequests
	}
}

// CanRetry rolls the window over and reports whether a token is available.
func (p *FixedWindowBurstyLimiterPolicy) CanRetry(now int64) bool {
	p.refillTokens(now)
	return p.tokens > 0
}

// NextRefillTime is msim's get_next_refill_time.
func (p *FixedWindowBurstyLimiterPolicy) NextRefillTime(now int64) int64 {
	return p.currentWindowStart(now) + p.Period
}

// Tokens exposes the live balance.
func (p *FixedWindowBurstyLimiterPolicy) Tokens() int { return p.tokens }

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// floorDiv is Python's // on ints: it floors toward negative infinity, where
// Go's / truncates toward zero. Monotonic time is never negative here, but the
// window arithmetic is written to match msim's exactly all the same.
func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}
