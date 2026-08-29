package rpcpolicy

import (
	"fmt"
	"math/rand"
	"sync"
)

// The four pure retry policies of msim/retry_policies.py: NoRetryPolicy,
// FixedBackoffRetryPolicy, ExponentialBackoffRetryPolicy and
// ExponentialBackoffWithJitterRetryPolicy.

// NoRetryPolicy always denies. Allowance 1 (the original attempt only).
type NoRetryPolicy struct{ basePolicy }

// NewNoRetryPolicy returns the allowance-1 policy.
func NewNoRetryPolicy() *NoRetryPolicy { return &NoRetryPolicy{} }

// NextDelay implements Policy.
func (p *NoRetryPolicy) NextDelay(RetryContext) DelayDecision { return DelayDecision{false, 0} }

// FixedBackoffRetryPolicy retries up to MaxAttempts total attempts, waiting a
// constant Delay between them.
type FixedBackoffRetryPolicy struct {
	basePolicy
	MaxAttempts int   // total attempts including the initial try
	Delay       int64 // nanoseconds
}

// NewFixedBackoffRetryPolicy validates exactly as msim's __post_init__.
func NewFixedBackoffRetryPolicy(maxAttempts int, delay int64) (*FixedBackoffRetryPolicy, error) {
	if maxAttempts < 1 {
		return nil, fmt.Errorf("max_attempts must be >= 1, got %d", maxAttempts)
	}
	if delay < 0 {
		return nil, fmt.Errorf("delay must be >= 0 ns, got %d", delay)
	}
	return &FixedBackoffRetryPolicy{MaxAttempts: maxAttempts, Delay: delay}, nil
}

// NextDelay implements Policy.
func (p *FixedBackoffRetryPolicy) NextDelay(ctx RetryContext) DelayDecision {
	if ctx.Attempt < p.MaxAttempts {
		return DelayDecision{true, p.Delay}
	}
	return DelayDecision{false, 0}
}

// ExponentialBackoffRetryPolicy doubles the delay each attempt, capped at
// MaxDelay.
type ExponentialBackoffRetryPolicy struct {
	basePolicy
	MaxAttempts  int
	InitialDelay int64
	MaxDelay     int64
}

// NewExponentialBackoffRetryPolicy validates exactly as msim's __post_init__.
func NewExponentialBackoffRetryPolicy(maxAttempts int, initialDelay, maxDelay int64) (*ExponentialBackoffRetryPolicy, error) {
	if maxAttempts < 1 {
		return nil, fmt.Errorf("max_attempts must be >= 1, got %d", maxAttempts)
	}
	if initialDelay < 0 || maxDelay < 0 {
		return nil, fmt.Errorf("delays must be >= 0 ns, got initial_delay=%d max_delay=%d", initialDelay, maxDelay)
	}
	return &ExponentialBackoffRetryPolicy{MaxAttempts: maxAttempts, InitialDelay: initialDelay, MaxDelay: maxDelay}, nil
}

// expBackoff is msim's min(initial_delay * 2**(attempt-1), max_delay),
// evaluated so it saturates at max_delay instead of overflowing int64. Python
// promotes to a bignum and then takes the min; doubling past the cap here can
// only produce the cap, so the two agree for every attempt number.
func expBackoff(initial, max int64, attempt int) int64 {
	d := initial
	for i := 1; i < attempt; i++ {
		if d >= max {
			return max
		}
		if d > max/2 {
			// 2*d would exceed the cap, and the cap is what min() returns.
			return max
		}
		d *= 2
	}
	if d > max {
		return max
	}
	return d
}

// NextDelay implements Policy.
func (p *ExponentialBackoffRetryPolicy) NextDelay(ctx RetryContext) DelayDecision {
	if ctx.Attempt < p.MaxAttempts {
		return DelayDecision{true, expBackoff(p.InitialDelay, p.MaxDelay, ctx.Attempt)}
	}
	return DelayDecision{false, 0}
}

// JitterMode selects how the exponential delay is randomised.
type JitterMode int

// The jitter modes msim implements. DECORRELATED is declared in msim and
// raises NotImplementedError; it has no YAML spelling here and is therefore
// not represented.
const (
	// JitterFull draws U(0, d).
	JitterFull JitterMode = iota
	// JitterEqual draws d/2 + U(0, d/2).
	JitterEqual
)

// String implements fmt.Stringer.
func (m JitterMode) String() string {
	switch m {
	case JitterFull:
		return "full"
	case JitterEqual:
		return "equal"
	default:
		return fmt.Sprintf("JitterMode(%d)", int(m))
	}
}

// ExponentialBackoffWithJitterRetryPolicy is msim's jittered exponential
// backoff. The random source is injectable so tests can pin it; production
// uses a mutex-guarded *rand.Rand rather than the global source, because
// several in-flight RPCs share one profile.
type ExponentialBackoffWithJitterRetryPolicy struct {
	basePolicy
	MaxAttempts  int
	InitialDelay int64
	MaxDelay     int64
	Mode         JitterMode

	mu     sync.Mutex
	random func() float64 // uniform on [0, 1)
}

// NewExponentialBackoffWithJitterRetryPolicy validates exactly as msim's
// __post_init__. random may be nil, in which case a seeded *rand.Rand is used.
func NewExponentialBackoffWithJitterRetryPolicy(maxAttempts int, initialDelay, maxDelay int64, mode JitterMode, random func() float64) (*ExponentialBackoffWithJitterRetryPolicy, error) {
	if maxAttempts < 1 {
		return nil, fmt.Errorf("max_attempts must be >= 1, got %d", maxAttempts)
	}
	if initialDelay < 0 || maxDelay < 0 {
		return nil, fmt.Errorf("delays must be >= 0 ns, got initial_delay=%d max_delay=%d", initialDelay, maxDelay)
	}
	if mode != JitterFull && mode != JitterEqual {
		return nil, fmt.Errorf("unknown jitter mode %d", int(mode))
	}
	if random == nil {
		r := rand.New(rand.NewSource(randomSeed()))
		random = r.Float64
	}
	return &ExponentialBackoffWithJitterRetryPolicy{
		MaxAttempts:  maxAttempts,
		InitialDelay: initialDelay,
		MaxDelay:     maxDelay,
		Mode:         mode,
		random:       random,
	}, nil
}

// NextDelay implements Policy.
//
// msim computes the jittered delay in floating point and truncates with int();
// the same truncation is reproduced here (int64 conversion truncates toward
// zero and the value is never negative).
func (p *ExponentialBackoffWithJitterRetryPolicy) NextDelay(ctx RetryContext) DelayDecision {
	if ctx.Attempt >= p.MaxAttempts {
		return DelayDecision{false, 0}
	}
	exp := float64(expBackoff(p.InitialDelay, p.MaxDelay, ctx.Attempt))
	p.mu.Lock()
	r := p.random()
	p.mu.Unlock()
	var delay float64
	switch p.Mode {
	case JitterEqual:
		// random.uniform(a, b) == a + (b-a)*random()
		delay = exp/2 + (exp/2)*r
	default:
		delay = exp * r
	}
	return DelayDecision{true, int64(delay)}
}
