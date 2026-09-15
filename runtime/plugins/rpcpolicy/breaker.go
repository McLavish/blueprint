package rpcpolicy

import "fmt"

// The two circuit breakers of msim/retry_policies.py, sharing the admission
// gate and the OPEN -> HALF_OPEN transition exactly as msim's
// _CircuitBreakerBase does.

// CBState is msim's CBState.
type CBState int

// Breaker states.
const (
	// CBClosed passes everything and delegates retries downstream.
	CBClosed CBState = iota
	// CBOpen sheds every root request and denies every retry.
	CBOpen
	// CBHalfOpen is the trial stage after the open delay.
	//
	// NOTE (inherited from msim, deliberately): probe admission is NOT capped.
	// AllowRequest admits every request while half-open instead of permitting a
	// fixed number of trial calls, so half-open offers no load protection on the
	// recovering service. Reproducing msim is the point; do not "fix" it here
	// without changing msim in the same commit.
	CBHalfOpen
)

// String implements fmt.Stringer.
func (s CBState) String() string {
	switch s {
	case CBClosed:
		return "closed"
	case CBOpen:
		return "open"
	case CBHalfOpen:
		return "half_open"
	default:
		return fmt.Sprintf("CBState(%d)", int(s))
	}
}

// breakerBase is the half both breakers share: admission, the OPEN ->
// HALF_OPEN transition and retry delegation. Only "what is the probe window"
// differs, so that is the one hook.
type breakerBase struct {
	halfOpenDelay int64
	underlying    Policy
	state         CBState
	openedAt      int64
	hasOpenedAt   bool

	resetProbeWindow func()
}

// GetState returns the current breaker state.
func (b *breakerBase) GetState() CBState { return b.state }

// Underlying implements Policy.
func (b *breakerBase) Underlying() Policy { return b.underlying }

// OnRequestStart implements Policy (the breaker itself banks nothing).
func (b *breakerBase) OnRequestStart(int64) {}

// Rollback implements Policy (the breaker charges nothing).
func (b *breakerBase) Rollback(RetryContext) {}

// SupportsDeferredRollback implements Policy.
func (b *breakerBase) SupportsDeferredRollback() bool { return false }

// AllowRequest implements Policy. Half-open admits EVERY request; see CBHalfOpen.
func (b *breakerBase) AllowRequest(now int64) bool {
	if b.state != CBOpen {
		return true
	}
	if !b.hasOpenedAt || b.halfOpenDelay == 0 || now-b.openedAt >= b.halfOpenDelay {
		b.state = CBHalfOpen
		b.resetProbeWindow()
		return true
	}
	return false
}

// NextDelay implements Policy: anything but CLOSED denies the retry outright.
func (b *breakerBase) NextDelay(ctx RetryContext) DelayDecision {
	if b.state != CBClosed {
		return DelayDecision{false, 0}
	}
	return b.underlying.NextDelay(ctx)
}

// boolRing is a fixed-capacity FIFO of outcomes, the Go stand-in for
// collections.deque(maxlen=n): appending to a full ring drops the oldest.
type boolRing struct {
	buf    []bool
	head   int
	n      int
	maxlen int
}

func newBoolRing(maxlen int) *boolRing {
	return &boolRing{buf: make([]bool, maxlen), maxlen: maxlen}
}

func (r *boolRing) append(v bool) {
	if r.n == r.maxlen {
		r.buf[r.head] = v
		r.head = (r.head + 1) % r.maxlen
		return
	}
	r.buf[(r.head+r.n)%r.maxlen] = v
	r.n++
}

func (r *boolRing) len() int { return r.n }

func (r *boolRing) clear() { r.head, r.n = 0, 0 }

func (r *boolRing) count(want bool) int {
	c := 0
	for i := 0; i < r.n; i++ {
		if r.buf[(r.head+i)%r.maxlen] == want {
			c++
		}
	}
	return c
}

// CountBasedCircuitBreakerPolicy opens on f failures out of the last n results
// and closes on s successes out of the next m probes.
type CountBasedCircuitBreakerPolicy struct {
	breakerBase
	failureThreshold [2]int // (failures, total)
	successThreshold [2]int // (successes, total)
	closedWindow     *boolRing
	openWindow       *boolRing
}

// NewCountBasedCircuitBreakerPolicy validates exactly as msim's __post_init__.
func NewCountBasedCircuitBreakerPolicy(failureThreshold, successThreshold [2]int, halfOpenDelay int64, underlying Policy) (*CountBasedCircuitBreakerPolicy, error) {
	f, ftot := failureThreshold[0], failureThreshold[1]
	s, stot := successThreshold[0], successThreshold[1]
	if ftot < 1 || f < 0 || f > ftot {
		return nil, fmt.Errorf("failure_threshold_ratio must be (failures, total) with 0 <= failures <= total and total >= 1, got %v", failureThreshold)
	}
	if stot < 1 || s < 0 || s > stot {
		return nil, fmt.Errorf("success_threshold_ratio must be (successes, total) with 0 <= successes <= total and total >= 1, got %v", successThreshold)
	}
	if halfOpenDelay < 0 {
		return nil, fmt.Errorf("half_open_delay must be >= 0 ns, got %d", halfOpenDelay)
	}
	if underlying == nil {
		return nil, fmt.Errorf("circuit breaker requires an underlying strategy")
	}
	p := &CountBasedCircuitBreakerPolicy{
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		closedWindow:     newBoolRing(ftot),
		openWindow:       newBoolRing(stot),
	}
	p.halfOpenDelay = halfOpenDelay
	p.underlying = underlying
	p.state = CBClosed
	p.resetProbeWindow = func() { p.openWindow.clear() }
	return p, nil
}

// AddResult implements ResultRecorder.
func (p *CountBasedCircuitBreakerPolicy) AddResult(success bool, now int64) {
	switch p.state {
	case CBClosed:
		p.closedWindow.append(success)
		if p.closedWindow.len() == p.closedWindow.maxlen {
			if p.closedWindow.count(false) >= p.failureThreshold[0] {
				p.state = CBOpen
				p.openWindow.clear()
				p.openedAt, p.hasOpenedAt = now, true
			}
		}
	case CBOpen:
		return
	case CBHalfOpen:
		p.openWindow.append(success)
		if p.openWindow.len() == p.openWindow.maxlen {
			if p.openWindow.count(true) >= p.successThreshold[0] {
				p.state = CBClosed
				p.closedWindow.clear()
				p.openedAt, p.hasOpenedAt = 0, false
			} else {
				p.state = CBOpen
				p.openWindow.clear()
				p.openedAt, p.hasOpenedAt = now, true
			}
		}
	}
}

// timedResult is one (timestamp, outcome) sample of the sliding window.
type timedResult struct {
	at int64
	ok bool
}

// TimeBasedCircuitBreakerPolicy is msim's sliding-time-window breaker.
type TimeBasedCircuitBreakerPolicy struct {
	breakerBase
	failureRate         float64
	successRate         float64
	minRequests         int
	windowDuration      int64
	halfOpenMinRequests int

	window           []timedResult // FIFO, oldest first
	failuresInWindow int
}

// NewTimeBasedCircuitBreakerPolicy validates exactly as msim's __post_init__.
func NewTimeBasedCircuitBreakerPolicy(failureRate, successRate float64, minRequests int, windowDuration int64, halfOpenDelay int64, halfOpenMinRequests int, underlying Policy) (*TimeBasedCircuitBreakerPolicy, error) {
	if !(failureRate >= 0 && failureRate <= 1) {
		return nil, fmt.Errorf("failure_threshold_rate must be in [0, 1], got %v", failureRate)
	}
	if !(successRate >= 0 && successRate <= 1) {
		return nil, fmt.Errorf("success_threshold_rate must be in [0, 1], got %v", successRate)
	}
	if minRequests < 1 {
		return nil, fmt.Errorf("min_requests must be >= 1, got %d", minRequests)
	}
	if halfOpenMinRequests < 1 {
		return nil, fmt.Errorf("half_open_min_requests must be >= 1, got %d", halfOpenMinRequests)
	}
	// A non-positive window evicts every prior result on the next AddResult
	// (now - t > window_duration holds for t == now), so the window could never
	// reach min_requests and the breaker would silently never open.
	if windowDuration <= 0 {
		return nil, fmt.Errorf("window_duration must be > 0 ns, got %d", windowDuration)
	}
	if halfOpenDelay < 0 {
		return nil, fmt.Errorf("half_open_delay must be >= 0 ns, got %d", halfOpenDelay)
	}
	if underlying == nil {
		return nil, fmt.Errorf("circuit breaker requires an underlying strategy")
	}
	p := &TimeBasedCircuitBreakerPolicy{
		failureRate:         failureRate,
		successRate:         successRate,
		minRequests:         minRequests,
		windowDuration:      windowDuration,
		halfOpenMinRequests: halfOpenMinRequests,
	}
	p.halfOpenDelay = halfOpenDelay
	p.underlying = underlying
	p.state = CBClosed
	p.resetProbeWindow = p.clearWindow
	return p, nil
}

func (p *TimeBasedCircuitBreakerPolicy) evictOld(now int64) {
	for len(p.window) > 0 && (now-p.window[0].at) > p.windowDuration {
		if !p.window[0].ok {
			p.failuresInWindow--
		}
		p.window = p.window[1:]
	}
}

func (p *TimeBasedCircuitBreakerPolicy) clearWindow() {
	p.window = nil
	p.failuresInWindow = 0
}

// AddResult implements ResultRecorder. Incremental counters instead of a full
// window rescan per result, as msim does.
func (p *TimeBasedCircuitBreakerPolicy) AddResult(success bool, now int64) {
	p.evictOld(now)
	if p.state != CBOpen {
		p.window = append(p.window, timedResult{at: now, ok: success})
		if !success {
			p.failuresInWindow++
		}
	}

	total := len(p.window)
	failures := p.failuresInWindow
	rate := 0.0
	if total > 0 {
		rate = float64(failures) / float64(total)
	}

	switch p.state {
	case CBClosed:
		if total >= p.minRequests && rate > p.failureRate {
			p.state = CBOpen
			p.openedAt, p.hasOpenedAt = now, true
		}
	case CBOpen:
		return
	case CBHalfOpen:
		// Half-open needs its own, smaller quorum: an open circuit suppresses
		// traffic, so requiring the closed-state quorum can strand the breaker
		// in HALF_OPEN forever.
		probes := p.minRequests
		if p.halfOpenMinRequests < probes {
			probes = p.halfOpenMinRequests
		}
		if probes < 1 {
			probes = 1
		}
		// Close when the probes' SUCCESS rate reaches the threshold -- the same
		// `>=` the count-based breaker applies to its success ratio. Compared on
		// the success side rather than as `rate < 1 - thr`: the strict form could
		// never be met at thr=1.0 (a breaker asked to close only on all-successful
		// probes re-opened on two successes), and the float `1 - 0.8` is not 0.2,
		// so 4 of 5 probes missed it too.
		successRate := 0.0
		if total > 0 {
			successRate = float64(total-failures) / float64(total)
		}
		if total >= probes && successRate >= p.successRate {
			p.state = CBClosed
			p.openedAt, p.hasOpenedAt = 0, false
			// Fresh metrics after recovery; otherwise half-open probe results
			// would seed the closed-state failure rate.
			p.clearWindow()
		} else if total >= probes {
			p.state = CBOpen
			p.openedAt, p.hasOpenedAt = now, true
			p.clearWindow()
		}
	}
}

// windowLen exposes the live sample count (tests assert the incremental
// counters match a full rescan).
func (p *TimeBasedCircuitBreakerPolicy) windowLen() int { return len(p.window) }
