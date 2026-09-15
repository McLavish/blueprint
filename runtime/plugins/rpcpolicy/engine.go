package rpcpolicy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// The client attempt loop (docs/PLAN.md WP1 "Client flow"), and the compilation
// of a ProfileConfig into the msim policy chain it names.

// Clock is the time seam. Production uses realClock; the engine tests drive a
// fake one so a backoff, a deadline and a strict end==deadline boundary are
// exact rather than flaky.
//
// NowNS is monotonic nanoseconds from a base captured at process start: msim's
// integer time model, which the policy layer needs because every rational
// (leaky) and integer (budget, fixed window) computation in it is exact.
//
// DeadlineNS and WithTimeout exist so that every strict half-open comparison is
// made against the deadline the callee actually received, in ONE base. Deriving
// it as `NowNS() + deadline.Sub(Now())` mixed two samples of a clock that moves
// between them, and deriving an attempt boundary as `NowNS() + timeout` while
// the attempt context was built from a different clock compared two unrelated
// timelines outright.
type Clock interface {
	Now() time.Time
	NowNS() int64
	// DeadlineNS converts an absolute deadline into the same monotonic base
	// NowNS counts in. It must not observe the clock.
	DeadlineNS(deadline time.Time) int64
	Sleep(ctx context.Context, d time.Duration) error
	// WithTimeout is context.WithTimeout on this clock's timeline, so the
	// deadline the context carries is convertible by DeadlineNS exactly.
	WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc)
}

type realClock struct{ base time.Time }

func newRealClock() realClock { return realClock{base: time.Now()} }

func (c realClock) Now() time.Time { return time.Now() }

func (c realClock) NowNS() int64 { return int64(time.Since(c.base)) }

// DeadlineNS implements Clock. base carries a monotonic reading, so a deadline
// minted by context.WithTimeout (which does too) subtracts monotonically and
// lands on exactly the scale NowNS reports.
func (c realClock) DeadlineNS(deadline time.Time) int64 { return int64(deadline.Sub(c.base)) }

func (c realClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WithTimeout implements Clock.
func (c realClock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

// deadlineNS converts a context deadline into the clock's monotonic ns. The
// value comes from ctx.Deadline() -- the deadline the callee really has -- and
// never from a second sample of the clock.
func deadlineNS(c Clock, ctx context.Context) (int64, bool) {
	d, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return c.DeadlineNS(d), true
}

// clampToDeadline is the instant a record ends at when the thing that ended it
// was the expiry of its own deadline: msim fires the caller's clock AT the
// deadline, so the record ends there however late the goroutine woke to hear
// about it.
//
// It is min(end, deadline), floored at the record's own start: an observation
// taken before the deadline is left where it landed -- the clamp never pushes a
// record's end FORWARD to a deadline nothing reached -- and a deadline that had
// already passed when the attempt began cannot drag the end before the start.
func clampToDeadline(start, end, deadline time.Time) time.Time {
	if end.After(deadline) {
		end = deadline
	}
	if end.Before(start) {
		return start
	}
	return end
}

// profile is a compiled ProfileConfig: the policy chain plus the two timeouts
// and the retryable-code set.
//
// mu guards the whole chain. msim is single-threaded, so every policy method is
// written as if it were; the client attempt loop therefore holds mu across
// AllowRequest/OnRequestStart and across AddResult/NextDelay/Rollback, and
// never across the RPC or the backoff sleep.
type profile struct {
	name          string
	timeout       time.Duration
	globalTimeout time.Duration
	retryOn       map[codes.Code]bool
	head          Policy

	mu sync.Mutex
}

// buildProfile compiles one profile, outermost first:
// rate_limiter > circuit_breaker > budget > retry. `leaky` REPLACES retry
// (msim's LeakyRateLimiterPolicy has no underlying strategy), so it lands at
// the innermost position instead of the outermost one.
func buildProfile(name string, cfg ProfileConfig) (*profile, error) {
	var inner Policy
	limiterIsLeaky := cfg.RateLimiter != nil && cfg.RateLimiter.Enabled && cfg.RateLimiter.Kind == limiterKindLeaky
	if limiterIsLeaky {
		p, err := NewLeakyRateLimiterPolicy(cfg.RateLimiter.MaxRequests, cfg.RateLimiter.MaxAttempts, cfg.RateLimiter.Period.Nanos())
		if err != nil {
			return nil, fmt.Errorf("profile %q rate_limiter: %w", name, err)
		}
		inner = p
	} else {
		p, err := buildRetry(cfg.Retry)
		if err != nil {
			return nil, fmt.Errorf("profile %q retry: %w", name, err)
		}
		inner = p
	}

	head := inner
	if cfg.Budget != nil && cfg.Budget.Enabled {
		p, err := NewRetryBudgetPolicy(cfg.Budget.BudgetRatio, cfg.Budget.MaxRetries, head)
		if err != nil {
			return nil, fmt.Errorf("profile %q budget: %w", name, err)
		}
		head = p
	}
	if cfg.CircuitBreaker != nil && cfg.CircuitBreaker.Enabled {
		p, err := buildBreaker(cfg.CircuitBreaker, head)
		if err != nil {
			return nil, fmt.Errorf("profile %q circuit_breaker: %w", name, err)
		}
		head = p
	}
	if cfg.RateLimiter != nil && cfg.RateLimiter.Enabled && !limiterIsLeaky {
		p, err := buildLimiter(cfg.RateLimiter, head)
		if err != nil {
			return nil, fmt.Errorf("profile %q rate_limiter: %w", name, err)
		}
		head = p
	}

	names := defaultRetryOn
	if cfg.Retry != nil && len(cfg.Retry.RetryOn) > 0 {
		names = cfg.Retry.RetryOn
	}
	retryOn := make(map[codes.Code]bool, len(names))
	for _, n := range names {
		c, ok := parseCode(n)
		if !ok {
			return nil, fmt.Errorf("profile %q retry_on: %q is not a gRPC status name", name, n)
		}
		retryOn[c] = true
	}

	return &profile{
		name:          name,
		timeout:       cfg.Timeout.Duration(),
		globalTimeout: cfg.GlobalTimeout.Duration(),
		retryOn:       retryOn,
		head:          head,
	}, nil
}

func buildRetry(cfg *RetryConfig) (Policy, error) {
	if cfg == nil || !cfg.Enabled {
		return NewNoRetryPolicy(), nil
	}
	switch cfg.Kind {
	case retryKindFixed:
		return NewFixedBackoffRetryPolicy(cfg.MaxAttempts, cfg.Delay.Nanos())
	case retryKindExponential:
		switch cfg.JitterMode {
		case "", "none":
			return NewExponentialBackoffRetryPolicy(cfg.MaxAttempts, cfg.InitialDelay.Nanos(), cfg.MaxDelay.Nanos())
		case "full":
			return NewExponentialBackoffWithJitterRetryPolicy(cfg.MaxAttempts, cfg.InitialDelay.Nanos(), cfg.MaxDelay.Nanos(), JitterFull, nil)
		case "equal":
			return NewExponentialBackoffWithJitterRetryPolicy(cfg.MaxAttempts, cfg.InitialDelay.Nanos(), cfg.MaxDelay.Nanos(), JitterEqual, nil)
		}
		return nil, fmt.Errorf("unknown jitter_mode %q", cfg.JitterMode)
	}
	return nil, fmt.Errorf("unknown retry kind %q", cfg.Kind)
}

func buildBreaker(cfg *CircuitBreakerConfig, underlying Policy) (Policy, error) {
	switch cfg.Kind {
	case breakerKindCount:
		f, err := ratioPair("failure_threshold_ratio", cfg.FailureThresholdRatio)
		if err != nil {
			return nil, err
		}
		s, err := ratioPair("success_threshold_ratio", cfg.SuccessThresholdRatio)
		if err != nil {
			return nil, err
		}
		return NewCountBasedCircuitBreakerPolicy(f, s, cfg.HalfOpenDelay.Nanos(), underlying)
	case breakerKindTime:
		halfOpenMin := cfg.HalfOpenMinRequests
		if halfOpenMin == 0 {
			halfOpenMin = 1
		}
		return NewTimeBasedCircuitBreakerPolicy(cfg.FailureThresholdRate, cfg.SuccessThresholdRate,
			cfg.MinRequests, cfg.WindowDuration.Nanos(), cfg.HalfOpenDelay.Nanos(), halfOpenMin, underlying)
	}
	return nil, fmt.Errorf("unknown circuit_breaker kind %q", cfg.Kind)
}

func buildLimiter(cfg *RateLimiterConfig, underlying Policy) (Policy, error) {
	switch cfg.Kind {
	case limiterKindBursty:
		return NewBurstyRateLimiterPolicy(cfg.MaxRequests, cfg.RefillRate, cfg.Period.Nanos(), underlying)
	case limiterKindFixedWindow:
		return NewFixedWindowBurstyLimiterPolicy(cfg.MaxRequests, cfg.Period.Nanos(), underlying)
	}
	return nil, fmt.Errorf("unknown rate_limiter kind %q for a wrapping limiter", cfg.Kind)
}

// engine runs the client attempt loop against a compiled profile.
type engine struct {
	clock   Clock
	log     *attemptLog
	service string
}

// invokeFunc issues one attempt on the wire. The context it receives already
// carries the per-attempt timeout and the outbound metadata.
type invokeFunc func(ctx context.Context) error

// execute is docs/PLAN.md WP1's "Client flow", a transcription of msim's
// ClientRuntime.start_request + _start_attempt + _on_attempt_done.
//
// peer is the dial target of the connection every attempt of this request goes
// out on (CONTRACTS.md §5); it is recorded even when the gate denies the
// request, because the connection it would have used is still known.
func (e *engine) execute(inbound context.Context, prof *profile, route, operation, peer string, invoke invokeFunc) error {
	parent, _ := traceCtxFrom(inbound)
	traceID := parent.traceID
	if traceID == "" {
		traceID = newTraceID()
	}
	if route == "" {
		route = parent.route
	}

	ctx := inbound
	if prof.globalTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = e.clock.WithTimeout(inbound, prof.globalTimeout)
		defer cancel()
	}
	rootDeadline, hasRootDeadline := deadlineNS(e.clock, ctx)

	// Admission: every layer must allow, outermost first, short-circuiting.
	prof.mu.Lock()
	if !allowPolicyRequest(prof.head, e.clock.NowNS()) {
		prof.mu.Unlock()
		now := e.clock.Now()
		_ = e.log.Write(&ClientRecord{
			baseRecord: baseRecord{
				Kind: "client", TraceID: traceID, SpanID: newSpanID(), ParentSpanID: parent.spanID,
				Service: e.service, Operation: operation,
				StartEpoch: epochOf(now), EndEpoch: epochOf(now), DurationMS: 0,
				IsError: true, ResponseCode: codes.Unavailable.String(),
			},
			Route: route, Profile: prof.name, Attempt: 1, Peer: peer, RetryDelayMS: 0,
			Gate: gateCircuitOpen, DropReason: dropNone, RetryDenied: deniedNone,
		})
		return status.Errorf(codes.Unavailable, "rpcpolicy: %s: circuit open", operation)
	}
	// One deposit per ADMITTED request, below the gate: a request an outer
	// circuit breaker sheds must not fund retries for the few that get through.
	notifyRequestStart(prof.head, e.clock.NowNS())
	prof.mu.Unlock()

	attempt := 1
	retryDelay := int64(0)
	for {
		spanID := newSpanID()

		// The attempt boundary is read back OFF the context the invoker is
		// handed, after it exists: context.WithTimeout already takes the min of
		// the per-attempt timeout and any inherited root deadline, and reading
		// it back is the only way the comparison below is guaranteed to be the
		// same instant the invoker will be cut at.
		attemptCtx, cancel := e.clock.WithTimeout(ctx, prof.timeout)
		attemptDeadlineWall, hasAttemptDeadline := attemptCtx.Deadline()
		attemptDeadline := e.clock.DeadlineNS(attemptDeadlineWall)
		outCtx := metadata.AppendToOutgoingContext(attemptCtx,
			TraceparentKey, formatTraceparent(traceID, spanID),
			RouteKey, route,
		)
		startWall := e.clock.Now()
		err := invoke(outCtx)
		endWall := e.clock.Now()
		end := e.clock.NowNS()
		// Whether it was THIS attempt's own deadline that ended it -- its
		// per-attempt timeout or the root deadline it inherited, whichever
		// WithTimeout made the nearer. Read before cancel(), which would turn the
		// same context's Err() into a plain Canceled.
		//
		// It is one of the two signals the clamp below accepts, and the weaker
		// one: it reports the context's TIMER, which fires a little after the
		// instant the deadline names (see there).
		localExpiry := errors.Is(attemptCtx.Err(), context.DeadlineExceeded)
		cancel()

		code := codeOf(err)
		// Strict half-open deadline: success requires end < deadline. The same
		// rule runs on the server side, so both books the same attempt the
		// same way.
		strictConverted := false
		if code == codes.OK && hasAttemptDeadline && end >= attemptDeadline {
			code, strictConverted = codes.DeadlineExceeded, true
			err = status.Errorf(codes.DeadlineExceeded, "rpcpolicy: %s: attempt finished at its deadline", operation)
		}
		success := code == codes.OK

		// msim books a timeout AT the deadline: the caller's clock fires there,
		// and the attempt ends there. A Go client learns of its own expiry only
		// when its goroutine is next scheduled, and the pipeline buckets every
		// caller-side column at the client record's END -- so a wake-up ten
		// milliseconds late moved the timeout into the next bucket, where the
		// simulator had booked it in this one.
		//
		// Only this attempt's OWN deadline may move the record: a DeadlineExceeded
		// the callee returned while the local context is still live is a real
		// observation at a real instant (the callee's deadline is its own, and
		// nearer), and so is every other status. The retry engine still decides at
		// wake-up, off `end`, because it has nothing else to decide with -- it is
		// the RECORD that ends at the deadline.
		//
		// TWO things say this attempt's own deadline passed, and either is enough.
		// The strict rule above is the stronger of them: converting the result IS
		// the assertion end >= attemptDeadline, made on the one clock both sides
		// compare against, while attemptCtx.Err() only becomes DeadlineExceeded
		// once the runtime has got around to running the context's timer callback.
		// An OK returned two microseconds past D and converted here while Err() was
		// still nil therefore used to end at the observation -- a late wake-up
		// included -- where msim books it at D.
		//
		// The reverse is possible and is ACCEPTED: a callee-originated
		// DeadlineExceeded that really arrived at 0.999 s under a local 1.000 s
		// deadline, whose goroutine was then descheduled past 1.000 s before it
		// read Err(), is clamped to 1.000 s. Once this goroutine has slept past its
		// own deadline, nothing in the process can still tell that earlier arrival
		// from the local expiry -- and of the two instants it could be given, the
		// deadline is the nearer to the one it really had.
		//
		// The clamp itself is min(now, deadline), floored at the record's start.
		if code == codes.DeadlineExceeded && (strictConverted || localExpiry) && hasAttemptDeadline {
			endWall = clampToDeadline(startWall, endWall, attemptDeadlineWall)
		}

		rec := &ClientRecord{
			baseRecord: baseRecord{
				Kind: "client", TraceID: traceID, SpanID: spanID, ParentSpanID: parent.spanID,
				Service: e.service, Operation: operation,
				StartEpoch: epochOf(startWall), EndEpoch: epochOf(endWall),
				DurationMS: msOf(endWall.Sub(startWall)),
				IsError:    !success, ResponseCode: code.String(),
			},
			Route: route, Profile: prof.name, Attempt: attempt, Peer: peer,
			RetryDelayMS: msOf(time.Duration(retryDelay)),
			Gate:         "", DropReason: dropReasonFor(code), RetryDenied: deniedNone,
		}

		// Abandoned by the caller, NOT failed. Return before any feedback: this
		// attempt is not evidence about the callee, and feeding it to a breaker
		// would trip it on work nobody waited for.
		if errors.Is(inbound.Err(), context.Canceled) {
			rec.ResponseCode = codes.Canceled.String()
			rec.IsError = true
			rec.DropReason = dropCancelled
			_ = e.log.Write(rec)
			if err == nil {
				err = status.FromContextError(context.Canceled).Err()
			}
			return err
		}

		prof.mu.Lock()
		updatePolicyResults(prof.head, success, end)
		if success {
			prof.mu.Unlock()
			_ = e.log.Write(rec)
			return nil
		}
		if !prof.retryOn[code] {
			prof.mu.Unlock()
			rec.RetryDenied = deniedNotRetryable
			_ = e.log.Write(rec)
			return err
		}
		rctx := RetryContext{Attempt: attempt, Now: end}
		decision := prof.head.NextDelay(rctx)
		nextStart := end + decision.Delay
		veto := hasRootDeadline && nextStart >= rootDeadline
		if !decision.Allow || veto {
			// NextDelay is side-effecting for throttling policies. When the
			// policy authorised the retry and the deadline then vetoes it,
			// refund the charge immediately -- allow=false charged nothing.
			if decision.Allow {
				rollbackPolicy(prof.head, rctx, false)
			}
			prof.mu.Unlock()
			if veto && decision.Allow {
				rec.RetryDenied = deniedDeadlineVeto
			} else {
				rec.RetryDenied = deniedExhausted
			}
			_ = e.log.Write(rec)
			return err
		}
		prof.mu.Unlock()
		_ = e.log.Write(rec)

		if serr := e.clock.Sleep(ctx, time.Duration(decision.Delay)); serr != nil {
			// The root gave up during the backoff. msim refunds through the
			// DEFERRED rollback path, which every policy here declines: a late
			// refund would rewind a reservation belonging to a different live
			// root. The charge therefore stands, which over-counts rather than
			// handing the same slot out twice.
			prof.mu.Lock()
			rollbackPolicy(prof.head, rctx, true)
			prof.mu.Unlock()
			if errors.Is(inbound.Err(), context.Canceled) {
				return status.FromContextError(context.Canceled).Err()
			}
			return err
		}
		retryDelay = decision.Delay
		attempt++
	}
}
