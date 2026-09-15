package rpcpolicy

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testMethod = "/grpc.NodeService/Call"

// testPeer is the dial target the engine records as `peer`, in the form the
// deployment writes it: <container>:<port> (CONTRACTS.md §5).
const testPeer = "svc_a_container:12345"

func newTestEngine(t *testing.T) (*engine, *fakeClock, *testLog) {
	t.Helper()
	c := newFakeClock()
	l := newTestLog(t)
	return &engine{clock: c, log: l.attemptLog, service: "svc-test"}, c, l
}

func testProfile(t *testing.T, doc string) *profile {
	t.Helper()
	cfg, err := ParseConfig([]byte(doc), "test.yaml")
	require.NoError(t, err)
	p, err := buildProfile(cfg.DefaultPolicy, cfg.Profiles[cfg.DefaultPolicy])
	require.NoError(t, err)
	return p
}

// A gate denial produces exactly ONE client record with gate set, start==end,
// and never touches the wire.
func TestEngineGateDeniedRootWritesOneRecordAndDoesNotInvoke(t *testing.T) {
	e, _, log := newTestEngine(t)
	breaker := mustCountBreaker(t, [2]int{1, 1}, [2]int{1, 1}, secondNS, NewNoRetryPolicy())
	breaker.AddResult(false, 0)
	require.Equal(t, CBOpen, breaker.GetState())
	prof := &profile{name: "p", timeout: 50 * time.Millisecond, retryOn: map[codes.Code]bool{codes.Unavailable: true}, head: breaker}

	calls := 0
	err := e.execute(context.Background(), prof, "root", testMethod, testPeer, func(context.Context) error {
		calls++
		return nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Zero(t, calls, "a gated root never reaches the wire")

	recs := log.ofKind("client")
	require.Len(t, recs, 1)
	assert.Equal(t, gateCircuitOpen, recs[0]["gate"])
	assert.Equal(t, "", recs[0]["drop_reason"])
	assert.Equal(t, recs[0]["start_epoch"], recs[0]["end_epoch"])
	assert.Equal(t, "Unavailable", recs[0]["response_code"])
	assert.Equal(t, true, recs[0]["is_error"])
	assert.Equal(t, float64(1), recs[0]["attempt"])
	assert.Equal(t, "root", recs[0]["route"])
	assert.Equal(t, testPeer, recs[0]["peer"],
		"a gate-denied record still names the connection the attempt would have used")
}

// Finagle's RetryFilter deposits per request that ENTERS the filter, so the
// deposit sits BELOW the gate: a request an open breaker sheds must not fund
// retries for the few that do get through.
func TestEngineShedRequestDoesNotDepositIntoAWrappedBudget(t *testing.T) {
	e, _, _ := newTestEngine(t)
	budget := mustBudget(t, 0.1, 1, mustFixed(t, 2, 0))
	for budget.CanRetry() { // drain, so deposits are visible below the cap
		budget.NextDelay(ctxAt(1, 0))
	}
	requireTokens(t, budget, big.NewInt(0))
	breaker := mustCountBreaker(t, [2]int{1, 1}, [2]int{1, 1}, 1000*secondNS, budget)
	breaker.AddResult(false, 0)
	require.Equal(t, CBOpen, breaker.GetState())
	prof := &profile{name: "p", timeout: 50 * time.Millisecond, retryOn: map[codes.Code]bool{codes.Unavailable: true}, head: breaker}

	for i := 0; i < 50; i++ {
		err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(context.Context) error { return nil })
		require.Error(t, err)
	}
	requireTokens(t, budget, big.NewInt(0))
}

// Results must reach EVERY layer, not just the outermost: RetryBudgetPolicy has
// no AddResult of its own, so the breaker only ever sees an outcome if the walk
// descends into the underlying strategy.
func TestEngineAttemptResultsReachAWrappedBreaker(t *testing.T) {
	e, clock, _ := newTestEngine(t)
	breaker := mustCountBreaker(t, [2]int{2, 2}, [2]int{1, 1}, 1000*secondNS, mustFixed(t, 3, 0))
	budget := mustBudget(t, 1.0, 5, breaker)
	prof := &profile{name: "p", timeout: 50 * time.Millisecond, retryOn: map[codes.Code]bool{codes.Unavailable: true}, head: budget}

	attempts := 0
	err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(context.Context) error {
		attempts++
		clock.Advance(time.Millisecond)
		return status.Error(codes.Unavailable, "boom")
	})
	require.Error(t, err)
	assert.Equal(t, 2, attempts, "both failures fed in")
	assert.Equal(t, CBOpen, breaker.GetState())
}

// One deposit per admitted root, reaching a budget wrapped by a breaker.
func TestEngineRequestStartsReachAWrappedBudget(t *testing.T) {
	e, _, _ := newTestEngine(t)
	budget := mustBudget(t, 0.1, 1, mustFixed(t, 10, 0))
	for budget.CanRetry() {
		budget.NextDelay(ctxAt(1, 0))
	}
	requireTokens(t, budget, big.NewInt(0))
	breaker := mustCountBreaker(t, [2]int{1, 1}, [2]int{1, 1}, 0, budget)
	prof := &profile{name: "p", timeout: 50 * time.Millisecond, retryOn: map[codes.Code]bool{codes.Unavailable: true}, head: breaker}

	for i := 0; i < 3; i++ {
		require.NoError(t, e.execute(context.Background(), prof, "", testMethod, testPeer, func(context.Context) error { return nil }))
	}
	requireTokens(t, budget, big.NewInt(3*budget.Deposit()))
}

// A retry the global deadline vetoes must not be charged: every throttling
// layer that withdrew during NextDelay must refund.
func TestEngineVetoedRetryRefundsEveryChargedLayer(t *testing.T) {
	backoff := 50 * time.Millisecond
	globalTimeout := 30 * time.Millisecond

	t.Run("budget", func(t *testing.T) {
		e, clock, log := newTestEngine(t)
		budget := mustBudget(t, 0.1, 5, mustFixed(t, 10, int64(backoff)))
		before := budget.Tokens()
		prof := &profile{name: "p", timeout: globalTimeout, globalTimeout: globalTimeout,
			retryOn: map[codes.Code]bool{codes.Unavailable: true}, head: budget}
		attempts := runFailing(t, e, clock, prof)
		assert.Equal(t, 1, attempts)
		assert.Equal(t, before, budget.Tokens())
		assert.Equal(t, deniedDeadlineVeto, log.ofKind("client")[0]["retry_denied"])
	})

	t.Run("bursty and fixed window", func(t *testing.T) {
		for _, mk := range []func(t *testing.T) (Policy, func() float64){
			func(t *testing.T) (Policy, func() float64) {
				p := mustBursty(t, 5, 0.0, secondNS, mustFixed(t, 10, int64(backoff)))
				return p, p.Tokens
			},
			func(t *testing.T) (Policy, func() float64) {
				p := mustFixedWindow(t, 5, secondNS, mustFixed(t, 10, int64(backoff)))
				return p, func() float64 { return float64(p.Tokens()) }
			},
		} {
			e, clock, _ := newTestEngine(t)
			pol, tokens := mk(t)
			before := tokens()
			prof := &profile{name: "p", timeout: globalTimeout, globalTimeout: globalTimeout,
				retryOn: map[codes.Code]bool{codes.Unavailable: true}, head: pol}
			attempts := runFailing(t, e, clock, prof)
			assert.Equal(t, 1, attempts)
			assert.Equal(t, before, tokens())
		}
	})

	t.Run("leaky", func(t *testing.T) {
		e, clock, _ := newTestEngine(t)
		// The leaky delay is 0 on an idle bucket, so NextDelay DOES reserve a
		// slot; the 5 ms deadline then vetoes the retry and Rollback must put
		// _next_at back. The invariant is that _next_at advances once per
		// ISSUED retry, never for a vetoed one.
		leaky := mustLeaky(t, 10, 10, secondNS)
		prof := &profile{name: "p", timeout: 5 * time.Millisecond, globalTimeout: 5 * time.Millisecond,
			retryOn: map[codes.Code]bool{codes.Unavailable: true}, head: leaky}
		attempts := runFailingWith(t, e, clock, prof, 10*time.Millisecond)
		assert.Equal(t, 1, attempts)
		assert.Nil(t, leaky.nextAt)
	})
}

// runFailing drives one root whose every attempt fails after 10 ms of virtual
// time.
func runFailing(t *testing.T, e *engine, clock *fakeClock, prof *profile) int {
	return runFailingWith(t, e, clock, prof, 10*time.Millisecond)
}

func runFailingWith(t *testing.T, e *engine, clock *fakeClock, prof *profile, serviceTime time.Duration) int {
	t.Helper()
	attempts := 0
	err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(context.Context) error {
		attempts++
		clock.Advance(serviceTime)
		return status.Error(codes.Unavailable, "boom")
	})
	require.Error(t, err)
	return attempts
}

// Strict half-open deadline: an attempt that finishes exactly AT its deadline
// is a deadline drop, not a success. The server side applies the same rule.
func TestEngineStrictDeadlineAtExactlyTheBoundary(t *testing.T) {
	e, clock, log := newTestEngine(t)
	prof := testProfile(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 50ms\n")

	err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(context.Context) error {
		clock.Advance(50 * time.Millisecond) // end == deadline
		return nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	recs := log.ofKind("client")
	require.Len(t, recs, 1)
	assert.Equal(t, "DeadlineExceeded", recs[0]["response_code"])
	assert.Equal(t, dropDeadline, recs[0]["drop_reason"])

	// One nanosecond earlier the same attempt succeeds.
	e2, clock2, log2 := newTestEngine(t)
	require.NoError(t, e2.execute(context.Background(), prof, "", testMethod, testPeer, func(context.Context) error {
		clock2.Advance(50*time.Millisecond - 1)
		return nil
	}))
	assert.Equal(t, "OK", log2.ofKind("client")[0]["response_code"])
}

// msim books a timeout AT the deadline. The pipeline buckets every caller-side
// column at the client record's END, so a goroutine that wakes late after its
// own deadline used to move the timeout into the next bucket -- and only that
// case: a status the callee really produced, at whatever instant, is an
// observation and stands.
func TestEngineARecordEndsAtItsOwnDeadlineOnlyWhenThatDeadlineEndedIt(t *testing.T) {
	t.Run("the attempt's own deadline, observed ten ms late", func(t *testing.T) {
		e, clock, log := newTestEngine(t)
		prof := testProfile(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 20ms\n")

		var deadline time.Time
		err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(ctx context.Context) error {
			d, ok := ctx.Deadline()
			require.True(t, ok)
			deadline = d
			<-ctx.Done()                         // the attempt's own deadline fires...
			clock.Advance(30 * time.Millisecond) // ...and this goroutine wakes ten ms later
			return status.FromContextError(ctx.Err()).Err()
		})
		require.Error(t, err)
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))

		recs := log.ofKind("client")
		require.Len(t, recs, 1)
		assert.Equal(t, epochOf(deadline), recs[0]["end_epoch"],
			"the record ends at the deadline, not when the goroutine woke to hear about it")
		assert.Equal(t, 20.0, recs[0]["duration_ms"])
		assert.Equal(t, dropDeadline, recs[0]["drop_reason"])
	})

	t.Run("the strict rule converted it, before the context's timer had fired", func(t *testing.T) {
		e, clock, log := newTestEngine(t)
		// An hour out, so the context's REAL timer cannot fire during the test:
		// the only thing here that can say the deadline passed is the strict
		// comparison, made on the clock the engine and the callee share.
		prof := testProfile(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1h\n")

		var deadline time.Time
		err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(ctx context.Context) error {
			d, ok := ctx.Deadline()
			require.True(t, ok)
			deadline = d
			clock.Advance(time.Hour + 2*time.Microsecond) // an OK, two microseconds past D
			require.NoError(t, ctx.Err(),
				"the context's timer callback has not run: the local expiry flag alone misses this")
			return nil
		})
		require.Error(t, err)
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))

		recs := log.ofKind("client")
		require.Len(t, recs, 1)
		assert.Equal(t, "DeadlineExceeded", recs[0]["response_code"])
		assert.Equal(t, epochOf(deadline), recs[0]["end_epoch"],
			"the conversion IS the assertion that the deadline passed, so the record ends at it")
		assert.Equal(t, 3600000.0, recs[0]["duration_ms"])
		assert.Equal(t, dropDeadline, recs[0]["drop_reason"])
	})

	t.Run("a DeadlineExceeded from the callee, while the local deadline is live", func(t *testing.T) {
		e, clock, log := newTestEngine(t)
		prof := testProfile(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n")

		err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(context.Context) error {
			clock.Advance(5 * time.Millisecond)
			return status.Error(codes.DeadlineExceeded, "the callee's own deadline, which is nearer")
		})
		require.Error(t, err)

		recs := log.ofKind("client")
		require.Len(t, recs, 1)
		assert.Equal(t, epochOf(clock.Now()), recs[0]["end_epoch"],
			"nothing local expired, so the record ends where the answer arrived")
		assert.Equal(t, 5.0, recs[0]["duration_ms"])
	})

	// The documented cost of clamping on the local expiry flag, pinned so that it
	// is a decision and not a surprise: the callee's answer really arrived one
	// millisecond inside the local deadline, and is booked AT it.
	t.Run("the accepted over-clamp: an arrival this goroutine slept past", func(t *testing.T) {
		e, clock, log := newTestEngine(t)
		prof := testProfile(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 20ms\n")

		var deadline time.Time
		err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(ctx context.Context) error {
			d, ok := ctx.Deadline()
			require.True(t, ok)
			deadline = d
			// The callee's own deadline, which is nearer, answers at 19 ms...
			clock.Advance(19 * time.Millisecond)
			calleeErr := status.Error(codes.DeadlineExceeded, "the callee's own deadline, which is nearer")
			// ...and this goroutine is not scheduled again until after 20 ms, by
			// which point nothing here can tell that arrival from the local expiry.
			<-ctx.Done()
			clock.Advance(5 * time.Millisecond)
			return calleeErr
		})
		require.Error(t, err)

		recs := log.ofKind("client")
		require.Len(t, recs, 1)
		assert.Equal(t, epochOf(deadline), recs[0]["end_epoch"],
			"ACCEPTED: booked at the local deadline, which is nearer to the true 19 ms than the wake-up is")
		assert.Equal(t, 20.0, recs[0]["duration_ms"])
	})

	t.Run("a success is never clamped", func(t *testing.T) {
		e, clock, log := newTestEngine(t)
		prof := testProfile(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n")

		require.NoError(t, e.execute(context.Background(), prof, "", testMethod, testPeer, func(context.Context) error {
			clock.Advance(7 * time.Millisecond)
			return nil
		}))
		recs := log.ofKind("client")
		require.Len(t, recs, 1)
		assert.Equal(t, epochOf(clock.Now()), recs[0]["end_epoch"])
		assert.Equal(t, 7.0, recs[0]["duration_ms"])
	})
}

// An attempt the caller abandoned is not evidence about the callee: it must
// reach no policy layer.
func TestEngineInboundCancelGivesNoFeedback(t *testing.T) {
	e, _, log := newTestEngine(t)
	spy := &spyPolicy{underlying: mustFixed(t, 5, 0)}
	prof := &profile{name: "p", timeout: 50 * time.Millisecond, retryOn: map[codes.Code]bool{codes.Unavailable: true}, head: spy}

	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	err := e.execute(ctx, prof, "", testMethod, testPeer, func(context.Context) error {
		attempts++
		cancel()
		return status.Error(codes.Unavailable, "boom")
	})
	require.Error(t, err)
	assert.Equal(t, 1, attempts)
	assert.Empty(t, spy.results, "a cancelled attempt must not reach add_result")
	assert.Equal(t, 1, spy.starts, "the root was still admitted, so it deposited")

	recs := log.ofKind("client")
	require.Len(t, recs, 1)
	assert.Equal(t, dropCancelled, recs[0]["drop_reason"])
	assert.Equal(t, "Canceled", recs[0]["response_code"])
	assert.Equal(t, "", recs[0]["retry_denied"])
}

// retry_on filters which codes are retried at all; a code outside it is denied
// before NextDelay is even asked.
func TestEngineRetryOnFiltering(t *testing.T) {
	doc := `
default_policy: a
profiles:
  a:
    timeout: 50ms
    retry:
      enabled: true
      kind: fixed
      max_attempts: 4
      delay: 1ms
      retry_on: [Unavailable]
`
	for _, tc := range []struct {
		code     codes.Code
		attempts int
		denied   string
	}{
		{codes.Unavailable, 4, deniedExhausted},
		{codes.ResourceExhausted, 1, deniedNotRetryable},
		{codes.Internal, 1, deniedNotRetryable},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			e, clock, log := newTestEngine(t)
			prof := testProfile(t, doc)
			attempts := 0
			err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(context.Context) error {
				attempts++
				clock.Advance(time.Millisecond)
				return status.Error(tc.code, "boom")
			})
			require.Error(t, err)
			assert.Equal(t, tc.attempts, attempts)
			recs := log.ofKind("client")
			require.Len(t, recs, tc.attempts)
			assert.Equal(t, tc.denied, recs[len(recs)-1]["retry_denied"])
			for i, r := range recs[:len(recs)-1] {
				assert.Equal(t, "", r["retry_denied"], "record %d", i)
			}
		})
	}
}

// Every attempt is bounded by the profile's `timeout`, and the backoff waited
// before it is reported on the record.
func TestEnginePerAttemptTimeoutAndRecordedBackoff(t *testing.T) {
	doc := `
default_policy: a
profiles:
  a:
    timeout: 40ms
    retry:
      enabled: true
      kind: fixed
      max_attempts: 3
      delay: 7ms
`
	e, clock, log := newTestEngine(t)
	prof := testProfile(t, doc)
	var budgets []time.Duration
	err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(ctx context.Context) error {
		d, ok := ctx.Deadline()
		require.True(t, ok, "every attempt carries the profile's per-attempt timeout")
		// Measured on the clock that minted the deadline, so the budget is the
		// exact per-attempt timeout rather than a real-time approximation of it.
		budgets = append(budgets, time.Duration(clock.DeadlineNS(d)-clock.NowNS()))
		clock.Advance(time.Millisecond)
		return status.Error(codes.Unavailable, "boom")
	})
	require.Error(t, err)
	require.Len(t, budgets, 3)
	for _, b := range budgets {
		assert.Equal(t, 40*time.Millisecond, b)
	}
	recs := log.ofKind("client")
	require.Len(t, recs, 3)
	assert.Equal(t, 0.0, recs[0]["retry_delay_ms"])
	assert.Equal(t, 7.0, recs[1]["retry_delay_ms"])
	assert.Equal(t, 7.0, recs[2]["retry_delay_ms"])
	assert.Equal(t, float64(1), recs[0]["attempt"])
	assert.Equal(t, float64(3), recs[2]["attempt"])
}

// Composition order: rate_limiter > circuit_breaker > budget > retry, and the
// global timeout bounds the ROOT while `timeout` bounds each attempt.
func TestEngineGlobalTimeoutBoundsTheRoot(t *testing.T) {
	doc := `
default_policy: a
profiles:
  a:
    timeout: 10ms
    global_timeout: 25ms
    retry:
      enabled: true
      kind: fixed
      max_attempts: 10
      delay: 1ms
`
	e, clock, _ := newTestEngine(t)
	prof := testProfile(t, doc)
	attempts := 0
	err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(context.Context) error {
		attempts++
		clock.Advance(10 * time.Millisecond)
		return status.Error(codes.Unavailable, "boom")
	})
	require.Error(t, err)
	// Each attempt is cut at its own 10 ms timeout (strict: end == deadline is
	// already too late), so: attempt 1 ends at 10 ms and retries at 11 ms;
	// attempt 2 ends at 21 ms and retries at 22 ms; attempt 3's deadline is the
	// ROOT deadline at 25 ms, and the retry after it would start at 33 ms,
	// which the global timeout vetoes.
	assert.Equal(t, 3, attempts)
}

// A success stops the loop and leaves one record per attempt made.
func TestEngineStopsOnFirstSuccess(t *testing.T) {
	doc := `
default_policy: a
profiles:
  a:
    timeout: 50ms
    retry:
      enabled: true
      kind: fixed
      max_attempts: 5
      delay: 1ms
`
	e, clock, log := newTestEngine(t)
	prof := testProfile(t, doc)
	attempts := 0
	err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(context.Context) error {
		attempts++
		clock.Advance(time.Millisecond)
		if attempts < 3 {
			return status.Error(codes.Unavailable, "boom")
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, attempts)
	recs := log.ofKind("client")
	require.Len(t, recs, 3)
	assert.Equal(t, "OK", recs[2]["response_code"])
	assert.Equal(t, false, recs[2]["is_error"])
}

// Every attempt carries a fresh span id under one trace id, and the parent is
// the enclosing span.
func TestEngineMintsAFreshSpanPerAttempt(t *testing.T) {
	doc := "default_policy: a\nprofiles:\n  a:\n    timeout: 50ms\n    retry:\n      enabled: true\n      kind: fixed\n      max_attempts: 3\n      delay: 0s\n"
	e, clock, log := newTestEngine(t)
	prof := testProfile(t, doc)
	parent := traceCtx{traceID: "0123456789abcdef0123456789abcdef", spanID: "fedcba9876543210", route: "root"}
	err := e.execute(withTraceCtx(context.Background(), parent), prof, "", testMethod, testPeer, func(context.Context) error {
		clock.Advance(time.Millisecond)
		return status.Error(codes.Unavailable, "boom")
	})
	require.Error(t, err)
	recs := log.ofKind("client")
	require.Len(t, recs, 3)
	seen := map[string]bool{}
	for _, r := range recs {
		assert.Equal(t, parent.traceID, r["trace_id"])
		assert.Equal(t, parent.spanID, r["parent_span_id"])
		assert.Equal(t, "root", r["route"], "the route is inherited when the call site passes none")
		assert.False(t, seen[r["span_id"].(string)])
		seen[r["span_id"].(string)] = true
	}
}

// The strict half-open comparison must be made against the deadline the
// INVOKER actually received. Deriving the boundary as "now + timeout" on one
// clock while the attempt context was minted from another put the two on
// different timelines; a clock that moves between observations exposes it.
func TestEngineClassifiesAgainstTheDeadlineTheInvokerReceived(t *testing.T) {
	prof := testProfile(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 50ms\n")

	run := func(offset int64) (codes.Code, *testLog) {
		clock := newTickClock(time.Millisecond)
		log := newTestLog(t)
		e := &engine{clock: clock, log: log.attemptLog, service: "svc-test"}
		seen := int64(-1)
		err := e.execute(context.Background(), prof, "", testMethod, testPeer, func(attemptCtx context.Context) error {
			d, ok := attemptCtx.Deadline()
			require.True(t, ok)
			seen = clock.DeadlineNS(d)
			// Pin the finish exactly `offset` ns from the invoker's own
			// deadline; nothing after this point moves the clock.
			clock.FreezeAt(seen + offset)
			return nil
		})
		require.NotEqual(t, int64(-1), seen, "the invoker must be handed a deadline")
		return status.Code(err), log
	}

	code, log := run(-1)
	assert.Equal(t, codes.OK, code, "one ns before the invoker's own deadline is still a success")
	assert.Equal(t, "OK", log.ofKind("client")[0]["response_code"])

	code, log = run(0)
	assert.Equal(t, codes.DeadlineExceeded, code, "finishing AT the invoker's own deadline is a drop")
	recs := log.ofKind("client")
	require.Len(t, recs, 1)
	assert.Equal(t, "DeadlineExceeded", recs[0]["response_code"])
	assert.Equal(t, dropDeadline, recs[0]["drop_reason"])
}

// The jittered delays are exact functions of the draw, so a fixed source pins
// them completely. msim computes them in floats and truncates with int(), which
// is not the same as rounding: 400 ms x (1 - 1e-9) is 399999999 ns, not 400 ms.
func TestExponentialJitterExactValuesUnderAFixedSource(t *testing.T) {
	const (
		initial = int64(100 * time.Millisecond)
		cap400  = int64(400 * time.Millisecond)
	)
	draws := []float64{0.0, 0.25, 0.5, 0.999999999}
	source := func() func() float64 {
		i := 0
		return func() float64 { r := draws[i]; i++; return r }
	}

	// exp per attempt: 100 ms, 200 ms, 400 ms, 400 ms (capped).
	full, err := NewExponentialBackoffWithJitterRetryPolicy(9, initial, cap400, JitterFull, source())
	require.NoError(t, err)
	assert.Equal(t, DelayDecision{true, 0}, full.NextDelay(ctxAt(1, 0)))
	assert.Equal(t, DelayDecision{true, int64(50 * time.Millisecond)}, full.NextDelay(ctxAt(2, 0)))
	assert.Equal(t, DelayDecision{true, int64(200 * time.Millisecond)}, full.NextDelay(ctxAt(3, 0)))
	assert.Equal(t, DelayDecision{true, int64(400*time.Millisecond) - 1}, full.NextDelay(ctxAt(4, 0)))

	equal, err := NewExponentialBackoffWithJitterRetryPolicy(9, initial, cap400, JitterEqual, source())
	require.NoError(t, err)
	assert.Equal(t, DelayDecision{true, int64(50 * time.Millisecond)}, equal.NextDelay(ctxAt(1, 0)))
	assert.Equal(t, DelayDecision{true, int64(125 * time.Millisecond)}, equal.NextDelay(ctxAt(2, 0)))
	assert.Equal(t, DelayDecision{true, int64(300 * time.Millisecond)}, equal.NextDelay(ctxAt(3, 0)))
	assert.Equal(t, DelayDecision{true, int64(400*time.Millisecond) - 1}, equal.NextDelay(ctxAt(4, 0)))
}
