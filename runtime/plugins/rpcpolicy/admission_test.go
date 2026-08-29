package rpcpolicy

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func intPtr(v int) *int { return &v }

func newTestStation(workers int, capacity *int, hold bool) *station {
	return newStation(&ServerConfig{
		Workers:                 workers,
		QueueCapacity:           capacity,
		HoldPermitThroughFanout: hold,
	}, newRealClock())
}

// waitFor spins until cond holds, so a test never depends on a sleep long
// enough to be "obviously" safe.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Microsecond)
	}
	t.Fatal("condition not reached within 5s")
}

// msim's four drop points, in the order submit_attempt tests them.
func TestStationDeadlineAtSubmit(t *testing.T) {
	st := newTestStation(4, intPtr(4), false)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	adm := st.Acquire(ctx)
	assert.Equal(t, outcomeDeadlineAtSubmit, adm.outcome)
	assert.Nil(t, adm.permit)
	assert.Equal(t, 0, st.Busy(), "an already-expired request never takes a worker")
}

func TestStationQueueFull(t *testing.T) {
	st := newTestStation(1, intPtr(0), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	adm := st.Acquire(context.Background())
	assert.Equal(t, outcomeQueueFull, adm.outcome)
	assert.Nil(t, adm.permit)
	assert.Equal(t, 1, adm.workersBusy)
	held.permit.release()
}

// A free worker is taken BEFORE the queue-full test, so queue_full can only be
// reported when every worker is busy (msim's submit_attempt order).
func TestStationFreeWorkerBeatsQueueFull(t *testing.T) {
	st := newTestStation(2, intPtr(0), false)
	a := st.Acquire(context.Background())
	b := st.Acquire(context.Background())
	assert.Equal(t, outcomeAdmitted, a.outcome)
	assert.Equal(t, outcomeAdmitted, b.outcome)
	assert.Equal(t, 0, a.workersBusy)
	assert.Equal(t, 1, b.workersBusy)
	c := st.Acquire(context.Background())
	assert.Equal(t, outcomeQueueFull, c.outcome)
	a.permit.release()
	b.permit.release()
}

// A queued request is not removed when its caller goes away: msim keeps it
// until a worker frees up and reports the expiry THERE.
func TestStationDeadlineAtDequeue(t *testing.T) {
	st := newTestStation(1, intPtr(4), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	var got admission
	done := make(chan struct{})
	go func() { got = st.Acquire(ctx); close(done) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	<-ctx.Done() // let the queued request expire while it waits
	held.permit.release()
	<-done
	assert.Equal(t, outcomeDeadlineAtDequeue, got.outcome)
	assert.Nil(t, got.permit)
	assert.Greater(t, got.wait, time.Duration(0))
	waitFor(t, func() bool { return st.Busy() == 0 })
}

// FCFS is load-bearing: the M/G/c/K model the campaign fits assumes it, and a
// buffered channel would hand the permit to an arbitrary blocked receiver.
func TestStationIsFCFSOverManyWaiters(t *testing.T) {
	const n = 12
	st := newTestStation(1, nil, false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	var mu sync.Mutex
	var order []int
	permits := make(chan *permit, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			adm := st.Acquire(context.Background())
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			permits <- adm.permit
		}(i)
		// Enqueue strictly one at a time, so the arrival order is the test's,
		// not the scheduler's.
		waitFor(t, func() bool { return st.QueueDepth() == i+1 })
	}

	held.permit.release()
	for i := 0; i < n; i++ {
		p := <-permits
		require.NotNil(t, p)
		p.release()
	}
	wg.Wait()

	want := make([]int, n)
	for i := range want {
		want[i] = i
	}
	assert.Equal(t, want, order)
}

// Occupancy is bounded by c in service plus K queued; everything beyond that is
// shed at submit.
func TestStationBoundsOccupancyAtCPlusK(t *testing.T) {
	const (
		c     = 3
		k     = 5
		extra = 7
	)
	st := newTestStation(c, intPtr(k), false)
	var shed, admitted atomic.Int64
	var busyMu sync.Mutex
	maxBusy := 0
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < c+k+extra; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			adm := st.Acquire(context.Background())
			if adm.outcome == outcomeQueueFull {
				shed.Add(1)
				return
			}
			assert.Equal(t, outcomeAdmitted, adm.outcome)
			admitted.Add(1)
			busyMu.Lock()
			if b := st.Busy(); b > maxBusy {
				maxBusy = b
			}
			busyMu.Unlock()
			<-release
			adm.permit.release()
		}()
	}
	waitFor(t, func() bool { return shed.Load() == extra })
	close(release)
	wg.Wait()

	assert.Equal(t, int64(extra), shed.Load())
	assert.Equal(t, int64(c+k), admitted.Load())
	assert.LessOrEqual(t, maxBusy, c)
	assert.Equal(t, 0, st.Busy())
	assert.Equal(t, 0, st.QueueDepth())
}

// queue_capacity: null is an unbounded queue: nothing is ever shed.
func TestStationUnboundedQueueNeverSheds(t *testing.T) {
	const n = 40
	st := newTestStation(1, nil, false)
	release := make(chan struct{})
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			adm := st.Acquire(context.Background())
			assert.Equal(t, outcomeAdmitted, adm.outcome)
			admitted.Add(1)
			<-release
			adm.permit.release()
		}()
	}
	waitFor(t, func() bool { return st.QueueDepth() == n-1 })
	close(release)
	wg.Wait()
	assert.Equal(t, int64(n), admitted.Load())
}

func TestReleasePermitIsIdempotentAndSafeWithoutOne(t *testing.T) {
	st := newTestStation(1, intPtr(1), false)
	adm := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, adm.outcome)
	ctx := withPermit(context.Background(), adm.permit)

	ReleasePermit(ctx)
	first := adm.permit.releasedEpoch()
	assert.Greater(t, first, 0.0)
	assert.Equal(t, 0, st.Busy())

	ReleasePermit(ctx)
	ReleasePermit(ctx)
	assert.Equal(t, first, adm.permit.releasedEpoch(), "release is once-only")
	assert.Equal(t, 0, st.Busy())

	// A context with no permit, and a nil permit, are both no-ops.
	ReleasePermit(context.Background())
	ReleasePermit(withPermit(context.Background(), nil))
}

// hold_permit_through_fanout (arm A4's deployment counterpart): the relay keeps
// its worker across the downstream call, so ReleasePermit does nothing.
func TestReleasePermitIsANoOpUnderHold(t *testing.T) {
	st := newTestStation(1, intPtr(1), true)
	adm := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, adm.outcome)
	ctx := withPermit(context.Background(), adm.permit)

	ReleasePermit(ctx)
	assert.Equal(t, 1, st.Busy(), "the permit is held through the fan-out")
	assert.Equal(t, 0.0, adm.permit.releasedEpoch())

	adm.permit.release() // the interceptor still releases at the end
	assert.Equal(t, 0, st.Busy())
}

// A permit that was never held reports permit_released_at 0.
func TestPermitReleasedEpochIsZeroWhenNeverHeld(t *testing.T) {
	var p *permit
	assert.Equal(t, 0.0, p.releasedEpoch())
	p.release() // nil-safe
}

// --- server-side integration of the station -------------------------------

func TestServeWritesARecordForEveryDropPoint(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 0\n")

	// deadline_at_submit
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		t.Fatal("handler must not run")
		return nil, nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))

	// queue_full: hold the only worker, then submit a second call
	enter := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_, _ = s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			close(enter)
			<-release
			return "ok", nil
		})
	}()
	<-enter
	_, err = s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		t.Error("handler must not run when the queue is full")
		return nil, nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	close(release)

	waitFor(t, func() bool { return len(s.tlog.ofKind("server")) >= 3 })
	recs := s.tlog.ofKind("server")
	outcomes := map[string]bool{}
	for _, r := range recs {
		outcomes[r["admission_outcome"].(string)] = true
		if r["admission_outcome"] != outcomeAdmitted {
			assert.Equal(t, 0.0, r["handler_ms"], "a dropped request never ran the handler")
		}
	}
	assert.True(t, outcomes[outcomeDeadlineAtSubmit])
	assert.True(t, outcomes[outcomeQueueFull])
	assert.True(t, outcomes[outcomeAdmitted])
}

func TestServeReportsDeadlineAtFinish(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 2\n  queue_capacity: 4\n")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		time.Sleep(60 * time.Millisecond)
		return "ok", nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, outcomeDeadlineAtFinish, recs[0]["admission_outcome"])
	assert.Greater(t, recs[0]["permit_released_at"], 0.0)
}

// strip_inbound_deadline (arm A1's deployment counterpart): the handler runs to
// completion after the caller has walked away.
func TestServeStripInboundDeadlineRunsToCompletionAfterCallerCancel(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 2\n  queue_capacity: 4\n  strip_inbound_deadline: true\n")
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan bool, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	resp, err := s.serve(ctx, nil, testMethod, func(hctx context.Context, _ interface{}) (interface{}, error) {
		time.Sleep(40 * time.Millisecond)
		finished <- hctx.Err() == nil
		return "ok", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "ok", resp)
	assert.True(t, <-finished, "the handler context must survive the caller's cancel")
	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, outcomeAdmitted, recs[0]["admission_outcome"])
	assert.Equal(t, "OK", recs[0]["response_code"])
}

// Without the strip, the same handler inherits the caller's cancellation.
func TestServeWithoutStripPropagatesTheCallerCancel(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 2\n  queue_capacity: 4\n")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	seen := make(chan error, 1)
	_, err := s.serve(ctx, nil, testMethod, func(hctx context.Context, _ interface{}) (interface{}, error) {
		<-hctx.Done()
		seen <- hctx.Err()
		return nil, status.FromContextError(hctx.Err()).Err()
	})
	require.Error(t, err)
	assert.Equal(t, context.Canceled, <-seen)
}

// A relay hands its worker back before the downstream call, so the station is
// free while the fan-out is outstanding.
func TestServeReleasePermitFreesTheWorkerMidHandler(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n")
	st := s.registry.Load().Station()
	inHandler := make(chan struct{})
	finish := make(chan struct{})
	go func() {
		_, _ = s.serve(context.Background(), nil, testMethod, func(hctx context.Context, _ interface{}) (interface{}, error) {
			ReleasePermit(hctx)
			close(inHandler)
			<-finish
			return "ok", nil
		})
	}()
	<-inHandler
	assert.Equal(t, 0, st.Busy(), "the permit was handed back before the downstream call")
	close(finish)
}

// --- cancellation while queued (CONTRACTS.md §5) ---------------------------

// msim's cancel_queued (service.py:305): a queued attempt whose CALLER cancels
// is tombstoned at once. It never takes a worker, and it discounts its queue
// slot at cancel time rather than when its tombstone reaches the head -- so the
// very next arrival is QUEUED, not shed.
func TestStationCancelWhileQueuedFreesItsSlotImmediately(t *testing.T) {
	st := newTestStation(1, intPtr(1), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	ctx, cancel := context.WithCancel(context.Background()) // deliberately no deadline
	got := make(chan admission, 1)
	go func() { got <- st.Acquire(ctx) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	cancel()
	select {
	case adm := <-got:
		assert.Equal(t, outcomeCancelledInQueue, adm.outcome)
		assert.Nil(t, adm.permit, "a cancelled waiter never takes a worker")
		assert.Equal(t, 0, adm.queueDepth, "the slot is discounted at cancel time")
		assert.Equal(t, 1, adm.workersBusy)
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled waiter must return promptly, not wait for a worker")
	}
	assert.Equal(t, 0, st.QueueDepth(), "the queue slot is back")
	assert.Equal(t, 1, st.Busy(), "the held worker is untouched")

	// Capacity restored: this arrival joins the queue instead of being shed.
	second := make(chan admission, 1)
	go func() { second <- st.Acquire(context.Background()) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })
	held.permit.release()
	adm := <-second
	require.Equal(t, outcomeAdmitted, adm.outcome, "the freed slot was reusable")
	adm.permit.release()
	waitFor(t, func() bool { return st.Busy() == 0 })
}

// An expired DEADLINE is the other rule: the waiter keeps its slot and is
// reported at dequeue, so a cancel and a timeout are not interchangeable.
func TestStationDeadlineDoesNotFreeTheSlotEarly(t *testing.T) {
	st := newTestStation(1, intPtr(1), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	got := make(chan admission, 1)
	go func() { got <- st.Acquire(ctx) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	<-ctx.Done()
	// Still queued: an expired deadline does not give the slot back, so the
	// station is still full and the next arrival is shed.
	assert.Equal(t, 1, st.QueueDepth())
	assert.Equal(t, outcomeQueueFull, st.Acquire(context.Background()).outcome)

	held.permit.release()
	adm := <-got
	assert.Equal(t, outcomeDeadlineAtDequeue, adm.outcome)
	assert.Nil(t, adm.permit)
	waitFor(t, func() bool { return st.Busy() == 0 })
}

// The cancel and the permit hand-off are both taken under the station mutex, so
// exactly one of them wins however they interleave.
func TestStationCancelRacingReleaseHasOneWinner(t *testing.T) {
	for i := 0; i < 200; i++ {
		st := newTestStation(1, intPtr(1), false)
		held := st.Acquire(context.Background())
		require.Equal(t, outcomeAdmitted, held.outcome)

		ctx, cancel := context.WithCancel(context.Background())
		got := make(chan admission, 1)
		go func() { got <- st.Acquire(ctx) }()
		waitFor(t, func() bool { return st.QueueDepth() == 1 })

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; held.permit.release() }()
		go func() { defer wg.Done(); <-start; cancel() }()
		close(start)
		wg.Wait()

		adm := <-got
		switch adm.outcome {
		case outcomeCancelledInQueue:
			require.Nil(t, adm.permit, "iteration %d: a cancelled waiter holds no permit", i)
		case outcomeAdmitted:
			require.NotNil(t, adm.permit, "iteration %d: an admitted waiter holds a permit", i)
			adm.permit.release()
		default:
			t.Fatalf("iteration %d: unexpected outcome %q", i, adm.outcome)
		}
		waitFor(t, func() bool { return st.Busy() == 0 && st.QueueDepth() == 0 })
		cancel()
	}
}

// A record is written for EVERY server-side outcome, cancelled_in_queue
// included: response_code Canceled, no handler, no permit.
func TestServeWritesACancelledInQueueRecord(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 1\n")
	st := s.registry.Load().Station()

	inHandler := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_, _ = s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			close(inHandler)
			<-release
			return "ok", nil
		})
	}()
	<-inHandler

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			t.Error("a cancelled waiter must never reach the handler")
			return nil, nil
		})
		done <- err
	}()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("the cancelled waiter did not return")
	}

	// The freed slot is immediately usable: this submit is queued, not shed.
	third := make(chan error, 1)
	go func() {
		_, err := s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			return "ok", nil
		})
		third <- err
	}()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })
	close(release)
	require.NoError(t, <-third)

	waitFor(t, func() bool { return len(s.tlog.ofKind("server")) == 3 })
	var cancelled map[string]interface{}
	for _, r := range s.tlog.ofKind("server") {
		assert.NotEqual(t, outcomeQueueFull, r["admission_outcome"], "the freed slot was reusable")
		if r["admission_outcome"] == outcomeCancelledInQueue {
			cancelled = r
		}
	}
	require.NotNil(t, cancelled, "every server-side outcome gets a record")
	assert.Equal(t, "Canceled", cancelled["response_code"])
	assert.Equal(t, true, cancelled["is_error"])
	assert.Equal(t, 0.0, cancelled["handler_ms"])
	assert.Equal(t, 0.0, cancelled["permit_released_at"], "no permit was ever held")
	assert.Equal(t, 0.0, cancelled["admission_queue_depth"])
}

// The server's strict finish rule is measured against the deadline the CONTEXT
// carries, converted against ONE clock base. Mixing two samples of a moving
// clock cut the request a tick early.
func TestServeClassifiesAgainstTheContextDeadline(t *testing.T) {
	const doc = "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n"

	run := func(offset int64) *testState {
		clock := newTickClock(time.Millisecond)
		s := newTestStateOn(t, clock, t.TempDir(), "svc-test", doc, nil, 0, nil)
		// An hour out, so only the strict finish rule can ever fire.
		ctx, cancel := context.WithDeadline(context.Background(), clock.Deadline(time.Hour))
		defer cancel()
		d, ok := ctx.Deadline()
		require.True(t, ok)
		deadline := clock.DeadlineNS(d)
		_, _ = s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			clock.FreezeAt(deadline + offset)
			return "ok", nil
		})
		return s
	}

	recs := run(-1).tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, outcomeAdmitted, recs[0]["admission_outcome"],
		"one ns before the context's own deadline is still a success")
	assert.Equal(t, "OK", recs[0]["response_code"])

	recs = run(0).tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, outcomeDeadlineAtFinish, recs[0]["admission_outcome"],
		"finishing AT the context's own deadline is a drop")
	assert.Equal(t, "DeadlineExceeded", recs[0]["response_code"])
}
