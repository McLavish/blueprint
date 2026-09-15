package rpcpolicy

import (
	"context"
	"math/rand"
	"runtime"
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
	return newTestStationOn(newRealClock(), workers, capacity, hold)
}

// newTestStationOn drives the station off a clock the test owns, so the strict
// now >= deadline boundary can be placed exactly instead of raced. It has no
// attempt log: a station driven directly has no attempt identities to name, so
// it writes no station events (see attemptID).
func newTestStationOn(clock Clock, workers int, capacity *int, hold bool) *station {
	return newStation(&ServerConfig{
		Workers:                 workers,
		QueueCapacity:           capacity,
		HoldPermitThroughFanout: hold,
	}, clock, nil)
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

// msim's drop points, in the order submit_attempt tests them.
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

// The attempt deadline is the CALLER's clock (msim's expire_in_queue): a
// request still waiting for a worker when it passes is reported AT that
// instant, not when the server next dequeues -- which used to let a client learn
// of its own 20 ms timeout only whenever a worker happened to free.
func TestStationDeadlineInQueueIsReportedAtTheDeadline(t *testing.T) {
	st := newTestStation(1, intPtr(4), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	before := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)

	got := make(chan admission, 1)
	go func() { got <- st.Acquire(ctx) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	// No worker is ever freed here: the report must arrive on the caller's clock
	// alone.
	var adm admission
	select {
	case adm = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("an expired waiter must return at its deadline, not wait for a worker")
	}
	assert.Equal(t, outcomeDeadlineInQueue, adm.outcome)
	assert.Nil(t, adm.permit, "an expired waiter never takes a worker")
	assert.True(t, adm.end.Equal(deadline), "the record ends AT the deadline")
	// wait == deadline - enqueue. Measured at the observation instead, it could
	// only be >= the 20 ms timeout, because a timer never fires early.
	assert.Greater(t, adm.wait, time.Duration(0))
	assert.Less(t, adm.wait, 20*time.Millisecond,
		"the wait ends at the deadline, not when this goroutine observed the timer")
	assert.LessOrEqual(t, adm.wait, deadline.Sub(before))
	assert.Equal(t, 0, adm.queueDepth, "the depth excludes the reporting attempt")
	assert.Equal(t, 1, adm.workersBusy)

	// It is still holding its slot: the server has not looked at the queue yet.
	assert.Equal(t, 1, st.QueueDepth())
	held.permit.release()
	waitFor(t, func() bool { return st.Busy() == 0 && st.QueueDepth() == 0 })
}

// deadline_at_dequeue survives for the exact tie alone: the dispatcher hands the
// permit over at the very instant the waiter's deadline passes (msim's
// _begin_service check). The station's clock is driven by hand onto the deadline
// exactly, while the context's real timer, an hour out, has not fired at all.
func TestStationDeadlineAtDequeueOnTheExactTie(t *testing.T) {
	clock := newFakeClock()
	st := newTestStationOn(clock, 1, intPtr(4), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	deadline := clock.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	got := make(chan admission, 1)
	go func() { got <- st.Acquire(ctx) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	clock.Advance(time.Hour) // the station's clock now reads exactly the deadline
	held.permit.release()

	adm := <-got
	assert.Equal(t, outcomeDeadlineAtDequeue, adm.outcome)
	assert.Nil(t, adm.permit, "the permit taken at the tie is handed straight back")
	assert.Equal(t, time.Hour, adm.wait)
	assert.True(t, adm.end.Equal(deadline),
		"the tie is booked at the deadline it tied with, not when this goroutine woke")
	waitFor(t, func() bool { return st.Busy() == 0 && st.QueueDepth() == 0 })
}

// msim's _start_next `if item.expired: continue`: an expired entry that surfaces
// at the head gives its slot back and is discarded -- no permit, no second
// report -- and the freed worker walks on to the next live waiter.
func TestStationPoppingAnExpiredEntryGrantsNoPermitAndSkipsToTheNext(t *testing.T) {
	st := newTestStation(1, intPtr(4), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	expiring := make(chan admission, 1)
	go func() { expiring <- st.Acquire(ctx) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	live := make(chan admission, 1)
	go func() { live <- st.Acquire(context.Background()) }()
	waitFor(t, func() bool { return st.QueueDepth() == 2 })

	first := <-expiring
	require.Equal(t, outcomeDeadlineInQueue, first.outcome)
	assert.Equal(t, 1, first.queueDepth, "the live waiter behind it, not itself")
	assert.Equal(t, 2, st.QueueDepth(), "the zombie is still ahead of the live entry")

	held.permit.release()
	adm := <-live
	require.Equal(t, outcomeAdmitted, adm.outcome, "the worker skipped the zombie and reached the next waiter")
	require.NotNil(t, adm.permit)
	assert.Equal(t, 1, st.Busy(), "one worker in service, never two: the zombie took none")
	assert.Equal(t, 0, st.QueueDepth(), "both entries left the queue")
	assert.Equal(t, 0, adm.queueDepth)
	assert.Equal(t, 0, adm.workersBusy, "the handover keeps busy constant")

	adm.permit.release()
	waitFor(t, func() bool { return st.Busy() == 0 })
}

// An expired waiter returns at its deadline and is discarded at the dequeue, so
// neither a goroutine nor a worker is left behind -- the failure mode the
// queued-cancellation path was fixed for once already.
func TestStationExpiredWaitersLeakNoGoroutinesOrPermits(t *testing.T) {
	const n = 32
	base := runtime.NumGoroutine()
	st := newTestStation(1, nil, false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	got := make(chan admission, n)
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		go func() { got <- st.Acquire(ctx) }()
		waitFor(t, func() bool { return st.QueueDepth() == i+1 })
	}
	for i := 0; i < n; i++ {
		select {
		case adm := <-got:
			assert.Equal(t, outcomeDeadlineInQueue, adm.outcome)
			assert.Nil(t, adm.permit)
		case <-time.After(5 * time.Second):
			t.Fatalf("expired waiter %d never returned: it is blocked on a worker", i)
		}
	}
	assert.Equal(t, n, st.QueueDepth(), "every expired entry still holds its slot")

	// One release drains the whole run of zombies and frees the worker: none of
	// them consumes it.
	held.permit.release()
	waitFor(t, func() bool { return st.Busy() == 0 && st.QueueDepth() == 0 })
	waitFor(t, func() bool { return runtime.NumGoroutine() <= base+2 })
}

// --- the deadline-versus-cancel rule (deadlineCancelSlack) -----------------
//
// gRPC gives the server a RELATIVE timeout, so a caller whose own deadline fires
// cancels the stream at about the instant the server's rebuilt deadline would
// have fired: the server sees Canceled or DeadlineExceeded by a coin flip. The
// INSTANT decides which msim drop reason it was, not the error.

func TestDeadlineCauseSeparatesTheDeadlineFromAnExplicitCancel(t *testing.T) {
	now := time.Now()

	live, cancelLive := context.WithCancel(context.Background())
	defer cancelLive()
	assert.False(t, deadlineCause(live, now), "a context that is not done yet has no cause")

	cancelledNoDeadline, cancelNoDeadline := context.WithCancel(context.Background())
	cancelNoDeadline()
	assert.False(t, deadlineCause(cancelledNoDeadline, now),
		"with no deadline at all, a cancel can only be a cancel")

	timedOut, cancelTimedOut := context.WithDeadline(context.Background(), now.Add(-time.Second))
	defer cancelTimedOut()
	assert.True(t, deadlineCause(timedOut, now), "DeadlineExceeded speaks for itself")

	// One CANCELLED context carrying a deadline, read at a series of instants:
	// the deadline is an hour of real time away, so only the instant passed in
	// decides -- no timer can race the assertions.
	d := now.Add(time.Hour)
	cancelled, cancelIt := context.WithDeadline(context.Background(), d)
	cancelIt()
	require.ErrorIs(t, cancelled.Err(), context.Canceled)

	assert.False(t, deadlineCause(cancelled, d.Add(-time.Hour)),
		"a cancel nowhere near the deadline is an explicit cancel")
	assert.False(t, deadlineCause(cancelled, d.Add(-deadlineCancelSlack-time.Nanosecond)),
		"one nanosecond beyond the slack is still an explicit cancel")
	assert.True(t, deadlineCause(cancelled, d.Add(-deadlineCancelSlack)),
		"the slack boundary itself is the deadline")
	assert.True(t, deadlineCause(cancelled, d.Add(-2*time.Millisecond)),
		"a cancel two milliseconds short of the deadline is the RST_STREAM the deadline sent")
	assert.True(t, deadlineCause(cancelled, d.Add(time.Millisecond)),
		"a cancel observed past the deadline is the deadline")
}

// A queued waiter whose caller cancels the stream AT its deadline is msim's
// DEADLINE, so it is booked deadline_in_queue and KEEPS its slot -- the
// difference that decides whether the next arrival is queued or shed.
func TestStationACancelAtTheDeadlineKeepsItsSlotAsADeadline(t *testing.T) {
	st := newTestStation(1, intPtr(4), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	got := make(chan admission, 1)
	go func() { got <- st.Acquire(ctx) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	cancel() // the caller's transport tears the stream down at its deadline
	adm := <-got
	assert.Equal(t, outcomeDeadlineInQueue, adm.outcome,
		"a Canceled within the slack of the deadline IS the deadline")
	assert.Nil(t, adm.permit)
	assert.True(t, adm.end.Equal(deadline), "the record ends AT the deadline")
	assert.Equal(t, 1, st.QueueDepth(), "a deadline keeps its slot; only a cancel gives it back")

	held.permit.release()
	waitFor(t, func() bool { return st.Busy() == 0 && st.QueueDepth() == 0 })
}

// ...while a cancel nowhere near the deadline is a genuine cancel, and gives its
// slot back at once (msim's cancel_queued).
func TestStationACancelFarFromItsDeadlineIsStillACancel(t *testing.T) {
	st := newTestStation(1, intPtr(4), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	got := make(chan admission, 1)
	go func() { got <- st.Acquire(ctx) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	cancel()
	adm := <-got
	assert.Equal(t, outcomeCancelledInQueue, adm.outcome)
	assert.True(t, adm.end.IsZero(), "a cancel is reported now, not at a deadline an hour away")
	assert.Equal(t, 0, st.QueueDepth(), "a cancel discounts its slot at cancel time")

	held.permit.release()
	waitFor(t, func() bool { return st.Busy() == 0 })
}

// --- the dispatcher decides the permit-versus-deadline race ----------------

// A waiter whose deadline has already passed when a worker frees is expired by
// the DISPATCHER, at the release instant, even though its own timer goroutine
// has not run: it takes no worker, the live waiter behind it starts THERE, and
// the expired one is still booked at its own deadline. Deciding this at wake-up
// instead let the zombie take the worker and made the live waiter queue on for
// nothing.
func TestStationDispatcherExpiresTheHeadAndServesTheNextWaiterAtTheReleaseInstant(t *testing.T) {
	clock := newFakeClock()
	st := newTestStationOn(clock, 1, intPtr(4), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	// An hour out on the station's clock, so the context's real timer cannot
	// fire during the test: only the dispatcher can decide this waiter.
	deadline := clock.Now().Add(time.Hour)
	ctxA, cancelA := context.WithDeadline(context.Background(), deadline)
	defer cancelA()
	gotA := make(chan admission, 1)
	go func() { gotA <- st.Acquire(ctxA) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	gotB := make(chan admission, 1)
	go func() { gotB <- st.Acquire(context.Background()) }()
	waitFor(t, func() bool { return st.QueueDepth() == 2 })

	clock.Advance(time.Hour + time.Millisecond) // past A's deadline
	held.permit.release()

	var admA admission
	select {
	case admA = <-gotA:
	case <-time.After(5 * time.Second):
		t.Fatal("the dispatcher must free an expired head's goroutine, not leave it blocked on a permit")
	}
	assert.Equal(t, outcomeDeadlineInQueue, admA.outcome)
	assert.Nil(t, admA.permit, "an expired head takes no worker")
	assert.True(t, admA.end.Equal(deadline), "it is booked at its deadline, not at the release")
	assert.Equal(t, time.Hour, admA.wait)
	assert.Equal(t, 1, admA.queueDepth, "B, not itself")
	assert.Equal(t, 1, admA.workersBusy)

	admB := <-gotB
	require.Equal(t, outcomeAdmitted, admB.outcome)
	require.NotNil(t, admB.permit)
	assert.Equal(t, time.Hour+time.Millisecond, admB.wait,
		"B starts when the worker freed, not when A's timer finally fires")
	assert.Equal(t, 1, st.Busy(), "one worker in service, never two")

	admB.permit.release()
	waitFor(t, func() bool { return st.Busy() == 0 && st.QueueDepth() == 0 })
}

// The mirror case: a worker that frees one millisecond BEFORE the deadline
// started serving the attempt in msim, which then cut it in service. Booked
// against a clock read after the goroutine woke -- 30 ms late here -- it turned
// into a queue expiry instead.
func TestStationAHandoffBeforeTheDeadlineIsAdmittedThoughTheWaiterWakesLate(t *testing.T) {
	clock := newHandoffClock()
	st := newTestStationOn(clock, 1, intPtr(4), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	ctx, cancel := context.WithDeadline(context.Background(), clock.Now().Add(time.Hour))
	defer cancel()
	got := make(chan admission, 1)
	go func() { got <- st.Acquire(ctx) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	clock.Advance(time.Hour - time.Millisecond) // one millisecond to spare
	// Every clock reading taken AFTER the hand-off is 30 ms late, as a goroutine
	// that was not scheduled promptly would see.
	clock.Arm(30 * time.Millisecond)
	held.permit.release()

	adm := <-got
	require.Equal(t, outcomeAdmitted, adm.outcome,
		"the worker really did free before the deadline: msim starts service here and cuts it in service")
	require.NotNil(t, adm.permit)
	assert.Equal(t, time.Hour-time.Millisecond, adm.wait,
		"the wait ends at the hand-off, not at the wake-up")
	assert.True(t, adm.end.IsZero())

	adm.permit.release()
	waitFor(t, func() bool { return st.Busy() == 0 && st.QueueDepth() == 0 })
}

// The exact tie consumes NO worker. msim's _begin_service drops a waiter whose
// deadline equals the dequeue instant BEFORE `in_flight += 1`, and _start_next
// carries the same freed worker straight on to the next live entry. Handing the
// worker to the tie and waiting for its goroutine to give it back let a live
// waiter behind it expire on a worker that was already free.
func TestStationTheDequeueTieTakesNoWorkerAndTheNextWaiterStartsThere(t *testing.T) {
	clock := newHandoffClock()
	st := newTestStationOn(clock, 1, intPtr(4), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	// Both deadlines are an hour out on the station's clock, so no real timer
	// can fire during the test: the dispatcher alone decides both waiters.
	deadline := clock.Now().Add(time.Hour)
	ctxB, cancelB := context.WithDeadline(context.Background(), deadline)
	defer cancelB()
	gotB := make(chan admission, 1)
	go func() { gotB <- st.Acquire(ctxB) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	ctxC, cancelC := context.WithDeadline(context.Background(), deadline.Add(10*time.Millisecond))
	defer cancelC()
	gotC := make(chan admission, 1)
	go func() { gotC <- st.Acquire(ctxC) }()
	waitFor(t, func() bool { return st.QueueDepth() == 2 })

	clock.Advance(time.Hour) // the worker frees at EXACTLY B's deadline
	// Every reading taken AFTER the release is 20 ms late, as B's goroutine
	// would see if it were the one to hand the worker on: C's deadline is 10 ms
	// past the tie, so a station that waits for B to wake expires C on a worker
	// that has been free since the tie.
	clock.Arm(20 * time.Millisecond)
	held.permit.release()

	// The dispatcher is already done: the tie was dropped and the worker handed
	// on to C inside that one release, before either goroutine was scheduled.
	assert.Equal(t, 0, st.QueueDepth(), "both waiters left the queue inside the release")
	assert.Equal(t, 1, st.Busy(), "one worker in service, never two")

	admB := <-gotB
	assert.Equal(t, outcomeDeadlineAtDequeue, admB.outcome)
	assert.Nil(t, admB.permit, "the tie is dropped before a worker is taken, so there is no permit to hand back")
	assert.True(t, admB.end.Equal(deadline), "the tie is booked at the deadline it tied with")
	assert.Equal(t, time.Hour, admB.wait)
	assert.Equal(t, 1, admB.queueDepth, "C, not itself")
	assert.Equal(t, 0, admB.workersBusy, "the worker had already been given back when msim reported this")

	var admC admission
	select {
	case admC = <-gotC:
	case <-time.After(5 * time.Second):
		t.Fatal("the tie must not hold the freed worker until its own goroutine is scheduled")
	}
	require.Equal(t, outcomeAdmitted, admC.outcome, "the freed worker walked on to the live waiter behind the tie")
	require.NotNil(t, admC.permit)
	assert.Equal(t, time.Hour, admC.wait,
		"C starts at the instant the tie was decided, not when B's goroutine finally woke")

	admC.permit.release()
	waitFor(t, func() bool { return st.Busy() == 0 && st.QueueDepth() == 0 })
}

// Every expired waiter is settled at ITS OWN deadline, wherever it sits in the
// queue -- not at the dequeue that happens to reach it. Snapshotting the depth
// at the dequeue while backdating the record to the deadline reported a queue
// that had already been drained by the entries in front: two zombies expiring in
// the same window must each still see the other, because an expired entry keeps
// its slot until the server reaches it.
func TestStationSettlesEveryExpiredWaiterAtItsOwnDeadline(t *testing.T) {
	clock := newFakeClock()
	st := newTestStationOn(clock, 1, intPtr(4), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	// B in front of A, both an hour out on the station's clock so that neither
	// goroutine can be woken by a real timer before the release below.
	deadlineB := clock.Now().Add(time.Hour)
	ctxB, cancelB := context.WithDeadline(context.Background(), deadlineB)
	defer cancelB()
	gotB := make(chan admission, 1)
	go func() { gotB <- st.Acquire(ctxB) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	deadlineA := deadlineB.Add(10 * time.Millisecond)
	ctxA, cancelA := context.WithDeadline(context.Background(), deadlineA)
	defer cancelA()
	gotA := make(chan admission, 1)
	go func() { gotA <- st.Acquire(ctxA) }()
	waitFor(t, func() bool { return st.QueueDepth() == 2 })

	clock.Advance(time.Hour + 20*time.Millisecond) // both deadlines are in the past
	held.permit.release()

	for _, tc := range []struct {
		name     string
		got      chan admission
		deadline time.Time
		wait     time.Duration
	}{
		{"B", gotB, deadlineB, time.Hour},
		{"A", gotA, deadlineA, time.Hour + 10*time.Millisecond},
	} {
		var adm admission
		select {
		case adm = <-tc.got:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s must be settled by the release, not left blocked on a permit", tc.name)
		}
		assert.Equal(t, outcomeDeadlineInQueue, adm.outcome, tc.name)
		assert.Nil(t, adm.permit, tc.name)
		assert.True(t, adm.end.Equal(tc.deadline), "%s ends at its own deadline", tc.name)
		assert.Equal(t, tc.wait, adm.wait, tc.name)
		assert.Equal(t, 1, adm.queueDepth,
			"%s: the other zombie still held its slot at this deadline", tc.name)
		assert.Equal(t, 1, adm.workersBusy, tc.name)
	}
	waitFor(t, func() bool { return st.Busy() == 0 && st.QueueDepth() == 0 })
}

// Whatever the interleaving of hand-off, deadline, cancel, in-service reap,
// early hand-back, and a holder descheduled between stamping the instant its
// attempt ended and claiming it, every attempt settles exactly once, takes at
// most c workers, never drives `busy` negative, is never handed a worker before
// it arrived, and leaves no goroutine behind.
//
// The admitted attempts carry their caller's deadline into service, so the
// station's own reaper races each holder's release for the same permit; a third
// of them hand the worker back early (msim's tandem release_worker) and then end
// some other way on top of that, and a third pause in that window, so the claim
// goes to whichever of the station and the holder reaches the mutex first -- and
// whoever loses reports the winner's claim rather than its own.
func TestStationMixedDeadlinesAndCancelsSettleExactlyOnce(t *testing.T) {
	const (
		workers = 3
		n       = 64
	)
	baseGoroutines := runtime.NumGoroutine()
	st := newTestStation(workers, nil, false)

	// Drawn up front: math/rand.Rand is not safe for concurrent use.
	rng := rand.New(rand.NewSource(20260915))
	type plan struct {
		kind      int // 0 deadline, 1 cancel, 2 neither
		ending    int // 0 plain release, 1 early hand-back, 2 stamp, pause, then claim
		fires     time.Duration
		holds     time.Duration
		stall     time.Duration
		handsBack bool
	}
	plans := make([]plan, n)
	for i := range plans {
		ending := rng.Intn(3)
		plans[i] = plan{
			kind:      rng.Intn(3),
			ending:    ending,
			fires:     time.Duration(rng.Intn(3000)) * time.Microsecond,
			holds:     time.Duration(rng.Intn(1500)) * time.Microsecond,
			stall:     time.Duration(rng.Intn(1500)) * time.Microsecond,
			handsBack: ending == 1,
		}
	}
	// Whether the STATION reaped a stalled holder's attempt at its deadline
	// before the holder got to the mutex, or the holder claimed it itself. Both
	// are exercised; which one wins each time is the scheduler's business, so it
	// is reported rather than asserted.
	var byStation, byHolder atomic.Int64

	// A watchdog samples occupancy throughout, so a negative or over-full `busy`
	// cannot hide between two attempts finishing.
	stop := make(chan struct{})
	bad := make(chan int, 1)
	var watchdog sync.WaitGroup
	watchdog.Add(1)
	go func() {
		defer watchdog.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if b := st.Busy(); b < 0 || b > workers {
				select {
				case bad <- b:
				default:
				}
				return
			}
		}
	}()

	var mu sync.Mutex
	outcomes := map[string]int{}
	var wg sync.WaitGroup
	for _, pl := range plans {
		wg.Add(1)
		go func(pl plan) {
			defer wg.Done()
			ctx, cancel := context.Background(), context.CancelFunc(func() {})
			switch pl.kind {
			case 0:
				ctx, cancel = context.WithTimeout(context.Background(), pl.fires)
			case 1:
				ctx, cancel = context.WithCancel(context.Background())
				time.AfterFunc(pl.fires, cancel)
			}
			defer cancel()

			adm := st.Acquire(ctx)
			mu.Lock()
			outcomes[adm.outcome]++
			mu.Unlock()
			switch adm.outcome {
			case outcomeAdmitted:
				if !assert.NotNil(t, adm.permit, "an admitted attempt holds a permit") {
					return
				}
				assert.GreaterOrEqual(t, adm.wait, time.Duration(0),
					"a hand-off is never booked before the arrival it serves")
				p := adm.permit
				if pl.handsBack {
					p.handBack(st.clock.Now())
					back := p.releasedEpoch()
					assert.Greater(t, back, 0.0, "a hand-back frees the worker at once")
					time.Sleep(pl.holds)
					p.release()
					assert.Equal(t, back, p.releasedEpoch(),
						"the occupancy ended at the hand-back; whatever ends the attempt does not move it")
					return
				}
				time.Sleep(pl.holds)
				if pl.ending == 2 {
					// The holder stamps the instant the attempt really ended and
					// is then descheduled before it can claim it -- the one window
					// the station cannot see into. Whichever of the two reaches
					// the mutex first decides the attempt, and the loser reports
					// the winner's claim rather than its own view of it.
					at := st.clock.Now()
					time.Sleep(pl.stall)
					c, won := p.claim(claimCompleted, at)
					if won {
						byHolder.Add(1)
						assert.Equal(t, claimCompleted, c.kind)
					} else {
						byStation.Add(1)
						assert.Equal(t, claimCut, c.kind,
							"nothing but the station's reap can take this attempt away")
					}
					assert.Same(t, c, p.claimed(), "the winning claim is the only one there is")
					assert.Greater(t, p.releasedEpoch(), 0.0, "the worker went back exactly once")
					return
				}
				p.release()
			case outcomeDeadlineAtSubmit, outcomeDeadlineInQueue, outcomeDeadlineAtDequeue, outcomeCancelledInQueue:
				assert.Nil(t, adm.permit, "%s holds no permit", adm.outcome)
			default:
				t.Errorf("unexpected outcome %q from an unbounded queue", adm.outcome)
			}
		}(pl)
	}
	wg.Wait()
	close(stop)
	watchdog.Wait()
	select {
	case b := <-bad:
		t.Fatalf("occupancy left the range [0, %d]: %d", workers, b)
	default:
	}

	total := 0
	for _, c := range outcomes {
		total += c
	}
	assert.Equal(t, n, total, "exactly one outcome per attempt, on every interleaving")
	t.Logf("stalled attempts claimed by the station's reap: %d, by their holder: %d",
		byStation.Load(), byHolder.Load())

	// A zombie that expired after the last release still holds its slot until the
	// server next looks at the queue -- so look, exactly as a later request would.
	drain := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, drain.outcome)
	drain.permit.release()
	assert.Equal(t, 0, st.Busy())
	assert.Equal(t, 0, st.QueueDepth())
	waitFor(t, func() bool { return runtime.NumGoroutine() <= baseGoroutines+2 })
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
	assert.Equal(t, true, recs[0]["occupancy_censored"],
		"the handler was still running when the deadline cut it")
}

// --- the in-service cut (msim's _finish_service at the deadline) -----------

// msim cuts an attempt in service AT its deadline and frees the worker THERE.
// Calling the handler synchronously let a handler that ignores its context hold
// capacity long past the deadline: with one worker, a 50 ms deadline and a
// handler sleeping on, msim admits the next arrival and this server shed it.
func TestServeCutsANonCooperativeHandlerAtItsDeadlineAndFreesTheWorker(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 0\n")
	st := s.registry.Load().Station()

	inHandler := make(chan struct{})
	blocked := make(chan struct{})
	defer close(blocked) // let the abandoned handler finish at the end of the test
	first := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	go func() {
		_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			close(inHandler)
			<-blocked // deliberately ignores its context, as a batch-2 handler may
			return "ok", nil
		})
		first <- err
	}()
	<-inHandler

	select {
	case err := <-first:
		require.Error(t, err)
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("the request must be cut at its deadline, not when the handler returns")
	}
	assert.Equal(t, 0, st.Busy(), "the worker is freed at the deadline, not at the handler's return")

	// The capacity msim has at 60 ms: the next arrival is served, not shed.
	resp, err := s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		return "ok", nil
	})
	require.NoError(t, err, "the freed worker was reusable")
	assert.Equal(t, "ok", resp)

	waitFor(t, func() bool { return len(s.tlog.ofKind("server")) == 2 })
	recs := s.tlog.ofKind("server")
	cut, served := recs[0], recs[1]
	assert.Equal(t, outcomeDeadlineAtFinish, cut["admission_outcome"])
	assert.Equal(t, "DeadlineExceeded", cut["response_code"])
	assert.Equal(t, true, cut["occupancy_censored"], "the worker was still held when the deadline cut it")
	assert.Equal(t, epochOf(deadline), cut["end_epoch"], "the record ends AT the deadline")
	assert.Greater(t, cut["permit_released_at"], 0.0)
	assert.Less(t, cut["permit_released_at"].(float64), epochOf(deadline)+0.25,
		"the permit went back at the deadline, not a second later")
	assert.Greater(t, cut["handler_ms"], 0.0, "the worker was held for the part of the handler that ran")
	assert.LessOrEqual(t, cut["handler_ms"].(float64), 50.0, "and no longer than the deadline allowed")

	assert.Equal(t, outcomeAdmitted, served["admission_outcome"])
	assert.Equal(t, false, served["occupancy_censored"], "a handler that ran to completion inside its deadline reports a complete occupancy")
}

// An abandoned handler is discarded outright: no second record, and its
// completion never reaches the failure roll (msim calls _fails_now only when the
// deadline did NOT cut the attempt).
func TestServeACutHandlerRollsNoFaultAndWritesOneRecord(t *testing.T) {
	epochMs, _ := epochNow()
	s := newTestStateWithFaults(t,
		"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n",
		scheduleOf(FaultRule{Method: testMethod, StartS: 0, EndS: 3600, PFail: 1.0}),
		epochMs, func() float64 { return 0.0 })

	inHandler := make(chan struct{})
	blocked := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		close(inHandler)
		<-blocked
		return "ok", nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "a p_fail of 1 must not overwrite the deadline")
	<-inHandler

	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1, "the abandoned handler writes no record of its own")
	assert.Equal(t, false, recs[0]["fault_hit"], "a cut attempt never reaches the roll")
	assert.Equal(t, true, recs[0]["occupancy_censored"])

	// And it stays one record after the handler finally returns.
	close(blocked)
	time.Sleep(20 * time.Millisecond)
	assert.Len(t, s.tlog.ofKind("server"), 1)
}

// An explicit cancel in service is msim's cancel_in_service: the worker is
// handed straight back, the attempt is censored, and it is NOT a deadline -- the
// admission outcome stays `admitted` and only the response code says the caller
// left.
func TestServeAnExplicitCancelInServiceFreesTheWorkerAndIsNotADeadline(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n")
	st := s.registry.Load().Station()

	// An hour out, so the cancel is nowhere near the deadline.
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	inHandler := make(chan struct{})
	blocked := make(chan struct{})
	defer close(blocked)
	done := make(chan error, 1)
	go func() {
		_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			close(inHandler)
			<-blocked
			return "ok", nil
		})
		done <- err
	}()
	<-inHandler
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("a cancel in service must hand the worker back at once")
	}
	assert.Equal(t, 0, st.Busy(), "cancellation reclaims capacity, it does not merely suppress the result")

	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, outcomeAdmitted, recs[0]["admission_outcome"], "a cancel is the caller's doing, not an admission drop")
	assert.Equal(t, "Canceled", recs[0]["response_code"])
	assert.Equal(t, true, recs[0]["occupancy_censored"], "CANCELLED in service is censored too")
	assert.Greater(t, recs[0]["permit_released_at"], 0.0)
}

// A caller whose own deadline fires cancels the stream: the server sees Canceled
// at about the instant its rebuilt deadline would have fired, and must book the
// DEADLINE anyway -- even though the strict now >= D_s check has not tripped.
func TestServeACancelAtTheDeadlineInServiceIsBookedAsTheDeadline(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n")
	st := s.registry.Load().Station()

	// Inside deadlineCancelSlack, so the cancel below is the deadline's own
	// RST_STREAM arriving a hair early.
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Millisecond)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	inHandler := make(chan struct{})
	blocked := make(chan struct{})
	defer close(blocked)
	done := make(chan error, 1)
	go func() {
		_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			close(inHandler)
			<-blocked
			return "ok", nil
		})
		done <- err
	}()
	<-inHandler
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err),
			"a cancel within the slack of the deadline is the deadline, not a cancel")
	case <-time.After(5 * time.Second):
		t.Fatal("the request was not cut")
	}
	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, outcomeDeadlineAtFinish, recs[0]["admission_outcome"])
	assert.Equal(t, "DeadlineExceeded", recs[0]["response_code"])
	assert.Equal(t, true, recs[0]["occupancy_censored"])
	assert.Equal(t, epochOf(deadline), recs[0]["end_epoch"],
		"a deadline-caused record ends at D_s, never at the instant it was observed")
	assert.Equal(t, 0, st.Busy())
}

// The cut abandons a goroutine per request; once the handlers it abandoned
// return, nothing is left behind.
func TestServeCutHandlersLeaveNoGoroutineOrPermitBehind(t *testing.T) {
	const n = 16
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 16\n  queue_capacity: 0\n")
	st := s.registry.Load().Station()
	baseGoroutines := runtime.NumGoroutine()

	blocked := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
				<-blocked
				return "ok", nil
			})
			assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
		}()
	}
	wg.Wait()
	assert.Equal(t, 0, st.Busy(), "every permit went back at its deadline")
	assert.Len(t, s.tlog.ofKind("server"), n, "one record per attempt, and only one")

	close(blocked) // the abandoned handlers return
	waitFor(t, func() bool { return runtime.NumGoroutine() <= baseGoroutines+2 })
	assert.Len(t, s.tlog.ofKind("server"), n, "and still write nothing on the way out")
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
	assert.Equal(t, false, recs[0]["occupancy_censored"],
		"a stripped context can never cut the work, so the sample is complete")
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

// An expired DEADLINE is the other rule: the waiter is told at its deadline but
// KEEPS its queue slot, so the zombie goes on shedding load until the dispatcher
// reaches it. msim's `_queued` discounts a cancel at cancel time and an expiry
// only at dequeue, and a cancel and a timeout are therefore not interchangeable.
func TestStationDeadlineDoesNotFreeTheSlotEarly(t *testing.T) {
	st := newTestStation(1, intPtr(1), false)
	held := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, held.outcome)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	got := make(chan admission, 1)
	go func() { got <- st.Acquire(ctx) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	adm := <-got // reported at the deadline, with no worker freed
	assert.Equal(t, outcomeDeadlineInQueue, adm.outcome)
	assert.Nil(t, adm.permit)

	// Still queued: an expired deadline does not give the slot back, so the
	// station is still full and the next arrival is shed.
	assert.Equal(t, 1, st.QueueDepth())
	assert.Equal(t, outcomeQueueFull, st.Acquire(context.Background()).outcome)

	// The server discovers the expiry only now, and discards the entry: the slot
	// comes back and no worker is taken.
	held.permit.release()
	waitFor(t, func() bool { return st.QueueDepth() == 0 })
	assert.Equal(t, 0, st.Busy(), "the discarded entry never took a worker")
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

// A record is written for the queued-deadline drop too, and it ends AT the
// deadline: the pipeline must see the instant the caller timed out, not the
// instant a worker happened to free.
func TestServeWritesADeadlineInQueueRecordEndingAtTheDeadline(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n")
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

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	done := make(chan error, 1)
	go func() {
		_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			t.Error("an expired waiter must never reach the handler")
			return nil, nil
		})
		done <- err
	}()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("the expired waiter did not return at its deadline")
	}
	// The only worker is still held, well past the deadline, and the record has
	// already been written: it was not written at a dequeue.
	assert.Equal(t, 1, st.QueueDepth(), "the expired entry still holds its slot")
	close(release)

	waitFor(t, func() bool { return len(s.tlog.ofKind("server")) == 2 })
	var expired map[string]interface{}
	for _, r := range s.tlog.ofKind("server") {
		if r["admission_outcome"] == outcomeDeadlineInQueue {
			expired = r
		}
	}
	require.NotNil(t, expired, "every server-side outcome gets a record")
	assert.Equal(t, "DeadlineExceeded", expired["response_code"])
	assert.Equal(t, true, expired["is_error"])
	assert.Equal(t, 0.0, expired["handler_ms"], "a dropped request never ran the handler")
	assert.Equal(t, 0.0, expired["permit_released_at"], "no permit was ever held")
	assert.Equal(t, 0.0, expired["admission_queue_depth"])
	assert.Equal(t, epochOf(deadline), expired["end_epoch"], "the record ends AT the deadline")
	assert.Less(t, expired["admission_wait_ms"].(float64), 20.0,
		"the wait ends at the deadline, not when the timer was observed")

	// The dispatcher has since reached the zombie and discarded it: no second
	// record, and the worker it skipped went back to the station.
	waitFor(t, func() bool { return st.QueueDepth() == 0 && st.Busy() == 0 })
	assert.Len(t, s.tlog.ofKind("server"), 2, "a discarded entry is not reported twice")
}

// strip_inbound_deadline (arm A1's deployment counterpart) reaches the queue
// too: the stripped context carries no deadline, so no caller-clock event can
// fire while the request waits. It is served when a worker frees, however late.
func TestServeStripInboundDeadlineNeverExpiresInQueue(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n  strip_inbound_deadline: true\n")
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

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			return "ok", nil
		})
		done <- err
	}()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	<-ctx.Done() // the caller's deadline passes...
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, 1, st.QueueDepth(), "a stripped context has no deadline to fire")
	select {
	case err := <-done:
		t.Fatalf("the stripped request must still be queued, got err=%v", err)
	default:
	}

	close(release)
	require.NoError(t, <-done, "it runs when a worker frees, however late")
	waitFor(t, func() bool { return len(s.tlog.ofKind("server")) == 2 })
	for _, r := range s.tlog.ofKind("server") {
		assert.Equal(t, outcomeAdmitted, r["admission_outcome"])
		assert.Equal(t, "OK", r["response_code"])
	}
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

// --- the attempt is classified at the instant it ENDED ---------------------

// msim finishes a service at min(begin + service, deadline) and classifies it
// THERE. A Go handler returns on its own goroutine and the interceptor hears
// about it whenever it is next scheduled: reading a fresh clock at that moment
// booked a handler that beat its deadline by a millisecond as a
// deadline_at_finish, because the interceptor woke a millisecond past it.
func TestServeBooksACompletionAtTheInstantTheHandlerReturned(t *testing.T) {
	clock := newHandoffClock()
	s := newTestStateOn(t, clock, t.TempDir(), "svc-test",
		"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n",
		nil, 0, nil)

	// An hour out on the station's clock, so the context's own timer cannot fire
	// during the test and only the two instants below decide the record.
	deadline := clock.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	resp, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		clock.Advance(time.Hour - time.Millisecond) // the handler finishes one ms inside its deadline
		clock.Arm(2 * time.Millisecond)             // every reading after the stamp is two ms past it
		return "ok", nil
	})
	require.NoError(t, err, "a handler that finished before its deadline succeeded, however late it was observed")
	assert.Equal(t, "ok", resp)

	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, outcomeAdmitted, recs[0]["admission_outcome"])
	assert.Equal(t, "OK", recs[0]["response_code"])
	assert.Equal(t, false, recs[0]["occupancy_censored"], "the handler ran to completion inside its deadline")
	assert.Equal(t, epochOf(deadline.Add(-time.Millisecond)), recs[0]["end_epoch"],
		"the record ends when the handler returned, not when the interceptor woke")
}

// The exact finish tie, and the order of the two checks. msim's _finish_service
// tests the deadline FIRST and returns without ever reaching _fails_now, so a
// p_fail of 1 cannot overwrite the deadline; and _complete_attempt books the
// occupancy as censored, because the worker was still held when the deadline
// took it.
func TestServeBooksTheExactFinishTieAsADeadlineAndRollsNoFault(t *testing.T) {
	clock := newFakeClock()
	s := newTestStateOn(t, clock, t.TempDir(), "svc-test",
		"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n",
		scheduleOf(FaultRule{Method: testMethod, StartS: 0, EndS: 36000, PFail: 1}),
		clock.Now().UnixMilli(), func() float64 { return 0.0 })

	deadline := clock.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		clock.Advance(time.Hour) // returns at EXACTLY its deadline
		return "ok", nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "a p_fail of 1 must not overwrite the deadline")

	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, outcomeDeadlineAtFinish, recs[0]["admission_outcome"])
	assert.Equal(t, false, recs[0]["fault_hit"], "msim checks the deadline before it rolls, and never reaches _fails_now on one")
	assert.Equal(t, true, recs[0]["occupancy_censored"], "the worker was still held when the deadline took it")
	assert.Equal(t, epochOf(deadline), recs[0]["end_epoch"])
	assert.Equal(t, epochOf(deadline), recs[0]["permit_released_at"])
}

// The station reaps an in-service deadline ITSELF, at the deadline, rather than
// waiting for the serving goroutine to be scheduled. With one worker and no
// queue, a permit whose deadline passed while nothing touched the station used
// to shed the next arrival queue_full; msim had freed that worker at the
// deadline and served it.
func TestServeSettlesAnInServiceDeadlineBeforeTheNextArrival(t *testing.T) {
	clock := newFakeClock()
	s := newTestStateOn(t, clock, t.TempDir(), "svc-test",
		"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 0\n",
		nil, 0, nil)
	st := s.registry.Load().Station()

	// An hour out on the STATION's clock: the serving goroutine's own ctx.Done()
	// cannot fire during the test, so the only thing that can free this worker at
	// its deadline is the station settling itself.
	deadline := clock.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	inHandler := make(chan struct{})
	blocked := make(chan struct{})
	cut := make(chan error, 1)
	go func() {
		_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			close(inHandler)
			<-blocked // deliberately ignores its context, as a batch-2 handler may
			return "ok", nil
		})
		cut <- err
	}()
	<-inHandler
	require.Equal(t, 1, st.Busy())

	// Ten milliseconds past the deadline, with nothing having touched the station
	// in between. msim freed this worker at the deadline, so the arrival is
	// served rather than shed.
	clock.Advance(time.Hour + 10*time.Millisecond)
	resp, err := s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		return "ok", nil
	})
	require.NoError(t, err, "queue_full here would mean the worker was still booked busy past its deadline")
	assert.Equal(t, "ok", resp)
	assert.Equal(t, 0, st.Busy())

	close(blocked) // the abandoned handler finally returns
	cutErr := <-cut
	require.Error(t, cutErr)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(cutErr))

	waitFor(t, func() bool { return len(s.tlog.ofKind("server")) == 2 })
	var reaped map[string]interface{}
	for _, r := range s.tlog.ofKind("server") {
		if r["admission_outcome"] == outcomeDeadlineAtFinish {
			reaped = r
		}
	}
	require.NotNil(t, reaped, "the reaped attempt still gets exactly one record")
	assert.Equal(t, epochOf(deadline), reaped["permit_released_at"], "the worker went back AT the deadline")
	assert.Equal(t, epochOf(deadline), reaped["end_epoch"])
	assert.Equal(t, true, reaped["occupancy_censored"], "the worker was still held when the deadline took it")
	assert.Len(t, s.tlog.ofKind("server"), 2, "and the abandoned handler writes nothing on its way out")
}

// occupancy_censored is a statement about the WORKER, not the handler. msim's
// early-release branch reports a COMPLETE occupancy sample even when the attempt
// itself is cut: a relay that hands its permit back after its local stage
// contributed a full 12 ms of occupancy, and censoring it would drop that sample
// from the service-time calibration entirely.
func TestServeOccupancyCensoringMirrorsTheEarlyRelease(t *testing.T) {
	for _, tc := range []struct {
		name         string
		handBack     bool
		wantCensored bool
	}{
		{"the worker was still held at the cut", false, true},
		{"the worker had already gone back", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock()
			s := newTestStateOn(t, clock, t.TempDir(), "svc-test",
				"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n",
				nil, 0, nil)

			// The deadline is an hour out on the station's clock; the relay's
			// local stage is the first 12 ms of it.
			deadline := clock.Now().Add(time.Hour)
			handBackAt := clock.Now().Add(12 * time.Millisecond)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()

			_, err := s.serve(ctx, nil, testMethod, func(hctx context.Context, _ interface{}) (interface{}, error) {
				clock.Advance(12 * time.Millisecond) // the local stage
				if tc.handBack {
					ReleasePermit(hctx)
				}
				clock.Advance(time.Hour - 12*time.Millisecond) // waiting downstream, to the deadline
				return "ok", nil
			})
			require.Error(t, err)
			assert.Equal(t, codes.DeadlineExceeded, status.Code(err))

			recs := s.tlog.ofKind("server")
			require.Len(t, recs, 1)
			assert.Equal(t, outcomeDeadlineAtFinish, recs[0]["admission_outcome"])
			assert.Equal(t, tc.wantCensored, recs[0]["occupancy_censored"])
			want := epochOf(deadline)
			if tc.handBack {
				want = epochOf(handBackAt)
			}
			assert.Equal(t, want, recs[0]["permit_released_at"],
				"the occupancy ends where the worker went back")
		})
	}
}

// The mirror of the case above, through the in-service cut rather than the
// finish tie: a relay hands its permit back, waits downstream, and is cut at its
// deadline. The occupancy it reports ended at the hand-back and is COMPLETE --
// censoring it would drop a full service-time sample from the calibration every
// time a downstream call ran long.
func TestServeAnEarlyReleaseIsNotCensoredByALaterCut(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n")
	st := s.registry.Load().Station()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	blocked := make(chan struct{})
	defer close(blocked) // let the abandoned handler finish at the end of the test

	_, err := s.serve(ctx, nil, testMethod, func(hctx context.Context, _ interface{}) (interface{}, error) {
		ReleasePermit(hctx) // the local stage is over; the downstream call is not
		<-blocked
		return "ok", nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	assert.Equal(t, 0, st.Busy())

	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, outcomeDeadlineAtFinish, recs[0]["admission_outcome"])
	assert.Equal(t, false, recs[0]["occupancy_censored"],
		"the worker had already gone back, so its occupancy is a complete observation")
	assert.Greater(t, recs[0]["permit_released_at"], 0.0)
	assert.Less(t, recs[0]["permit_released_at"].(float64), epochOf(deadline),
		"the occupancy ended at the hand-back, well before the deadline that cut the attempt")
}

// --- a claim is taken and applied in ONE critical section -------------------
//
// The claim is the single statement of how an attempt ended: it is taken under
// the station mutex, at the instant the attempt really ended, and applied there
// and then. Taking it lock-free and applying it later let a completion and the
// station's reap of the same permit interleave, and the attempt then had two
// truths -- a record written from one and a worker committed by the other.

// A claim taken late is still applied at the instant it NAMES: the handler
// returned at 50 ms and its goroutine reached the station only at 60, and the
// worker went back at 50 either way. With K=0 the arrival that follows proves it:
// it is admitted only because that worker really did go back.
func TestStationAppliesALateClaimAtTheInstantItNames(t *testing.T) {
	for _, tc := range []struct {
		name     string
		capacity *int
	}{
		{"K=1", intPtr(1)},
		{"K=0", intPtr(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock()
			st := newTestStationOn(clock, 1, tc.capacity, false)

			// P holds the only worker, under a deadline far enough out that the
			// station has no reason to reap it.
			ctxP, cancelP := context.WithDeadline(context.Background(), clock.Now().Add(time.Hour))
			defer cancelP()
			held := st.Acquire(ctxP)
			require.Equal(t, outcomeAdmitted, held.outcome)

			// The handler returns at 50 ms and stamps its completion THERE...
			clock.Advance(50 * time.Millisecond)
			completedAt := clock.Now()
			// ...and is descheduled; only at 60 ms does it reach the station.
			clock.Advance(10 * time.Millisecond)
			c, won := held.permit.claim(claimCompleted, completedAt)
			require.True(t, won)
			assert.Equal(t, claimCompleted, c.kind)
			assert.Equal(t, epochOf(completedAt), held.permit.releasedEpoch(),
				"the worker went back where the handler returned, not where the station heard of it")
			assert.Equal(t, 0, st.Busy())

			adm := st.Acquire(context.Background())
			require.Equal(t, outcomeAdmitted, adm.outcome,
				"msim freed this worker at 50 ms, so the arrival takes it")
			require.NotNil(t, adm.permit)
			assert.Equal(t, time.Duration(0), adm.wait,
				"a free worker is taken on the fast path: no queueing, and no negative wait")
			assert.Equal(t, 0, st.QueueDepth())
			assert.Equal(t, 1, st.Busy(), "one worker in service, never two")

			// Claiming again moves nothing: the attempt ended once.
			again, won := held.permit.claim(claimCompleted, clock.Now())
			assert.False(t, won)
			assert.Same(t, c, again)
			assert.Equal(t, epochOf(completedAt), held.permit.releasedEpoch())
			assert.Equal(t, 1, st.Busy())

			adm.permit.release()
			assert.Equal(t, 0, st.Busy())
		})
	}
}

// The other half of the rule: NO REWRITING COMMITTED TIME. A claim that reaches
// the station after it has already committed decisions past the claim's instant
// is applied at lastSettled, never backdated -- so a hand-off can never precede
// the arrival of the waiter it hands to.
func TestStationAppliesALateClaimAtTheCommittedInstant(t *testing.T) {
	clock := newFakeClock()
	st := newTestStationOn(clock, 1, intPtr(2), false)
	ctxP, cancelP := context.WithDeadline(context.Background(), clock.Now().Add(time.Hour))
	defer cancelP()
	held := st.Acquire(ctxP)
	require.Equal(t, outcomeAdmitted, held.outcome)

	// The handler returns at 50 ms, but its goroutine has not reached the station.
	clock.Advance(50 * time.Millisecond)
	completedAt := clock.Now()

	// At 60 ms an arrival is queued: the station commits to a picture in which
	// the only worker is still busy.
	clock.Advance(10 * time.Millisecond)
	arrivedAt := clock.Now()
	got := make(chan admission, 1)
	go func() { got <- st.Acquire(context.Background()) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	// Only NOW does the holder reach the station with its 50 ms completion.
	_, won := held.permit.claim(claimCompleted, completedAt)
	require.True(t, won)

	adm := <-got
	require.Equal(t, outcomeAdmitted, adm.outcome)
	require.NotNil(t, adm.permit)
	assert.Equal(t, time.Duration(0), adm.wait,
		"the hand-off is booked at 60 ms, where the station really was -- never at 50, before this waiter existed")
	assert.GreaterOrEqual(t, adm.wait, time.Duration(0))
	assert.Equal(t, epochOf(arrivedAt), held.permit.releasedEpoch(),
		"permit_released_at is the clamped instant; the record's own end still says 50 ms")

	adm.permit.release()
	waitFor(t, func() bool { return st.Busy() == 0 && st.QueueDepth() == 0 })
}

// A hand-back is separate, immutable history, and no terminal claim supersedes
// it. ReleasePermit gave the worker back at 50 ms; the station must not reap the
// attempt at its deadline afterwards, and the cut that does end it must book the
// occupancy as the COMPLETE one that ended at 50 rather than a censored one
// ending at the deadline.
func TestStationAHandBackIsNotSupersededByALaterCut(t *testing.T) {
	clock := newFakeClock()
	st := newTestStationOn(clock, 1, intPtr(4), false)
	deadline := clock.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	held := st.Acquire(ctx)
	require.Equal(t, outcomeAdmitted, held.outcome)

	// The relay's local stage ends at 50 ms: ReleasePermit hands the worker back
	// there, while the attempt runs on downstream.
	clock.Advance(50 * time.Millisecond)
	handBackAt := clock.Now()
	held.permit.handBack(handBackAt)
	assert.Equal(t, 0, st.Busy(), "the worker went back at the hand-back")

	// The station is next touched past the permit's deadline -- where it would
	// reap an attempt that still held a worker. This one does not.
	clock.Advance(time.Hour)
	adm := st.Acquire(context.Background())
	require.Equal(t, outcomeAdmitted, adm.outcome,
		"the worker went back at 50 ms, so this arrival takes it")
	assert.Nil(t, held.permit.claimed(), "a permit whose worker is already back is not the station's to cut")

	_, handedBack, released := held.permit.endSnapshot()
	assert.True(t, handedBack,
		"occupancy_censored is false: the worker had already gone back, so nothing truncated that sample")
	assert.Equal(t, epochOf(handBackAt), released,
		"applied at 50 ms: lastSettled had not passed it, so the clamp does not bite")

	// The attempt is cut at its deadline afterwards. It closes the record and
	// releases nothing: the worker went back at the hand-back.
	_, won := held.permit.claim(claimCut, deadline)
	require.True(t, won)
	_, handedBack, released = held.permit.endSnapshot()
	assert.True(t, handedBack, "a terminal claim never supersedes the hand-back")
	assert.Equal(t, epochOf(handBackAt), released)
	assert.Equal(t, 1, st.Busy(), "only the arrival's worker; the cut freed nothing")

	adm.permit.release()
	waitFor(t, func() bool { return st.Busy() == 0 })
}

// The race, end to end, with the station winning it: the handler stamps its
// completion one millisecond inside the deadline and is descheduled before it
// can claim it, and the station reaps the permit at the deadline while it is
// parked there. The claim the reap took is the attempt's -- one record, the cut,
// at the deadline, censored and rolling no fault -- however inviting the
// handler's "ok" looks by the time the interceptor reads it.
//
// Stamping the completion and taking the claim used to be two steps. Between
// them the reap could advance committed time to D and then lose the claim to the
// completion, and the record said "success at D - 1 ms" over a station that had
// held the worker to D; or the reap took the claim and the interceptor wrote an
// ordinary success from `res` anyway. Two truths, one attempt.
func TestServeACompletionThatLosesToTheReapIsBookedAsTheCut(t *testing.T) {
	clock := newFakeClock()
	s := newTestStateOn(t, clock, t.TempDir(), "svc-test",
		"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n",
		scheduleOf(FaultRule{Method: testMethod, StartS: 0, EndS: 36000, PFail: 1}),
		clock.Now().UnixMilli(), func() float64 { return 0.0 })
	st := s.registry.Load().Station()

	stamped, proceed := make(chan struct{}), make(chan struct{})
	s.beforeClaim = func() {
		close(stamped)
		<-proceed
	}

	// An hour out on the STATION's clock, so no real timer can fire during the
	// test: the only thing that can cut this attempt is the station's own reap.
	deadline := clock.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	served := make(chan error, 1)
	go func() {
		_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			clock.Advance(time.Hour - time.Millisecond) // finishes one ms inside its deadline
			return "ok", nil
		})
		served <- err
	}()
	<-stamped

	// The handler has stamped D - 1 ms and is parked in front of its claim. Time
	// reaches the deadline, and an arrival that is already expired settles the
	// station without mutating it: the reaper alone takes this permit.
	clock.Advance(time.Millisecond)
	expiredCtx, cancelExpired := context.WithDeadline(context.Background(), clock.Now().Add(-time.Millisecond))
	defer cancelExpired()
	require.Equal(t, outcomeDeadlineAtSubmit, st.Acquire(expiredCtx).outcome)
	assert.Equal(t, 0, st.Busy(), "the reap freed the worker at the deadline")

	close(proceed)
	err := <-served
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))

	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1, "one record per attempt, whichever contender took the claim")
	assert.Equal(t, outcomeDeadlineAtFinish, recs[0]["admission_outcome"])
	assert.Equal(t, "DeadlineExceeded", recs[0]["response_code"])
	assert.Equal(t, epochOf(deadline), recs[0]["end_epoch"],
		"the record ends where the claim that WON says it did, not where the handler stamped")
	assert.Equal(t, epochOf(deadline), recs[0]["permit_released_at"],
		"and the worker went back at the same instant the record ends")
	assert.Equal(t, true, recs[0]["occupancy_censored"], "the worker was still held when the deadline took it")
	assert.Equal(t, false, recs[0]["fault_hit"], "a cut attempt never reaches the roll, p_fail 1 or not")
}

// The mirror, with the handler winning it: the same handler, parked in the same
// window, gets to the mutex FIRST. Its completion is the claim, so the record is
// the success msim books at D - 1 ms, the worker went back there, and the reap
// that would have come afterwards has nothing left to take.
func TestServeACompletionThatBeatsTheReapIsBookedAsTheCompletion(t *testing.T) {
	clock := newFakeClock()
	s := newTestStateOn(t, clock, t.TempDir(), "svc-test",
		"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n",
		nil, 0, nil)
	st := s.registry.Load().Station()

	// Only the first attempt is held in the window; the arrival at the end of the
	// test runs straight through.
	stamped, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.beforeClaim = func() {
		once.Do(func() {
			close(stamped)
			<-proceed
		})
	}

	deadline := clock.Now().Add(time.Hour)
	completedAt := deadline.Add(-time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	served := make(chan error, 1)
	go func() {
		_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			clock.Advance(time.Hour - time.Millisecond)
			return "ok", nil
		})
		served <- err
	}()
	<-stamped
	close(proceed) // it reaches the station before anything else does
	require.NoError(t, <-served)

	assert.Equal(t, 0, st.Busy())
	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, outcomeAdmitted, recs[0]["admission_outcome"])
	assert.Equal(t, "OK", recs[0]["response_code"])
	assert.Equal(t, epochOf(completedAt), recs[0]["end_epoch"])
	assert.Equal(t, epochOf(completedAt), recs[0]["permit_released_at"],
		"the worker went back where the handler returned")
	assert.Equal(t, false, recs[0]["occupancy_censored"])

	// Past the deadline the station has nothing to reap: the attempt ended, and
	// its worker is free for the next arrival.
	clock.Advance(2 * time.Millisecond)
	resp, err := s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		return "ok", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "ok", resp)
	assert.Len(t, s.tlog.ofKind("server"), 2, "and the completed attempt is not reported a second time")
}

// An attempt is CLASSIFIED at the instant it ended, not at the instant its claim
// is taken. A downstream call answers with its OWN DeadlineExceeded one
// millisecond inside D_s, under a parent context that is still live; the worker
// goroutine is descheduled in front of its claim, and the parent expires while it
// is parked. msim books the completion at D_s - 1 ms: admitted, the downstream's
// code as the response code, a complete occupancy, and one fault roll there.
//
// Reading a fresh ctx.Err() at the claim instead booked a cut at D_s --
// deadline_at_finish, censored, rolling nothing -- because by then the parent had
// ended and the downstream's status is a context code too. The state the work
// captured with its stamp is about the instant that matters; the later reading is
// about a different one.
func TestServeAStampedCompletionIsNotTurnedIntoACutByALaterExpiry(t *testing.T) {
	const doc = "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n"

	// One attempt, parked between its stamp and its claim: the downstream answers
	// at E = D_s - 1 ms while the parent is live, the worker is held there, the
	// parent then ends, and only then does the claim proceed.
	run := func(t *testing.T, schedule *FaultSchedule, roll func() float64) (*testState, time.Time, error) {
		t.Helper()
		clock := newFakeClock()
		s := newTestStateOn(t, clock, t.TempDir(), "svc-test", doc, schedule, clock.Now().UnixMilli(), roll)

		deadline := clock.Now().Add(time.Hour)
		completedAt := deadline.Add(-time.Millisecond)
		ctx := newDescheduledCtx(deadline)

		stamped, proceed := make(chan struct{}), make(chan struct{})
		s.beforeClaim = func() {
			close(stamped)
			<-proceed
		}

		served := make(chan error, 1)
		go func() {
			_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
				clock.Advance(time.Hour - time.Millisecond)
				// A DOWNSTREAM deadline: this handler's own context is live here,
				// and its callee's deadline was the nearer one.
				return nil, status.Error(codes.DeadlineExceeded, "the downstream's own deadline, which is nearer")
			})
			served <- err
		}()
		<-stamped

		clock.Advance(time.Millisecond) // D_s, and the parent context ends there
		ctx.expire(context.DeadlineExceeded)
		close(proceed) // the parked worker claims what it stamped at E
		return s, completedAt, <-served
	}

	t.Run("the completion stands, at the instant the work stamped", func(t *testing.T) {
		s, completedAt, err := run(t, nil, nil)
		require.Error(t, err)
		assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "the downstream's status, passed through")
		assert.Equal(t, 0, s.registry.Load().Station().Busy())

		recs := s.tlog.ofKind("server")
		require.Len(t, recs, 1)
		assert.Equal(t, outcomeAdmitted, recs[0]["admission_outcome"],
			"the work ran to completion inside D_s; nothing dropped it")
		assert.Equal(t, "DeadlineExceeded", recs[0]["response_code"],
			"the downstream's code, not a cut of this server's making")
		assert.Equal(t, false, recs[0]["occupancy_censored"],
			"that occupancy sample really ended where it says it did")
		assert.Equal(t, epochOf(completedAt), recs[0]["end_epoch"])
		assert.Equal(t, epochOf(completedAt), recs[0]["permit_released_at"],
			"and the worker went back there, not at D_s")
		assert.Equal(t, false, recs[0]["fault_hit"])
	})

	t.Run("and rolls the fault a cut would never have reached", func(t *testing.T) {
		s, completedAt, err := run(t,
			scheduleOf(FaultRule{Method: testMethod, StartS: 0, EndS: 36000, PFail: 1}),
			func() float64 { return 0.0 })
		require.Error(t, err)
		assert.Equal(t, codes.Unavailable, status.Code(err),
			"the roll at E overwrote the downstream's status, as it does for any completion")

		recs := s.tlog.ofKind("server")
		require.Len(t, recs, 1)
		assert.Equal(t, outcomeAdmitted, recs[0]["admission_outcome"])
		assert.Equal(t, true, recs[0]["fault_hit"],
			"a completion reaches _fails_now; a cut at D_s never does")
		assert.Equal(t, "Unavailable", recs[0]["response_code"])
		assert.Equal(t, epochOf(completedAt), recs[0]["end_epoch"])
	})

	// The mirror, and the behaviour the rule keeps: work that returned BECAUSE its
	// own context ended captured a non-nil error with its stamp, and is the
	// in-service cut it has always been -- whichever of the two cuts deadlineCause
	// says it is.
	t.Run("work that stopped because its context stopped is still the cut", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			// stopBefore is how far inside D_s the work gave up. The cancel is
			// well outside deadlineCancelSlack, or it would BE the deadline.
			stopBefore     time.Duration
			ctxErr         error
			code           codes.Code
			outcome        string
			endsAtDeadline bool
		}{
			{"an explicit cancel, far from the deadline", time.Second, context.Canceled, codes.Canceled, outcomeAdmitted, false},
			// AT the deadline, not before it: a context reports DeadlineExceeded only
			// once its deadline has passed, so a stop the deadline caused is stamped
			// at or after it (claimWork refuses the impossible pair).
			{"the caller's deadline", 0, context.DeadlineExceeded, codes.DeadlineExceeded, outcomeDeadlineAtFinish, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				clock := newFakeClock()
				s := newTestStateOn(t, clock, t.TempDir(), "svc-test", doc, nil, 0, nil)

				deadline := clock.Now().Add(time.Hour)
				stoppedAt := deadline.Add(-tc.stopBefore)
				ctx := newDescheduledCtx(deadline)

				_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
					clock.Advance(time.Hour - tc.stopBefore)
					// The handler observes its own context ending and gives up
					// there: the error it returns IS that context's.
					ctx.expire(tc.ctxErr)
					return nil, status.FromContextError(tc.ctxErr).Err()
				})
				require.Error(t, err)
				assert.Equal(t, tc.code, status.Code(err))
				assert.Equal(t, 0, s.registry.Load().Station().Busy())

				end := stoppedAt
				if tc.endsAtDeadline {
					end = deadline
				}
				recs := s.tlog.ofKind("server")
				require.Len(t, recs, 1)
				assert.Equal(t, tc.outcome, recs[0]["admission_outcome"])
				assert.Equal(t, tc.code.String(), recs[0]["response_code"])
				assert.Equal(t, true, recs[0]["occupancy_censored"],
					"the worker was still held when the context ended the attempt")
				assert.Equal(t, epochOf(end), recs[0]["end_epoch"])
			})
		}
	})
}

// F6: two events falling at the SAME instant fire in msim's heap order, which is
// (time, seq) -- and seq is where the event was SCHEDULED, not where it falls. A
// worker freed at 10 ms takes W1 into service, so W1's cut event is scheduled
// there; W2 has been queued since 2 ms with the same deadline, so its expiry was
// scheduled long before. msim fires W2's expiry first, with W1's worker still
// busy, and only then cuts W1. Reaping the permit first tied W2 at the dequeue
// instead (deadline_at_dequeue, busy 0) -- a different outcome, a different
// queue depth and a different busy count for the same physical event.
//
// The mirror case, where the release really does come first in that order, is
// TestStationTheDequeueTieTakesNoWorkerAndTheNextWaiterStartsThere: there the
// freeing permit was minted before the waiter was ever enqueued, so the tie
// stands.
func TestStationEqualTimestampsFireInScheduleOrder(t *testing.T) {
	clock := newFakeClock()
	st := newTestStationOn(clock, 1, intPtr(4), false)
	held := st.Acquire(context.Background()) // no deadline: it frees only when told
	require.Equal(t, outcomeAdmitted, held.outcome)

	// An hour out on the station's clock, so no real timer can fire during the
	// test and only the station's own order decides.
	deadline := clock.Now().Add(time.Hour)

	clock.Advance(time.Millisecond) // t = 1 ms
	ctx1, cancel1 := context.WithDeadline(context.Background(), deadline)
	defer cancel1()
	got1 := make(chan admission, 1)
	go func() { got1 <- st.Acquire(ctx1) }()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	clock.Advance(time.Millisecond) // t = 2 ms
	ctx2, cancel2 := context.WithDeadline(context.Background(), deadline)
	defer cancel2()
	got2 := make(chan admission, 1)
	go func() { got2 <- st.Acquire(ctx2) }()
	waitFor(t, func() bool { return st.QueueDepth() == 2 })

	// t = 10 ms: the held worker frees and W1 enters service. Its cut event is
	// scheduled HERE, behind W2's expiry, which was scheduled at 2 ms.
	clock.Advance(8 * time.Millisecond)
	held.permit.release()
	adm1 := <-got1
	require.Equal(t, outcomeAdmitted, adm1.outcome)
	require.NotNil(t, adm1.permit)

	// Both events are now in the past, and an arrival settles them: it carries a
	// fresh seq, so everything already scheduled fires before it.
	clock.Advance(time.Hour - 10*time.Millisecond + time.Millisecond)
	require.True(t, clock.Now().After(deadline))
	arrival := st.Acquire(context.Background())

	adm2 := <-got2
	assert.Equal(t, outcomeDeadlineInQueue, adm2.outcome,
		"W2's expiry was scheduled first, so it fires first: expire_in_queue, not the dequeue tie")
	assert.Nil(t, adm2.permit)
	assert.True(t, adm2.end.Equal(deadline), "it is booked at its own deadline")
	assert.Equal(t, 1, adm2.workersBusy,
		"W1 was still in service when W2 expired; the cut at the same instant comes second")
	assert.Equal(t, 0, adm2.queueDepth, "the depth excludes the reporting attempt")

	// W1's own cut followed, freeing its worker at the deadline; the zombie the
	// dispatcher then met was discarded, and the arrival took the worker.
	assert.Equal(t, epochOf(deadline), adm1.permit.releasedEpoch())
	require.Equal(t, outcomeAdmitted, arrival.outcome)
	assert.Equal(t, 0, st.QueueDepth(), "both waiters left the queue")
	assert.Equal(t, 1, st.Busy())
	arrival.permit.release()
	waitFor(t, func() bool { return st.Busy() == 0 })
}

// --- queue_depth_at_end (F1) -----------------------------------------------

// parkWaiters queues n waiters that carry no deadline, one at a time so the FIFO
// order is the test's rather than the scheduler's. They can only ever leave the
// queue by being dispatched.
func parkWaiters(t *testing.T, st *station, n int) chan admission {
	t.Helper()
	got := make(chan admission, n)
	base := st.QueueDepth()
	for i := 0; i < n; i++ {
		go func() { got <- st.Acquire(context.Background()) }()
		want := base + i + 1
		waitFor(t, func() bool { return st.QueueDepth() == want })
	}
	return got
}

// queue_depth_at_end is msim's `queue_size` argument to on_done, path by path.
// `queue_avg_at_attempt_end` averages exactly that on the simulator's side, so a
// record reporting the ADMISSION depth instead compared two different
// quantities -- and diverged most where the queue moves fastest.
func TestQueueDepthAtEndMirrorsMsim(t *testing.T) {
	for _, tc := range []struct {
		name string
		msim string
		want int
		run  func(t *testing.T) int
	}{
		{
			name: outcomeDeadlineAtSubmit,
			msim: "submit_attempt: settled_on_done(..., self.queue_len(), ...) before anything joins the queue",
			want: 2,
			run: func(t *testing.T) int {
				st := newTestStation(1, intPtr(4), false)
				held := st.Acquire(context.Background())
				require.Equal(t, outcomeAdmitted, held.outcome)
				defer held.permit.release()
				parkWaiters(t, st, 2)
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
				defer cancel()
				adm := st.Acquire(ctx)
				require.Equal(t, outcomeDeadlineAtSubmit, adm.outcome)
				return adm.queueDepthAtEnd
			},
		},
		{
			name: outcomeQueueFull,
			msim: "submit_attempt: settled_on_done(..., DropReason.QUEUE_FULL, self.queue_len(), ...)",
			want: 2,
			run: func(t *testing.T) int {
				st := newTestStation(1, intPtr(2), false)
				held := st.Acquire(context.Background())
				require.Equal(t, outcomeAdmitted, held.outcome)
				defer held.permit.release()
				parkWaiters(t, st, 2)
				adm := st.Acquire(context.Background())
				require.Equal(t, outcomeQueueFull, adm.outcome)
				return adm.queueDepthAtEnd
			},
		},
		{
			name: outcomeDeadlineInQueue,
			msim: "expire_in_queue: settled_on_done(..., self.queue_len() - 1, ...)",
			want: 1,
			run: func(t *testing.T) int {
				st := newTestStation(1, intPtr(4), false)
				held := st.Acquire(context.Background())
				require.Equal(t, outcomeAdmitted, held.outcome)
				defer held.permit.release()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				got := make(chan admission, 1)
				go func() { got <- st.Acquire(ctx) }()
				waitFor(t, func() bool { return st.QueueDepth() == 1 })
				parkWaiters(t, st, 1)
				adm := <-got
				require.Equal(t, outcomeDeadlineInQueue, adm.outcome)
				return adm.queueDepthAtEnd
			},
		},
		{
			name: outcomeCancelledInQueue,
			msim: "cancel_queued: self._queued -= 1, then on_done(..., self.queue_len(), ...)",
			want: 1,
			run: func(t *testing.T) int {
				st := newTestStation(1, intPtr(4), false)
				held := st.Acquire(context.Background())
				require.Equal(t, outcomeAdmitted, held.outcome)
				defer held.permit.release()
				ctx, cancel := context.WithCancel(context.Background())
				got := make(chan admission, 1)
				go func() { got <- st.Acquire(ctx) }()
				waitFor(t, func() bool { return st.QueueDepth() == 1 })
				parkWaiters(t, st, 1)
				cancel()
				adm := <-got
				require.Equal(t, outcomeCancelledInQueue, adm.outcome)
				return adm.queueDepthAtEnd
			},
		},
		{
			name: outcomeDeadlineAtDequeue,
			msim: "_start_next pops (self._queued -= 1), then _begin_service reports self.queue_len()",
			want: 1,
			run: func(t *testing.T) int {
				clock := newFakeClock()
				st := newTestStationOn(clock, 1, intPtr(4), false)
				held := st.Acquire(context.Background())
				require.Equal(t, outcomeAdmitted, held.outcome)
				deadline := clock.Now().Add(time.Hour)
				ctx, cancel := context.WithDeadline(context.Background(), deadline)
				defer cancel()
				got := make(chan admission, 1)
				go func() { got <- st.Acquire(ctx) }()
				waitFor(t, func() bool { return st.QueueDepth() == 1 })
				live := parkWaiters(t, st, 1)
				clock.Advance(time.Hour) // the worker frees at EXACTLY the deadline
				held.permit.release()
				adm := <-got
				require.Equal(t, outcomeDeadlineAtDequeue, adm.outcome)
				(<-live).permit.release()
				return adm.queueDepthAtEnd
			},
		},
		{
			name: "admitted, completed in service",
			msim: "_complete_attempt: on_done(..., self.queue_len(), ...) BEFORE _start_next",
			want: 2,
			run: func(t *testing.T) int {
				st := newTestStation(1, intPtr(4), false)
				held := st.Acquire(context.Background())
				require.Equal(t, outcomeAdmitted, held.outcome)
				parkWaiters(t, st, 2)
				held.permit.release()
				depth, _, _ := held.permit.endSnapshot()
				return depth
			},
		},
		{
			name: outcomeDeadlineAtFinish + ", reaped by the station",
			msim: "_finish_service at the deadline -> _complete_attempt: self.queue_len() before _start_next",
			want: 2,
			run: func(t *testing.T) int {
				clock := newFakeClock()
				st := newTestStationOn(clock, 1, intPtr(4), false)
				deadline := clock.Now().Add(time.Hour)
				ctx, cancel := context.WithDeadline(context.Background(), deadline)
				defer cancel()
				held := st.Acquire(ctx)
				require.Equal(t, outcomeAdmitted, held.outcome)
				parkWaiters(t, st, 2)
				clock.Advance(time.Hour + time.Millisecond)
				// An already-expired arrival settles the station without mutating
				// it: the reaper alone frees this worker.
				expiredCtx, cancelExpired := context.WithDeadline(context.Background(), clock.Now().Add(-time.Millisecond))
				defer cancelExpired()
				require.Equal(t, outcomeDeadlineAtSubmit, st.Acquire(expiredCtx).outcome)
				depth, _, released := held.permit.endSnapshot()
				assert.Equal(t, epochOf(deadline), released, "the worker went back AT the deadline")
				return depth
			},
		},
		{
			name: "admitted, cancelled in service",
			msim: "cancel_in_service -> _complete_attempt: self.queue_len() before _start_next",
			want: 2,
			run: func(t *testing.T) int {
				st := newTestStation(1, intPtr(4), false)
				held := st.Acquire(context.Background())
				require.Equal(t, outcomeAdmitted, held.outcome)
				parkWaiters(t, st, 2)
				_, won := held.permit.claim(claimCancelled, st.clock.Now())
				require.True(t, won)
				depth, _, _ := held.permit.endSnapshot()
				return depth
			},
		},
		{
			name: "the worker had already been handed back",
			msim: "finalize with released=True: on_done(..., self.queue_len(), ...) at the FINALIZE instant",
			want: 1,
			run: func(t *testing.T) int {
				st := newTestStation(1, intPtr(4), false)
				held := st.Acquire(context.Background())
				require.Equal(t, outcomeAdmitted, held.outcome)
				parkWaiters(t, st, 2)
				// release_worker runs _start_next, so one waiter has left the
				// queue by the time the attempt finalizes.
				held.permit.handBack(st.clock.Now())
				waitFor(t, func() bool { return st.QueueDepth() == 1 })
				held.permit.release()
				depth, handedBack, _ := held.permit.endSnapshot()
				assert.True(t, handedBack)
				return depth
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.run(t), tc.msim)
		})
	}
}

// Every server record carries it, on the admission paths as well as the
// in-service ones.
func TestServeWritesQueueDepthAtEnd(t *testing.T) {
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

	// One queued attempt, then one shed: the shed request's end IS its
	// admission, so it reports the depth it arrived into.
	queued := make(chan error, 1)
	go func() {
		_, err := s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			return "ok", nil
		})
		queued <- err
	}()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })
	_, err := s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		t.Error("a shed request never reaches the handler")
		return nil, nil
	})
	require.Error(t, err)

	close(release)
	require.NoError(t, <-queued)
	waitFor(t, func() bool { return len(s.tlog.ofKind("server")) == 3 })

	byOutcome := map[string]map[string]interface{}{}
	for _, r := range s.tlog.ofKind("server") {
		if r["admission_outcome"] == outcomeAdmitted && r["admission_wait_ms"].(float64) > 0 {
			byOutcome["dequeued"] = r
			continue
		}
		byOutcome[r["admission_outcome"].(string)] = r
	}
	require.Contains(t, byOutcome, outcomeQueueFull)
	assert.Equal(t, 1.0, byOutcome[outcomeQueueFull]["queue_depth_at_end"],
		"one entry was queued when this one was shed")
	assert.Equal(t, 1.0, byOutcome[outcomeQueueFull]["admission_queue_depth"])
	require.Contains(t, byOutcome, outcomeAdmitted)
	assert.Equal(t, 1.0, byOutcome[outcomeAdmitted]["queue_depth_at_end"],
		"the first request finished with one attempt still queued behind it")
	assert.Equal(t, 0.0, byOutcome[outcomeAdmitted]["admission_queue_depth"],
		"...which is NOT the depth it was admitted into")
	require.Contains(t, byOutcome, "dequeued")
	assert.Equal(t, 0.0, byOutcome["dequeued"]["queue_depth_at_end"])
}

// --- discard events --------------------------------------------------------

// An expired queue entry keeps its slot until the dispatcher pops it, and its
// own record was written at its deadline -- so nothing in the log says when the
// slot really came back, and the pipeline cannot reconstruct queued(t) exactly.
// The station says so itself, with one discard event per dropped entry.
func TestServeWritesADiscardEventWhenTheDispatcherDropsAnExpiredEntry(t *testing.T) {
	clock := newFakeClock()
	s := newTestStateOn(t, clock, t.TempDir(), "svc-test",
		"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n",
		nil, 0, nil)
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

	// A queued attempt whose caller-clock deadline is an hour out on the
	// STATION's clock: no real timer fires, so only the station expires it.
	deadline := clock.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			t.Error("an expired waiter must never reach the handler")
			return nil, nil
		})
		done <- err
	}()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })

	// Ten milliseconds past that deadline the held worker frees: settle expires
	// the entry AT its deadline, and the dispatcher then meets the zombie and
	// drops it HERE.
	clock.Advance(time.Hour + 10*time.Millisecond)
	discardedAt := clock.Now()
	close(release)
	require.Error(t, <-done)
	waitFor(t, func() bool { return st.QueueDepth() == 0 && st.Busy() == 0 })
	waitFor(t, func() bool { return len(s.tlog.ofKind(kindDiscard)) == 1 })

	discards := s.tlog.ofKind(kindDiscard)
	require.Len(t, discards, 1)
	assertKeySet(t, discardKeys, discards[0])

	var expired map[string]interface{}
	for _, r := range s.tlog.ofKind("server") {
		if r["admission_outcome"] == outcomeDeadlineInQueue {
			expired = r
		}
	}
	require.NotNil(t, expired, "the entry reported its own drop at its deadline")
	assert.Equal(t, expired["trace_id"], discards[0]["trace_id"])
	assert.Equal(t, expired["span_id"], discards[0]["span_id"], "the discard joins its record by span")
	assert.Equal(t, expired["parent_span_id"], discards[0]["parent_span_id"])
	assert.Equal(t, expired["service"], discards[0]["service"])
	assert.Equal(t, expired["operation"], discards[0]["operation"])
	assert.Equal(t, epochOf(deadline), expired["end_epoch"],
		"the attempt's own record still ends at the deadline it was told at")
	assert.Equal(t, epochOf(discardedAt), discards[0]["at_epoch"],
		"and the discard is the instant the dispatcher popped it, which nothing else in the log marks")
	assert.Len(t, s.tlog.ofKind("server"), 2, "a discarded entry is not reported twice")
}

// A queue that drains normally writes no discard events at all: only an entry
// the dispatcher DROPS produces one.
func TestServeWritesNoDiscardEventForAWaiterThatIsServed(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: 4\n")
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
	queued := make(chan error, 1)
	go func() {
		_, err := s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			return "ok", nil
		})
		queued <- err
	}()
	waitFor(t, func() bool { return st.QueueDepth() == 1 })
	close(release)
	require.NoError(t, <-queued)
	waitFor(t, func() bool { return len(s.tlog.ofKind("server")) == 2 })
	assert.Empty(t, s.tlog.ofKind(kindDiscard))
}
