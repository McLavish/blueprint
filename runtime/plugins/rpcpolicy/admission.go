package rpcpolicy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// The admission station: docs/PLAN.md D4's capacity mechanism, a transcription
// of msim's ServiceRuntime.submit_attempt / _begin_service. `c` permits over a
// FIFO queue of `K`, with msim's drop points.
//
// The waiter list is explicit rather than a buffered channel because FCFS is
// load-bearing: a channel hands the permit to an arbitrary blocked receiver,
// and the queueing model the campaign fits (M/G/c/K) assumes first-come
// first-served. It also makes queue depth observable, which every server
// record carries.
//
// msim's station is a single-threaded event loop over a heap ordered by
// (time, seq). This one has no loop, so the heap lives in the station itself:
// every pending deadline -- a queued waiter's expiry, and the cut of an
// in-service attempt that still holds a worker -- is a station EVENT, and settle
// replays the due ones in (time, seq) order, under the station mutex, before the
// mutation that triggered it runs. Nothing is ever acted on at the instant a
// goroutine happened to be scheduled.
//
// Everything else an attempt can do to the station -- ending it, handing the
// worker back early -- is done by a goroutine that TAKES the mutex and is
// applied inside that one critical section, at the instant it names. There is no
// second, lock-free channel by which the station can learn of an outcome, and so
// no window in which two goroutines can hold two different truths about one
// attempt.

// Admission outcomes (CONTRACTS.md §5).
const (
	outcomeAdmitted         = "admitted"
	outcomeQueueFull        = "queue_full"
	outcomeDeadlineAtSubmit = "deadline_at_submit"
	// outcomeDeadlineInQueue is msim's expire_in_queue (service.py): the
	// attempt was still waiting for a worker when the CALLER's deadline passed,
	// and is reported at exactly that instant.
	outcomeDeadlineInQueue = "deadline_in_queue"
	// outcomeDeadlineAtDequeue is the exact tie, and nothing else: the
	// dispatcher reaches a waiter at the very instant its deadline passes
	// (msim's _begin_service check). A hand-off strictly BEFORE the deadline is
	// admitted -- msim starts service there and cuts it in service -- and a
	// hand-off strictly after cannot happen, because settle expires such a
	// waiter before any dispatcher can reach it.
	//
	// The tie itself is broken by seq, exactly as msim's heap does it: the
	// waiter's expiry carries the seq of its ENQUEUE and the freeing worker's
	// event carries the seq of the permit's MINT, so a worker freeing at a
	// head's deadline is the tie only when the release comes first in that
	// order. Otherwise the expiry event fired first and the entry is simply a
	// zombie the dispatcher discards.
	//
	// The tie consumes NO worker: msim drops it before `in_flight += 1` and
	// _start_next walks on to the next live entry.
	outcomeDeadlineAtDequeue = "deadline_at_dequeue"
	outcomeDeadlineAtFinish  = "deadline_at_finish"
	outcomeCancelledInQueue  = "cancelled_in_queue"
)

// unboundedQueue is the sentinel for `queue_capacity: null`.
const unboundedQueue = -1

// waiterState is the single-winner handshake between the dispatcher handing a
// permit to the head of the queue and a caller leaving it (cancelled, or its
// deadline passed). Every transition happens under station.mu, so exactly one
// of them takes effect.
type waiterState int

const (
	waiterQueued waiterState = iota
	waiterHandedOff
	waiterCancelled
	// waiterExpired: the caller's deadline passed while this entry waited, so it
	// reports deadline_in_queue AT that deadline. Unlike a cancel, the entry
	// KEEPS its queue slot until the dispatcher reaches it, which is what makes
	// the occupancy the model fits c + K rather than "whatever survived the
	// deadline"; releaseWorkerLocked then discards it silently -- no permit, no
	// second report (msim's _start_next `if item.expired: continue`) -- and
	// writes a discard event so the log says WHEN the slot came back.
	//
	// EITHER side may set it. Usually the station does, in settle, at the
	// instant the deadline passed. But the waiter's own goroutine sets it too,
	// when its context fires with no intervening mutation to settle against.
	// Whoever sets it, exactly one report follows -- the station closes `ready`
	// so the waiter can never block behind a permit that is not coming, and the
	// waiter reports whichever way it wakes.
	waiterExpired
	// waiterTied is the exact dequeue tie: a worker freed at the very instant
	// this waiter's deadline passed, and got there first in (time, seq) order.
	// msim's _begin_service drops it there, BEFORE `in_flight += 1`, and
	// _start_next carries the same freed worker on to the next live entry -- so
	// the tie takes no permit and delays nobody. Deciding it at the tied
	// goroutine's wake-up instead let it hold the freed worker until it was next
	// scheduled, and a live waiter behind it expired waiting for a worker that
	// was already free.
	waiterTied
)

type waiter struct {
	ready chan struct{}
	// enqueuedAt, deadline, hasDeadline, seq and id are fixed before the waiter
	// joins the FIFO and never change, so any goroutine may read them.
	enqueuedAt  time.Time
	deadline    time.Time
	hasDeadline bool
	// seq is this entry's place in the station's event order. msim schedules
	// expire_in_queue at ENQUEUE, so the expiry event carries the seq the
	// arrival took -- which is what decides the exact tie against a worker
	// freeing at the same instant.
	seq int64
	// id names this attempt on a station event (the discard the dispatcher
	// writes). Read off the context at admission, so the event carries exactly
	// the identity fields of the attempt's own server record.
	id attemptID
	// state, handoffAt, permit, queueDepth, workersBusy and queueDepthAtEnd are
	// all guarded by station.mu, and all written by the station at the instant
	// it decides this waiter's fate.
	state waiterState
	// handoffAt is the instant the permit was handed over -- the instant the
	// worker really freed, which is what every deadline comparison downstream is
	// made against. A fresh clock reading taken after the waiter's goroutine
	// woke would book a scheduling delay as queueing time.
	handoffAt time.Time
	// permit is minted BY the dispatcher, at the hand-off instant, exactly as
	// msim's _begin_service takes the worker inside _start_next. Built on the
	// waking goroutine instead, an attempt handed a worker just under its
	// deadline held it -- unreapable, because the station did not know it
	// existed -- until that goroutine was next scheduled.
	permit *permit
	// Sampled at the instant the permit is handed over (or the entry expired),
	// so the record reports the state this request actually observed.
	queueDepth  int
	workersBusy int
	// queueDepthAtEnd is msim's `queue_size` argument to on_done: the queue
	// length at the instant THIS attempt ended. For every drop decided at
	// admission it is the same sample as queueDepth -- admission is the end --
	// but the two are different measurements and the pipeline reads them as
	// such.
	queueDepthAtEnd int
}

// expiredInQueue is the deadline_in_queue report. Every instant in it is the
// DEADLINE, never the instant this goroutine (or the station) observed it:
// msim fires the caller's own clock AT the deadline, so the wait is
// deadline - enqueue and the record ends there. Only ever called for a waiter
// that has one.
func (w *waiter) expiredInQueue(depth, busy int) admission {
	return admission{
		outcome:     outcomeDeadlineInQueue,
		wait:        w.deadline.Sub(w.enqueuedAt),
		queueDepth:  depth,
		workersBusy: busy,
		// msim's expire_in_queue reports `self.queue_len() - 1`, which is this
		// same depth: the attempt's admission and its end are one instant.
		queueDepthAtEnd: depth,
		end:             w.deadline,
	}
}

type station struct {
	clock    Clock
	workers  int
	capacity int // unboundedQueue for no bound
	hold     bool
	// log carries the station's OWN events (the discard below). nil in tests
	// that drive the station directly; attemptLog.Write is nil-safe.
	log *attemptLog

	mu sync.Mutex
	// seq is the station-wide event counter, msim's Simulator._seq. It breaks
	// ties between events that fall at the same instant, in the order they were
	// scheduled.
	seq     int64
	busy    int
	waiters []*waiter
	// permits is every permit whose attempt has not ended yet -- its copy of the
	// _finish_service events msim keeps on its heap. A permit joins when it is
	// minted and leaves when its terminal claim is applied, which is one event
	// later than the instant its worker went back whenever the worker went back
	// early. `c` is a few tens at most, so a slice scan beats a heap.
	permits []*permit
	// lastSettled is the station's committed clock: the latest instant it has
	// already made decisions for. An event that reaches the station late is
	// applied HERE rather than at its own instant, because the interval before it
	// is spent (see applyEventLocked).
	lastSettled time.Time
	// nextDeadline is the earliest DEADLINE still pending anywhere in the
	// station -- a queued waiter's expiry or a held permit's reap, which are the
	// only events there are. It is a LOWER BOUND, kept exact by settle's own
	// recompute and only ever made stale-early by a removal, so `now <
	// nextDeadline` is a sound O(1) proof that there is nothing to replay.
	// Without it every mutation would walk a queue that an unbounded
	// `queue_capacity` lets grow into the thousands.
	nextDeadline time.Time
	hasNext      bool
	// discards are station events produced under mu and written after it is
	// dropped: no I/O ever happens inside the critical section.
	discards []*DiscardRecord
}

func newStation(cfg *ServerConfig, clock Clock, log *attemptLog) *station {
	capacity := unboundedQueue
	if cfg.QueueCapacity != nil {
		capacity = *cfg.QueueCapacity
	}
	return &station{
		clock:    clock,
		workers:  cfg.Workers,
		capacity: capacity,
		hold:     cfg.HoldPermitThroughFanout,
		log:      log,
	}
}

// claimKind is how an attempt that held a permit ENDED. Exactly one is ever
// claimed per permit.
//
// The early hand-back (ReleasePermit, msim's tandem release_worker) is NOT one
// of them. It is a different fact about a different thing -- the WORKER went
// back, while the attempt runs on -- and it is kept as separate, immutable
// history on the permit. No terminal claim supersedes it: the occupancy ended
// at the hand-back, and a cut that lands afterwards censors nothing.
type claimKind int

const (
	// claimCompleted: the handler (or the injected sleep in front of it)
	// returned. `at` is the instant it returned, stamped by the worker goroutine
	// itself -- not the instant the interceptor was next scheduled to hear
	// about it.
	claimCompleted claimKind = iota
	// claimCut: the attempt was cut at the CALLER's deadline. `at` is the
	// deadline, whether the cut was booked by the serving goroutine's own
	// ctx.Done() or by the station's reaper (settle) at the deadline itself.
	claimCut
	// claimCancelled: the caller explicitly abandoned the attempt in service
	// (msim's cancel_in_service). `at` is the instant the cancel was observed.
	claimCancelled
)

// claim is the once-only outcome of one permit: which way the attempt ended and
// at which instant. Three goroutines race for it -- the worker goroutine at
// completion, the interceptor at ctx.Done(), and the station's reaper at the
// deadline -- and exactly one takes it, so exactly one record is written, at the
// instant the winner names.
//
// Taking it and APPLYING it are ONE critical section under the station mutex
// (permit.claim), never two steps. Taken lock-free -- by an atomic CAS -- and
// applied later, the two could interleave: a handler stamping a completion at
// D - 1 ms could take the
// claim just after the station's reap had already advanced committed time to D
// and just before that reap took the claim itself, and the attempt then had two
// truths -- a record saying "success at D - 1 ms" over a station that had held
// the worker to D, or a station that had cut the attempt at D under an
// interceptor still classifying its handler's result as an ordinary success.
// Under the mutex there is one winner, and the loser's goroutine reports the
// winner's claim (see runtimeState.serve).
type claim struct {
	kind claimKind
	at   time.Time
}

// permit is one held worker.
type permit struct {
	// st is nil for a detached permit: a server flow running with no `server:`
	// block holds no worker, but still needs the claim so that its record is
	// classified exactly once.
	st          *station
	deadline    time.Time
	hasDeadline bool
	// seq is this permit's place in the station's event order. msim schedules
	// the finish/cut event inside _begin_service, so everything this permit does
	// to the station -- its hand-back, its terminal claim, the reap at its
	// deadline -- carries the seq of the MINT.
	seq int64

	// detachedMu guards `outcome` for a permit with no station, and nothing
	// else: the same once-only rule, with no event order to fit into and no
	// worker to move.
	detachedMu sync.Mutex

	// releasedAt is the wall-clock UnixNano at which the WORKER went back, 0
	// while it is still held. Atomic because the record's permit_released_at is
	// read off it outside the station mutex; it is only ever written under it.
	releasedAt atomic.Int64

	// Everything below is guarded by st.mu -- by detachedMu for `outcome` when
	// there is no station. The station applies this permit's events under that
	// mutex, so a hand-back racing a cut is serialised by the very mutex that
	// moves the worker.
	//
	// outcome is the terminal claim: taken and applied in one critical section,
	// so a permit still registered with the station has none, and one that has
	// one has already left s.permits.
	outcome *claim
	// handedBackAt is the instant ReleasePermit gave the WORKER back. It is
	// separate, immutable history that no terminal claim supersedes -- the
	// occupancy ended there, and a cut landing afterwards censors nothing. (The
	// worker goes back at that instant or, if the station has already committed
	// decisions past it, at the committed one; permit_released_at reports which,
	// and this field stays the true observation.)
	handedBackAt *time.Time
	released     bool
	handedBack   bool
	// queueDepthAtEnd is msim's `queue_size` at on_done, sampled at the instant
	// the terminal outcome was APPLIED: _complete_attempt reports queue_len()
	// before _start_next dispatches, and the early-release branch of finalize
	// reports it at the finalize instant.
	queueDepthAtEnd int
}

// claimLocked takes the terminal claim, once. Callers hold the mutex that guards
// it. It reports the claim in force and whether THIS call is the one that took
// it.
func (p *permit) claimLocked(kind claimKind, at time.Time) (*claim, bool) {
	if p.outcome != nil {
		return p.outcome, false
	}
	p.outcome = &claim{kind: kind, at: at}
	return p.outcome, true
}

// claim ends the attempt: it takes the terminal claim AND applies it, in one
// critical section, at the instant it names.
//
// The station is settled to that instant first, through this permit's own event
// key, so everything msim would have run before it runs before it -- including
// the reap of this very permit at its own deadline, which is what turns a
// completion stamped at or past the deadline into the cut msim books there
// (_finish_service is scheduled at min(begin + service, deadline) and frees the
// worker THERE). The claim is then applied where msim applies it: at the instant
// it names, and never earlier than the station's committed clock
// (applyEventLocked).
//
// It reports the claim in force and whether this call is the one that took it. A
// loser changes nothing at all; its caller reports the winner's claim.
func (p *permit) claim(kind claimKind, at time.Time) (*claim, bool) {
	if p == nil {
		return &claim{kind: kind, at: at}, true
	}
	if p.st == nil {
		p.detachedMu.Lock()
		defer p.detachedMu.Unlock()
		return p.claimLocked(kind, at)
	}
	s := p.st
	s.mu.Lock()
	defer s.unlock()
	s.settle(at, p.seq)
	c, won := p.claimLocked(kind, at)
	if won {
		s.applyEventLocked(stationEvent{at: at, seq: p.seq, kind: eventTerminal, p: p})
	}
	return c, won
}

// claimed is the terminal claim in force, or nil while the attempt is still
// running. The claim is immutable once taken, so a reader needs the mutex only
// to see it.
func (p *permit) claimed() *claim {
	if p == nil {
		return nil
	}
	if p.st == nil {
		p.detachedMu.Lock()
		defer p.detachedMu.Unlock()
		return p.outcome
	}
	p.st.mu.Lock()
	defer p.st.unlock()
	return p.outcome
}

// release hands the worker back now, claiming the attempt as completed. It is
// the plain "this attempt is over" path used outside the server flow and by the
// station's own tests.
func (p *permit) release() {
	if p == nil || p.st == nil {
		return
	}
	p.claim(claimCompleted, p.st.clock.Now())
}

// handBack is msim's early release_worker (the tandem branch of
// _begin_service): the WORKER goes back now while the attempt runs on. The
// occupancy sample ends here and is COMPLETE, so whatever cuts the attempt
// later is not censoring an occupancy this station ever held.
//
// Recorded and applied in one critical section, like the terminal claim, and
// behind the same settle: if the station had already cut this attempt at its
// deadline, the worker went back THERE and this hand-back frees nothing.
func (p *permit) handBack(at time.Time) {
	if p == nil || p.st == nil {
		return
	}
	s := p.st
	s.mu.Lock()
	defer s.unlock()
	s.settle(at, p.seq)
	if p.handedBackAt != nil {
		return
	}
	p.handedBackAt = &at
	s.applyEventLocked(stationEvent{at: at, seq: p.seq, kind: eventHandBack, p: p})
}

// endSnapshot is the state the station recorded when this permit's terminal
// outcome was applied: msim's queue_size at on_done, whether the worker had
// already gone back (its occupancy is then a COMPLETE observation, msim's
// `released` flag in _begin_service), and permit_released_at.
func (p *permit) endSnapshot() (queueDepth int, handedBack bool, releasedAt float64) {
	if p == nil {
		return 0, false, 0
	}
	if p.st == nil {
		return 0, false, p.releasedEpoch()
	}
	p.st.mu.Lock()
	defer p.st.unlock()
	return p.queueDepthAtEnd, p.handedBack, p.releasedEpoch()
}

// releasedEpoch is the record's permit_released_at: 0 if never held.
func (p *permit) releasedEpoch() float64 {
	if p == nil {
		return 0
	}
	ns := p.releasedAt.Load()
	if ns == 0 {
		return 0
	}
	return float64(ns) / 1e9
}

// admission is what Acquire reports back to the server interceptor.
type admission struct {
	permit      *permit
	outcome     string
	wait        time.Duration
	queueDepth  int
	workersBusy int
	// queueDepthAtEnd is msim's `queue_size` at on_done. Set on every outcome
	// decided at admission -- for those the end IS the admission; an admitted
	// attempt carries its own on the permit instead.
	queueDepthAtEnd int
	// end overrides the instant the server record closes at. Zero on every path
	// that is decided now; set by the two outcomes msim books at the DEADLINE
	// rather than at the instant a goroutine observed it -- deadline_in_queue
	// and the deadline_at_dequeue tie.
	end time.Time
}

// Acquire runs msim's submit_attempt: the deadline check at submit, the
// free-worker fast path, the queue-full shed, then the FIFO wait, the
// caller-clock deadline while queued, and the exact-tie check at dequeue.
//
// Note the order: a request whose deadline has already passed is dropped BEFORE
// the queue-full test, and a free worker is taken BEFORE it too -- so
// queue_full can only be reported when every worker is busy, exactly as msim.
//
// The attempt deadline is the CALLER's clock (msim's expire_in_queue,
// service.py). A request still waiting for a worker when its deadline passes is
// reported deadline_in_queue at exactly that instant -- not when the server next
// dequeues, which used to make the client learn of its own timeout whenever a
// worker happened to free. The entry is NOT removed: it keeps its queue slot,
// still counts towards K and still sheds queue_full on later arrivals, until the
// dispatcher reaches it and discards it silently -- no permit, no second record,
// one discard event. That is what keeps the occupancy the model fits at c + K.
//
// A queued request whose caller CANCELS is a different drop point (msim's
// cancel_queued): it is tombstoned immediately, gives its slot back at once,
// takes no worker, and is reported cancelled_in_queue. Which of the two a
// finished context is, is deadlineCause's call, not ctx.Err()'s -- see the
// deadlineCancelSlack comment.
//
// Nothing after the wait consults the clock. Every permit-versus-deadline race
// is settled by the station at the instant it really happened (settle and
// releaseWorkerLocked) and read back off the waiter here, so a goroutine that is
// scheduled late reports the station's history rather than its own.
func (s *station) Acquire(ctx context.Context) admission {
	start := s.clock.Now()
	deadline, hasDeadline := ctx.Deadline()

	s.mu.Lock()
	// The arrival is msim's submit_attempt, and it takes a FRESH seq: every
	// event already scheduled for this instant is ahead of it in the heap and
	// fires before the request is looked at.
	seq := s.nextSeqLocked()
	s.settle(start, seq)
	if expired(ctx, start) {
		depth, busy := len(s.waiters), s.busy
		s.unlock()
		return admission{
			outcome: outcomeDeadlineAtSubmit, queueDepth: depth, workersBusy: busy,
			// msim reports self.queue_len() here: the request never joined the
			// queue, so its end is its admission.
			queueDepthAtEnd: depth,
		}
	}
	if s.busy < s.workers {
		s.busy++
		depth, busy := len(s.waiters), s.busy-1
		p := s.newPermitLocked(deadline, hasDeadline)
		s.unlock()
		return admission{permit: p, outcome: outcomeAdmitted, queueDepth: depth, workersBusy: busy}
	}
	if s.capacity != unboundedQueue && len(s.waiters) >= s.capacity {
		depth, busy := len(s.waiters), s.busy
		s.unlock()
		return admission{
			outcome: outcomeQueueFull, queueDepth: depth, workersBusy: busy,
			queueDepthAtEnd: depth,
		}
	}
	w := &waiter{
		ready:      make(chan struct{}),
		enqueuedAt: start,
		deadline:   deadline, hasDeadline: hasDeadline,
		seq: seq, id: attemptIDFrom(ctx),
	}
	s.waiters = append(s.waiters, w)
	if hasDeadline {
		s.noteDeadlineLocked(deadline)
	}
	s.unlock()

	select {
	case <-w.ready:
	case <-ctx.Done():
		now := s.clock.Now()
		byDeadline := deadlineCause(ctx, now) && w.hasDeadline
		s.mu.Lock()
		if byDeadline {
			// This goroutine is hearing its OWN expiry event, which msim fired at
			// the deadline: settle through that event's place in the order, so
			// everything msim would have run first has run. Never past `now`,
			// though -- an RST_STREAM arriving a hair early IS this attempt's
			// deadline, but it is not licence to run the whole station's clock
			// forward to an instant that has not happened.
			at := w.deadline
			if now.Before(at) {
				at = now
			}
			s.settle(at, w.seq)
		} else {
			s.settle(now, s.nextSeqLocked())
		}
		if w.state == waiterQueued {
			if byDeadline {
				// settle did not reach this entry's own event (the deadline is
				// still ahead of `now`), so fire it here: the report is the
				// deadline's either way. The entry stays in s.waiters, so the
				// depth it reports EXCLUDES itself, as every other path here does
				// (msim's `self.queue_len() - 1`).
				s.expireWaiterLocked(w)
				depth, busy := w.queueDepth, w.workersBusy
				s.unlock()
				return w.expiredInQueue(depth, busy)
			}
			w.state = waiterCancelled
			s.removeWaiterLocked(w)
			// Depth AFTER the slot is returned, as in msim's cancel_queued: the
			// tombstone is discounted at cancel time, not when it reaches the
			// head of the queue. That is both what this attempt observed and
			// what it reports as its end.
			depth, busy := len(s.waiters), s.busy
			w.queueDepthAtEnd = depth
			s.unlock()
			return admission{
				outcome:         outcomeCancelledInQueue,
				wait:            now.Sub(w.enqueuedAt),
				queueDepth:      depth,
				workersBusy:     busy,
				queueDepthAtEnd: depth,
			}
		}
		// waiterHandedOff, waiterExpired or waiterTied: the station got there
		// first and closed w.ready in every one of them, so this cannot block
		// and the tail below reports whichever it decided -- once, whichever
		// select branch ran.
		s.unlock()
		<-w.ready
	}

	s.mu.Lock()
	state, handoffAt, p := w.state, w.handoffAt, w.permit
	depth, busy, endDepth := w.queueDepth, w.workersBusy, w.queueDepthAtEnd
	s.unlock()
	switch state {
	case waiterExpired:
		// The station found this entry already past its deadline, and expired it
		// THERE. msim's expire_in_queue event had already fired by then, so the
		// report is the deadline's, not the dequeue's.
		return w.expiredInQueue(depth, busy)
	case waiterTied:
		// The exact tie, and the only dequeue msim still reports as a deadline
		// (_begin_service's check). It consumed no worker: the station carried
		// the freed one straight on to the next live waiter.
		return admission{
			outcome:         outcomeDeadlineAtDequeue,
			wait:            handoffAt.Sub(w.enqueuedAt),
			queueDepth:      depth,
			workersBusy:     busy,
			queueDepthAtEnd: endDepth,
			end:             w.deadline,
		}
	}

	// Handed off strictly before the deadline: a worker that freed there started
	// serving this attempt (msim's _begin_service), however late this goroutine
	// woke to hear about it, and the in-service cut is what ends it. The permit
	// was minted at that instant, so the station could already have reaped it at
	// the deadline while this goroutine was still asleep -- serve() reads the
	// claim, not the clock.
	return admission{permit: p, outcome: outcomeAdmitted, wait: handoffAt.Sub(w.enqueuedAt), queueDepth: depth, workersBusy: busy}
}

// --- the event heap --------------------------------------------------------

// eventKind is which of msim's heap events this is. All three carry the seq of
// the thing that scheduled them, which is what orders two of them falling at the
// same instant.
//
// Only the two DEADLINES are ever found pending by settle, because only a
// deadline is a standing appointment nobody has to be running to keep. The other
// two kinds are applied by the goroutine that takes the station mutex to cause
// them, in the same critical section; they are events all the same, and go
// through applyEventLocked, so that the clamp on committed time and the order of
// the work they trigger are written down exactly once.
type eventKind int

const (
	// eventExpiry is msim's expire_in_queue, scheduled at ENQUEUE: the caller's
	// clock fired while the attempt was still waiting for a worker.
	eventExpiry eventKind = iota
	// eventHandBack is msim's early release_worker, applied by ReleasePermit:
	// the worker goes back while the attempt runs on.
	eventHandBack
	// eventTerminal is the attempt's end: msim's _finish_service (a completion,
	// or the cut at min(begin + service, deadline)) and cancel_in_service. It is
	// the terminal claim, whether a contender took it (permit.claim) or the
	// station reaped a permit whose deadline passed while it still held a worker.
	eventTerminal
)

// stationEvent is one entry of msim's event heap, restricted to this station.
type stationEvent struct {
	at   time.Time
	seq  int64
	kind eventKind
	w    *waiter
	p    *permit
}

// notAfter is the heap order: (at, seq) <= (other, seq).
func (e stationEvent) notAfter(at time.Time, seq int64) bool {
	if e.at.Equal(at) {
		return e.seq <= seq
	}
	return e.at.Before(at)
}

// before is the heap order, strictly, between two events.
func (e stationEvent) before(o stationEvent) bool {
	if e.at.Equal(o.at) {
		return e.seq < o.seq
	}
	return e.at.Before(o.at)
}

// settle replays every station event that fell due at or before (now, seq),
// in msim's own order, before the mutation occupying that key runs.
//
// msim's station is a single-threaded event loop: a queued attempt's deadline
// and an in-service attempt's cut are EVENTS on its heap, and they fire at their
// instant in (time, seq) order whether or not anything else is happening. A Go
// station has no loop -- each deadline is a goroutine's timer, seen only when
// that goroutine is next scheduled. So everything that fell due between the last
// mutation and this one is replayed HERE, in that same order, under the station
// mutex:
//
//   - a queued waiter whose deadline passed expires AT that deadline, keeping
//     its slot (msim's expire_in_queue), snapshotting the queue depth and busy
//     count as they stood then;
//   - a permit whose deadline passed while it still held a worker is REAPED
//     there, claimed as a cut and freed (msim's _finish_service at
//     min(begin + service, deadline) -> _complete_attempt -> _start_next), so
//     the goroutine serving it writes the cut msim booked and rolls no fault.
//
// `seq` is the key the calling mutation itself occupies: an arrival takes a
// fresh one, so every event already scheduled for this instant fires before it;
// a permit claiming its own outcome or handing its worker back passes the
// permit's seq, so events scheduled BEFORE that permit entered service still fire
// first. That is what reproduces msim's heap order at equal timestamps.
//
// The loop re-scans after every application, because applying one event can
// mint a successor permit whose own events may be due in the same window.
//
// The snapshots are EXACT, not approximations: only a mutation can change the
// queue composition or `busy`, an expiry changes neither (an expired entry keeps
// its slot), and every mutation settles first -- so the state a replayed event
// sees is the state that stood at its instant.
func (s *station) settle(now time.Time, seq int64) {
	if s.mayHaveDueEventLocked(now) {
		for {
			ev, ok := s.earliestDueEventLocked(now, seq)
			if !ok {
				break
			}
			s.applyEventLocked(ev)
		}
		s.recomputeNextDeadlineLocked()
	}
	if now.After(s.lastSettled) {
		s.lastSettled = now
	}
}

// mayHaveDueEventLocked is the O(1) proof that settle has nothing to do. Every
// event settle can find is a DEADLINE -- a queued waiter's expiry or a held
// permit's reap -- and nextDeadline is a lower bound on all of them, so
// `now < nextDeadline` proves there is nothing to replay. Nothing is ever
// published behind the station's back for the bound to miss: an outcome is taken
// and applied under this mutex or not at all.
func (s *station) mayHaveDueEventLocked(now time.Time) bool {
	return s.hasNext && !s.nextDeadline.After(now)
}

// earliestDueEventLocked is the head of the station's heap, restricted to
// events that are due (at <= now) and not after the caller's own key. Reached
// only through settle, whose O(1) bound has already proved that something is
// due; the queue is never walked to find out that nothing is.
func (s *station) earliestDueEventLocked(now time.Time, seq int64) (stationEvent, bool) {
	var best stationEvent
	found := false
	for _, p := range s.permits {
		ev, ok := p.dueEventLocked(now)
		if !ok || !ev.notAfter(now, seq) {
			continue
		}
		if !found || ev.before(best) {
			best, found = ev, true
		}
	}
	for _, w := range s.waiters {
		if w.state != waiterQueued || !w.hasDeadline || w.deadline.After(now) {
			continue
		}
		ev := stationEvent{at: w.deadline, seq: w.seq, kind: eventExpiry, w: w}
		if !ev.notAfter(now, seq) {
			continue
		}
		if !found || ev.before(best) {
			best, found = ev, true
		}
	}
	return best, found
}

// applyEventLocked runs one event, at the instant the station can honestly run
// it. Every call applies something: a contender that finds the claim already
// taken never gets here, so committed time never moves for an event with nothing
// to do -- which is how a reap that lost the race for a claim used to strand the
// winner's completion behind a clock already advanced to the deadline.
//
// NO REWRITING COMMITTED TIME: an event whose instant is earlier than
// lastSettled is applied AT lastSettled. The station has already decided what
// happened in that interval -- a request was admitted, another was shed, a
// worker was handed over -- and msim, which would have processed this event
// first, would have decided them differently. The closest honest replay is
// therefore "as early as we still can", not a retroactive rewrite that would
// hand a worker over before the waiter arrived. The record still carries the
// true instant wherever the record's own field says so (a completion's `end` is
// its completedAt, an expiry's is the deadline); it is permit_released_at, and
// any hand-off this event causes, that land at the clamped instant.
func (s *station) applyEventLocked(ev stationEvent) {
	if ev.kind == eventTerminal && ev.p.outcome == nil {
		// The reap, and the only place the station claims an attempt itself: the
		// deadline of a permit that still holds a worker. Taken BEFORE committed
		// time moves below, under the same mutex every other contender takes it
		// under, so the attempt cannot be both reaped and completed.
		ev.p.claimLocked(claimCut, ev.at)
	}
	at := ev.at
	if at.Before(s.lastSettled) {
		at = s.lastSettled
	}
	s.lastSettled = at
	switch ev.kind {
	case eventExpiry:
		s.expireWaiterLocked(ev.w)
	case eventHandBack:
		ev.p.releaseLocked(at, true)
	case eventTerminal:
		p := ev.p
		// msim's _complete_attempt reports queue_len() BEFORE _start_next
		// dispatches, and the early-release branch of finalize reports it at the
		// finalize instant -- which is this instant either way.
		p.queueDepthAtEnd = len(s.waiters)
		s.dropPermitLocked(p)
		// A no-op when the worker already went back at the hand-back: that
		// occupancy is closed and complete, and this claim releases nothing.
		p.releaseLocked(at, false)
	}
}

// expireWaiterLocked fires msim's expire_in_queue for one waiter, at any
// position in the queue and not only at the head -- a server does not scan its
// backlog, but the CALLERS' clocks do not wait for it to.
func (s *station) expireWaiterLocked(w *waiter) {
	w.state = waiterExpired
	// The depth EXCLUDES this attempt, and every other zombie in the queue
	// still counts: an expired entry keeps its slot until the dispatcher
	// reaches it, so two attempts expiring in the same window each see the
	// other.
	w.queueDepth, w.workersBusy = len(s.waiters)-1, s.busy
	w.queueDepthAtEnd = w.queueDepth
	close(w.ready)
}

// dueEventLocked is this permit's pending event, when it fell due at or before
// `now`.
//
// There is exactly one it can ever be: the REAP of its own deadline, msim's
// _finish_service at min(begin + service, deadline). Only a permit still HOLDING
// a worker is the station's to cut -- one whose worker already went back moves no
// capacity, and its record is the interceptor's to write when the attempt really
// ends.
//
// A terminal claim and an early hand-back are taken and applied inside one
// critical section, so neither is ever found pending here: a permit that has a
// claim has already left s.permits.
func (p *permit) dueEventLocked(now time.Time) (stationEvent, bool) {
	if p.outcome != nil || p.released || !p.hasDeadline || p.deadline.After(now) {
		return stationEvent{}, false
	}
	return stationEvent{at: p.deadline, seq: p.seq, kind: eventTerminal, p: p}, true
}

// releaseLocked hands this permit's worker back at `at`, once. It reports
// whether THIS call is the one that freed the worker -- false when the worker
// had already gone back (a hand-back, or a reaper that got there at the
// deadline).
func (p *permit) releaseLocked(at time.Time, handBack bool) bool {
	if p.released {
		return false
	}
	p.released = true
	p.handedBack = handBack
	p.releasedAt.Store(at.UnixNano())
	p.st.releaseWorkerLocked(at)
	return true
}

// newPermitLocked mints a permit for a worker that has just been taken, and
// registers it so settle can apply the events it owns.
func (s *station) newPermitLocked(deadline time.Time, hasDeadline bool) *permit {
	p := &permit{st: s, deadline: deadline, hasDeadline: hasDeadline, seq: s.nextSeqLocked()}
	s.permits = append(s.permits, p)
	if hasDeadline {
		s.noteDeadlineLocked(deadline)
	}
	return p
}

func (s *station) dropPermitLocked(p *permit) {
	for i, x := range s.permits {
		if x == p {
			s.permits = append(s.permits[:i], s.permits[i+1:]...)
			return
		}
	}
}

// nextSeqLocked hands out the next event key. Callers hold mu.
func (s *station) nextSeqLocked() int64 {
	s.seq++
	return s.seq
}

// noteDeadlineLocked keeps nextDeadline a lower bound on every pending deadline.
func (s *station) noteDeadlineLocked(d time.Time) {
	if !s.hasNext || d.Before(s.nextDeadline) {
		s.nextDeadline, s.hasNext = d, true
	}
}

// recomputeNextDeadlineLocked makes the bound exact again, after a settle has
// fired everything that was due.
func (s *station) recomputeNextDeadlineLocked() {
	s.hasNext = false
	for _, w := range s.waiters {
		if w.state == waiterQueued && w.hasDeadline {
			s.noteDeadlineLocked(w.deadline)
		}
	}
	for _, p := range s.permits {
		if !p.released && p.hasDeadline {
			s.noteDeadlineLocked(p.deadline)
		}
	}
}

// releaseWorkerLocked hands the freed worker straight to the head of the queue,
// keeping `busy` constant across the handover -- that is what makes the
// occupancy bound exactly c in service plus K queued.
//
// This is msim's _start_next loop. Callers hold st.mu, inside a settle that has
// already applied every event ahead of this one, so every deadline that fell
// earlier has already fired. Walking the head, it can meet:
//
//   - an entry that is no longer waiterQueued: settled already, by settle or by
//     its own goroutine. Popped and skipped (msim's `if item.expired:
//     continue`), so the slot comes back and nothing more is owed -- except the
//     discard event, which is the only place the log can say when that happened.
//   - the exact tie (deadline <= now, and its expiry event has not fired, so it
//     is behind this release in the heap): reported deadline_at_dequeue and
//     popped WITHOUT the worker, which walks on to the next live entry -- msim's
//     _begin_service drops it before `in_flight += 1`.
//   - anything else: handed the permit, minted here, with `now` recorded on it.
func (s *station) releaseWorkerLocked(now time.Time) {
	for len(s.waiters) > 0 {
		w := s.waiters[0]
		s.waiters = s.waiters[1:]
		if w.state != waiterQueued {
			if w.state == waiterExpired {
				s.noteDiscardLocked(w, now)
			}
			continue
		}
		at := now
		if at.Before(w.enqueuedAt) {
			// Belt and braces. lastSettled is at least the enqueue instant of
			// every queued waiter (the arrival settled to it), and no event is
			// ever applied before lastSettled, so a hand-off cannot precede the
			// arrival it serves. If one ever did, the wait would be negative and
			// the record would show service starting before the request existed.
			at = w.enqueuedAt
		}
		if w.hasDeadline && !w.deadline.After(at) {
			// The tie. msim reports it with the queue depth AFTER the pop
			// (_start_next decrements `_queued` before calling _begin_service)
			// and with the worker already free (`in_flight` was decremented by
			// _complete_attempt), which is what busy-1 says here.
			w.state = waiterTied
			w.handoffAt = at
			w.queueDepth, w.workersBusy = len(s.waiters), s.busy-1
			w.queueDepthAtEnd = w.queueDepth
			close(w.ready)
			// Logged for symmetry with the zombies above: this entry also left
			// the queue at an instant its own record (which ends at the deadline
			// it tied with) happens to name, and the pipeline reads one rule.
			s.noteDiscardLocked(w, at)
			continue
		}
		w.state = waiterHandedOff
		w.handoffAt = at
		w.queueDepth = len(s.waiters)
		w.workersBusy = s.busy - 1
		w.permit = s.newPermitLocked(w.deadline, w.hasDeadline)
		// Closed while still holding the mutex, so a cancel or a deadline
		// arriving at the same instant either sees waiterQueued (and wins,
		// before the hand-off) or sees waiterHandedOff with the channel already
		// closed. close never blocks, so nothing waits on the lock for it.
		close(w.ready)
		return
	}
	s.busy--
}

// removeWaiterLocked drops a cancelled waiter out of the FIFO. Callers hold mu.
func (s *station) removeWaiterLocked(w *waiter) {
	for i, x := range s.waiters {
		if x == w {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			return
		}
	}
}

// noteDiscardLocked queues the station event that says WHEN an expired queue
// entry really gave its slot back. The entry's own record was written at its
// deadline and cannot say: nothing else in the log marks the instant, and
// without it the pipeline cannot reconstruct queued(t) exactly.
func (s *station) noteDiscardLocked(w *waiter, at time.Time) {
	if s.log == nil {
		return
	}
	s.discards = append(s.discards, w.id.discardRecord(at))
}

// unlock drops the station mutex and writes whatever station events the
// critical section produced. The records are built under the lock and written
// outside it: a station that held its mutex across a syscall would serialise
// every arrival behind the log.
func (s *station) unlock() {
	pending := s.discards
	s.discards = nil
	s.mu.Unlock()
	for _, rec := range pending {
		_ = s.log.Write(rec)
	}
}

// Busy reports the workers currently held (tests assert the c+K bound). It is
// an observation, not a mutation, so it reports what the station has actually
// done rather than settling it first.
func (s *station) Busy() int {
	s.mu.Lock()
	defer s.unlock()
	return s.busy
}

// QueueDepth reports the live waiter count. An entry whose caller-clock
// deadline has passed is still one of them until the dispatcher reaches it: the
// server has not looked at it yet, so it still occupies the backlog and still
// sheds load (msim's `_queued`, which discounts cancels but not expiries). It
// is an observation too, and does not settle.
func (s *station) QueueDepth() int {
	s.mu.Lock()
	defer s.unlock()
	return len(s.waiters)
}

// expired is the strict half-open deadline test at submit: now >= deadline is
// already too late.
func expired(ctx context.Context, now time.Time) bool {
	d, ok := ctx.Deadline()
	if !ok {
		return false
	}
	return !now.Before(d)
}

// deadlineCancelSlack is how close to a context's deadline a plain Canceled is
// read as that deadline rather than as an explicit cancel.
//
// gRPC propagates a RELATIVE timeout (the `grpc-timeout` header), so the server
// rebuilds the deadline on receipt: the deadline D_s the server context carries
// is the caller's D plus the forward latency of the request frame. When the
// caller's own deadline fires, its transport sends RST_STREAM(CANCEL), which
// cancels the server context with context.Canceled at about the same instant
// D_s's own timer would have fired with context.DeadlineExceeded. Which of the
// two arrives first is a coin flip, decided by the jitter between the forward
// latency of the request frame and that of the RST frame -- and the two are
// DIFFERENT msim drop reasons (DEADLINE against CANCELLED) that the pipeline
// books differently: a cancelled attempt is excluded from failure_retry
// entirely, and a cancelled_in_queue waiter gives its queue slot back while an
// expired one keeps it. Reading the error alone therefore makes the same
// physical event land in two different columns at random.
//
// So the INSTANT decides, not the error. The cost is the only ambiguity gRPC
// leaves: a genuine explicit cancel issued in the last 5 ms before the deadline
// is counted as the deadline. 5 ms is far above the sub-millisecond frame jitter
// the race is made of, and far below any timeout the campaign configures.
const deadlineCancelSlack = 5 * time.Millisecond

// deadlineCause reports whether a finished context ended because the CALLER's
// deadline passed (msim's DropReason.DEADLINE) rather than because the caller
// explicitly cancelled it (DropReason.CANCELLED). It is the one rule the station
// and the server flow both apply wherever they interpret ctx.Err().
//
// A context that is not done yet has no cause; one with no deadline at all can
// only have been cancelled; DeadlineExceeded speaks for itself; and a Canceled
// observed at `now` within deadlineCancelSlack of the deadline is the caller's
// deadline arriving as an RST_STREAM.
func deadlineCause(ctx context.Context, now time.Time) bool {
	err := ctx.Err()
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	d, ok := ctx.Deadline()
	return ok && !d.After(now.Add(deadlineCancelSlack))
}

type permitCtxKey struct{}

func withPermit(ctx context.Context, p *permit) context.Context {
	return context.WithValue(ctx, permitCtxKey{}, p)
}

func permitFrom(ctx context.Context) *permit {
	p, _ := ctx.Value(permitCtxKey{}).(*permit)
	return p
}

// ReleasePermit hands the admission permit back before the handler returns, so
// a relay does not hold a worker across its downstream call (msim's
// hold_worker_during_fanout=False, the default).
//
// Idempotent; a no-op when the request holds no permit, when the interceptor is
// inert, and when the station is configured with hold_permit_through_fanout
// (arm A4's deployment counterpart), where the permit is deliberately held to
// the end of the handler.
func ReleasePermit(ctx context.Context) {
	p := permitFrom(ctx)
	if p == nil || p.st == nil || p.st.hold {
		return
	}
	p.handBack(p.st.clock.Now())
}
