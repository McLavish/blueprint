package rpcpolicy

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// The admission station: docs/PLAN.md D4's capacity mechanism, a transcription
// of msim's ServiceRuntime.submit_attempt / _begin_service. `c` permits over a
// FIFO queue of `K`, with msim's four drop points.
//
// The waiter list is explicit rather than a buffered channel because FCFS is
// load-bearing: a channel hands the permit to an arbitrary blocked receiver,
// and the queueing model the campaign fits (M/G/c/K) assumes first-come
// first-served. It also makes queue depth observable, which every server
// record carries.

// Admission outcomes (CONTRACTS.md §5).
const (
	outcomeAdmitted          = "admitted"
	outcomeQueueFull         = "queue_full"
	outcomeDeadlineAtSubmit  = "deadline_at_submit"
	outcomeDeadlineAtDequeue = "deadline_at_dequeue"
	outcomeDeadlineAtFinish  = "deadline_at_finish"
)

// unboundedQueue is the sentinel for `queue_capacity: null`.
const unboundedQueue = -1

type waiter struct {
	ready chan struct{}
	// Sampled at the instant the permit is handed over, so the record reports
	// the state this request actually observed.
	queueDepth  int
	workersBusy int
}

type station struct {
	clock    Clock
	workers  int
	capacity int // unboundedQueue for no bound
	hold     bool

	mu      sync.Mutex
	busy    int
	waiters []*waiter
}

func newStation(cfg *ServerConfig, clock Clock) *station {
	capacity := unboundedQueue
	if cfg.QueueCapacity != nil {
		capacity = *cfg.QueueCapacity
	}
	return &station{
		clock:    clock,
		workers:  cfg.Workers,
		capacity: capacity,
		hold:     cfg.HoldPermitThroughFanout,
	}
}

// permit is one held worker. release is idempotent (sync.Once), so the
// interceptor may release defensively after a handler that already called
// ReleasePermit.
type permit struct {
	st         *station
	once       sync.Once
	releasedAt atomic.Int64 // wall-clock UnixNano, 0 while held
}

func (p *permit) release() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		p.releasedAt.Store(p.st.clock.Now().UnixNano())
		p.st.releaseWorker()
	})
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
}

// Acquire runs msim's submit_attempt: the deadline check at submit, the
// free-worker fast path, the queue-full shed, then the FIFO wait and the
// deadline check at dequeue.
//
// Note the order: a request whose deadline has already passed is dropped BEFORE
// the queue-full test, and a free worker is taken BEFORE it too -- so
// queue_full can only be reported when every worker is busy, exactly as msim.
//
// A queued request is NOT removed when its caller goes away: msim keeps it in
// the queue until a worker frees up and reports deadline_at_dequeue there, and
// the c+K occupancy the model fits depends on that.
func (s *station) Acquire(ctx context.Context) admission {
	start := s.clock.Now()
	if expired(ctx, start) {
		s.mu.Lock()
		depth, busy := len(s.waiters), s.busy
		s.mu.Unlock()
		return admission{outcome: outcomeDeadlineAtSubmit, queueDepth: depth, workersBusy: busy}
	}

	s.mu.Lock()
	if s.busy < s.workers {
		s.busy++
		depth, busy := len(s.waiters), s.busy-1
		s.mu.Unlock()
		return admission{permit: &permit{st: s}, outcome: outcomeAdmitted, queueDepth: depth, workersBusy: busy}
	}
	if s.capacity != unboundedQueue && len(s.waiters) >= s.capacity {
		depth, busy := len(s.waiters), s.busy
		s.mu.Unlock()
		return admission{outcome: outcomeQueueFull, queueDepth: depth, workersBusy: busy}
	}
	w := &waiter{ready: make(chan struct{})}
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()

	<-w.ready
	wait := s.clock.Now().Sub(start)
	p := &permit{st: s}
	if expired(ctx, s.clock.Now()) {
		p.release()
		return admission{outcome: outcomeDeadlineAtDequeue, wait: wait, queueDepth: w.queueDepth, workersBusy: w.workersBusy}
	}
	return admission{permit: p, outcome: outcomeAdmitted, wait: wait, queueDepth: w.queueDepth, workersBusy: w.workersBusy}
}

// releaseWorker hands the freed worker straight to the head of the queue,
// keeping `busy` constant across the handover -- that is what makes the
// occupancy bound exactly c in service plus K queued.
func (s *station) releaseWorker() {
	s.mu.Lock()
	if len(s.waiters) > 0 {
		w := s.waiters[0]
		s.waiters = s.waiters[1:]
		w.queueDepth = len(s.waiters)
		w.workersBusy = s.busy - 1
		s.mu.Unlock()
		close(w.ready)
		return
	}
	s.busy--
	s.mu.Unlock()
}

// Busy reports the workers currently held (tests assert the c+K bound).
func (s *station) Busy() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.busy
}

// QueueDepth reports the live waiter count.
func (s *station) QueueDepth() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiters)
}

// expired is the strict half-open deadline test at submit and dequeue:
// now >= deadline is already too late.
func expired(ctx context.Context, now time.Time) bool {
	d, ok := ctx.Deadline()
	if !ok {
		return false
	}
	return !now.Before(d)
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
	p.release()
}
