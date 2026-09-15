package rpcpolicy

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The server half of the public API (CONTRACTS.md §6): the admission station,
// the fault injector inside its permit, and one server record per outcome.

// ServerOptions returns the options a generated gRPC server must install. nil
// when inert.
func ServerOptions() []grpc.ServerOption {
	st := currentState()
	if st.inert {
		return nil
	}
	return []grpc.ServerOption{grpc.UnaryInterceptor(st.unaryServerInterceptor())}
}

func (s *runtimeState) unaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return s.serve(ctx, req, info.FullMethod, handler)
	}
}

// serve is docs/PLAN.md WP1's "Server flow".
func (s *runtimeState) serve(ctx context.Context, req interface{}, fullMethod string, handler grpc.UnaryHandler) (interface{}, error) {
	reg := s.registry.Load()
	traceID, parentSpan, route := traceFromMetadata(ctx)
	spanID := newSpanID()

	if reg != nil && reg.server != nil && reg.server.StripInboundDeadline {
		// Arm A1's deployment counterpart: the handler no longer inherits the
		// caller's deadline OR its cancellation, so the work runs to completion
		// even after the caller has walked away. context.WithoutCancel keeps
		// the values (the permit, the trace) and drops both.
		ctx = context.WithoutCancel(ctx)
	}
	ctx = withTraceCtx(ctx, traceCtx{traceID: traceID, spanID: spanID, route: route})
	// The identity the station stamps on its own events (the discard it writes
	// when it drops an expired queue entry): the same fields the record below
	// carries, so the two join on span_id.
	ctx = withAttemptID(ctx, attemptID{
		traceID: traceID, spanID: spanID, parentSpanID: parentSpan,
		service: s.service, operation: fullMethod,
	})

	startWall := s.clock.Now()
	deadline, hasDeadline := ctx.Deadline()

	rec := &ServerRecord{
		baseRecord: baseRecord{
			Kind: "server", TraceID: traceID, SpanID: spanID, ParentSpanID: parentSpan,
			Service: s.service, Operation: fullMethod,
			StartEpoch: epochOf(startWall),
		},
	}

	st := reg.Station()
	adm := admission{outcome: outcomeAdmitted}
	if st != nil {
		adm = st.Acquire(ctx)
	}
	rec.AdmissionOutcome = adm.outcome
	rec.AdmissionWaitMS = msOf(adm.wait)
	rec.AdmissionQueueDepth = adm.queueDepth
	rec.AdmissionWorkersBusy = adm.workersBusy
	// For a drop the end IS the admission, so the station's own sample is the
	// end-of-attempt depth too. An admitted attempt overwrites this from its
	// permit when its outcome is applied.
	rec.QueueDepthAtEnd = adm.queueDepthAtEnd

	if adm.outcome != outcomeAdmitted {
		code := codes.DeadlineExceeded
		switch adm.outcome {
		case outcomeQueueFull:
			code = codes.ResourceExhausted
		case outcomeCancelledInQueue:
			// The caller walked away while queued: Canceled, not a deadline --
			// msim's DropReason.CANCELLED (CONTRACTS.md §5).
			code = codes.Canceled
		}
		err := status.Errorf(code, "rpcpolicy: %s: %s", fullMethod, adm.outcome)
		// A drop the station timed itself carries the instant: deadline_in_queue
		// and the deadline_at_dequeue tie both end AT the caller's deadline, as
		// msim's expire_in_queue and _begin_service do, rather than whenever the
		// serving goroutine was next scheduled. Every other drop is decided now.
		//
		// None of them is occupancy-censored: no worker was ever held, which is
		// exactly what msim reports on all four of these paths (False).
		end := adm.end
		if end.IsZero() {
			end = s.clock.Now()
		}
		s.finishServerRecord(rec, startWall, end, code, 0)
		return nil, err
	}

	p := adm.permit
	if p == nil {
		// No `server:` block, so no worker and no station. The permit is still
		// minted, detached, because the claim below is what makes the record
		// exactly-once regardless of who observes the attempt's end first.
		p = &permit{deadline: deadline, hasDeadline: hasDeadline}
	}
	ctx = withPermit(ctx, p)

	// D14: inside the permit, in this order -- injected latency occupies a
	// worker, the handler occupies a worker, and the failure roll happens after
	// the service time rather than before it.
	//
	// The two halves of a fault are read at two different instants on purpose.
	// The additive latency is what OCCUPIES the permit, so it is fixed at
	// admission; the failure probability is evaluated at the instant the handler
	// RETURNED, which is where msim's _finish_service calls _fails_now(now).
	addLatencyMS := s.faults.activeLatency(fullMethod)
	ph := &servePhase{injectedStart: s.clock.Now()}
	work := func() serveResult {
		var out serveResult
		if addLatencyMS > 0 {
			injected, ok := millisToDuration(addLatencyMS)
			if !ok {
				// Unrepresentable as a Duration: hold the permit until the
				// deadline cuts it, which is the only honest reading of "wait
				// ~forever".
				injected = time.Duration(math.MaxInt64)
			}
			// Through the clock seam, like every other instant in the policy
			// layer: the injected latency IS service time, so a test that drives
			// virtual time must be able to spend it exactly rather than really
			// sleeping through it.
			if serr := s.clock.Sleep(ctx, injected); serr != nil {
				// Cut inside the injected latency: the handler never runs, and
				// the classification below is the same in-service cut. The
				// context's state is read BEFORE the stamp, as at the handler's
				// return below.
				out.ctxErrAtCompletion = ctx.Err()
				out.completedAt = s.clock.Now()
				out.injectedMS = msSinceClamped(ph.injectedStart, out.completedAt)
				out.err = serr
				return out
			}
			out.injectedMS = msSinceClamped(ph.injectedStart, s.clock.Now())
		}
		handlerStart := s.clock.Now()
		ph.enterHandler(out.injectedMS, handlerStart)
		out.resp, out.err = handler(ctx, req)
		// Two reads the moment the handler returned, in THIS order: the state of
		// the context first, the instant second. A context that ends between the
		// two reads is then seen by the stamp (the instant is at or past its
		// deadline) and not by the captured state, so it can never be blamed
		// for a completion that preceded it; read the other way round, a
		// deadline firing in that gap turned a completion at D_s - 1 ms into a
		// cut at D_s. Together they are the handler's measured duration, the
		// instant the attempt's outcome is claimed at, and -- through
		// ctxErrAtCompletion -- what that outcome IS: msim finishes the service
		// THERE, on the state that stood THERE, not when the interceptor's
		// goroutine is next scheduled to hear about it.
		out.ctxErrAtCompletion = ctx.Err()
		if s.betweenCompletionReads != nil {
			s.betweenCompletionReads()
		}
		out.completedAt = s.clock.Now()
		out.handlerMS = msSinceClamped(handlerStart, out.completedAt)
		return out
	}

	// claimWork ends the attempt with the claim the finished work deserves, on
	// the goroutine that ran it.
	//
	// Work that stopped BECAUSE its own context stopped -- a ctx-aware handler,
	// the injected sleep giving up -- did not run to completion: that is the same
	// in-service cut the interceptor books from the other side, and claiming it as
	// one here is what keeps the claim, and not a second reading of the result,
	// the single statement of how the attempt ended. Which cut it is, is
	// deadlineCause's call rather than ctx.Err()'s (see deadlineCancelSlack), and
	// a DEADLINE is booked AT the deadline.
	//
	// Whether it did stop because of its context is read off the state the work
	// captured AT ITS STAMP, in the same observation as completedAt, and never off
	// a fresh ctx.Err() taken here: that one answers about a different instant.
	// A downstream call returning its own DeadlineExceeded at D_s - 1 ms under a
	// live parent is a COMPLETION, and stays one however long this goroutine is
	// then descheduled -- asking the context again after it expired at D_s turned
	// that completion into a cut at D_s (deadline_at_finish, censored, rolling no
	// fault) where msim books the completion at D_s - 1 ms: admitted, the
	// downstream's code as the response code, a complete occupancy, and one roll.
	// A captured error is final -- a context's Err() never changes once it is
	// non-nil -- so deadlineCause below is reading the very error the stamp saw.
	//
	// Everything else is a completion, claimed at the instant the work stamped --
	// not at the instant this or any other goroutine was next scheduled to hear
	// about it. The claim may still LOSE: the station reaps a permit whose
	// deadline passed while it held a worker, and permit.claim settles the station
	// through this permit's own event key before taking it, so a completion
	// stamped at or past the deadline meets that reap first and the cut stands.
	claimWork := func(r serveResult) *claim {
		// The seam is the whole window between the stamp and the claim, the
		// classification included: a test parks the worker HERE, moves the world
		// on, and proves that what the claim says is what the work observed.
		if s.beforeClaim != nil {
			s.beforeClaim()
		}
		kind, at := claimCompleted, r.completedAt
		// A captured DeadlineExceeded can only have caused the stop if the stop
		// is at or past the deadline it names: a stamp strictly before the
		// deadline is a completion whatever the context reports (a fake clock
		// can put the two out of order; a real one cannot, since the context
		// state is read first), and the strict finish rule below then has the
		// last word on where a completion at or past D_s is booked.
		expiredBeforeStop := errors.Is(r.ctxErrAtCompletion, context.DeadlineExceeded) &&
			hasDeadline && r.completedAt.Before(deadline)
		if r.ctxErrAtCompletion != nil && isContextCode(r.err) && !expiredBeforeStop {
			kind = claimCancelled
			if deadlineCause(ctx, r.completedAt) {
				kind = claimCut
				if hasDeadline {
					at = deadline
				}
			}
		}
		c, _ := p.claim(kind, at)
		return c
	}

	var res serveResult
	var c *claim
	if ctx.Done() == nil {
		// Nothing can cut this request short: it carries neither a deadline nor
		// a cancellation (arm A1's strip_inbound_deadline, or a caller that sent
		// neither). The work runs on this goroutine, and the permit is held for
		// exactly as long as it takes -- which is the point of that arm.
		res = work()
		c = claimWork(res)
	} else {
		// msim cuts an attempt in service AT its deadline and frees the worker
		// THERE: _finish_service is scheduled at min(begin + service, deadline),
		// and cancel_in_service hands the worker straight back. Calling the
		// handler synchronously let a handler that ignores its context hold
		// capacity long past the deadline -- with one worker, a 50 ms deadline
		// and a handler sleeping a second, msim admits the next arrival at 60 ms
		// and this server shed it queue_full. So the work runs beside the cut
		// and whichever CLAIMS the attempt first decides it; this goroutine
		// writes the one record either way.
		done := make(chan struct{})
		var workClaim *claim
		go func() {
			r := work()
			workClaim = claimWork(r)
			res = r
			close(done)
		}()
		select {
		case <-done:
			c = workClaim
		case <-ctx.Done():
			select {
			case <-done:
				// The work finished in the same instant the context ended.
				// Resolving the tie here rather than letting the outer select
				// pick at random is what keeps the record deterministic on that
				// boundary; the claim says which way it really went.
				c = workClaim
			default:
				now := s.clock.Now()
				kind, at := claimCancelled, now
				if deadlineCause(ctx, now) {
					kind = claimCut
					if hasDeadline {
						at = deadline
					}
				}
				var won bool
				c, won = p.claim(kind, at)
				if !won && c.kind == claimCompleted {
					// The work won the race. Its result is the attempt's, and
					// this goroutine may not read it until the worker has
					// written it.
					<-done
				}
			}
		}
	}

	// The claim that WON is the attempt's outcome, whoever took it: the record is
	// classified from it and never from `res`, which is one contender's view of an
	// attempt the station may already have ended some other way. A handler result
	// that arrives after a committed cut is not a second truth about the same
	// attempt -- it is the result of work nobody was still waiting for.
	if c.kind != claimCompleted {
		return s.cutInService(rec, startWall, fullMethod, p, ph, c)
	}

	// The strict half-open finish rule, applied to the instant the completion was
	// claimed at -- the instant the work stamped. msim's _finish_service tests the
	// deadline FIRST and never reaches _fails_now on one, so the tie rolls
	// nothing; and because the worker was still held at the deadline,
	// _complete_attempt books the occupancy as censored.
	//
	// A permit the station could reap has already been cut above, at the deadline,
	// by that reap. What is left for this branch is the completion the station
	// could not reap: a detached permit (no `server:` block, so no station), and
	// one whose worker went back early -- where msim reports the occupancy as
	// complete, which is what endSnapshot's handedBack says.
	if hasDeadline && !c.at.Before(deadline) {
		rec.AdmissionOutcome = outcomeDeadlineAtFinish
		rec.InjectedMS, rec.HandlerMS = ph.cutAt(deadline)
		depth, handedBack, released := p.endSnapshot()
		rec.QueueDepthAtEnd, rec.OccupancyCensored = depth, !handedBack
		s.finishServerRecord(rec, startWall, deadline, codes.DeadlineExceeded, released)
		return nil, status.Errorf(codes.DeadlineExceeded, "rpcpolicy: %s: handler finished at its deadline", fullMethod)
	}

	rec.InjectedMS, rec.HandlerMS = res.injectedMS, res.handlerMS
	resp, err := res.resp, res.err

	// D14: the roll is made AT the completion instant -- the instant the claim
	// names, which is where the work stamped -- on the fault window in force
	// there (msim's _fails_now(sim.timestep)), so a handler that runs into or out
	// of a short window is governed by the probability that was live when it
	// finished rather than whenever this goroutine got around to asking.
	if pFail := s.faults.pFailAt(fullMethod, c.at); pFail > 0 && s.faults.nextRoll() < pFail {
		rec.FaultHit = true
		resp, err = nil, status.Error(codes.Unavailable, "injected fault")
	}

	// The handler ran to completion inside its deadline, so the occupancy is a
	// complete observation.
	depth, _, released := p.endSnapshot()
	rec.QueueDepthAtEnd = depth
	s.finishServerRecord(rec, startWall, c.at, codeOf(err), released)
	return resp, err
}

// cutInService books the in-service cut named by `c`: the attempt ended while it
// held (or had held) a worker, and not by finishing. msim frees the worker AT
// that instant, which the claim already did, and not whenever a handler that
// ignores its context finally returns. The handler goroutine is abandoned
// outright -- its result is discarded, it writes no record, it rolls no fault,
// and it feeds nothing back to a breaker or a budget -- so exactly one record is
// written for the attempt either way.
//
// Which cut it is, is deadlineCause's call rather than ctx.Err()'s (see
// deadlineCancelSlack). A DEADLINE closes the record AT D_s with
// deadline_at_finish and DeadlineExceeded, the same outcome a handler that
// finished at its deadline carries, so the retire sidecar can count both as
// expired_in_service. An explicit cancel closes it at the cancel: msim's
// cancel_in_service abandons the work without it being a drop of the server's
// making, so the admission outcome stays `admitted` and only the response code
// says the caller left.
//
// occupancy_censored follows msim's _complete_attempt exactly: TRUE when the
// worker was still held at the cut, FALSE when it had already been handed back
// (ReleasePermit, msim's tandem release_worker) -- that occupancy sample ended
// completely at the hand-back and is not truncated by whatever cut the attempt
// afterwards.
func (s *runtimeState) cutInService(rec *ServerRecord, startWall time.Time, fullMethod string, p *permit, ph *servePhase, c *claim) (interface{}, error) {
	end, code, why := c.at, codes.Canceled, "cancelled in service"
	if c.kind == claimCut {
		rec.AdmissionOutcome = outcomeDeadlineAtFinish
		code, why = codes.DeadlineExceeded, "cut at its deadline in service"
	}
	rec.InjectedMS, rec.HandlerMS = ph.cutAt(end)
	depth, handedBack, released := p.endSnapshot()
	rec.QueueDepthAtEnd, rec.OccupancyCensored = depth, !handedBack
	s.finishServerRecord(rec, startWall, end, code, released)
	return nil, status.Errorf(code, "rpcpolicy: %s: %s", fullMethod, why)
}

// serveResult is what the in-permit work reports back. It is written by the
// worker goroutine and read only after its `done` channel closes, so a cut
// attempt can never read a half-written one.
type serveResult struct {
	resp       interface{}
	err        error
	injectedMS float64
	handlerMS  float64
	// completedAt is the single clock observation taken the moment the work
	// stopped -- the instant the attempt's outcome is claimed at, the instant
	// the fault is rolled at, and the instant the record ends at.
	completedAt time.Time
	// ctxErrAtCompletion is the context's state at that SAME observation: nil if
	// the work ran out under a live context, and otherwise the error that ended
	// it. Whether work stopped BECAUSE its context stopped is a fact about the
	// instant it stopped, so it is captured there rather than asked again at the
	// claim, which may be arbitrarily later and about a context that has since
	// ended.
	ctxErrAtCompletion error
}

// servePhase is the worker goroutine's progress, published under its own mutex
// so that a cut arriving on the OTHER side of the select can say where the
// permit was when the context ended: still inside the injected latency, or in
// the handler.
type servePhase struct {
	mu            sync.Mutex
	injectedStart time.Time
	injectedMS    float64
	handlerStart  time.Time
	inHandler     bool
}

func (ph *servePhase) enterHandler(injectedMS float64, at time.Time) {
	ph.mu.Lock()
	ph.injectedMS, ph.handlerStart, ph.inHandler = injectedMS, at, true
	ph.mu.Unlock()
}

// cutAt splits a permit cut short at `at` into the two halves the record
// carries. msim reports the occupancy up to the cut (_finish_service's
// `sim.timestep - begin_at`), so the half that was running takes the time up to
// `at` and the half that never started takes zero.
func (ph *servePhase) cutAt(at time.Time) (injectedMS, handlerMS float64) {
	ph.mu.Lock()
	defer ph.mu.Unlock()
	if ph.inHandler {
		return ph.injectedMS, msSinceClamped(ph.handlerStart, at)
	}
	return msSinceClamped(ph.injectedStart, at), 0
}

// isContextCode reports whether an error is a context's own -- context.Canceled
// or context.DeadlineExceeded, raw or as the status status.FromContextError
// produces. Read ONLY together with the context's state as it stood when the
// work stamped its end (serveResult.ctxErrAtCompletion): on its own it cannot
// tell a handler's own downstream deadline from the one that cut this request,
// and a reading taken later cannot tell which of the two instants it is about.
func isContextCode(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch codeOf(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	}
	return false
}

// msSinceClamped is end - start in float milliseconds, floored at zero: a cut
// booked at the deadline can land BEFORE the phase it cuts began, when the
// permit was handed over just under the deadline and the serving goroutine was
// scheduled just over it.
func msSinceClamped(start, end time.Time) float64 {
	if d := end.Sub(start); d > 0 {
		return msOf(d)
	}
	return 0
}

// finishServerRecord closes a server record at `end`. Every path but one passes
// the instant it finished at; deadline_in_queue passes the deadline itself,
// because that is when the caller was told.
func (s *runtimeState) finishServerRecord(rec *ServerRecord, start, end time.Time, code codes.Code, releasedAt float64) {
	rec.EndEpoch = epochOf(end)
	rec.DurationMS = msOf(end.Sub(start))
	rec.ResponseCode = code.String()
	rec.IsError = code != codes.OK
	rec.PermitReleasedAt = releasedAt
	_ = s.log.Write(rec)
}
