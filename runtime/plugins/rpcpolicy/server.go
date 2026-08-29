package rpcpolicy

import (
	"context"
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

	startWall := s.clock.Now()
	deadlineNSVal, hasDeadline := deadlineNS(s.clock, ctx)

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

	if adm.outcome != outcomeAdmitted {
		code := codes.DeadlineExceeded
		if adm.outcome == outcomeQueueFull {
			code = codes.ResourceExhausted
		}
		err := status.Errorf(code, "rpcpolicy: %s: %s", fullMethod, adm.outcome)
		s.finishServerRecord(rec, startWall, code, 0)
		return nil, err
	}

	p := adm.permit
	ctx = withPermit(ctx, p)

	// D14: inside the permit, in this order -- injected latency occupies a
	// worker, the handler occupies a worker, and the failure roll happens after
	// the service time rather than before it.
	addLatencyMS, pFail := s.faults.active(fullMethod)
	var resp interface{}
	var err error
	injectedStart := s.clock.Now()
	if addLatencyMS > 0 {
		if serr := sleepCtx(ctx, time.Duration(addLatencyMS*float64(time.Millisecond))); serr != nil {
			rec.InjectedMS = msOf(s.clock.Now().Sub(injectedStart))
			code := codeOf(serr)
			if code == codes.DeadlineExceeded {
				// Expired in service, while holding a worker: the same class of
				// outcome as an expiry during the handler, so it carries the
				// same admission outcome and the retire sidecar can count it as
				// expired_in_service.
				rec.AdmissionOutcome = outcomeDeadlineAtFinish
			}
			p.release()
			s.finishServerRecord(rec, startWall, code, p.releasedEpoch())
			return nil, serr
		}
		rec.InjectedMS = msOf(s.clock.Now().Sub(injectedStart))
	}

	handlerStart := s.clock.Now()
	resp, err = handler(ctx, req)
	rec.HandlerMS = msOf(s.clock.Now().Sub(handlerStart))

	if pFail > 0 && s.faults.nextRoll() < pFail {
		rec.FaultHit = true
		resp, err = nil, status.Error(codes.Unavailable, "injected fault")
	}

	// Strict half-open finish rule: success requires end < deadline. The client
	// applies the same rule to the same attempt, so both sides book it alike.
	endNS := s.clock.NowNS()
	if hasDeadline && endNS >= deadlineNSVal {
		rec.AdmissionOutcome = outcomeDeadlineAtFinish
		resp, err = nil, status.Errorf(codes.DeadlineExceeded, "rpcpolicy: %s: handler finished at its deadline", fullMethod)
	}
	p.release()
	s.finishServerRecord(rec, startWall, codeOf(err), p.releasedEpoch())
	return resp, err
}

func (s *runtimeState) finishServerRecord(rec *ServerRecord, start time.Time, code codes.Code, releasedAt float64) {
	end := s.clock.Now()
	rec.EndEpoch = epochOf(end)
	rec.DurationMS = msOf(end.Sub(start))
	rec.ResponseCode = code.String()
	rec.IsError = code != codes.OK
	rec.PermitReleasedAt = releasedAt
	s.log.Write(rec)
}
