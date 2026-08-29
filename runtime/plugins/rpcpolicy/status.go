package rpcpolicy

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Drop reasons written into a client attempt-log record (CONTRACTS.md §5).
const (
	dropNone          = ""
	dropDeadline      = "deadline"
	dropQueueFull     = "queue_full"
	dropServerFailure = "server_failure"
	dropCancelled     = "cancelled"
)

// Reasons a retry was not issued after the final attempt (CONTRACTS.md §5).
const (
	deniedNone         = ""
	deniedNotRetryable = "not_retryable"
	deniedExhausted    = "exhausted"
	deniedDeadlineVeto = "deadline_veto"
)

// gateCircuitOpen is the only non-empty value of the client record's `gate`
// field: an admission gate (circuit breaker or rate limiter) refused the root
// before anything reached the wire.
const gateCircuitOpen = "circuit_open"

// codeOf maps an error to the gRPC code recorded in `response_code`. A raw
// context error escapes while an attempt waits on a backoff or an admission
// permit, before any status is produced, so it is translated the way
// status.FromContextError does.
func codeOf(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	if s, ok := status.FromError(err); ok {
		return s.Code()
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return status.FromContextError(err).Code()
	}
	return codes.Unknown
}

// dropReasonFor maps a gRPC code to the pipeline's drop-reason vocabulary.
// ResourceExhausted is the admission station's queue-full status, Unavailable
// the injected/served failure, DeadlineExceeded either side's strict half-open
// deadline, Canceled an abandoned attempt.
func dropReasonFor(c codes.Code) string {
	switch c {
	case codes.OK:
		return dropNone
	case codes.DeadlineExceeded:
		return dropDeadline
	case codes.ResourceExhausted:
		return dropQueueFull
	case codes.Canceled:
		return dropCancelled
	default:
		return dropServerFailure
	}
}

// parseCode turns a gRPC status name from the policy YAML's retry_on list into
// a code.
//
// The spelling is codes.Code.String() -- "Unavailable", "DeadlineExceeded" --
// which is the same spelling the attempt log's response_code carries, so a
// retry_on list and a recorded record can be compared by eye. It is NOT
// codes.Code.UnmarshalJSON's spelling ("UNAVAILABLE", "CANCELLED"), so the
// table is built from String() rather than delegating.
func parseCode(name string) (codes.Code, bool) {
	c, ok := codeByName[name]
	return c, ok
}

var codeByName = func() map[string]codes.Code {
	m := make(map[string]codes.Code, 17)
	for c := codes.OK; c <= codes.Unauthenticated; c++ {
		m[c.String()] = c
	}
	return m
}()
