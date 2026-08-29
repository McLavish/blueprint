package rpcpolicy

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"strings"

	"google.golang.org/grpc/metadata"
)

// W3C trace-context propagation, cut down to what the attempt log needs: a
// 32-hex trace id, a 16-hex span id and the sampled flag. No OpenTelemetry
// dependency -- nothing in the pipeline reads spans (docs/PLAN.md D6), and the
// join keys are the ids in the JSONL records.

// Metadata / header keys carried between processes (CONTRACTS.md §5).
const (
	// TraceparentKey is the W3C header and gRPC metadata key.
	TraceparentKey = "traceparent"
	// RouteKey carries the front-door route so a downstream profile lookup can
	// be route-scoped.
	RouteKey = "x-rpcpolicy-route"
)

// traceCtx is what an inbound request establishes and an outbound call
// inherits: children take SpanID as their parent.
type traceCtx struct {
	traceID string
	spanID  string
	route   string
}

type traceCtxKey struct{}

func withTraceCtx(ctx context.Context, tc traceCtx) context.Context {
	return context.WithValue(ctx, traceCtxKey{}, tc)
}

func traceCtxFrom(ctx context.Context) (traceCtx, bool) {
	tc, ok := ctx.Value(traceCtxKey{}).(traceCtx)
	return tc, ok
}

// formatTraceparent renders "00-<trace>-<span>-01".
func formatTraceparent(traceID, spanID string) string {
	return "00-" + traceID + "-" + spanID + "-01"
}

// parseTraceparent accepts a W3C traceparent and returns (traceID, spanID).
// A malformed or all-zero value is rejected, so the caller mints a fresh trace
// rather than joining every request into one bogus trace id.
func parseTraceparent(v string) (string, string, bool) {
	parts := strings.Split(strings.TrimSpace(v), "-")
	if len(parts) < 4 {
		return "", "", false
	}
	version, traceID, spanID := parts[0], parts[1], parts[2]
	if len(version) != 2 || len(traceID) != 32 || len(spanID) != 16 {
		return "", "", false
	}
	if !isHex(version) || !isHex(traceID) || !isHex(spanID) {
		return "", "", false
	}
	if version == "ff" || isAllZero(traceID) || isAllZero(spanID) {
		return "", "", false
	}
	return strings.ToLower(traceID), strings.ToLower(spanID), true
}

func isHex(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil
}

func isAllZero(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}

// newTraceID mints a 32-hex trace id.
func newTraceID() string { return randomHex(16) }

// newSpanID mints a 16-hex span id. One per record: every client attempt gets
// its own, so a retry is a distinct span of the same trace.
func newSpanID() string { return randomHex(8) }

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := crand.Read(b); err != nil {
		// crypto/rand cannot fail on any platform this runs on; if it somehow
		// does, an unusable id is worse than a panic at the call site.
		panic("rpcpolicy: crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// randomSeed produces a process-unique seed for the jitter RNG.
func randomSeed() int64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		panic("rpcpolicy: crypto/rand: " + err.Error())
	}
	return int64(binary.LittleEndian.Uint64(b[:]) >> 1)
}

// traceFromMetadata reads the inbound gRPC metadata. A request that arrives
// without a usable traceparent starts a fresh trace with no parent, which is
// what CONTRACTS.md §5 spells as parent_span_id "".
func traceFromMetadata(ctx context.Context) (traceID, parentSpanID, route string) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if vs := md.Get(TraceparentKey); len(vs) > 0 {
			if t, s, ok := parseTraceparent(vs[0]); ok {
				traceID, parentSpanID = t, s
			}
		}
		if vs := md.Get(RouteKey); len(vs) > 0 {
			route = vs[0]
		}
	}
	if traceID == "" {
		traceID = newTraceID()
	}
	return traceID, parentSpanID, route
}
