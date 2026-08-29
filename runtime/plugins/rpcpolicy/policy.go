package rpcpolicy

// The policy interface and the four chain walks the client attempt loop
// performs, ported from msim's retry_policies.RetryPolicy and
// caller.ClientRuntime._policy_chain / _allow_policy_request /
// _notify_request_start / _update_policy_results / _rollback_policy.

// RetryContext is msim's RetryContext: the 1-based attempt number of the
// attempt that just finished, and the current time in nanoseconds.
type RetryContext struct {
	// Attempt starts at 1 (the original request is attempt 1).
	Attempt int
	// Now is monotonic nanoseconds; rate limiters need it.
	Now int64
}

// DelayDecision is msim's DelayDecision(allow, delay).
type DelayDecision struct {
	Allow bool
	Delay int64 // nanoseconds
}

// Policy is msim's RetryPolicy. Every method has a default implementation in
// basePolicy, so a new policy only overrides what it actually changes.
type Policy interface {
	// AllowRequest is the admission gate, consulted once per ROOT request on
	// every layer of the chain.
	AllowRequest(now int64) bool
	// NextDelay decides whether the client may retry and after how long. It is
	// side-effecting for every throttling policy: quota is charged only on the
	// path that returns Allow=true.
	NextDelay(ctx RetryContext) DelayDecision
	// OnRequestStart is called once per admitted root request, below the gate.
	OnRequestStart(now int64)
	// Rollback undoes the side effects of the immediately preceding NextDelay.
	Rollback(ctx RetryContext)
	// Underlying is the next layer in, or nil.
	Underlying() Policy
	// SupportsDeferredRollback reports whether Rollback is safe to apply LATER
	// than the NextDelay it undoes. False for every policy in this package, for
	// the reasons msim documents: single-slot undo state, or a capped refilling
	// balance that would MINT quota if a refund arrived after a refill.
	SupportsDeferredRollback() bool
}

// ResultRecorder is msim's `add_result` protocol: the layers (circuit
// breakers) that learn from attempt outcomes.
type ResultRecorder interface {
	AddResult(success bool, now int64)
}

// basePolicy supplies msim's RetryPolicy defaults. Embedders must still define
// NextDelay; there is deliberately no default, so a policy that forgets it
// fails to compile rather than silently never retrying.
type basePolicy struct{}

func (basePolicy) AllowRequest(now int64) bool    { return true }
func (basePolicy) OnRequestStart(now int64)       {}
func (basePolicy) Rollback(ctx RetryContext)      {}
func (basePolicy) Underlying() Policy             { return nil }
func (basePolicy) SupportsDeferredRollback() bool { return false }

// policyChain yields every layer of a wrapping policy, outermost first. One
// walker for all four traversals, as in msim: a wrapper that one traversal
// reached and another missed would silently see inconsistent notifications,
// results or refunds.
func policyChain(p Policy) []Policy {
	var out []Policy
	for cur := p; cur != nil; cur = cur.Underlying() {
		out = append(out, cur)
	}
	return out
}

// allowPolicyRequest requires EVERY layer to allow the request.
//
// Short-circuiting is load-bearing, not an optimisation: msim uses
// `all(layer.allow_request(now) for layer in chain)`, whose generator stops at
// the first False, and a circuit breaker's allow_request has the
// OPEN -> HALF_OPEN side effect. Evaluating inner layers after an outer denial
// would flip an inner breaker into its probe stage on a request that never ran.
func allowPolicyRequest(p Policy, now int64) bool {
	for cur := p; cur != nil; cur = cur.Underlying() {
		if !cur.AllowRequest(now) {
			return false
		}
	}
	return true
}

// notifyRequestStart announces an admitted root request to every layer.
func notifyRequestStart(p Policy, now int64) {
	for cur := p; cur != nil; cur = cur.Underlying() {
		cur.OnRequestStart(now)
	}
}

// updatePolicyResults records an attempt outcome on every layer that tracks
// results.
func updatePolicyResults(p Policy, success bool, now int64) {
	for cur := p; cur != nil; cur = cur.Underlying() {
		if r, ok := cur.(ResultRecorder); ok {
			r.AddResult(success, now)
		}
	}
}

// rollbackPolicy refunds a retry NextDelay authorised but the client never
// issued. Every layer that charged sits on the chain, so every layer gets the
// chance to refund.
//
// deferred=true marks a refund issued long after the charge (an inbound cancel
// arriving mid-backoff). Rollback is specified as undoing the IMMEDIATELY
// preceding NextDelay, so layers that keep single-slot undo state are skipped:
// with several roots sharing one policy a late refund would rewind a different
// root's live reservation. No policy in this package opts in, so a deferred
// rollback refunds nothing at all -- conservative: it over-counts rather than
// handing the same slot out twice.
func rollbackPolicy(p Policy, ctx RetryContext, deferred bool) {
	for cur := p; cur != nil; cur = cur.Underlying() {
		if !deferred || cur.SupportsDeferredRollback() {
			cur.Rollback(ctx)
		}
	}
}

// Compile-time proof that every class of msim/retry_policies.py is present and
// implements the interface. The list is the equivalence gate's inventory: ten
// policy classes, of which four record results or are recorded through.
var (
	_ Policy = (*NoRetryPolicy)(nil)
	_ Policy = (*FixedBackoffRetryPolicy)(nil)
	_ Policy = (*ExponentialBackoffRetryPolicy)(nil)
	_ Policy = (*ExponentialBackoffWithJitterRetryPolicy)(nil)
	_ Policy = (*CountBasedCircuitBreakerPolicy)(nil)
	_ Policy = (*TimeBasedCircuitBreakerPolicy)(nil)
	_ Policy = (*RetryBudgetPolicy)(nil)
	_ Policy = (*LeakyRateLimiterPolicy)(nil)
	_ Policy = (*BurstyRateLimiterPolicy)(nil)
	_ Policy = (*FixedWindowBurstyLimiterPolicy)(nil)

	_ ResultRecorder = (*CountBasedCircuitBreakerPolicy)(nil)
	_ ResultRecorder = (*TimeBasedCircuitBreakerPolicy)(nil)
)
