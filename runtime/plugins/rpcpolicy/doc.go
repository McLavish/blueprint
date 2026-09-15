// Package rpcpolicy carries every client- and server-side resilience mechanism
// of the retry-pathology campaign in one gRPC/HTTP interceptor bundle.
//
// The reference semantics for every mechanism is the discrete-event simulator
// msim (sp_microservices_simulator/src/msim): caller.py for the client attempt
// loop, retry_policies.py for the ten policy classes, service.py for the
// admission station and its drop points. Where a Go idiom and msim
// disagree, msim wins: the campaign's whole point is that a matched simulation
// and a deployed system produce the same numbers.
//
// Three things msim knows exactly have no direct equivalent on a real server,
// and are reconstructed rather than observed:
//
//   - A drop's reason. msim fires DEADLINE and CANCELLED as distinct events;
//     gRPC collapses them, because a caller whose deadline fires cancels the
//     stream and the server's rebuilt deadline fires at about the same instant.
//     deadlineCause (admission.go) decides by the INSTANT, not by ctx.Err().
//
//   - A drop's time. msim books an expiry at the deadline, a hand-off at the
//     instant a worker frees, an early hand-back where the relay handed it back,
//     and an in-service cut at the deadline; a goroutine learns of any of them
//     only when it is next scheduled. So the station keeps msim's event heap
//     itself, ordered the same way, by (time, seq):
//
//     A queued waiter's expiry carries the seq of its ENQUEUE (msim schedules
//     expire_in_queue there) and everything a permit does -- its hand-back, its
//     terminal claim, the station's reap of it at its deadline -- carries the
//     seq of the permit's MINT (msim schedules the finish event inside
//     _begin_service). station.settle(now, seq) replays every event due at or
//     before that key, one at a time and re-scanning, because applying one can
//     mint a successor permit whose own events may be due; only then does the
//     caller's own mutation run. An arrival takes a fresh seq, so everything
//     already standing at its instant fires before it; a permit claiming its own
//     outcome passes its own, so events scheduled before it entered service
//     still fire first. That is what makes a worker freeing at exactly a
//     waiter's deadline the dequeue tie in one order and a plain expiry in the
//     other, just as msim's heap does.
//
//     Only the two DEADLINES are ever found PENDING by a settle, because a
//     deadline is the one appointment nobody has to be running to keep.
//     Everything else an attempt does to the station it does by taking the
//     station mutex, and it is applied inside that same critical section, at the
//     instant it names -- there is no lock-free side channel by which the
//     station can learn of an outcome, and therefore no window in which two
//     goroutines hold two different truths about one attempt.
//
//     The one rule that is not msim's is NO REWRITING COMMITTED TIME: the
//     station remembers the latest instant it has already decided things for,
//     and an event that reaches it later than that is applied THERE rather than
//     backdated. msim would have run the event first and decided that interval
//     differently; since the station cannot un-admit a request it has already
//     admitted, the closest honest replay is "as early as we still can". The
//     record still carries the true instant wherever its own field says so (a
//     completion ends where the handler returned, an expiry at the deadline);
//     it is permit_released_at, and any hand-off the event causes, that land at
//     the clamped instant -- which is also what keeps a hand-off from ever
//     preceding the arrival of the waiter it serves. Committed time moves only
//     when an event really is applied.
//
//     What none of this recovers is the PUBLICATION WINDOW: between a handler
//     really returning and its goroutine stamping the clock and reaching the
//     station mutex, no other goroutine can know that worker is free. The
//     instant the record carries is still the stamped one, and the claim is
//     still taken in msim's order relative to everything the station knows
//     about; what a late stamp costs is the capacity decisions other arrivals
//     made in between, which the station will not rewrite.
//
//   - Which goroutine gets to say how an attempt ended. The handler's goroutine,
//     the interceptor's ctx.Done(), and the station's reaper all race for it, so
//     each permit carries a once-only claim {kind, at}, taken and applied under
//     the station mutex in one step: the winner names the outcome and the
//     instant, the interceptor writes the one record FROM THAT CLAIM -- never
//     from its own view of the handler's result -- and the losers write nothing,
//     roll nothing and feed nothing back. The worker's claim is also CLASSIFIED
//     at the instant it names: it captures the context's state in the same
//     observation as its completion stamp, so work that finished inside its
//     deadline under a live context stays a completion however long its
//     goroutine is descheduled before it can claim. Only work that stopped
//     BECAUSE its own context ended is the in-service cut -- a downstream's own
//     DeadlineExceeded, returned under a parent that expires a millisecond
//     later, is a completion carrying that code. A handler result that arrives
//     after a
//     committed cut is not a second truth about the attempt; it is the result of
//     work nobody was still waiting for. The early hand-back is NOT one of those
//     outcomes: it is a different fact about a different thing (the WORKER went
//     back, while the attempt runs on), kept as separate immutable history on
//     the permit that no terminal claim ever supersedes -- so a cut landing
//     afterwards censors an occupancy that had already ended completely.
//
// The same principle places the CLIENT record: when an attempt's own deadline
// is what ended it -- its per-attempt timeout or the root deadline it inherited,
// whichever was nearer -- the record ends AT that deadline (clampToDeadline),
// because msim's caller clock fires there and the pipeline buckets every
// caller-side column at the record's end. A late wake-up would otherwise move a
// timeout into the next bucket. The retry engine still DECIDES at wake-up, which
// nothing can change; it is only the record that is placed. A DeadlineExceeded
// the callee produced while the local context was still live is a real
// observation at a real instant and is left where it landed, as is every other
// status. The root record follows the same rule when a root deadline exists.
//
// That the deadline passed is asserted by EITHER the strict finish rule having
// converted the attempt's own OK (end >= deadline on the one clock both sides
// compare against) or the attempt context reporting DeadlineExceeded -- the
// first because the context's timer callback may not have run yet when a result
// arrives microseconds past the deadline, the second because a result can arrive
// with no local comparison to make. The cost of accepting either is one
// knowingly over-clamped case: a callee's DeadlineExceeded that arrived before
// the local deadline but was not read until after it ends at the deadline
// instead, which is the nearer of the two instants still available once the
// goroutine has slept past its own.
//
// The attempt log carries one further record the simulator has no need of. An
// expired queue entry keeps its slot until the dispatcher reaches it, and its
// own record was written at its deadline, so nothing in the log would say when
// the slot really came back; a `discard` event names the attempt and that
// instant, which is what lets the pipeline reconstruct queued(t) exactly.
//
// # Where this package is knowingly not msim
//
// Two differences are structural rather than bugs, and the campaign is designed
// around them:
//
//   - THE FAULT ROLL IS AT HANDLER COMPLETION. D14 puts the roll after the
//     service time, at the instant the handler returned (server.go). msim rolls
//     after the node's LOCAL service and BEFORE it fans out (chain.py,
//     _on_local_done: _fails_now, then _launch_fanout). For a faulted node whose
//     handler calls downstream, this package therefore rolls after those calls
//     returned and msim rolls before they were made. The campaign's faulted
//     services are leaves -- Single's svc_a, Multichain's svc_c -- or nodes
//     whose local work IS their whole handler (hotel's profile), and there the
//     two instants coincide. A faulted RELAY with rpcpolicy children is outside
//     this package's exactness claim: the roll would see a later fault window,
//     and a failure would be preceded by downstream work msim never did.
//
//   - EQUAL-INSTANT ORDER FOR A PERMIT HELD THROUGH FAN-OUT. Every event a
//     permit owns carries the seq of its MINT, msim's _begin_service. For the
//     held-permit arm (hold_permit_through_fanout), msim schedules the parent's
//     in-service deadline guard later -- when the fan-out BEGINS (chain.py,
//     _launch_fanout) -- so at an exactly equal timestamp that guard sorts AFTER
//     the expiry of a waiter enqueued in between, while this station's reap,
//     carrying the mint seq, sorts before it. The tie is then labelled
//     deadline_at_dequeue here and deadline_in_queue there, with one worker's
//     difference in that record's busy snapshot. Two events landing on the same
//     wall-clock nanosecond is a measure-zero coincidence in a deployment, so
//     the campaign accepts the divergence rather than tracking a second seq.
//
// Time inside the policy layer is int64 nanoseconds measured from a monotonic
// base captured at process start, exactly as msim's integer time model. Wall
// clock is used only for the epochs written into the attempt log.
//
// The package is INERT unless RPCPOLICY_CONFIG names a policy file: every
// public entry point then degrades to the upstream Blueprint behaviour
// (nil interceptor options, an identity HTTP middleware, and a per-call
// context.WithTimeout of the caller-supplied fallback). A configuration that
// exists but cannot be loaded panics at startup rather than silently recording
// a run of a policy nobody configured.
//
// Public API (docs/CONTRACTS.md §6):
//
//	ServerOptions() []grpc.ServerOption
//	DialOptions() []grpc.DialOption
//	CallContext(ctx, fallback) (context.Context, context.CancelFunc)
//	DefaultCallTimeout
//	HTTPMiddleware() func(http.Handler) http.Handler
//	ReleasePermit(ctx)
package rpcpolicy
