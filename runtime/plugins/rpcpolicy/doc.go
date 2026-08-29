// Package rpcpolicy carries every client- and server-side resilience mechanism
// of the retry-pathology campaign in one gRPC/HTTP interceptor bundle.
//
// The reference semantics for every mechanism is the discrete-event simulator
// msim (sp_microservices_simulator/src/msim): caller.py for the client attempt
// loop, retry_policies.py for the ten policy classes, service.py for the
// admission station and its four drop points. Where a Go idiom and msim
// disagree, msim wins: the campaign's whole point is that a matched simulation
// and a deployed system produce the same numbers.
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
