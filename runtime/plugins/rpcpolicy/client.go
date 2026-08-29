package rpcpolicy

import (
	"context"
	"time"

	"google.golang.org/grpc"
)

// The client half of the public API (CONTRACTS.md §6) and the unary client
// interceptor that runs the attempt loop.

// DefaultCallTimeout is the per-call timeout upstream Blueprint hard-codes in
// plugins/grpc/grpccodegen/clientgen.go. It is used only when this package is
// inert; when a policy file is bound, the profile's `timeout` bounds each
// attempt and `global_timeout` bounds the root.
const DefaultCallTimeout = time.Second

// DialOptions returns the dial options generated clients must install. nil when
// inert, so an upstream example dials exactly as before.
func DialOptions() []grpc.DialOption {
	st := currentState()
	if st.inert {
		return nil
	}
	return []grpc.DialOption{grpc.WithUnaryInterceptor(st.unaryClientInterceptor())}
}

// CallContext bounds one generated client call.
//
// Inert: context.WithTimeout(ctx, fallback), reproducing upstream's per-call
// 1 s. Active: the context is returned unchanged with a no-op cancel, because
// the attempt loop owns every deadline -- a caller-side timeout wrapped around
// the loop would cut the retries the profile asked for.
func CallContext(ctx context.Context, fallback time.Duration) (context.Context, context.CancelFunc) {
	st := currentState()
	if st.inert {
		return context.WithTimeout(ctx, fallback)
	}
	return ctx, func() {}
}

func (s *runtimeState) unaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		reg := s.registry.Load()
		tc, _ := traceCtxFrom(ctx)
		prof := reg.Lookup(tc.route, method)
		if prof == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		return s.engine.execute(ctx, prof, tc.route, method, func(attemptCtx context.Context) error {
			return invoker(attemptCtx, method, req, reply, cc, opts...)
		})
	}
}
