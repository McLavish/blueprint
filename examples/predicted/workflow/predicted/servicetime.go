package predicted

import (
	"context"
	"math"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc/status"
)

// Service-time emulation: the deployment counterpart of msim's
// `service.py` draw, `rng.lognormvariate(log(median), sigma)` floored at
// 100 microseconds.
//
// Sleep only (PLAN.md D16). The draw is independent of load, so the deployed
// service is M/G/c/K by construction and the calibration of run K4/K8 must
// recover the configured median, sigma, workers and queue capacity.

// serviceTime burns one node's own service time.
type serviceTime struct {
	medianMS float64
	sigma    float64
	floor    time.Duration
}

func newServiceTime(cfg *NodeConfig) *serviceTime {
	sigma := cfg.Service.Sigma
	if sigma < MinSigma {
		sigma = MinSigma
	}
	return &serviceTime{
		medianMS: cfg.Service.MedianMS,
		sigma:    sigma,
		floor:    time.Duration(cfg.Service.FloorUS * float64(time.Microsecond)),
	}
}

// draw returns one lognormal sample, floored:
//
//	max(floor, exp(ln(median) + sigma * N(0,1)))
//
// math/rand/v2's top-level source is goroutine-safe and unseeded (each process
// gets its own stream), which is what CONTRACTS.md §3 asks for: the recording
// is compared against the simulator in distribution, never sample by sample, so
// a shared seed across the two sides would buy nothing and a shared seed across
// the containers of one system would be actively misleading.
func (s *serviceTime) draw() time.Duration {
	ms := math.Exp(math.Log(s.medianMS) + s.sigma*rand.NormFloat64())
	d := time.Duration(ms * float64(time.Millisecond))
	if d < s.floor {
		return s.floor
	}
	return d
}

// burn sleeps one draw, honouring the caller's deadline.
//
// A cancelled or expired context returns the gRPC status of the context error
// (DeadlineExceeded / Canceled) rather than the bare context error, so the
// interceptor's server record and the caller's client record book the same
// response_code for the same event (CONTRACTS.md §5).
func (s *serviceTime) burn(ctx context.Context) error {
	return sleepCtx(ctx, s.draw())
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}
