package predicted

import (
	"context"
	"math"
	"sort"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mustConfig(t *testing.T, median, sigma, floorUS float64) *NodeConfig {
	t.Helper()
	cfg := &NodeConfig{Service: ServiceConfig{
		Name: "test", MedianMS: median, Sigma: sigma,
		Mode: ModeSleep, FloorUS: floorUS, Fanout: FanoutSerial,
	}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("test config invalid: %v", err)
	}
	return cfg
}

// The floor is msim's: `service.py` floors every draw at 100 microseconds, so a
// tiny median cannot produce a zero-length service time and a station with a
// zero-length service never queues.
func TestDrawIsFloored(t *testing.T) {
	// A 1 microsecond median under a 100 microsecond floor: every draw must
	// land exactly on the floor, not merely above zero.
	st := newServiceTime(mustConfig(t, 0.001, 0.5, 100))
	floor := 100 * time.Microsecond
	for i := 0; i < 2000; i++ {
		if d := st.draw(); d < floor {
			t.Fatalf("draw %v is below the %v floor", d, floor)
		}
	}

	// A zero floor is legal and then the only bound is the lognormal's own
	// support, which is strictly positive.
	st = newServiceTime(mustConfig(t, 20, 0.5, 0))
	for i := 0; i < 200; i++ {
		if d := st.draw(); d <= 0 {
			t.Fatalf("draw %v is not positive", d)
		}
	}
}

// Distribution sanity, not a goodness-of-fit test: the sample median must sit on
// the configured median (that is what the lognormal median parameter means) and
// a larger sigma must widen the spread. If this drifts, the K4/K8 calibration is
// fitting a distribution the node is not drawing.
func TestDrawDistribution(t *testing.T) {
	const n = 20001
	medianOf := func(st *serviceTime) float64 {
		xs := make([]float64, n)
		for i := range xs {
			xs[i] = float64(st.draw()) / float64(time.Millisecond)
		}
		sort.Float64s(xs)
		return xs[n/2]
	}

	got := medianOf(newServiceTime(mustConfig(t, 20, 0.5, 100)))
	if math.Abs(got-20)/20 > 0.05 {
		t.Errorf("sample median %.3f ms is more than 5%% from the configured 20 ms", got)
	}

	// sigma = 0 (floored at MinSigma) is effectively deterministic.
	stDet := newServiceTime(mustConfig(t, 20, 0, 0))
	for i := 0; i < 200; i++ {
		ms := float64(stDet.draw()) / float64(time.Millisecond)
		if math.Abs(ms-20) > 0.01 {
			t.Fatalf("sigma=0 draw %.6f ms is not 20 ms", ms)
		}
	}

	// The 90th percentile of a lognormal is median*exp(sigma*z_.9),
	// z_.9 = 1.2816: 38.1 ms at sigma 0.5, 20 ms at sigma 0.
	p90 := func(st *serviceTime) float64 {
		xs := make([]float64, n)
		for i := range xs {
			xs[i] = float64(st.draw()) / float64(time.Millisecond)
		}
		sort.Float64s(xs)
		return xs[n*9/10]
	}
	wide := p90(newServiceTime(mustConfig(t, 20, 0.5, 100)))
	want := 20 * math.Exp(0.5*1.2815515655446004)
	if math.Abs(wide-want)/want > 0.05 {
		t.Errorf("sample p90 %.3f ms is more than 5%% from the analytic %.3f ms", wide, want)
	}
}

// A caller that walks away must not leave a worker sleeping out the rest of a
// draw, and the error it gets back must be the gRPC status the interceptor's
// server record and the caller's client record will both book.
func TestBurnHonoursCancellation(t *testing.T) {
	st := newServiceTime(mustConfig(t, 10000, 0.0001, 100)) // ~10 s draws

	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(20 * time.Millisecond)
			cancel()
		}()
		start := time.Now()
		err := st.burn(ctx)
		elapsed := time.Since(start)
		if err == nil {
			t.Fatal("burn returned nil after the context was cancelled")
		}
		if got := status.Code(err); got != codes.Canceled {
			t.Errorf("status code = %v, want Canceled (err %v)", got, err)
		}
		if elapsed > 2*time.Second {
			t.Errorf("burn slept %v after cancellation; it should return promptly", elapsed)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := st.burn(ctx)
		elapsed := time.Since(start)
		if got := status.Code(err); got != codes.DeadlineExceeded {
			t.Errorf("status code = %v, want DeadlineExceeded (err %v)", got, err)
		}
		if elapsed > 2*time.Second {
			t.Errorf("burn slept %v past its deadline", elapsed)
		}
	})

	t.Run("already dead on entry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		// A short draw, so a burn that ignored the dead context would still
		// return nil quickly: this asserts the check, not the timing.
		short := newServiceTime(mustConfig(t, 0.1, 0.0001, 0))
		if err := short.burn(ctx); status.Code(err) != codes.Canceled {
			t.Errorf("status code = %v, want Canceled (err %v)", status.Code(err), err)
		}
	})
}

func TestBurnCompletes(t *testing.T) {
	st := newServiceTime(mustConfig(t, 5, 0.0001, 100))
	start := time.Now()
	if err := st.burn(context.Background()); err != nil {
		t.Fatalf("burn returned %v", err)
	}
	if elapsed := time.Since(start); elapsed < 4*time.Millisecond {
		t.Errorf("burn returned after %v; a 5 ms draw should take about 5 ms", elapsed)
	}
}
