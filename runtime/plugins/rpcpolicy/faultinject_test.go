package rpcpolicy

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func scheduleOf(rules ...FaultRule) *FaultSchedule { return &FaultSchedule{Rules: rules} }

func epochNow() (int64, time.Time) {
	now := time.Now()
	return now.UnixMilli(), now
}

// D14: inside the permit the order is injected latency -> handler -> failure
// roll, so the failure lands AFTER the service time was spent, exactly as
// msim's station rolls at completion.
func TestFaultOrderIsLatencyThenHandlerThenRoll(t *testing.T) {
	epochMs, _ := epochNow()
	s := newTestStateWithFaults(t,
		"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 2\n  queue_capacity: 4\n",
		scheduleOf(FaultRule{Method: testMethod, StartS: 0, EndS: 3600, AddLatencyMS: 40, PFail: 1.0}),
		epochMs, func() float64 { return 0.0 })

	handlerRan := false
	var handlerAt time.Duration
	start := time.Now()
	_, err := s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		handlerRan = true
		handlerAt = time.Since(start)
		return "ok", nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Equal(t, "injected fault", status.Convert(err).Message())
	assert.True(t, handlerRan, "the roll happens after the handler, not instead of it")
	assert.GreaterOrEqual(t, handlerAt, 40*time.Millisecond, "the injected latency is spent before the handler")

	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, true, recs[0]["fault_hit"])
	assert.GreaterOrEqual(t, recs[0]["injected_ms"], 40.0)
	assert.Greater(t, recs[0]["handler_ms"], -1.0)
	assert.Equal(t, outcomeAdmitted, recs[0]["admission_outcome"])
}

// A deadline expiring during the injected sleep discards the handler entirely.
func TestFaultLatencyAbortsWhenTheDeadlineExpires(t *testing.T) {
	epochMs, _ := epochNow()
	s := newTestStateWithFaults(t,
		"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 2\n  queue_capacity: 4\n",
		scheduleOf(FaultRule{Method: testMethod, StartS: 0, EndS: 3600, AddLatencyMS: 500}),
		epochMs, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ran := false
	_, err := s.serve(ctx, nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		ran = true
		return "ok", nil
	})
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	assert.False(t, ran, "the handler result is discarded when the deadline expires during the sleep")
	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assert.Equal(t, 0.0, recs[0]["handler_ms"])
	assert.Greater(t, recs[0]["permit_released_at"], 0.0)
	assert.Equal(t, outcomeDeadlineAtFinish, recs[0]["admission_outcome"],
		"expiring in service is the same class of outcome whether it happens in the injected sleep or the handler")
	assert.GreaterOrEqual(t, recs[0]["injected_ms"], 15.0, "the worker was held for the part of the sleep that ran")
}

// The window is half-open [start_s, end_s), matching msim's TimeInterval and
// the Python pipeline's `lo <= start_epoch < hi`.
func TestActiveFaultWindowIsHalfOpen(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	epochMs := base.UnixMilli()
	sched := scheduleOf(FaultRule{Method: testMethod, StartS: 10, EndS: 30, AddLatencyMS: 5, PFail: 0.25})

	for _, tc := range []struct {
		at      time.Duration
		latency float64
		pfail   float64
	}{
		{9999 * time.Millisecond, 0, 0},
		{10 * time.Second, 5, 0.25},
		{29999 * time.Millisecond, 5, 0.25},
		{30 * time.Second, 0, 0}, // the closed end is outside
		{31 * time.Second, 0, 0},
	} {
		lat, pf := activeFault(sched, epochMs, base.Add(tc.at), testMethod)
		assert.Equal(t, tc.latency, lat, "at %v", tc.at)
		assert.Equal(t, tc.pfail, pf, "at %v", tc.at)
	}
}

// Sub-millisecond and millisecond-aligned windows must not be truncated away.
func TestActiveFaultSubMillisecondWindowFires(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	sched := scheduleOf(FaultRule{Method: testMethod, StartS: 1.0001, EndS: 1.0009, PFail: 1})
	lat, pf := activeFault(sched, base.UnixMilli(), base.Add(1000500*time.Microsecond), testMethod)
	assert.Equal(t, 0.0, lat)
	assert.Equal(t, 1.0, pf)

	aligned := scheduleOf(FaultRule{Method: testMethod, StartS: 1.0, EndS: 1.001, PFail: 1})
	_, pf = activeFault(aligned, base.UnixMilli(), base.Add(1000*time.Millisecond+500*time.Microsecond), testMethod)
	assert.Equal(t, 1.0, pf)

	zeroWidth := scheduleOf(FaultRule{Method: testMethod, StartS: 1.0, EndS: 1.0, PFail: 1})
	_, pf = activeFault(zeroWidth, base.UnixMilli(), base.Add(time.Second), testMethod)
	assert.Equal(t, 0.0, pf, "a zero-width window never fires")
}

// Overlapping rules compose the way msim's service.py composes overlapping
// injections: latencies SUM, p_fail takes the MAX.
func TestActiveFaultComposesOverlappingRules(t *testing.T) {
	base := time.UnixMilli(1_700_000_000_000)
	sched := scheduleOf(
		FaultRule{Method: testMethod, StartS: 0, EndS: 100, AddLatencyMS: 10, PFail: 0.4},
		FaultRule{Method: testMethod, StartS: 50, EndS: 150, AddLatencyMS: 2.5, PFail: 0.65},
		FaultRule{Method: "/grpc.Other/M", StartS: 0, EndS: 1000, AddLatencyMS: 999, PFail: 1},
	)
	lat, pf := activeFault(sched, base.UnixMilli(), base.Add(60*time.Second), testMethod)
	assert.Equal(t, 12.5, lat)
	assert.Equal(t, 0.65, pf)

	// Only the rules naming this method apply.
	lat, pf = activeFault(sched, base.UnixMilli(), base.Add(120*time.Second), testMethod)
	assert.Equal(t, 2.5, lat)
	assert.Equal(t, 0.65, pf)

	// A nil schedule is inert.
	lat, pf = activeFault(nil, base.UnixMilli(), base, testMethod)
	assert.Equal(t, 0.0, lat)
	assert.Equal(t, 0.0, pf)
}

func TestLoadFaultsValidation(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
		return p
	}

	_, err := LoadFaults(write("unknown.yaml", "rules:\n  - method: m\n    p_fial: 1\n"))
	assert.ErrorContains(t, err, "field")

	_, err = LoadFaults(write("two.yaml", "rules: []\n---\nrules: []\n"))
	assert.ErrorContains(t, err, "single YAML document")

	_, err = LoadFaults(write("nan.yaml", "rules:\n  - method: m\n    p_fail: .nan\n"))
	assert.ErrorContains(t, err, "not a finite number")

	_, err = LoadFaults(write("range.yaml", "rules:\n  - method: m\n    p_fail: 1.5\n"))
	assert.ErrorContains(t, err, "outside [0,1]")

	_, err = LoadFaults(write("order.yaml", "rules:\n  - method: m\n    start_s: 5\n    end_s: 4\n"))
	assert.ErrorContains(t, err, "before start_s")

	_, err = LoadFaults(write("neg.yaml", "rules:\n  - method: m\n    add_latency_ms: -1\n"))
	assert.ErrorContains(t, err, "negative")

	empty, err := LoadFaults(write("empty.yaml", "rules: []\n"))
	require.NoError(t, err)
	assert.Empty(t, empty.Rules)

	// Fractional milliseconds survive: an int field would truncate them away.
	frac, err := LoadFaults(write("frac.yaml", "rules:\n  - method: m\n    start_s: 0.5\n    end_s: 1.25\n    add_latency_ms: 0.125\n    p_fail: 0.65\n"))
	require.NoError(t, err)
	require.Len(t, frac.Rules, 1)
	assert.Equal(t, 0.125, frac.Rules[0].AddLatencyMS)
	assert.Equal(t, 0.65, frac.Rules[0].PFail)
	assert.Equal(t, 1.25, frac.Rules[0].EndS)

	_, err = LoadFaults(filepath.Join(dir, "missing.yaml"))
	assert.Error(t, err)
}

// Both env vars or neither: exactly one is a misconfigured experiment that
// would silently record a fault-free run as a faulted one.
func TestFaultInjectorFromEnvPairing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "faults.yaml")
	require.NoError(t, os.WriteFile(path, []byte("rules: []\n"), 0o644))

	t.Run("neither", func(t *testing.T) {
		t.Setenv("FAULT_CONFIG_PATH", "")
		t.Setenv("FAULT_EPOCH_MS", "")
		f := faultInjectorFromEnv()
		lat, pf := f.active(testMethod)
		assert.Equal(t, 0.0, lat)
		assert.Equal(t, 0.0, pf)
	})
	t.Run("both", func(t *testing.T) {
		t.Setenv("FAULT_CONFIG_PATH", path)
		t.Setenv("FAULT_EPOCH_MS", "1700000000000")
		f := faultInjectorFromEnv()
		require.NotNil(t, f.schedule)
		assert.Equal(t, int64(1700000000000), f.epochMs)
	})
	t.Run("only epoch", func(t *testing.T) {
		t.Setenv("FAULT_CONFIG_PATH", "")
		t.Setenv("FAULT_EPOCH_MS", "1700000000000")
		assert.PanicsWithValue(t,
			"rpcpolicy/faultinject: FAULT_EPOCH_MS is set but FAULT_CONFIG_PATH is empty; set both to arm fault injection, neither to disable it",
			func() { faultInjectorFromEnv() })
	})
	t.Run("only path", func(t *testing.T) {
		t.Setenv("FAULT_CONFIG_PATH", path)
		t.Setenv("FAULT_EPOCH_MS", "")
		assert.PanicsWithValue(t,
			"rpcpolicy/faultinject: FAULT_CONFIG_PATH is set but FAULT_EPOCH_MS is empty; set both to arm fault injection, neither to disable it",
			func() { faultInjectorFromEnv() })
	})
	t.Run("unparsable epoch", func(t *testing.T) {
		t.Setenv("FAULT_CONFIG_PATH", path)
		t.Setenv("FAULT_EPOCH_MS", "soon")
		assert.Panics(t, func() { faultInjectorFromEnv() })
	})
	t.Run("unloadable schedule", func(t *testing.T) {
		bad := filepath.Join(dir, "bad.yaml")
		require.NoError(t, os.WriteFile(bad, []byte("rules:\n  - p_fial: 1\n"), 0o644))
		t.Setenv("FAULT_CONFIG_PATH", bad)
		t.Setenv("FAULT_EPOCH_MS", "1700000000000")
		assert.Panics(t, func() { faultInjectorFromEnv() })
	})
}

// The roll source decides exactly where the fault fires.
func TestFaultFiresExactlyWhereTheRollSourceSaysSo(t *testing.T) {
	epochMs, _ := epochNow()
	rolls := []float64{0.9, 0.1, 0.5, 0.49}
	i := 0
	s := newTestStateWithFaults(t,
		"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 4\n  queue_capacity: 8\n",
		scheduleOf(FaultRule{Method: testMethod, StartS: 0, EndS: 3600, PFail: 0.5}),
		epochMs, func() float64 { r := rolls[i]; i++; return r })

	var codesSeen []codes.Code
	for range rolls {
		_, err := s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
			return "ok", nil
		})
		codesSeen = append(codesSeen, status.Code(err))
	}
	assert.Equal(t, []codes.Code{codes.OK, codes.Unavailable, codes.OK, codes.Unavailable}, codesSeen)

	hits := 0
	for _, r := range s.tlog.ofKind("server") {
		if r["fault_hit"] == true {
			hits++
		}
	}
	assert.Equal(t, 2, hits)
}

// A method the schedule does not name passes straight through.
func TestFaultOnlyAffectsTheMethodTheRuleNames(t *testing.T) {
	epochMs, _ := epochNow()
	s := newTestStateWithFaults(t,
		"default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n",
		scheduleOf(FaultRule{Method: "/grpc.Other/M", StartS: 0, EndS: 3600, PFail: 1}),
		epochMs, func() float64 { return 0 })
	resp, err := s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		return "ok", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "ok", resp)
}
