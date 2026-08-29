package rpcpolicy

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/status"
)

// Fault injection (docs/CONTRACTS.md §4), ported from
// hotelreservation-experiments/faultinject/faultinject.go. The one behavioural
// change is where it sits: INSIDE the admission permit, so injected latency
// occupies a worker exactly as real service time does -- which is what makes
// the deployed station and msim's station the same queue.

// FaultSchedule is the whole file.
type FaultSchedule struct {
	Rules []FaultRule `yaml:"rules"`
}

// FaultRule is one time-windowed fault bound to an exact gRPC full method.
// Every field is optional and a missing key is a legal zero.
//
// AddLatencyMS is float64 rather than an integer: yaml.v3 truncates a
// fractional scalar into an integer field without reporting an error, so an
// integer field would silently drop sub-millisecond faults.
type FaultRule struct {
	Method       string  `yaml:"method"`
	StartS       float64 `yaml:"start_s"`
	EndS         float64 `yaml:"end_s"`
	AddLatencyMS float64 `yaml:"add_latency_ms"`
	PFail        float64 `yaml:"p_fail"`
}

// LoadFaults reads and validates a fault schedule.
func LoadFaults(path string) (*FaultSchedule, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var schedule FaultSchedule
	if err := decodeStrict(data, &schedule); err != nil {
		return nil, fmt.Errorf("faultinject: %s: %w", path, err)
	}
	if err := schedule.validate(); err != nil {
		return nil, err
	}
	return &schedule, nil
}

func (s *FaultSchedule) validate() error {
	for i, rule := range s.Rules {
		// A non-finite scalar compares false against every bound below, so the
		// range checks are no filter for one: YAML spells .nan/.inf, a .nan
		// p_fail is silently inert, and a non-finite bound is worse than inert
		// -- float64 -> time.Duration conversion is undefined for it, so the
		// window would get an arbitrary anchor instead of an obvious failure.
		for _, field := range []struct {
			name  string
			value float64
		}{
			{"start_s", rule.StartS},
			{"end_s", rule.EndS},
			{"add_latency_ms", rule.AddLatencyMS},
			{"p_fail", rule.PFail},
		} {
			if math.IsNaN(field.value) || math.IsInf(field.value, 0) {
				return fmt.Errorf("faultinject: rule %d: %s %v is not a finite number", i, field.name, field.value)
			}
		}
		switch {
		case rule.PFail < 0 || rule.PFail > 1:
			return fmt.Errorf("faultinject: rule %d: p_fail %v is outside [0,1]", i, rule.PFail)
		case rule.EndS < rule.StartS:
			return fmt.Errorf("faultinject: rule %d: end_s %v is before start_s %v", i, rule.EndS, rule.StartS)
		case rule.AddLatencyMS < 0:
			return fmt.Errorf("faultinject: rule %d: add_latency_ms %v is negative", i, rule.AddLatencyMS)
		}
		// A finite-but-astronomical bound still has no time.Duration: the
		// float64 -> int64 conversion of an out-of-range value is undefined in
		// Go, so the window would get an arbitrary anchor. Reject it here
		// instead, where the file name is still in hand.
		for _, field := range []struct {
			name string
			ok   bool
		}{
			{"start_s", secondsFits(rule.StartS)},
			{"end_s", secondsFits(rule.EndS)},
			{"add_latency_ms", millisFits(rule.AddLatencyMS)},
		} {
			if !field.ok {
				return fmt.Errorf("faultinject: rule %d: %s does not fit a time.Duration", i, field.name)
			}
		}
	}
	return nil
}

// maxDurationNS is 2^63 as a float64: the first value a time.Duration cannot
// hold. float64(math.MaxInt64) rounds UP to exactly this, so it is also the
// only correct bound to compare a rounded float against.
const maxDurationNS = 9223372036854775808.0

// roundToDuration is msim's core.s_to_ns / ms_to_ns: int(round(value * unit)).
// Python's round() breaks ties to EVEN, which math.RoundToEven reproduces and
// math.Round (ties away from zero) does not -- they disagree on exactly the
// half-nanosecond inputs a hand-written schedule is most likely to contain.
func roundToDuration(value, unit float64) (time.Duration, bool) {
	ns := math.RoundToEven(value * unit)
	if math.IsNaN(ns) || ns >= maxDurationNS || ns < -maxDurationNS {
		return 0, false
	}
	return time.Duration(ns), true
}

func secondsToDuration(s float64) (time.Duration, bool) {
	return roundToDuration(s, float64(time.Second))
}

func millisToDuration(ms float64) (time.Duration, bool) {
	return roundToDuration(ms, float64(time.Millisecond))
}

func secondsFits(s float64) bool { _, ok := secondsToDuration(s); return ok }

func millisFits(ms float64) bool { _, ok := millisToDuration(ms); return ok }

// faultInjector answers "what is being injected right now" for a method.
// now and roll are seams for tests; nil means time.Now and a private,
// mutex-guarded rand.
type faultInjector struct {
	schedule *FaultSchedule
	epochMs  int64
	now      func() time.Time

	mu   sync.Mutex
	rand *rand.Rand
	roll func() float64
}

func newFaultInjector(schedule *FaultSchedule, epochMs int64, now func() time.Time, roll func() float64) *faultInjector {
	f := &faultInjector{schedule: schedule, epochMs: epochMs, now: now, roll: roll}
	if f.now == nil {
		f.now = time.Now
	}
	if f.roll == nil {
		f.rand = rand.New(rand.NewSource(randomSeed()))
	}
	return f
}

func (f *faultInjector) nextRoll() float64 {
	if f.roll != nil {
		return f.roll()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rand.Float64()
}

// active returns (added latency, p_fail) for one method at the current instant.
func (f *faultInjector) active(method string) (float64, float64) {
	if f == nil {
		return 0, 0
	}
	return activeFault(f.schedule, f.epochMs, f.now(), method)
}

// activeLatency is the additive latency for a method at the current instant.
// It is read ONCE, at admission, because that latency is what occupies the
// permit.
func (f *faultInjector) activeLatency(method string) float64 {
	latency, _ := f.active(method)
	return latency
}

// activePFail is the active p_fail for a method at the current instant: the max
// over the live windows, re-read immediately before the roll. msim's
// _finish_service evaluates _fails_now(now) at the COMPLETION instant
// (service.py:435 -> :216), so a handler that runs into or out of a short
// window is governed by the probability in force when it finishes, not by the
// one in force when it started.
func (f *faultInjector) activePFail(method string) float64 {
	_, pFail := f.active(method)
	return pFail
}

// activeFault composes the rules that name `method` and are live at `now`.
func activeFault(schedule *FaultSchedule, epochMs int64, now time.Time, method string) (float64, float64) {
	if schedule == nil {
		return 0, 0
	}
	// Nanoseconds since the shared run epoch. msim's fault intervals are ns
	// ints (core.s_to_ns = int(round(s*1e9)), TimeInterval.contains), so the
	// bounds round the same way here. Truncating both bounds to whole
	// milliseconds made any window that began and ended inside one millisecond
	// inert. The epoch itself is only millisecond-resolution (FAULT_EPOCH_MS),
	// so this makes window WIDTHS exact, not the absolute anchor.
	elapsed := now.Sub(time.UnixMilli(epochMs))
	var addLatencyMS float64
	var pFail float64
	for _, rule := range schedule.Rules {
		if rule.Method != method {
			continue
		}
		start, startOK := secondsToDuration(rule.StartS)
		end, endOK := secondsToDuration(rule.EndS)
		if !startOK || !endOK {
			// LoadFaults rejects these; a schedule built in code cannot be
			// trusted to have gone through it, and an unrepresentable window is
			// better skipped than anchored arbitrarily.
			continue
		}
		// The window is half-open, [start_s, end_s), and the Python pipeline
		// depends on it: it selects in-window records with lo <= start_epoch < hi.
		if elapsed < start || elapsed >= end {
			continue
		}
		// Overlapping rules compose the way msim composes overlapping
		// injections (service.py): latencies SUM in _adjust_latency, p_fail
		// takes the MAX in _fails_now.
		addLatencyMS += rule.AddLatencyMS
		if rule.PFail > pFail {
			pFail = rule.PFail
		}
	}
	return addLatencyMS, pFail
}

// sleepCtx waits out d, or gives up as soon as the caller does. The context
// error is returned as a status so it keeps its Canceled/DeadlineExceeded code.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

// faultInjectorFromEnv builds the injector from FAULT_CONFIG_PATH +
// FAULT_EPOCH_MS.
//
// Both unset (or both empty) means fault injection is intentionally off;
// anything else -- one of the pair missing, or a set-but-invalid value --
// panics, so a misconfigured experiment fails at startup instead of silently
// recording a fault-free run as a faulted one.
func faultInjectorFromEnv() *faultInjector {
	path := os.Getenv("FAULT_CONFIG_PATH")
	epoch := os.Getenv("FAULT_EPOCH_MS")
	switch {
	case path == "" && epoch == "":
		return newFaultInjector(nil, 0, nil, nil)
	case path == "":
		panic("rpcpolicy/faultinject: FAULT_EPOCH_MS is set but FAULT_CONFIG_PATH is empty; set both to arm fault injection, neither to disable it")
	case epoch == "":
		panic("rpcpolicy/faultinject: FAULT_CONFIG_PATH is set but FAULT_EPOCH_MS is empty; set both to arm fault injection, neither to disable it")
	}

	epochMs, err := strconv.ParseInt(epoch, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("rpcpolicy/faultinject: FAULT_EPOCH_MS %q is not an integer: %v", epoch, err))
	}
	schedule, err := LoadFaults(path)
	if err != nil {
		panic(fmt.Sprintf("rpcpolicy/faultinject: cannot load FAULT_CONFIG_PATH %q: %v", path, err))
	}
	return newFaultInjector(schedule, epochMs, nil, nil)
}
