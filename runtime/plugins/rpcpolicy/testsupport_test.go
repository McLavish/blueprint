package rpcpolicy

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMain keeps a SIGHUP handler registered for the whole binary. The reload
// tests signal the test process itself, and between two runtimeState lifetimes
// there would otherwise be no handler at all -- whereupon SIGHUP's default
// action terminates the test binary.
func TestMain(m *testing.M) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	go func() {
		for range ch {
		}
	}()
	os.Exit(m.Run())
}

// fakeClock drives the policy layer's integer-nanosecond time by hand, so a
// backoff, a deadline and a strict end==deadline boundary are exact instead of
// racing the scheduler.
//
// The wall clock it reports is a real instant plus the accumulated virtual
// offset, which keeps epochs in the attempt log monotone and plausible.
type fakeClock struct {
	mu   sync.Mutex
	base time.Time
	ns   int64
}

func newFakeClock() *fakeClock { return &fakeClock{base: time.Now()} }

func (c *fakeClock) NowNS() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ns
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.base.Add(time.Duration(c.ns))
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.ns += int64(d)
	c.mu.Unlock()
}

// Sleep advances virtual time instantly; a context that is already done still
// wins, which is how an inbound cancel reaches the backoff.
func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Advance(d)
	return ctx.Err()
}

// testLog is an attempt log in a temp directory plus a decoder for what landed
// in it.
type testLog struct {
	*attemptLog
	t *testing.T
}

func newTestLog(t *testing.T) *testLog {
	t.Helper()
	return newTestLogIn(t, t.TempDir(), "svc-test")
}

func newTestLogIn(t *testing.T, dir, service string) *testLog {
	t.Helper()
	l, err := newAttemptLog(dir, service, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	return &testLog{attemptLog: l, t: t}
}

// records returns every line decoded into a generic map, so a test can assert
// on the exact key set the pipeline will see.
func (l *testLog) records() []map[string]interface{} {
	l.t.Helper()
	l.Flush()
	return readRecords(l.t, l.Path())
}

// readRecords decodes a JSONL attempt log from disk.
func readRecords(t *testing.T, path string) []map[string]interface{} {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	var out []map[string]interface{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var m map[string]interface{}
		require.NoError(t, json.Unmarshal(sc.Bytes(), &m))
		out = append(out, m)
	}
	require.NoError(t, sc.Err())
	return out
}

func (l *testLog) ofKind(kind string) []map[string]interface{} {
	var out []map[string]interface{}
	for _, r := range l.records() {
		if r["kind"] == kind {
			out = append(out, r)
		}
	}
	return out
}

func keysOf(m map[string]interface{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// spyPolicy counts the chain callbacks so a test can prove an attempt was (or
// was not) fed back into the policy chain.
type spyPolicy struct {
	underlying Policy
	results    []bool
	starts     int
	rollbacks  int
}

func (p *spyPolicy) AllowRequest(now int64) bool { return p.underlying.AllowRequest(now) }
func (p *spyPolicy) NextDelay(c RetryContext) DelayDecision {
	return p.underlying.NextDelay(c)
}
func (p *spyPolicy) OnRequestStart(now int64) { p.starts++; p.underlying.OnRequestStart(now) }
func (p *spyPolicy) Rollback(c RetryContext)  { p.rollbacks++; p.underlying.Rollback(c) }
func (p *spyPolicy) Underlying() Policy       { return p.underlying }
func (p *spyPolicy) SupportsDeferredRollback() bool {
	return p.underlying.SupportsDeferredRollback()
}
func (p *spyPolicy) AddResult(success bool, now int64) { p.results = append(p.results, success) }

// testState is a runtimeState wired for a test: a real clock (the admission
// station and the injector are genuinely concurrent), a temp-directory attempt
// log, and a policy file supplied inline.
type testState struct {
	*runtimeState
	tlog *testLog
}

func newTestState(t *testing.T, doc string) *testState {
	t.Helper()
	return newTestStateWithFaults(t, doc, nil, 0, nil)
}

func newTestStateWithFaults(t *testing.T, doc string, schedule *FaultSchedule, epochMs int64, roll func() float64) *testState {
	t.Helper()
	return newTestStateIn(t, t.TempDir(), "svc-test", doc, schedule, epochMs, roll)
}

func newTestStateIn(t *testing.T, dir, service, doc string, schedule *FaultSchedule, epochMs int64, roll func() float64) *testState {
	t.Helper()
	cfg, err := ParseConfig([]byte(doc), "test.yaml")
	require.NoError(t, err)
	clock := newRealClock()
	reg, err := buildRegistry(cfg, sha256Hex([]byte(doc)), nil, clock)
	require.NoError(t, err)
	l := newTestLogIn(t, dir, service)
	s := &runtimeState{
		service:    service,
		configPath: "",
		clock:      clock,
		log:        l.attemptLog,
		engine:     &engine{clock: clock, log: l.attemptLog, service: service},
		faults:     newFaultInjector(schedule, epochMs, nil, roll),
		stopReload: make(chan struct{}),
	}
	s.registry.Store(reg)
	return &testState{runtimeState: s, tlog: l}
}
