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

// DeadlineNS is the inverse of Now: a deadline minted on this clock's timeline
// converts back to the exact ns NowNS would report at that instant.
func (c *fakeClock) DeadlineNS(deadline time.Time) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int64(deadline.Sub(c.base))
}

// WithTimeout mints the deadline on the VIRTUAL clock, so the boundary the
// engine reads back off the context is the same integer the test advanced to.
// The real timer underneath is only a backstop; the tests never wait for it.
func (c *fakeClock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithDeadline(ctx, c.Now().Add(d))
}

// handoffClock is a fakeClock that LEAPS forward once, on the next observation
// after Arm: that observation still reads the instant the test placed, and every
// observation after it reads `leap` later.
//
// It exists to separate two instants the station used to conflate: the instant a
// worker is handed over -- the dispatcher's single observation, taken inside
// permit.release -- and the instant the waiting goroutine is next scheduled,
// which is every observation after it. A station that decides the
// permit-versus-deadline race at wake-up reads the late one.
type handoffClock struct {
	*fakeClock
	mu    sync.Mutex
	leap  time.Duration
	armed bool
}

func newHandoffClock() *handoffClock { return &handoffClock{fakeClock: newFakeClock()} }

// Arm makes the NEXT observation the last one to read the current instant.
func (c *handoffClock) Arm(leap time.Duration) {
	c.mu.Lock()
	c.leap, c.armed = leap, true
	c.mu.Unlock()
}

func (c *handoffClock) Now() time.Time {
	now := c.fakeClock.Now()
	c.mu.Lock()
	leap, armed := c.leap, c.armed
	c.armed = false
	c.mu.Unlock()
	if armed {
		c.fakeClock.Advance(leap)
	}
	return now
}

// tickClock advances virtual time by one tick on EVERY observation, so the two
// clock samples a "NowNS() + (deadline - Now())" conversion mixes can no longer
// agree, and an attempt boundary derived from anything other than the context
// the invoker received lands on a different instant. Freeze stops the ticking
// so a test can place the finish exactly.
type tickClock struct {
	mu     sync.Mutex
	base   time.Time
	ns     int64
	tick   time.Duration
	frozen bool
}

func newTickClock(tick time.Duration) *tickClock {
	return &tickClock{base: time.Now(), tick: tick}
}

func (c *tickClock) observe() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	ns := c.ns
	if !c.frozen {
		c.ns += int64(c.tick)
	}
	return ns
}

func (c *tickClock) NowNS() int64 { return c.observe() }

func (c *tickClock) Now() time.Time { return c.base.Add(time.Duration(c.observe())) }

func (c *tickClock) DeadlineNS(deadline time.Time) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int64(deadline.Sub(c.base))
}

func (c *tickClock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithDeadline(ctx, c.Now().Add(d))
}

func (c *tickClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.ns += int64(d)
	c.mu.Unlock()
	return ctx.Err()
}

// FreezeAt pins virtual time at ns and stops the per-observation tick, so every
// later observation reports exactly ns.
func (c *tickClock) FreezeAt(ns int64) {
	c.mu.Lock()
	c.ns = ns
	c.frozen = true
	c.mu.Unlock()
}

// Deadline mints an absolute deadline d from now on this clock's timeline.
func (c *tickClock) Deadline(d time.Duration) time.Time { return c.Now().Add(d) }

// Sleep advances virtual time instantly; a context that is already done still
// wins, which is how an inbound cancel reaches the backoff.
func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Advance(d)
	return ctx.Err()
}

// descheduledCtx is the one thing a real context cannot express: the goroutine
// running the WORK sees the caller's context end, while the goroutine running
// the INTERCEPTOR has not yet been scheduled to hear about it. Err() reports the
// end from the moment the test calls expire; the Done channel is never closed,
// so a select parked on it stays parked exactly as a descheduled goroutine's
// would.
//
// In a deployment the two are one instant, and which goroutine reacts first is a
// scheduling race. This double pins the side of that race where the goroutine
// that STAMPED the end of the work is also the one that claims it -- the only
// side on which the worker's own classification of its work decides the record,
// and therefore the only side on which it can be tested at all.
type descheduledCtx struct {
	context.Context
	deadline time.Time
	done     chan struct{}

	mu  sync.Mutex
	err error
}

func newDescheduledCtx(deadline time.Time) *descheduledCtx {
	return &descheduledCtx{Context: context.Background(), deadline: deadline, done: make(chan struct{})}
}

func (c *descheduledCtx) Deadline() (time.Time, bool) { return c.deadline, true }

func (c *descheduledCtx) Done() <-chan struct{} { return c.done }

func (c *descheduledCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// expire is the caller's context ending: every goroutine that READS the context
// sees it from here on, and one parked on Done() sees nothing yet.
func (c *descheduledCtx) expire(err error) {
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
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
	return newTestStateOn(t, newRealClock(), dir, service, doc, schedule, epochMs, roll)
}

func newTestStateOn(t *testing.T, clock Clock, dir, service, doc string, schedule *FaultSchedule, epochMs int64, roll func() float64) *testState {
	t.Helper()
	cfg, err := ParseConfig([]byte(doc), "test.yaml")
	require.NoError(t, err)
	l := newTestLogIn(t, dir, service)
	reg, err := buildRegistry(cfg, sha256Hex([]byte(doc)), nil, clock, l.attemptLog)
	require.NoError(t, err)
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
