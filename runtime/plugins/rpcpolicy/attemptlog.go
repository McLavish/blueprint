package rpcpolicy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// The local attempt log of docs/PLAN.md D6: one JSONL file per process, read
// offline by tools/harness. No tracing backend is involved; these records ARE
// the measurement.

// DefaultLogDir is where records go when RPCPOLICY_LOG_DIR is unset.
const DefaultLogDir = "/var/log/rpcpolicy"

// logBufferSize is the writer's buffer. At 1900 rps x 6 attempts the record
// stream is ~1e6 lines per recording, so the syscall rate matters more than
// the latency of the last partial line -- which the 1 s ticker bounds anyway.
const logBufferSize = 1 << 20

// logFlushInterval bounds how long a record can sit unflushed. The recorder
// sleeps 6 s after the run before rsyncing, so 1 s is ample.
const logFlushInterval = time.Second

// baseRecord is the common half of every client/server/root record
// (CONTRACTS.md §5). Embedded, so it flattens into the same JSON object.
type baseRecord struct {
	Kind         string  `json:"kind"`
	TraceID      string  `json:"trace_id"`
	SpanID       string  `json:"span_id"`
	ParentSpanID string  `json:"parent_span_id"`
	Service      string  `json:"service"`
	Operation    string  `json:"operation"`
	StartEpoch   float64 `json:"start_epoch"`
	EndEpoch     float64 `json:"end_epoch"`
	DurationMS   float64 `json:"duration_ms"`
	IsError      bool    `json:"is_error"`
	ResponseCode string  `json:"response_code"`
}

// ClientRecord is one client-side attempt.
type ClientRecord struct {
	baseRecord
	Route   string `json:"route"`
	Profile string `json:"profile"`
	Attempt int    `json:"attempt"`
	// Peer is the gRPC dial target of the connection the attempt was issued on,
	// as configured (`svc_a_container:12345`). The pipeline maps its host through
	// placement.yaml to the callee service, so it is written on a gate-denied
	// record too -- that attempt never reached the wire, but the connection it
	// would have used is known.
	Peer         string  `json:"peer"`
	RetryDelayMS float64 `json:"retry_delay_ms"`
	Gate         string  `json:"gate"`
	DropReason   string  `json:"drop_reason"`
	RetryDenied  string  `json:"retry_denied"`
}

// ServerRecord is one server-side outcome, including every admission drop
// (then HandlerMS is 0).
type ServerRecord struct {
	baseRecord
	AdmissionOutcome     string  `json:"admission_outcome"`
	AdmissionWaitMS      float64 `json:"admission_wait_ms"`
	AdmissionQueueDepth  int     `json:"admission_queue_depth"`
	AdmissionWorkersBusy int     `json:"admission_workers_busy"`
	InjectedMS           float64 `json:"injected_ms"`
	HandlerMS            float64 `json:"handler_ms"`
	// QueueDepthAtEnd is msim's `queue_size` argument to on_done: the length of
	// this service's queue at the instant THIS attempt ended, which is what
	// `queue_avg_at_attempt_end` averages on the simulator's side.
	//
	// It is a different measurement from admission_queue_depth, which is the
	// depth the attempt saw when it ARRIVED. The two coincide on every outcome
	// decided at admission (the attempt's end is its admission) and diverge for
	// every attempt that was served: comparing msim's end-of-attempt average
	// against a deployed admission-time depth compares two different quantities
	// exactly where the queue is moving fastest.
	//
	// Per path, mirroring service.py: queue_len() for deadline_at_submit and
	// queue_full; queue_len() - 1 for deadline_in_queue (an attempt is not part
	// of the queue it observes); queue_len() after the slot came back for
	// cancelled_in_queue; queue_len() after the pop for the deadline_at_dequeue
	// tie; queue_len() before _start_next dispatches for every in-service end
	// (a completion, a cut at the deadline, a cancel in service); and
	// queue_len() at the finalize instant when the worker had already been
	// handed back.
	QueueDepthAtEnd int `json:"queue_depth_at_end"`
	// OccupancyCensored is msim's occupancy_censored, and it is a statement
	// about the WORKER OCCUPANCY this record reports, not about the handler: the
	// permit was still held when the attempt was cut, so handler_ms (plus
	// injected_ms) is a LOWER BOUND on the occupancy the server would have
	// contributed and a censored-data estimator must not read it as a complete
	// observation.
	//
	// True exactly where msim's _complete_attempt sets it: an attempt whose
	// worker was still held when a DEADLINE or a CANCEL ended it, whether it was
	// cut mid-handler, cut inside the injected latency, or found to have
	// finished at or past its deadline (msim's _finish_service checks the
	// deadline before it rolls anything, and books the tie as a DEADLINE).
	//
	// False when the handler ran to completion inside its deadline, false on
	// every admission drop -- none of them ever held a worker -- and false when
	// the permit had already been handed back by ReleasePermit before the cut:
	// msim's early-release branch reports that occupancy as COMPLETE, because it
	// ended at the hand-back and nothing truncated it.
	OccupancyCensored bool    `json:"occupancy_censored"`
	PermitReleasedAt  float64 `json:"permit_released_at"`
	FaultHit          bool    `json:"fault_hit"`
}

// DiscardRecord is the station event that says WHEN an expired queue entry
// really gave its slot back.
//
// An attempt whose caller-clock deadline passes while it waits is told at that
// deadline, but it KEEPS its queue slot until the dispatcher reaches it and
// drops it (msim's _start_next `if item.expired: continue`). Its own server
// record is therefore written at the deadline and says nothing about the instant
// the slot came back, and nothing else in the log marks it -- so the pipeline
// cannot reconstruct queued(t) exactly. One discard event per entry the
// dispatcher drops closes that gap.
//
// It carries the identity fields of that attempt's ServerRecord and nothing
// else: it is not a span, it has no duration, no outcome and no code. `at_epoch`
// is a float epoch second like end_epoch.
type DiscardRecord struct {
	Kind         string  `json:"kind"`
	TraceID      string  `json:"trace_id"`
	SpanID       string  `json:"span_id"`
	ParentSpanID string  `json:"parent_span_id"`
	Service      string  `json:"service"`
	Operation    string  `json:"operation"`
	AtEpoch      float64 `json:"at_epoch"`
}

// kindDiscard is the `kind` the pipeline selects discard events by, alongside
// "client", "server", "root" and "event".
const kindDiscard = "discard"

// attemptID is how a station event names the attempt it is about: exactly the
// identity fields that attempt's own ServerRecord carries, so the two join on
// span_id.
//
// It reaches the station on the CONTEXT, like the deadline and the
// cancellation: it is per-attempt data the server flow establishes before
// admission, and the station only ever reads it. A station driven without one
// (the package's own station tests) simply has nothing to name and writes no
// station events.
type attemptID struct {
	traceID      string
	spanID       string
	parentSpanID string
	service      string
	operation    string
}

func (id attemptID) discardRecord(at time.Time) *DiscardRecord {
	return &DiscardRecord{
		Kind:         kindDiscard,
		TraceID:      id.traceID,
		SpanID:       id.spanID,
		ParentSpanID: id.parentSpanID,
		Service:      id.service,
		Operation:    id.operation,
		AtEpoch:      epochOf(at),
	}
}

type attemptIDKey struct{}

func withAttemptID(ctx context.Context, id attemptID) context.Context {
	return context.WithValue(ctx, attemptIDKey{}, id)
}

func attemptIDFrom(ctx context.Context) attemptID {
	id, _ := ctx.Value(attemptIDKey{}).(attemptID)
	return id
}

// RootRecord is the front-door HTTP request.
type RootRecord struct {
	baseRecord
	Route      string `json:"route"`
	HTTPStatus int    `json:"http_status"`
}

// EventRecord marks a policy reload (F9). It shares no fields with the three
// span-shaped records beyond `kind` and `service`.
//
// Note is optional and omitted when empty, so the key set of CONTRACTS.md §5 is
// unchanged for every event that has nothing extra to say. It carries the
// reason a reload failed, and the fact that a `server:` block differed and was
// therefore ignored.
type EventRecord struct {
	Kind    string  `json:"kind"`
	Name    string  `json:"name"`
	Epoch   float64 `json:"epoch"`
	SHA256  string  `json:"sha256"`
	Service string  `json:"service"`
	Note    string  `json:"note,omitempty"`
}

// Reload event names.
const (
	eventPolicyReload       = "policy_reload"
	eventPolicyReloadFailed = "policy_reload_failed"
)

// attemptLog is the buffered JSONL writer. Records are marshalled under the
// lock so the byte stream is line-atomic even with a hundred goroutines.
type attemptLog struct {
	mu     sync.Mutex
	f      *os.File
	w      *bufio.Writer
	enc    *json.Encoder
	closed bool
	stop   chan struct{}
	once   sync.Once
	// flusher joins the ticker goroutine before Close touches the writer.
	flusher sync.WaitGroup
	path    string
}

// ErrLogClosed is returned by Write and Flush after Close. Accepting a write
// into a closed log and reporting success loses the record silently, which is
// exactly the failure the attempt log exists to make impossible.
var ErrLogClosed = errors.New("rpcpolicy: attempt log is closed")

// newAttemptLog opens $dir/<service>-<startUnixMs>.jsonl and starts the flush
// ticker.
func newAttemptLog(dir, service string, startUnixMs int64) (*attemptLog, error) {
	if dir == "" {
		dir = DefaultLogDir
	}
	if service == "" {
		service = "unknown"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("rpcpolicy: cannot create log dir %q: %w", dir, err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%s-%d.jsonl", service, startUnixMs))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("rpcpolicy: cannot open attempt log %q: %w", path, err)
	}
	w := bufio.NewWriterSize(f, logBufferSize)
	enc := json.NewEncoder(w)
	l := &attemptLog{f: f, w: w, enc: enc, stop: make(chan struct{}), path: path}
	l.flusher.Add(1)
	go l.flushLoop()
	return l, nil
}

func (l *attemptLog) flushLoop() {
	defer l.flusher.Done()
	t := time.NewTicker(logFlushInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			_ = l.Flush()
		case <-l.stop:
			return
		}
	}
}

// Write appends one record. The RPC path ignores the error on purpose -- losing
// a record must not fail the RPC the record describes -- but it is REPORTED, so
// a write after Close cannot pass for a write that landed.
func (l *attemptLog) Write(rec interface{}) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrLogClosed
	}
	return l.enc.Encode(rec) // json.Encoder appends the newline
}

// Flush pushes the buffer to the file.
func (l *attemptLog) Flush() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrLogClosed
	}
	return l.w.Flush()
}

// Close stops the ticker, JOINS it, and flushes. Idempotent.
func (l *attemptLog) Close() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() { close(l.stop) })
	// Join before taking the lock: the ticker may be inside Flush right now, and
	// closing the file underneath it would race the buffered writer.
	l.flusher.Wait()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if err := l.w.Flush(); err != nil {
		_ = l.f.Close()
		return err
	}
	return l.f.Close()
}

// Path is the file the records go to.
func (l *attemptLog) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

// epochOf renders a wall-clock instant as Unix seconds with sub-microsecond
// resolution, the float the pipeline buckets on.
func epochOf(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}

// msOf renders a duration as float milliseconds.
func msOf(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
