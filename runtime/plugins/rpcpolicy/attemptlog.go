package rpcpolicy

import (
	"bufio"
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
	Route        string  `json:"route"`
	Profile      string  `json:"profile"`
	Attempt      int     `json:"attempt"`
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
	PermitReleasedAt     float64 `json:"permit_released_at"`
	FaultHit             bool    `json:"fault_hit"`
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
