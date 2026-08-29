package rpcpolicy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The exact key sets of docs/CONTRACTS.md §5. The Python pipeline indexes these
// by name, so an added or renamed key is a breaking change and this test is the
// gate for it.
var (
	baseKeys = []string{
		"kind", "trace_id", "span_id", "parent_span_id", "service", "operation",
		"start_epoch", "end_epoch", "duration_ms", "is_error", "response_code",
	}
	clientKeys = append(append([]string{}, baseKeys...),
		"route", "profile", "attempt", "peer", "retry_delay_ms", "gate", "drop_reason", "retry_denied")
	serverKeys = append(append([]string{}, baseKeys...),
		"admission_outcome", "admission_wait_ms", "admission_queue_depth", "admission_workers_busy",
		"injected_ms", "handler_ms", "permit_released_at", "fault_hit")
	rootKeys  = append(append([]string{}, baseKeys...), "route", "http_status")
	eventKeys = []string{"kind", "name", "epoch", "sha256", "service"}
)

func assertKeySet(t *testing.T, want []string, rec map[string]interface{}) {
	t.Helper()
	got := keysOf(rec)
	sort.Strings(got)
	w := append([]string{}, want...)
	sort.Strings(w)
	assert.Equal(t, w, got)
}

func TestClientRecordKeySet(t *testing.T) {
	e, clock, log := newTestEngine(t)
	prof := testProfile(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 50ms\n")
	err := e.execute(context.Background(), prof, "root", testMethod, testPeer, func(context.Context) error {
		clock.Advance(time.Millisecond)
		return status.Error(codes.Unavailable, "boom")
	})
	require.Error(t, err)
	recs := log.ofKind("client")
	require.Len(t, recs, 1)
	assertKeySet(t, clientKeys, recs[0])

	assert.Equal(t, "client", recs[0]["kind"])
	assert.Len(t, recs[0]["trace_id"], 32)
	assert.Len(t, recs[0]["span_id"], 16)
	assert.Equal(t, "", recs[0]["parent_span_id"])
	assert.Equal(t, "svc-test", recs[0]["service"])
	assert.Equal(t, testMethod, recs[0]["operation"])
	assert.Equal(t, "Unavailable", recs[0]["response_code"])
	assert.Equal(t, true, recs[0]["is_error"])
	assert.InDelta(t, 1.0, recs[0]["duration_ms"], 0.001)
	assert.Equal(t, testPeer, recs[0]["peer"], "the dial target of the connection the attempt went out on")
}

func TestServerRecordKeySet(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 2\n  queue_capacity: 4\n")
	_, err := s.serve(context.Background(), nil, testMethod, func(context.Context, interface{}) (interface{}, error) {
		return "ok", nil
	})
	require.NoError(t, err)
	recs := s.tlog.ofKind("server")
	require.Len(t, recs, 1)
	assertKeySet(t, serverKeys, recs[0])
	assert.Equal(t, "OK", recs[0]["response_code"])
	assert.Equal(t, false, recs[0]["is_error"])
	assert.Equal(t, outcomeAdmitted, recs[0]["admission_outcome"])
	assert.Equal(t, 0.0, recs[0]["admission_queue_depth"])
	assert.Equal(t, 0.0, recs[0]["admission_workers_busy"])
	assert.Equal(t, false, recs[0]["fault_hit"])
}

func TestRootRecordKeySet(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nroutes:\n  Root: root\n")
	h := s.httpMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"Ret0": 7}`))
	}))
	req := httptest.NewRequest(http.MethodGet, "/Root?key=3", nil)
	req.Header.Set(TraceparentKey, "00-0123456789abcdef0123456789abcdef-fedcba9876543210-01")
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	recs := s.tlog.ofKind("root")
	require.Len(t, recs, 1)
	assertKeySet(t, rootKeys, recs[0])
	assert.Equal(t, "HTTP GET /Root", recs[0]["operation"])
	assert.Equal(t, "0123456789abcdef0123456789abcdef", recs[0]["trace_id"])
	assert.Equal(t, "fedcba9876543210", recs[0]["parent_span_id"])
	assert.Equal(t, "root", recs[0]["route"])
	assert.Equal(t, float64(200), recs[0]["http_status"])
	assert.Equal(t, "OK", recs[0]["response_code"])
	assert.Equal(t, false, recs[0]["is_error"])
}

func TestRootRecordCapturesANonOKStatus(t *testing.T) {
	s := newTestState(t, "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n")
	h := s.httpMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusServiceUnavailable)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/Root", nil))
	recs := s.tlog.ofKind("root")
	require.Len(t, recs, 1)
	assert.Equal(t, float64(503), recs[0]["http_status"])
	assert.Equal(t, "Unavailable", recs[0]["response_code"])
	assert.Equal(t, true, recs[0]["is_error"])
	assert.Equal(t, "root", recs[0]["route"], "the default route key is the lowercased path")
	assert.Len(t, recs[0]["trace_id"], 32, "a request without traceparent starts a fresh trace")
	assert.Equal(t, "", recs[0]["parent_span_id"])
}

func TestEventRecordKeySet(t *testing.T) {
	log := newTestLog(t)
	log.Write(&EventRecord{Kind: "event", Name: eventPolicyReload, Epoch: 1.5, SHA256: "abc", Service: "svc-test"})
	recs := log.ofKind("event")
	require.Len(t, recs, 1)
	assertKeySet(t, eventKeys, recs[0])
	assert.Equal(t, eventPolicyReload, recs[0]["name"])
	assert.Equal(t, 1.5, recs[0]["epoch"])
	assert.Equal(t, "abc", recs[0]["sha256"])
}

// The file name is $RPCPOLICY_LOG_DIR/<OTEL_SERVICE_NAME>-<startUnixMs>.jsonl.
func TestAttemptLogFileName(t *testing.T) {
	dir := t.TempDir()
	l, err := newAttemptLog(dir, "svc-A", 1717171717171)
	require.NoError(t, err)
	defer l.Close()
	assert.Equal(t, filepath.Join(dir, "svc-A-1717171717171.jsonl"), l.Path())

	// The directory is created when it does not exist.
	nested := filepath.Join(dir, "a", "b")
	l2, err := newAttemptLog(nested, "svc-B", 1)
	require.NoError(t, err)
	defer l2.Close()
	_, err = os.Stat(nested)
	require.NoError(t, err)
}

// One JSON object per line, flushed by the ticker without an explicit Flush.
func TestAttemptLogIsLineDelimitedAndTickerFlushed(t *testing.T) {
	dir := t.TempDir()
	l, err := newAttemptLog(dir, "svc-tick", 1)
	require.NoError(t, err)
	defer l.Close()
	for i := 0; i < 5; i++ {
		l.Write(&EventRecord{Kind: "event", Name: "policy_reload", Epoch: float64(i), Service: "svc-tick"})
	}
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(l.Path())
		return err == nil && len(b) > 0
	}, 3*time.Second, 20*time.Millisecond, "the 1 s flush ticker must push records without an explicit Flush")

	recs := readRecords(t, l.Path())
	require.Len(t, recs, 5)
	for i, r := range recs {
		assert.Equal(t, float64(i), r["epoch"])
	}
}

// Records from many goroutines stay line-atomic.
func TestAttemptLogIsConcurrencySafe(t *testing.T) {
	log := newTestLog(t)
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func(g int) {
			for i := 0; i < 100; i++ {
				log.Write(&EventRecord{Kind: "event", Name: "policy_reload", Epoch: float64(g), Service: "svc-test"})
			}
			done <- struct{}{}
		}(g)
	}
	for g := 0; g < 8; g++ {
		<-done
	}
	assert.Len(t, log.records(), 800)
}

// A write after Close must be REPORTED, not silently accepted: a record the
// pipeline never sees, with nothing to say it was dropped, is exactly the
// failure the attempt log exists to make impossible.
func TestAttemptLogRejectsWritesAfterClose(t *testing.T) {
	l := newTestLog(t)
	require.NoError(t, l.Write(&EventRecord{Kind: "event", Name: "before"}))
	require.NoError(t, l.Close())

	assert.ErrorIs(t, l.Write(&EventRecord{Kind: "event", Name: "after"}), ErrLogClosed)
	assert.ErrorIs(t, l.Flush(), ErrLogClosed)
	require.NoError(t, l.Close(), "Close is idempotent")

	recs := readRecords(t, l.Path())
	require.Len(t, recs, 1, "the record written after Close never reached the file")
	assert.Equal(t, "before", recs[0]["name"])
}
