package rpcpolicy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
)

const unaryCallMethod = "/grpc.testing.TestService/UnaryCall"

// leafServer is the terminal node: it does nothing but answer, so every failure
// in the e2e run comes from the injector.
type leafServer struct {
	grpc_testing.UnimplementedTestServiceServer
	mu        sync.Mutex
	calls     int
	seenRoute []string
}

func (s *leafServer) UnaryCall(ctx context.Context, _ *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	s.mu.Lock()
	s.calls++
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.seenRoute = append(s.seenRoute, firstOr(md.Get(RouteKey), ""))
	} else {
		s.seenRoute = append(s.seenRoute, "<no metadata>")
	}
	s.mu.Unlock()
	return &grpc_testing.SimpleResponse{}, nil
}

func firstOr(vs []string, def string) string {
	if len(vs) == 0 {
		return def
	}
	return vs[0]
}

// relayServer forwards to the leaf, handing its admission permit back first --
// the shape docs/CONTRACTS.md §7 gives RelayNode.
type relayServer struct {
	grpc_testing.UnimplementedTestServiceServer
	downstream grpc_testing.TestServiceClient
}

func (s *relayServer) UnaryCall(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	ReleasePermit(ctx)
	return s.downstream.UnaryCall(ctx, req)
}

func startGRPC(t *testing.T, s *testState, register func(*grpc.Server)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.UnaryInterceptor(s.unaryServerInterceptor()))
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func dial(t *testing.T, s *testState, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(s.unaryClientInterceptor()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// A three-tier in-process system -- HTTP front door -> relay -> leaf -- with the
// leaf's method faulted at p_fail 1. It exercises every moving part at once:
// the attempt loop, the admission station, the injector inside the permit, the
// trace chain and the JSONL files.
func TestEndToEndThreeTiersUnderAPermanentFault(t *testing.T) {
	logDir := t.TempDir()
	epochMs := time.Now().Add(-time.Second).UnixMilli()

	clientProfile := func(maxAttempts int) string {
		return "default_policy: top\nprofiles:\n  top:\n    timeout: 2s\n    retry:\n      enabled: true\n      kind: fixed\n      max_attempts: " +
			itoa(maxAttempts) + "\n      delay: 1ms\n"
	}
	serverBlock := "server:\n  workers: 4\n  queue_capacity: 8\n"

	leafState := newTestStateIn(t, logDir, "svc-leaf",
		"default_policy: none\nprofiles:\n  none:\n    timeout: 2s\n"+serverBlock,
		scheduleOf(FaultRule{Method: unaryCallMethod, StartS: 0, EndS: 3600, PFail: 1.0}),
		epochMs, func() float64 { return 0.0 })
	relayState := newTestStateIn(t, logDir, "svc-relay", clientProfile(2)+serverBlock, nil, 0, nil)
	edgeState := newTestStateIn(t, logDir, "svc-edge", clientProfile(2)+"routes:\n  Root: root\n", nil, 0, nil)

	leaf := &leafServer{}
	leafAddr := startGRPC(t, leafState, func(g *grpc.Server) { grpc_testing.RegisterTestServiceServer(g, leaf) })

	relay := &relayServer{downstream: grpc_testing.NewTestServiceClient(dial(t, relayState, leafAddr))}
	relayAddr := startGRPC(t, relayState, func(g *grpc.Server) { grpc_testing.RegisterTestServiceServer(g, relay) })

	edgeClient := grpc_testing.NewTestServiceClient(dial(t, edgeState, relayAddr))
	handler := edgeState.httpMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := edgeClient.UnaryCall(r.Context(), &grpc_testing.SimpleRequest{}); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"Ret0": 0}`))
	}))

	httpSrv := httptest.NewServer(handler)
	defer httpSrv.Close()

	req, err := http.NewRequest(http.MethodGet, httpSrv.URL+"/Root?key=1", nil)
	require.NoError(t, err)
	req.Header.Set(TraceparentKey, "00-11111111111111111111111111111111-2222222222222222-01")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	// --- attempt counts ---------------------------------------------------
	// max_attempts 2 at each of the two client tiers, and the leaf always
	// fails, so: 2 edge attempts x 2 relay attempts = 4 calls at the leaf.
	edgeClients := edgeState.tlog.ofKind("client")
	relayServers := relayState.tlog.ofKind("server")
	relayClients := relayState.tlog.ofKind("client")
	leafServers := leafState.tlog.ofKind("server")
	require.Len(t, edgeClients, 2)
	require.Len(t, relayServers, 2)
	require.Len(t, relayClients, 4)
	require.Len(t, leafServers, 4)
	leaf.mu.Lock()
	assert.Equal(t, 4, leaf.calls, "the handler runs before the roll, so every attempt reaches it")
	assert.Equal(t, []string{"root", "root", "root", "root"}, leaf.seenRoute,
		"x-rpcpolicy-route survives both hops")
	leaf.mu.Unlock()

	// Every client record names the connection it went out on, so the pipeline
	// can resolve caller -> callee without a service registry.
	for i, r := range edgeClients {
		assert.Equal(t, relayAddr, r["peer"], "edge record %d dialed the relay", i)
	}
	for i, r := range relayClients {
		assert.Equal(t, leafAddr, r["peer"], "relay record %d dialed the leaf", i)
	}

	// The last attempt of each client says why it stopped.
	assert.Equal(t, deniedExhausted, edgeClients[1]["retry_denied"])
	assert.Equal(t, "", edgeClients[0]["retry_denied"])
	assert.Equal(t, 0.0, edgeClients[0]["retry_delay_ms"])
	assert.Equal(t, 1.0, edgeClients[1]["retry_delay_ms"])

	// --- fault and admission ---------------------------------------------
	for i, r := range leafServers {
		assert.Equal(t, true, r["fault_hit"], "leaf record %d", i)
		assert.Equal(t, "Unavailable", r["response_code"])
		assert.Equal(t, outcomeAdmitted, r["admission_outcome"])
		assert.Equal(t, 0.0, r["admission_queue_depth"])
		assert.Equal(t, 0.0, r["admission_wait_ms"])
		assert.Greater(t, r["permit_released_at"], 0.0)
		assert.Equal(t, 0.0, r["injected_ms"], "this schedule injects failure, not latency")
	}
	for _, r := range relayServers {
		assert.Equal(t, false, r["fault_hit"])
		assert.Equal(t, "Unavailable", r["response_code"])
		assert.Equal(t, outcomeAdmitted, r["admission_outcome"])
		assert.Greater(t, r["permit_released_at"], 0.0)
	}
	for _, r := range relayClients {
		assert.Equal(t, dropServerFailure, r["drop_reason"])
		assert.Equal(t, "root", r["route"])
		assert.Equal(t, "top", r["profile"])
		assert.Equal(t, unaryCallMethod, r["operation"])
	}

	// --- the trace chain --------------------------------------------------
	roots := edgeState.tlog.ofKind("root")
	require.Len(t, roots, 1)
	root := roots[0]
	assert.Equal(t, "11111111111111111111111111111111", root["trace_id"])
	assert.Equal(t, "2222222222222222", root["parent_span_id"], "the driver's span is the root's parent")
	assert.Equal(t, "root", root["route"])
	assert.Equal(t, float64(503), root["http_status"])
	assert.Equal(t, "HTTP GET /Root", root["operation"])

	for _, r := range append(append(append(edgeClients, relayServers...), relayClients...), leafServers...) {
		assert.Equal(t, root["trace_id"], r["trace_id"], "one trace id for the whole request")
	}
	// root -> edge client -> relay server -> relay client -> leaf server
	for _, r := range edgeClients {
		assert.Equal(t, root["span_id"], r["parent_span_id"])
	}
	// One-to-one per hop AND per attempt, not merely "some client span": the
	// mapping is asserted as MULTISET equality, so a retry whose server record
	// pointed at the wrong attempt's span -- or two server records sharing one
	// parent -- fails here. Set membership would accept both.
	assert.ElementsMatch(t, spanIDs(edgeClients), parentSpanIDs(relayServers),
		"every edge attempt has exactly one relay server record, and vice versa")
	assert.ElementsMatch(t, spanIDs(relayClients), parentSpanIDs(leafServers),
		"every relay attempt has exactly one leaf server record, and vice versa")
	// The relay's own client attempts hang off the server record that issued
	// them: 2 relay server records x 2 attempts each.
	relayServerSpans := spanIDs(relayServers)
	for _, r := range relayClients {
		assert.Contains(t, relayServerSpans, r["parent_span_id"])
	}
	for _, span := range relayServerSpans {
		assert.Equal(t, 2, countParent(relayClients, span),
			"each relay server record fathered exactly its own two attempts")
	}
	// 1 root + 2 edge client + 2 relay server + 4 relay client + 4 leaf server
	assert.Len(t, uniqueSpans(append(append(append(append(edgeClients, relayServers...), relayClients...), leafServers...), root)), 13,
		"every record has its own span id")

	// --- the JSONL files --------------------------------------------------
	entries, err := os.ReadDir(logDir)
	require.NoError(t, err)
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name()] = true
	}
	for _, want := range []string{"svc-edge-1.jsonl", "svc-relay-1.jsonl", "svc-leaf-1.jsonl"} {
		assert.True(t, names[want], "expected %s in %s, got %v", want, logDir, names)
		info, err := os.Stat(filepath.Join(logDir, want))
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(0))
	}
}

// The same three tiers with the fault disarmed: the request succeeds on the
// first attempt at every tier.
func TestEndToEndSucceedsWithoutAFault(t *testing.T) {
	logDir := t.TempDir()
	serverBlock := "server:\n  workers: 4\n  queue_capacity: 8\n"
	clientDoc := "default_policy: top\nprofiles:\n  top:\n    timeout: 2s\n    retry:\n      enabled: true\n      kind: fixed\n      max_attempts: 3\n      delay: 1ms\n"

	leafState := newTestStateIn(t, logDir, "ok-leaf", "default_policy: none\nprofiles:\n  none:\n    timeout: 2s\n"+serverBlock, nil, 0, nil)
	relayState := newTestStateIn(t, logDir, "ok-relay", clientDoc+serverBlock, nil, 0, nil)
	edgeState := newTestStateIn(t, logDir, "ok-edge", clientDoc, nil, 0, nil)

	leaf := &leafServer{}
	leafAddr := startGRPC(t, leafState, func(g *grpc.Server) { grpc_testing.RegisterTestServiceServer(g, leaf) })
	relay := &relayServer{downstream: grpc_testing.NewTestServiceClient(dial(t, relayState, leafAddr))}
	relayAddr := startGRPC(t, relayState, func(g *grpc.Server) { grpc_testing.RegisterTestServiceServer(g, relay) })
	edgeClient := grpc_testing.NewTestServiceClient(dial(t, edgeState, relayAddr))

	handler := edgeState.httpMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := edgeClient.UnaryCall(r.Context(), &grpc_testing.SimpleRequest{}); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"Ret0": 0}`))
	}))
	httpSrv := httptest.NewServer(handler)
	defer httpSrv.Close()

	resp, err := http.Get(httpSrv.URL + "/Root?key=1")
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Len(t, edgeState.tlog.ofKind("client"), 1)
	assert.Len(t, relayState.tlog.ofKind("client"), 1)
	assert.Len(t, leafState.tlog.ofKind("server"), 1)
	assert.Equal(t, "OK", leafState.tlog.ofKind("server")[0]["response_code"])
	assert.Equal(t, false, leafState.tlog.ofKind("server")[0]["is_error"])
	roots := edgeState.tlog.ofKind("root")
	require.Len(t, roots, 1)
	assert.Equal(t, float64(200), roots[0]["http_status"])
}

// `peer` is the dial address as written, with the scheme grpc may prepend
// stripped back off (CONTRACTS.md §5).
func TestPeerOfIsTheDialTargetWithoutItsScheme(t *testing.T) {
	for _, target := range []string{"svc_a_container:12345", "dns:///svc_a_container:12345", "passthrough:///svc_a_container:12345"} {
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		assert.Equal(t, "svc_a_container:12345", peerOf(conn), "target %q", target)
		_ = conn.Close()
	}
	assert.Equal(t, "", peerOf(nil))
}

// The public API of CONTRACTS.md §6 is inert without RPCPOLICY_CONFIG and
// active with it.
func TestPublicAPIInertAndActive(t *testing.T) {
	t.Setenv(EnvConfig, "")
	t.Setenv(EnvLogDir, t.TempDir())
	t.Setenv(EnvServiceName, "svc-api")
	t.Setenv("FAULT_CONFIG_PATH", "")
	t.Setenv("FAULT_EPOCH_MS", "")
	resetStateForTest()
	t.Cleanup(resetStateForTest)

	assert.Nil(t, ServerOptions())
	assert.Nil(t, DialOptions())
	ctx, cancel := CallContext(context.Background(), 250*time.Millisecond)
	d, ok := ctx.Deadline()
	assert.True(t, ok, "inert CallContext applies the upstream per-call timeout")
	assert.InDelta(t, float64(250*time.Millisecond), float64(time.Until(d)), float64(50*time.Millisecond))
	cancel()

	base := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	assert.Equal(t, reflect.ValueOf(base).Pointer(), reflect.ValueOf(HTTPMiddleware()(base)).Pointer(),
		"inert middleware is the identity")
	ReleasePermit(context.Background()) // no panic

	// Now bind a policy file and re-initialise.
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte("default_policy: a\nprofiles:\n  a:\n    timeout: 30ms\nserver:\n  workers: 2\n  queue_capacity: 2\n"), 0o644))
	t.Setenv(EnvConfig, path)
	t.Setenv(EnvLogDir, dir)
	resetStateForTest()

	assert.Len(t, ServerOptions(), 1)
	assert.Len(t, DialOptions(), 1)
	ctx2, cancel2 := CallContext(context.Background(), 250*time.Millisecond)
	_, ok = ctx2.Deadline()
	assert.False(t, ok, "an active CallContext leaves every deadline to the attempt loop")
	cancel2()
	assert.NotEqual(t, reflect.ValueOf(base).Pointer(), reflect.ValueOf(HTTPMiddleware()(base)).Pointer())
	assert.Equal(t, time.Second, DefaultCallTimeout)
}

// A method with no binding falls through to default_policy; a route-scoped key
// beats the bare method.
func TestLookupOrder(t *testing.T) {
	doc := `
default_policy: fallback
profiles:
  fallback:
    timeout: 1s
  bare:
    timeout: 2s
  scoped:
    timeout: 3s
method_policies:
  "/grpc.NodeService/Call": bare
  "hotels|/grpc.NodeService/Call": scoped
`
	cfg, err := ParseConfig([]byte(doc), "test.yaml")
	require.NoError(t, err)
	reg, err := buildRegistry(cfg, "sha", nil, newRealClock())
	require.NoError(t, err)

	assert.Equal(t, 3*time.Second, reg.Lookup("hotels", testMethod).timeout)
	assert.Equal(t, 2*time.Second, reg.Lookup("other", testMethod).timeout)
	assert.Equal(t, 2*time.Second, reg.Lookup("", testMethod).timeout)
	assert.Equal(t, time.Second, reg.Lookup("hotels", "/grpc.Other/M").timeout)
	assert.Equal(t, time.Second, reg.Lookup("", "/grpc.Other/M").timeout)
}

func TestRouteForUsesTheRoutesMapThenTheLowercasedPath(t *testing.T) {
	cfg, err := ParseConfig([]byte("default_policy: a\nroutes:\n  SearchHandler: hotels\nprofiles:\n  a:\n    timeout: 1s\n"), "test.yaml")
	require.NoError(t, err)
	reg, err := buildRegistry(cfg, "sha", nil, newRealClock())
	require.NoError(t, err)
	assert.Equal(t, "hotels", reg.RouteFor("/SearchHandler"))
	assert.Equal(t, "root", reg.RouteFor("/Root"))
	assert.Equal(t, "recommendhandler", reg.RouteFor("/RecommendHandler"))
}

// parentSpanIDs is the multiset of parents a set of records names.
func parentSpanIDs(recs []map[string]interface{}) []interface{} {
	out := make([]interface{}, 0, len(recs))
	for _, r := range recs {
		out = append(out, r["parent_span_id"])
	}
	return out
}

func countParent(recs []map[string]interface{}, span interface{}) int {
	n := 0
	for _, r := range recs {
		if r["parent_span_id"] == span {
			n++
		}
	}
	return n
}

func spanIDs(recs []map[string]interface{}) []interface{} {
	out := make([]interface{}, 0, len(recs))
	for _, r := range recs {
		out = append(out, r["span_id"])
	}
	return out
}

func uniqueSpans(recs []map[string]interface{}) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range recs {
		s, _ := r["span_id"].(string)
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func itoa(v int) string { return strconv.Itoa(v) }
