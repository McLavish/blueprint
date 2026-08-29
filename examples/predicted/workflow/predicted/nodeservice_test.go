package predicted

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeNode records what a relay or an edge passed downstream.
type fakeNode struct {
	calls []struct {
		key   int64
		depth int64
	}
	ret int64
	err error
}

func (f *fakeNode) Call(ctx context.Context, key int64, depth int64) (int64, error) {
	f.calls = append(f.calls, struct {
		key   int64
		depth int64
	}{key, depth})
	if f.err != nil {
		return 0, f.err
	}
	return f.ret, nil
}

func writeConfig(t *testing.T, medianMS float64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node.yaml")
	body := "service:\n  name: test\n  median_ms: " +
		strconv.FormatFloat(medianMS, 'f', -1, 64) +
		"\n  sigma: 0.0001\n  mode: sleep\n  floor_us: 0\n  fanout: serial\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLeafNodeCall(t *testing.T) {
	t.Setenv(EnvNodeConfigPath, "")
	leaf, err := NewLeafNode(context.Background(), writeConfig(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	got, err := leaf.Call(context.Background(), 41, 1)
	if err != nil {
		t.Fatal(err)
	}
	// key + depth: a Single answer (depth 0) and a Multichain answer (depth 2)
	// are distinguishable at the front door.
	if got != 42 {
		t.Errorf("Call(41, 1) = %d, want 42", got)
	}
}

func TestLeafNodeCallPropagatesCancellation(t *testing.T) {
	t.Setenv(EnvNodeConfigPath, "")
	leaf, err := NewLeafNode(context.Background(), writeConfig(t, 5000))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := leaf.Call(ctx, 1, 0); status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("code = %v, want DeadlineExceeded (err %v)", status.Code(err), err)
	}
}

func TestRelayNodeCallsDownstreamAtDepthPlusOne(t *testing.T) {
	t.Setenv(EnvNodeConfigPath, "")
	down := &fakeNode{ret: 99}
	relay, err := NewRelayNode(context.Background(), down, writeConfig(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	// ReleasePermit runs here with the interceptor inert (no RPCPOLICY_CONFIG in
	// a unit test), which must be a no-op rather than a panic.
	got, err := relay.Call(context.Background(), 7, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got != 99 {
		t.Errorf("relay returned %d, want the downstream's 99", got)
	}
	if len(down.calls) != 1 {
		t.Fatalf("downstream called %d times, want 1", len(down.calls))
	}
	if down.calls[0].key != 7 || down.calls[0].depth != 4 {
		t.Errorf("downstream got (key=%d, depth=%d), want (7, 4)", down.calls[0].key, down.calls[0].depth)
	}
}

func TestRelayNodeDoesNotCallDownstreamAfterCancellation(t *testing.T) {
	t.Setenv(EnvNodeConfigPath, "")
	down := &fakeNode{ret: 1}
	relay, err := NewRelayNode(context.Background(), down, writeConfig(t, 5000))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := relay.Call(ctx, 1, 0); status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("code = %v, want DeadlineExceeded (err %v)", status.Code(err), err)
	}
	if len(down.calls) != 0 {
		t.Errorf("downstream was called %d times after the relay's own deadline expired", len(down.calls))
	}
}

func TestEdgeNodeRootStartsAtDepthZero(t *testing.T) {
	down := &fakeNode{ret: 5}
	edge, err := NewEdgeNode(context.Background(), down)
	if err != nil {
		t.Fatal(err)
	}
	got, err := edge.Root(context.Background(), 11)
	if err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Errorf("Root returned %d, want the downstream's 5", got)
	}
	if len(down.calls) != 1 || down.calls[0].key != 11 || down.calls[0].depth != 0 {
		t.Errorf("downstream calls = %v, want one (key=11, depth=0)", down.calls)
	}
}

// The edge burns no service time of its own: its whole job is to own a
// client-side policy. A measurable sleep here would put a second station in
// front of the one the campaign is measuring.
func TestEdgeNodeHasNoServiceTime(t *testing.T) {
	edge, err := NewEdgeNode(context.Background(), &fakeNode{})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for i := 0; i < 100; i++ {
		if _, err := edge.Root(context.Background(), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("100 Root calls took %v; the edge should add no service time", elapsed)
	}
}

// The two workflow interfaces are what the wiring spec's type parameters point
// at, so a signature drift here is a compile error rather than a puzzling
// "unable to find service interfaces" from the Blueprint compiler.
var (
	_ NodeService = (*LeafNode)(nil)
	_ NodeService = (*RelayNode)(nil)
	_ EdgeService = (*EdgeNode)(nil)
)
