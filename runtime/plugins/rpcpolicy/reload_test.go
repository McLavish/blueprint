package rpcpolicy

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// twoProfileConfig has one profile the reload edits and one it leaves alone, so
// the "unchanged profiles keep their state" rule is observable.
const twoProfileConfig = `
default_policy: keep
profiles:
  keep:
    timeout: 50ms
    circuit_breaker:
      enabled: true
      kind: count
      failure_threshold_ratio: [1, 1]
      success_threshold_ratio: [1, 1]
      half_open_delay: 1000s
  edited:
    timeout: %s
method_policies:
  "/grpc.NodeService/Call": edited
`

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
}

// newWatchedState starts a real process state on a temp policy file. The
// background watchers are left running only where the test is about them;
// elsewhere they would race the explicit reload() calls and add events of their
// own.
func newWatchedState(t *testing.T, body string) (*runtimeState, string) {
	t.Helper()
	s, path := newLiveState(t, body)
	s.stopOnce.Do(func() { close(s.stopReload) })
	return s, path
}

func newLiveState(t *testing.T, body string) (*runtimeState, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	writeFile(t, path, body)
	t.Setenv("FAULT_CONFIG_PATH", "")
	t.Setenv("FAULT_EPOCH_MS", "")
	s := initialize(path, filepath.Join(dir, "logs"), "svc-reload")
	t.Cleanup(s.shutdown)
	return s, path
}

func TestReloadSwapsTheRegistryAndRecordsTheEvent(t *testing.T) {
	body := sprintfConfig(twoProfileConfig, "10ms")
	s, path := newWatchedState(t, body)
	before := s.registry.Load()
	assert.Equal(t, 10*time.Millisecond, before.Lookup("", testMethod).timeout)
	assert.Equal(t, sha256Hex([]byte(body)), before.SHA256())

	updated := sprintfConfig(twoProfileConfig, "77ms")
	writeFile(t, path, updated)
	s.reload()

	after := s.registry.Load()
	assert.Equal(t, 77*time.Millisecond, after.Lookup("", testMethod).timeout)
	assert.Equal(t, sha256Hex([]byte(updated)), after.SHA256())

	events := readRecords(t, s.log.Path())
	require.Len(t, events, 1)
	assert.Equal(t, "event", events[0]["kind"])
	assert.Equal(t, eventPolicyReload, events[0]["name"])
	assert.Equal(t, sha256Hex([]byte(updated)), events[0]["sha256"])
	assert.Equal(t, "svc-reload", events[0]["service"])
	assert.Greater(t, events[0]["epoch"], 1.0e9)
}

// Profiles whose serialized block is unchanged keep their objects, so breaker
// and budget state survives a reload that only touched a different profile.
func TestReloadKeepsTheStateOfUnchangedProfiles(t *testing.T) {
	s, path := newWatchedState(t, sprintfConfig(twoProfileConfig, "10ms"))
	before := s.registry.Load()
	keep := before.byName["keep"]
	breaker := keep.head.(*CountBasedCircuitBreakerPolicy)
	breaker.AddResult(false, 0)
	require.Equal(t, CBOpen, breaker.GetState())

	writeFile(t, path, sprintfConfig(twoProfileConfig, "77ms"))
	s.reload()

	after := s.registry.Load()
	assert.Same(t, keep, after.byName["keep"], "an unchanged profile keeps its object")
	assert.NotSame(t, before.byName["edited"], after.byName["edited"], "an edited profile is rebuilt")
	assert.Equal(t, CBOpen, after.byName["keep"].head.(*CountBasedCircuitBreakerPolicy).GetState(),
		"the breaker's open state survived the reload")
}

// `server:` is not hot-reloaded: resizing a live queue mid-recording would
// change the capacity the calibration recovered.
func TestReloadDoesNotResizeTheStation(t *testing.T) {
	body := "default_policy: a\nprofiles:\n  a:\n    timeout: 10ms\nserver:\n  workers: 3\n  queue_capacity: 5\n"
	s, path := newWatchedState(t, body)
	st := s.registry.Load().Station()
	require.NotNil(t, st)
	require.Equal(t, 3, st.workers)

	writeFile(t, path, "default_policy: a\nprofiles:\n  a:\n    timeout: 10ms\nserver:\n  workers: 9\n  queue_capacity: 5\n")
	s.reload()

	after := s.registry.Load().Station()
	assert.Same(t, st, after)
	assert.Equal(t, 3, after.workers)
}

// An invalid new file keeps the OLD registry -- the opposite of startup, where
// an unloadable file panics, because a live recording must not be destroyed by
// a bad edit.
func TestReloadKeepsTheOldRegistryOnAnInvalidFile(t *testing.T) {
	s, path := newWatchedState(t, sprintfConfig(twoProfileConfig, "10ms"))
	before := s.registry.Load()

	for _, bad := range []string{
		"default_policy: keep\nprofiles:\n  keep:\n    timeout: 10ms\n    nonsense: 1\n", // unknown key
		"default_policy: missing\nprofiles: {}\n",                                        // unknown default
		"::::not yaml::::\n", // not YAML at all
		"default_policy: keep\nprofiles:\n  keep:\n    timeout: 0s\n", // fails validation
	} {
		writeFile(t, path, bad)
		s.reload()
		assert.Same(t, before, s.registry.Load(), "registry must not change for %q", bad)
	}

	events := readRecords(t, s.log.Path())
	require.Len(t, events, 4)
	for _, e := range events {
		assert.Equal(t, eventPolicyReloadFailed, e["name"])
	}
}

// A rewrite with identical bytes is not a reload: the harness asserts on the
// event, so it must mean something.
func TestReloadIgnoresAContentIdenticalRewrite(t *testing.T) {
	body := sprintfConfig(twoProfileConfig, "10ms")
	s, path := newWatchedState(t, body)
	before := s.registry.Load()
	time.Sleep(10 * time.Millisecond)
	writeFile(t, path, body)
	s.reload()
	assert.Same(t, before, s.registry.Load())
	assert.Empty(t, readRecords(t, s.log.Path()))
}

// The watcher polls mtime every 200 ms and must notice a rewrite well within
// one second.
func TestFileWatcherDetectsARewrite(t *testing.T) {
	s, path := newLiveState(t, sprintfConfig(twoProfileConfig, "10ms"))
	writeFile(t, path, sprintfConfig(twoProfileConfig, "123ms"))

	require.Eventually(t, func() bool {
		return s.registry.Load().Lookup("", testMethod).timeout == 123*time.Millisecond
	}, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		recs := readRecords(t, s.log.Path())
		return len(recs) == 1 && recs[0]["name"] == eventPolicyReload
	}, time.Second, 10*time.Millisecond)
}

// SIGHUP triggers the same path as the file watcher.
func TestSighupTriggersAReload(t *testing.T) {
	s, path := newLiveState(t, sprintfConfig(twoProfileConfig, "10ms"))
	// Stop the poller from racing us to the change by writing and immediately
	// signalling; whichever wins, the assertion below is the same.
	writeFile(t, path, sprintfConfig(twoProfileConfig, "321ms"))
	require.NoError(t, syscall.Kill(syscall.Getpid(), syscall.SIGHUP))

	require.Eventually(t, func() bool {
		return s.registry.Load().Lookup("", testMethod).timeout == 321*time.Millisecond
	}, 2*time.Second, 5*time.Millisecond)
}

// A configured-but-unloadable file panics at STARTUP.
func TestInitializePanicsOnAnUnloadableConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	writeFile(t, path, "default_policy: missing\nprofiles: {}\n")
	t.Setenv("FAULT_CONFIG_PATH", "")
	t.Setenv("FAULT_EPOCH_MS", "")
	assert.Panics(t, func() { initialize(path, filepath.Join(dir, "logs"), "svc-boom") })

	assert.Panics(t, func() { initialize(filepath.Join(dir, "absent.yaml"), filepath.Join(dir, "logs"), "svc-boom") })
}

func sprintfConfig(format string, arg string) string {
	return fmt.Sprintf(format, arg)
}
