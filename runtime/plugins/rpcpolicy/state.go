package rpcpolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Process state: the atomically swappable registry, the attempt log, the fault
// injector, and the inert mode that keeps upstream Blueprint examples working
// unchanged.

// Environment variables (CONTRACTS.md §1).
const (
	// EnvConfig names the policy YAML. Unset => the package is inert.
	EnvConfig = "RPCPOLICY_CONFIG"
	// EnvLogDir is where attempt-log JSONL files go.
	EnvLogDir = "RPCPOLICY_LOG_DIR"
	// EnvServiceName is the `service` field of every record.
	EnvServiceName = "OTEL_SERVICE_NAME"
)

type runtimeState struct {
	inert      bool
	service    string
	configPath string
	clock      Clock
	log        *attemptLog
	engine     *engine
	faults     *faultInjector
	registry   atomic.Pointer[Registry]

	// beforeClaim is a test-only seam, nil in production and set before the
	// state is used. It runs on the goroutine that ran the work, in the one
	// window the station cannot see into: after the work stamped the instant it
	// ended at and the state it ended under, and before either is turned into a
	// claim under the station mutex. The tests hold a handler exactly there to
	// make the station's reaper win, or lose, on purpose rather than by
	// scheduling luck -- and to let the world move on inside that window (a
	// deadline passing, a context ending) where a goroutine that re-read it
	// would be reading about a different instant than the one it is claiming.
	beforeClaim func()
	// betweenCompletionReads is a second test-only seam, nil in production: it
	// runs on the work's goroutine between its two observations at the
	// handler's return -- after the context's state was read and before the
	// instant is stamped -- so a test can end the context exactly there and
	// prove that an expiry in that gap is not blamed for a completion that
	// preceded it.
	betweenCompletionReads func()

	stopReload chan struct{}
	stopOnce   sync.Once
	// watchers joins the file poller and the SIGHUP handler at shutdown, so the
	// attempt log is never closed underneath a goroutine still writing to it.
	watchers sync.WaitGroup

	// reloadMu serializes the WHOLE reload transaction (read -> hash -> parse ->
	// build -> store -> event) across the poller and the signal handler. It also
	// guards readFailed and reloadHook.
	reloadMu   sync.Mutex
	readFailed bool
	// reloadHook is a test-only barrier inside the transaction. Set before the
	// watchers start; read under reloadMu.
	reloadHook func(stage string)
}

var (
	stateMu    sync.Mutex
	stateValue atomic.Pointer[runtimeState]
)

// currentState initialises the package on first use and returns the shared
// state.
//
// Initialisation is lazy rather than in an init() so tests can drive several
// configurations in one binary; in a generated container the first call happens
// from ServerOptions/DialOptions during startup, which is where CONTRACTS.md
// §2 wants the load error to surface.
func currentState() *runtimeState {
	if s := stateValue.Load(); s != nil {
		return s
	}
	stateMu.Lock()
	defer stateMu.Unlock()
	if s := stateValue.Load(); s != nil {
		return s
	}
	s := initialize(os.Getenv(EnvConfig), os.Getenv(EnvLogDir), os.Getenv(EnvServiceName))
	stateValue.Store(s)
	return s
}

// initialize builds the process state. A configured-but-unloadable policy file
// PANICS: a run that records itself as policy-driven while no policy was ever
// applied is worse than a crash loop.
func initialize(configPath, logDir, service string) *runtimeState {
	if configPath == "" {
		return &runtimeState{inert: true}
	}
	clock := newRealClock()
	if service == "" {
		service = "unknown"
	}
	log, err := newAttemptLog(logDir, service, time.Now().UnixMilli())
	if err != nil {
		panic(fmt.Sprintf("rpcpolicy: %v", err))
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		panic(fmt.Sprintf("rpcpolicy: cannot read %s=%q: %v", EnvConfig, configPath, err))
	}
	cfg, err := ParseConfig(data, configPath)
	if err != nil {
		panic(fmt.Sprintf("rpcpolicy: %v", err))
	}
	reg, err := buildRegistry(cfg, sha256Hex(data), nil, clock, log)
	if err != nil {
		panic(fmt.Sprintf("rpcpolicy: %s: %v", configPath, err))
	}
	s := &runtimeState{
		service:    service,
		configPath: configPath,
		clock:      clock,
		log:        log,
		engine:     &engine{clock: clock, log: log, service: service},
		faults:     faultInjectorFromEnv(),
		stopReload: make(chan struct{}),
	}
	s.registry.Store(reg)
	s.startWatching()
	return s
}

// shutdown stops the reload watchers and flushes the log. Only tests call it;
// a container is killed, not asked politely.
//
// The watchers are JOINED before the log is closed. Closing first left the
// poller and the signal handler free to write a reload event into a closed log,
// which the writer accepted silently -- a record the pipeline would then never
// see, with nothing to say it had been dropped.
func (s *runtimeState) shutdown() {
	if s == nil || s.inert {
		return
	}
	s.stopOnce.Do(func() { close(s.stopReload) })
	s.watchers.Wait()
	_ = s.log.Close()
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// resetStateForTest tears the process state down and forces the next
// currentState() to re-initialise from the environment. Test-only.
func resetStateForTest() {
	stateMu.Lock()
	defer stateMu.Unlock()
	if s := stateValue.Load(); s != nil {
		s.shutdown()
	}
	stateValue.Store(nil)
}
