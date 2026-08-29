package rpcpolicy

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The full CONTRACTS.md §2 policy file, used as the base every negative case
// mutates.
const fullConfigYAML = `
default_policy: none
routes:
  SearchHandler: hotels
profiles:
  none:
    timeout: 50ms
    global_timeout: 0s
  top:
    timeout: 50ms
    global_timeout: 10s
    retry:
      enabled: true
      kind: exponential
      max_attempts: 3
      initial_delay: 25ms
      max_delay: 200ms
      jitter_mode: equal
      retry_on: [Unavailable, DeadlineExceeded]
    budget:
      enabled: true
      budget_ratio: 0.1
      max_retries: 5
    circuit_breaker:
      enabled: true
      kind: count
      failure_threshold_ratio: [5, 10]
      success_threshold_ratio: [2, 2]
      half_open_delay: 5s
    rate_limiter:
      enabled: true
      kind: bursty
      max_requests: 200
      period: 1s
      refill_rate: 200.0
server:
  workers: 16
  queue_capacity: 20
  strip_inbound_deadline: false
  hold_permit_through_fanout: false
method_policies:
  "/grpc.NodeService/Call": top
  "hotels|/grpc.NodeService/Call": none
`

func TestLoadConfigParsesTheFullSchema(t *testing.T) {
	cfg, err := ParseConfig([]byte(fullConfigYAML), "test.yaml")
	require.NoError(t, err)
	assert.Equal(t, "none", cfg.DefaultPolicy)
	assert.Equal(t, map[string]string{"SearchHandler": "hotels"}, cfg.Routes)

	top := cfg.Profiles["top"]
	assert.Equal(t, 50*time.Millisecond, top.Timeout.Duration())
	assert.Equal(t, 10*time.Second, top.GlobalTimeout.Duration())
	require.NotNil(t, top.Retry)
	assert.Equal(t, "exponential", top.Retry.Kind)
	assert.Equal(t, 3, top.Retry.MaxAttempts)
	assert.Equal(t, 25*time.Millisecond, top.Retry.InitialDelay.Duration())
	assert.Equal(t, "equal", top.Retry.JitterMode)
	assert.Equal(t, []string{"Unavailable", "DeadlineExceeded"}, top.Retry.RetryOn)
	require.NotNil(t, top.Budget)
	assert.InDelta(t, 0.1, top.Budget.BudgetRatio, 1e-12)
	require.NotNil(t, top.CircuitBreaker)
	assert.Equal(t, []int{5, 10}, top.CircuitBreaker.FailureThresholdRatio)
	assert.Equal(t, 5*time.Second, top.CircuitBreaker.HalfOpenDelay.Duration())
	require.NotNil(t, top.RateLimiter)
	assert.Equal(t, "bursty", top.RateLimiter.Kind)

	require.NotNil(t, cfg.Server)
	assert.Equal(t, 16, cfg.Server.Workers)
	require.NotNil(t, cfg.Server.QueueCapacity)
	assert.Equal(t, 20, *cfg.Server.QueueCapacity)
}

func TestLoadConfigCompilesTheProfileChainOutermostFirst(t *testing.T) {
	cfg, err := ParseConfig([]byte(fullConfigYAML), "test.yaml")
	require.NoError(t, err)
	p, err := buildProfile("top", cfg.Profiles["top"])
	require.NoError(t, err)
	chain := policyChain(p.head)
	require.Len(t, chain, 4)
	assert.IsType(t, &BurstyRateLimiterPolicy{}, chain[0])
	assert.IsType(t, &CountBasedCircuitBreakerPolicy{}, chain[1])
	assert.IsType(t, &RetryBudgetPolicy{}, chain[2])
	assert.IsType(t, &ExponentialBackoffWithJitterRetryPolicy{}, chain[3])
}

func TestLoadConfigRejectsUnknownKeys(t *testing.T) {
	for name, doc := range map[string]string{
		"top level":  "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nnonsense: 1\n",
		"profile":    "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    enabeld: true\n",
		"retry typo": "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    retry:\n      enabeld: true\n",
		"server":     "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  spin: true\n",
		// docs/PLAN.md: there is no bulkhead stage, because msim has none. The
		// old failsafe-go schema carried one, so strictness must reject it
		// rather than let a stale profile load with the block ignored.
		"bulkhead": "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    bulkhead:\n      enabled: true\n      max_concurrent: 4\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseConfig([]byte(doc), "test.yaml")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "field")
		})
	}
}

func TestLoadConfigRejectsATrailingDocument(t *testing.T) {
	doc := "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n---\ndefault_policy: b\n"
	_, err := ParseConfig([]byte(doc), "test.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "single YAML document")
}

func TestLoadConfigRejectsAnEmptyDocument(t *testing.T) {
	_, err := ParseConfig(nil, "test.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "default_policy is required")
}

func TestLoadConfigRejectsAnUnknownDefaultPolicy(t *testing.T) {
	doc := "default_policy: missing\nprofiles:\n  a:\n    timeout: 1s\n"
	_, err := ParseConfig([]byte(doc), "test.yaml")
	assert.ErrorContains(t, err, `default_policy "missing" does not exist`)
}

func TestLoadConfigRejectsAnUnknownProfileReference(t *testing.T) {
	doc := "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nmethod_policies:\n  \"/grpc.S/M\": nope\n"
	_, err := ParseConfig([]byte(doc), "test.yaml")
	assert.ErrorContains(t, err, `unknown profile "nope"`)
}

// The key-prefix rule of CONTRACTS.md §2: a typo that binds nothing would fall
// through to default_policy and silently measure the wrong policy.
func TestLoadConfigEnforcesTheMethodPolicyKeyPrefixRule(t *testing.T) {
	base := "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nmethod_policies:\n  %q: a\n"
	for _, key := range []string{
		"grpc.NodeService/Call",   // no leading slash
		"/NodeService/Call",       // not the grpc. namespace
		"/grpc.NodeService",       // no method
		"/grpc.NodeService/",      // empty method
		"/grpc.A/B/C",             // too many segments
		"|/grpc.NodeService/Call", // empty route
	} {
		t.Run(key, func(t *testing.T) {
			_, err := ParseConfig(fmtDoc(base, key), "test.yaml")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "method_policies key")
		})
	}
	for _, key := range []string{"/grpc.NodeService/Call", "root|/grpc.NodeService/Call"} {
		t.Run("valid "+key, func(t *testing.T) {
			_, err := ParseConfig(fmtDoc(base, key), "test.yaml")
			require.NoError(t, err)
		})
	}
}

func TestLoadConfigRejectsLeakyTogetherWithRetry(t *testing.T) {
	doc := `
default_policy: a
profiles:
  a:
    timeout: 1s
    retry:
      enabled: true
      kind: fixed
      max_attempts: 3
      delay: 10ms
    rate_limiter:
      enabled: true
      kind: leaky
      max_requests: 10
      max_attempts: 3
      period: 1s
`
	_, err := ParseConfig([]byte(doc), "test.yaml")
	assert.ErrorContains(t, err, "leaky replaces the retry block")
}

func TestLoadConfigAcceptsLeakyAlone(t *testing.T) {
	doc := `
default_policy: a
profiles:
  a:
    timeout: 1s
    rate_limiter:
      enabled: true
      kind: leaky
      max_requests: 10
      max_attempts: 3
      period: 1s
`
	cfg, err := ParseConfig([]byte(doc), "test.yaml")
	require.NoError(t, err)
	p, err := buildProfile("a", cfg.Profiles["a"])
	require.NoError(t, err)
	chain := policyChain(p.head)
	require.Len(t, chain, 1)
	assert.IsType(t, &LeakyRateLimiterPolicy{}, chain[0], "leaky REPLACES retry rather than wrapping it")
}

// queue_capacity: null is the unbounded queue; the key being absent means the
// same thing, and 0 means "no queue at all".
func TestLoadConfigDistinguishesNullFromZeroQueueCapacity(t *testing.T) {
	null := "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 2\n  queue_capacity: null\n"
	cfg, err := ParseConfig([]byte(null), "test.yaml")
	require.NoError(t, err)
	assert.Nil(t, cfg.Server.QueueCapacity)

	zero := "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 2\n  queue_capacity: 0\n"
	cfg, err = ParseConfig([]byte(zero), "test.yaml")
	require.NoError(t, err)
	require.NotNil(t, cfg.Server.QueueCapacity)
	assert.Equal(t, 0, *cfg.Server.QueueCapacity)

	absent := "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 2\n"
	cfg, err = ParseConfig([]byte(absent), "test.yaml")
	require.NoError(t, err)
	assert.Nil(t, cfg.Server.QueueCapacity)
}

func TestValidateAppliesEveryRule(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want string
	}{
		{"timeout zero", "default_policy: a\nprofiles:\n  a:\n    timeout: 0s\n", "timeout must be > 0"},
		{"timeout negative", "default_policy: a\nprofiles:\n  a:\n    timeout: -1s\n", "timeout must be > 0"},
		{"global_timeout negative", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    global_timeout: -1s\n", "global_timeout must be >= 0"},
		{"retry kind", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    retry:\n      enabled: true\n      kind: linear\n      max_attempts: 2\n", "retry kind must be"},
		{"retry max_attempts", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    retry:\n      enabled: true\n      kind: fixed\n      max_attempts: 0\n", "retry max_attempts must be >= 1"},
		{"retry delay", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    retry:\n      enabled: true\n      kind: fixed\n      max_attempts: 2\n      delay: -1ms\n", "retry delay must be >= 0"},
		{"retry exponential delays", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    retry:\n      enabled: true\n      kind: exponential\n      max_attempts: 2\n      initial_delay: -1ms\n", "must be >= 0"},
		{"retry jitter_mode", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    retry:\n      enabled: true\n      kind: exponential\n      max_attempts: 2\n      jitter_mode: decorrelated\n", "jitter_mode must be none, full or equal"},
		{"retry jitter on fixed", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    retry:\n      enabled: true\n      kind: fixed\n      max_attempts: 2\n      jitter_mode: full\n", "applies to kind exponential only"},
		{"retry_on unknown", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    retry:\n      enabled: true\n      kind: fixed\n      max_attempts: 2\n      retry_on: [Nope]\n", "not a gRPC status name"},
		{"budget ratio", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    budget:\n      enabled: true\n      budget_ratio: 0\n      max_retries: 1\n", "budget_ratio must be in (0, 1]"},
		{"budget ratio nan", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    budget:\n      enabled: true\n      budget_ratio: .nan\n      max_retries: 1\n", "finite"},
		{"budget max_retries", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    budget:\n      enabled: true\n      budget_ratio: 0.1\n      max_retries: 0\n", "max_retries must be >= 1"},
		{"breaker kind", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    circuit_breaker:\n      enabled: true\n      kind: adaptive\n", "circuit_breaker kind must be"},
		{"breaker ratio shape", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    circuit_breaker:\n      enabled: true\n      kind: count\n      failure_threshold_ratio: [1]\n      success_threshold_ratio: [1, 1]\n      half_open_delay: 1s\n", "two-element list"},
		{"breaker ratio range", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    circuit_breaker:\n      enabled: true\n      kind: count\n      failure_threshold_ratio: [3, 2]\n      success_threshold_ratio: [1, 1]\n      half_open_delay: 1s\n", "failure_threshold_ratio"},
		{"breaker half_open_delay", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    circuit_breaker:\n      enabled: true\n      kind: count\n      failure_threshold_ratio: [1, 1]\n      success_threshold_ratio: [1, 1]\n      half_open_delay: -1s\n", "half_open_delay must be >= 0"},
		{"breaker rate", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    circuit_breaker:\n      enabled: true\n      kind: time\n      failure_threshold_rate: 1.5\n      success_threshold_rate: 0.5\n      min_requests: 2\n      window_duration: 1s\n      half_open_delay: 1s\n", "failure_threshold_rate"},
		{"breaker rate nan", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    circuit_breaker:\n      enabled: true\n      kind: time\n      failure_threshold_rate: .nan\n      success_threshold_rate: 0.5\n      min_requests: 2\n      window_duration: 1s\n      half_open_delay: 1s\n", "finite"},
		{"breaker window", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    circuit_breaker:\n      enabled: true\n      kind: time\n      failure_threshold_rate: 0.5\n      success_threshold_rate: 0.5\n      min_requests: 2\n      window_duration: 0s\n      half_open_delay: 1s\n", "window_duration must be > 0"},
		{"breaker min_requests", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    circuit_breaker:\n      enabled: true\n      kind: time\n      failure_threshold_rate: 0.5\n      success_threshold_rate: 0.5\n      min_requests: 0\n      window_duration: 1s\n      half_open_delay: 1s\n", "min_requests must be >= 1"},
		{"limiter kind", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    rate_limiter:\n      enabled: true\n      kind: smooth\n", "rate_limiter kind must be"},
		{"limiter max_requests", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    rate_limiter:\n      enabled: true\n      kind: bursty\n      max_requests: 0\n      period: 1s\n      refill_rate: 1\n", "max_requests"},
		{"limiter period", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    rate_limiter:\n      enabled: true\n      kind: fixed_window\n      max_requests: 1\n      period: 0s\n", "period must be > 0"},
		{"limiter refill nan", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    rate_limiter:\n      enabled: true\n      kind: bursty\n      max_requests: 1\n      period: 1s\n      refill_rate: .inf\n", "finite"},
		{"leaky max_attempts", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\n    rate_limiter:\n      enabled: true\n      kind: leaky\n      max_requests: 1\n      period: 1s\n      max_attempts: 0\n", "max_attempts"},
		{"server workers", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 0\n", "workers must be >= 1"},
		{"server queue_capacity", "default_policy: a\nprofiles:\n  a:\n    timeout: 1s\nserver:\n  workers: 1\n  queue_capacity: -1\n", "queue_capacity must be >= 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseConfig([]byte(tc.doc), "test.yaml")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// A disabled block is inert: no field of it is validated, and the profile
// compiles to NoRetry.
func TestDisabledBlocksAreInert(t *testing.T) {
	doc := `
default_policy: a
profiles:
  a:
    timeout: 1s
    retry:
      enabled: false
      kind: nonsense
      max_attempts: -3
    budget:
      enabled: false
      budget_ratio: 99
      max_retries: -1
    circuit_breaker:
      enabled: false
      kind: nonsense
    rate_limiter:
      enabled: false
      kind: nonsense
`
	cfg, err := ParseConfig([]byte(doc), "test.yaml")
	require.NoError(t, err)
	p, err := buildProfile("a", cfg.Profiles["a"])
	require.NoError(t, err)
	assert.IsType(t, &NoRetryPolicy{}, p.head)
	assert.Equal(t, len(defaultRetryOn), len(p.retryOn))
}

func TestDurationUnmarshalYAML(t *testing.T) {
	doc := "default_policy: a\nprofiles:\n  a:\n    timeout: 1500us\n"
	cfg, err := ParseConfig([]byte(doc), "test.yaml")
	require.NoError(t, err)
	assert.Equal(t, 1500*time.Microsecond, cfg.Profiles["a"].Timeout.Duration())

	bad := "default_policy: a\nprofiles:\n  a:\n    timeout: 50\n"
	_, err = ParseConfig([]byte(bad), "test.yaml")
	require.Error(t, err)
}

func TestLoadConfigReadsAFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte(fullConfigYAML), 0o644))
	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "none", cfg.DefaultPolicy)

	_, err = LoadConfig(filepath.Join(dir, "missing.yaml"))
	require.Error(t, err)
}

func fmtDoc(base string, key string) []byte {
	return []byte(fmt.Sprintf(base, key))
}
