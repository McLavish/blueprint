package rpcpolicy

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// The policy YAML of docs/CONTRACTS.md §2, decoded strictly. Ported from
// hotelreservation-experiments/resilience/config.go, with msim-native parameter
// names so the interceptor and the matched simulator read the same numbers with
// no mapping in between.

// Config is one policy file: one document, one container.
type Config struct {
	DefaultPolicy  string                   `yaml:"default_policy"`
	Routes         map[string]string        `yaml:"routes"`
	Profiles       map[string]ProfileConfig `yaml:"profiles"`
	Server         *ServerConfig            `yaml:"server"`
	MethodPolicies map[string]string        `yaml:"method_policies"`
}

// ProfileConfig is one named policy stack. Composition is outermost first:
// rate_limiter > circuit_breaker > budget > retry.
type ProfileConfig struct {
	Timeout       Duration `yaml:"timeout"`
	GlobalTimeout Duration `yaml:"global_timeout"`

	Retry          *RetryConfig          `yaml:"retry"`
	Budget         *BudgetConfig         `yaml:"budget"`
	CircuitBreaker *CircuitBreakerConfig `yaml:"circuit_breaker"`
	RateLimiter    *RateLimiterConfig    `yaml:"rate_limiter"`
}

// RetryConfig is msim's FixedBackoff / ExponentialBackoff(+Jitter) policies.
// Absent, or enabled:false, means NoRetry (allowance 1).
type RetryConfig struct {
	Enabled      bool     `yaml:"enabled"`
	Kind         string   `yaml:"kind"` // fixed | exponential
	MaxAttempts  int      `yaml:"max_attempts"`
	Delay        Duration `yaml:"delay"`
	InitialDelay Duration `yaml:"initial_delay"`
	MaxDelay     Duration `yaml:"max_delay"`
	JitterMode   string   `yaml:"jitter_mode"` // none | full | equal
	RetryOn      []string `yaml:"retry_on"`
}

// BudgetConfig is msim's RetryBudgetPolicy.
type BudgetConfig struct {
	Enabled     bool    `yaml:"enabled"`
	BudgetRatio float64 `yaml:"budget_ratio"`
	MaxRetries  int     `yaml:"max_retries"`
}

// CircuitBreakerConfig carries both msim breakers; which fields apply is
// decided by Kind.
type CircuitBreakerConfig struct {
	Enabled bool   `yaml:"enabled"`
	Kind    string `yaml:"kind"` // count | time

	// kind: count
	FailureThresholdRatio []int `yaml:"failure_threshold_ratio"`
	SuccessThresholdRatio []int `yaml:"success_threshold_ratio"`

	// kind: time
	FailureThresholdRate float64  `yaml:"failure_threshold_rate"`
	SuccessThresholdRate float64  `yaml:"success_threshold_rate"`
	MinRequests          int      `yaml:"min_requests"`
	WindowDuration       Duration `yaml:"window_duration"`
	HalfOpenMinRequests  int      `yaml:"half_open_min_requests"`

	// both kinds
	HalfOpenDelay Duration `yaml:"half_open_delay"`
}

// RateLimiterConfig carries the three msim limiters. `leaky` REPLACES the retry
// block, as in msim, where LeakyRateLimiterPolicy has no underlying strategy.
type RateLimiterConfig struct {
	Enabled     bool     `yaml:"enabled"`
	Kind        string   `yaml:"kind"` // leaky | bursty | fixed_window
	MaxRequests int      `yaml:"max_requests"`
	Period      Duration `yaml:"period"`
	RefillRate  float64  `yaml:"refill_rate"`  // bursty only
	MaxAttempts int      `yaml:"max_attempts"` // leaky only
}

// ServerConfig configures the admission station. Absent means no station at
// all (a goroutine per request, upstream gRPC behaviour).
type ServerConfig struct {
	Workers                 int  `yaml:"workers"`
	QueueCapacity           *int `yaml:"queue_capacity"` // null = unbounded
	StripInboundDeadline    bool `yaml:"strip_inbound_deadline"`
	HoldPermitThroughFanout bool `yaml:"hold_permit_through_fanout"`
}

// Retry kinds.
const (
	retryKindFixed       = "fixed"
	retryKindExponential = "exponential"
)

// Breaker kinds.
const (
	breakerKindCount = "count"
	breakerKindTime  = "time"
)

// Limiter kinds.
const (
	limiterKindLeaky       = "leaky"
	limiterKindBursty      = "bursty"
	limiterKindFixedWindow = "fixed_window"
)

// defaultRetryOn is CONTRACTS.md §2's documented default when retry_on is
// absent.
var defaultRetryOn = []string{"Unavailable", "DeadlineExceeded", "ResourceExhausted", "Aborted"}

// LoadConfig reads and validates a policy file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseConfig(data, path)
}

// ParseConfig decodes and validates policy YAML that is already in memory.
func ParseConfig(data []byte, path string) (*Config, error) {
	var cfg Config
	if err := decodeStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("rpcpolicy: %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("rpcpolicy: %s: %w", path, err)
	}
	return &cfg, nil
}

// decodeStrict decodes exactly one YAML document into out, rejecting any key
// that does not map to a struct field.
//
// yaml.Unmarshal drops an unknown key silently, so `enabeld: true` would leave
// the policy at its zero value and `method_polices:` would drop every binding
// to default_policy -- both load clean and make a run measure a configuration
// nobody asked for. A trailing document is likewise discarded by a single
// Unmarshal, so reject that too. Strictness is also what rejects the
// `bulkhead:` block the failsafe-go schema used to carry: this package has no
// bulkhead, because msim has none.
func decodeStrict(data []byte, out interface{}) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	// io.EOF means an empty document; leave out at its zero value and let
	// Validate report it ("default_policy is required").
	if err := dec.Decode(out); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if err := dec.Decode(new(interface{})); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return errors.New("expected a single YAML document")
	}
	return nil
}

// requireFinite rejects the values every range check below silently admits:
// NaN compares false against both bounds, and an infinity clears any one-sided
// bound. YAML spells both (.nan/.inf).
func requireFinite(field string, v float64) error {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Errorf("%s must be a finite number (got %v): YAML .nan/.inf compare false against every range check", field, v)
	}
	return nil
}

// Validate applies every rule of CONTRACTS.md §2. Profiles are validated in
// sorted order so a file with several bad profiles always names the same one.
func (c *Config) Validate() error {
	if c.DefaultPolicy == "" {
		return errors.New("default_policy is required")
	}
	if _, ok := c.Profiles[c.DefaultPolicy]; !ok {
		return fmt.Errorf("default_policy %q does not exist", c.DefaultPolicy)
	}
	names := make([]string, 0, len(c.Profiles))
	for name := range c.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := validateProfile(c.Profiles[name]); err != nil {
			return fmt.Errorf("profile %q: %w", name, err)
		}
	}
	methods := make([]string, 0, len(c.MethodPolicies))
	for m := range c.MethodPolicies {
		methods = append(methods, m)
	}
	sort.Strings(methods)
	for _, method := range methods {
		if err := validateBindingKey(method); err != nil {
			return err
		}
		if _, ok := c.Profiles[c.MethodPolicies[method]]; !ok {
			return fmt.Errorf("method %q references unknown profile %q", method, c.MethodPolicies[method])
		}
	}
	if c.Server != nil {
		if c.Server.Workers < 1 {
			return fmt.Errorf("server workers must be >= 1, got %d", c.Server.Workers)
		}
		if c.Server.QueueCapacity != nil && *c.Server.QueueCapacity < 0 {
			return fmt.Errorf("server queue_capacity must be >= 0 or null (unbounded), got %d", *c.Server.QueueCapacity)
		}
	}
	return nil
}

// validateBindingKey enforces CONTRACTS.md §2's key-prefix rule: a
// method_policies key is either a bare gRPC full method ("/grpc.<Iface>/<M>")
// or a route-scoped one ("<route>|/grpc.<Iface>/<M>"). Anything else is a typo
// that would bind nothing and silently fall through to default_policy.
func validateBindingKey(key string) error {
	method := key
	if i := strings.Index(key, "|"); i >= 0 {
		route := key[:i]
		method = key[i+1:]
		if route == "" {
			return fmt.Errorf("method_policies key %q has an empty route before %q", key, "|")
		}
	}
	if !strings.HasPrefix(method, "/grpc.") {
		return fmt.Errorf(`method_policies key %q must be "/grpc.<Iface>/<Method>" or "<route>|/grpc.<Iface>/<Method>"`, key)
	}
	if strings.Count(method, "/") != 2 || strings.HasSuffix(method, "/") {
		return fmt.Errorf(`method_policies key %q must be "/grpc.<Iface>/<Method>" or "<route>|/grpc.<Iface>/<Method>"`, key)
	}
	return nil
}

func validateProfile(p ProfileConfig) error {
	if p.Timeout.Duration() <= 0 {
		return errors.New("timeout must be > 0")
	}
	if p.GlobalTimeout.Duration() < 0 {
		return errors.New("global_timeout must be >= 0 (0s means no root deadline)")
	}
	if err := validateRetry(p.Retry); err != nil {
		return err
	}
	if err := validateBudget(p.Budget); err != nil {
		return err
	}
	if err := validateBreaker(p.CircuitBreaker); err != nil {
		return err
	}
	if err := validateLimiter(p.RateLimiter); err != nil {
		return err
	}
	// msim's LeakyRateLimiterPolicy IS the retry policy -- it carries
	// max_attempts itself and has no underlying strategy -- so a profile that
	// configures both is asking for two different allowances at once.
	if p.RateLimiter != nil && p.RateLimiter.Enabled && p.RateLimiter.Kind == limiterKindLeaky &&
		p.Retry != nil && p.Retry.Enabled {
		return errors.New("rate_limiter kind leaky replaces the retry block (msim's LeakyRateLimiterPolicy has no underlying strategy); enable one or the other")
	}
	return nil
}

func validateRetry(r *RetryConfig) error {
	if r == nil || !r.Enabled {
		return nil
	}
	switch r.Kind {
	case retryKindFixed, retryKindExponential:
	default:
		return fmt.Errorf("retry kind must be %q or %q, got %q", retryKindFixed, retryKindExponential, r.Kind)
	}
	if r.MaxAttempts < 1 {
		return fmt.Errorf("retry max_attempts must be >= 1 (total attempts including the first), got %d", r.MaxAttempts)
	}
	switch r.Kind {
	case retryKindFixed:
		if r.Delay.Duration() < 0 {
			return fmt.Errorf("retry delay must be >= 0, got %v", r.Delay.Duration())
		}
	case retryKindExponential:
		if r.InitialDelay.Duration() < 0 || r.MaxDelay.Duration() < 0 {
			return fmt.Errorf("retry initial_delay and max_delay must be >= 0, got %v and %v",
				r.InitialDelay.Duration(), r.MaxDelay.Duration())
		}
	}
	switch r.JitterMode {
	case "", "none", "full", "equal":
	default:
		return fmt.Errorf("retry jitter_mode must be none, full or equal, got %q", r.JitterMode)
	}
	if r.JitterMode == "full" || r.JitterMode == "equal" {
		if r.Kind != retryKindExponential {
			return fmt.Errorf("retry jitter_mode %q applies to kind exponential only", r.JitterMode)
		}
	}
	for _, name := range r.RetryOn {
		if _, ok := parseCode(name); !ok {
			return fmt.Errorf("retry retry_on contains %q, which is not a gRPC status name", name)
		}
	}
	return nil
}

func validateBudget(b *BudgetConfig) error {
	if b == nil || !b.Enabled {
		return nil
	}
	if err := requireFinite("budget budget_ratio", b.BudgetRatio); err != nil {
		return err
	}
	if !(b.BudgetRatio > 0 && b.BudgetRatio <= 1) {
		return fmt.Errorf("budget budget_ratio must be in (0, 1], got %v", b.BudgetRatio)
	}
	if b.MaxRetries < 1 {
		return fmt.Errorf("budget max_retries must be >= 1, got %d", b.MaxRetries)
	}
	return nil
}

func validateBreaker(b *CircuitBreakerConfig) error {
	if b == nil || !b.Enabled {
		return nil
	}
	if b.HalfOpenDelay.Duration() < 0 {
		return fmt.Errorf("circuit_breaker half_open_delay must be >= 0, got %v", b.HalfOpenDelay.Duration())
	}
	switch b.Kind {
	case breakerKindCount:
		f, err := ratioPair("failure_threshold_ratio", b.FailureThresholdRatio)
		if err != nil {
			return err
		}
		s, err := ratioPair("success_threshold_ratio", b.SuccessThresholdRatio)
		if err != nil {
			return err
		}
		if _, err := NewCountBasedCircuitBreakerPolicy(f, s, b.HalfOpenDelay.Nanos(), NewNoRetryPolicy()); err != nil {
			return fmt.Errorf("circuit_breaker %w", err)
		}
	case breakerKindTime:
		if err := requireFinite("circuit_breaker failure_threshold_rate", b.FailureThresholdRate); err != nil {
			return err
		}
		if err := requireFinite("circuit_breaker success_threshold_rate", b.SuccessThresholdRate); err != nil {
			return err
		}
		halfOpenMin := b.HalfOpenMinRequests
		if halfOpenMin == 0 {
			halfOpenMin = 1 // msim's default
		}
		if _, err := NewTimeBasedCircuitBreakerPolicy(b.FailureThresholdRate, b.SuccessThresholdRate,
			b.MinRequests, b.WindowDuration.Nanos(), b.HalfOpenDelay.Nanos(), halfOpenMin, NewNoRetryPolicy()); err != nil {
			return fmt.Errorf("circuit_breaker %w", err)
		}
	default:
		return fmt.Errorf("circuit_breaker kind must be %q or %q, got %q", breakerKindCount, breakerKindTime, b.Kind)
	}
	return nil
}

func ratioPair(field string, v []int) ([2]int, error) {
	if len(v) != 2 {
		return [2]int{}, fmt.Errorf("circuit_breaker %s must be a two-element list [count, total], got %v", field, v)
	}
	return [2]int{v[0], v[1]}, nil
}

func validateLimiter(l *RateLimiterConfig) error {
	if l == nil || !l.Enabled {
		return nil
	}
	switch l.Kind {
	case limiterKindLeaky:
		if _, err := NewLeakyRateLimiterPolicy(l.MaxRequests, l.MaxAttempts, l.Period.Nanos()); err != nil {
			return fmt.Errorf("rate_limiter %w", err)
		}
	case limiterKindBursty:
		if err := requireFinite("rate_limiter refill_rate", l.RefillRate); err != nil {
			return err
		}
		if _, err := NewBurstyRateLimiterPolicy(l.MaxRequests, l.RefillRate, l.Period.Nanos(), NewNoRetryPolicy()); err != nil {
			return fmt.Errorf("rate_limiter %w", err)
		}
	case limiterKindFixedWindow:
		if _, err := NewFixedWindowBurstyLimiterPolicy(l.MaxRequests, l.Period.Nanos(), NewNoRetryPolicy()); err != nil {
			return fmt.Errorf("rate_limiter %w", err)
		}
	default:
		return fmt.Errorf("rate_limiter kind must be %q, %q or %q, got %q",
			limiterKindLeaky, limiterKindBursty, limiterKindFixedWindow, l.Kind)
	}
	return nil
}
