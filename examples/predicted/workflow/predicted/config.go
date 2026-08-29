package predicted

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"gopkg.in/yaml.v3"
)

// The node-emulation config of docs/CONTRACTS.md §3, decoded strictly.
//
// The file is MOUNTED, never baked: plugins/workflow/wiring.go turns a string
// constructor argument into an ir.IRValue, and the docker-compose plugin then
// declines to pass a config node that already has a value as an environment
// variable -- so a value passed through the wiring spec would end up inside the
// image and every sweep arm would need a recompile. The wiring spec passes a
// PATH; the numbers behind it are a container restart away.

// EnvNodeConfigPath overrides the path baked into the wiring spec
// (CONTRACTS.md §1). `harness deploy render` sets it per container.
const EnvNodeConfigPath = "NODE_CONFIG_PATH"

// NodeConfig is one node.yaml.
type NodeConfig struct {
	Service ServiceConfig `yaml:"service"`
}

// ServiceConfig is msim's ServiceConfig, restricted to what a predicted node
// needs: a lognormal service time and the two enumerations the campaign fixed
// to a single value each (PLAN.md D16, CONTRACTS.md §3).
type ServiceConfig struct {
	// Name is informational. OTEL_SERVICE_NAME is authoritative for the
	// `service` field of every attempt-log record; this is here so a mounted
	// file says out loud which node it belongs to.
	Name string `yaml:"name"`

	// MedianMS is the lognormal median in milliseconds; > 0.
	MedianMS float64 `yaml:"median_ms"`

	// Sigma is the lognormal shape; >= 0, floored at 1e-6 when drawing so a
	// configured 0 is a (numerically) deterministic service time rather than a
	// division by zero inside math.Exp's argument.
	Sigma float64 `yaml:"sigma"`

	// Mode must be "sleep". msim's service.py has no CPU model: the station
	// draws a duration and the request occupies a worker for it. A spin mode
	// would burn CPU and make service time depend on load, which is exactly the
	// coupling the matched simulation cannot express (PLAN.md D4/D16).
	Mode string `yaml:"mode"`

	// FloorUS floors every draw, as msim floors it at 100 microseconds.
	FloorUS float64 `yaml:"floor_us"`

	// Fanout must be "serial": a predicted node has zero or one downstream.
	Fanout string `yaml:"fanout"`
}

const (
	// ModeSleep is the only accepted `mode`.
	ModeSleep = "sleep"
	// FanoutSerial is the only accepted `fanout`.
	FanoutSerial = "serial"
	// MinSigma is the floor applied to sigma when drawing.
	MinSigma = 1e-6
)

// LoadNodeConfig reads the node config for this process.
//
// `path` is the value baked into the wiring spec; NODE_CONFIG_PATH overrides it
// when set to a non-empty value.
func LoadNodeConfig(path string) (*NodeConfig, error) {
	if env := os.Getenv(EnvNodeConfigPath); env != "" {
		path = env
	}
	if path == "" {
		return nil, fmt.Errorf("predicted: no node config path (set %s or pass one in the wiring spec)", EnvNodeConfigPath)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("predicted: cannot read node config %q: %w", path, err)
	}
	return ParseNodeConfig(data, path)
}

// ParseNodeConfig decodes and validates one node.yaml.
func ParseNodeConfig(data []byte, path string) (*NodeConfig, error) {
	var cfg NodeConfig
	if err := decodeStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("predicted: %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("predicted: %s: %w", path, err)
	}
	return &cfg, nil
}

// decodeStrict decodes exactly one YAML document, rejecting any key that does
// not map to a struct field.
//
// yaml.Unmarshal drops an unknown key silently, so `median: 20` (instead of
// `median_ms`) would leave the median at 0 and the node would serve every
// request instantly while the recording claimed a 20 ms service time. A second
// document would likewise be discarded, so reject that too.
func decodeStrict(data []byte, out interface{}) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
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

// Validate enforces CONTRACTS.md §3. Non-finite values are rejected explicitly:
// NaN compares false against both bounds and an infinity clears a one-sided
// one, and YAML spells both (.nan/.inf).
func (c *NodeConfig) Validate() error {
	s := c.Service
	if !isFinite(s.MedianMS) || s.MedianMS <= 0 {
		return fmt.Errorf("service.median_ms must be > 0, got %v", s.MedianMS)
	}
	if !isFinite(s.Sigma) || s.Sigma < 0 {
		return fmt.Errorf("service.sigma must be >= 0, got %v", s.Sigma)
	}
	if !isFinite(s.FloorUS) || s.FloorUS < 0 {
		return fmt.Errorf("service.floor_us must be >= 0, got %v", s.FloorUS)
	}
	if s.Mode != ModeSleep {
		return fmt.Errorf("service.mode must be %q, got %q", ModeSleep, s.Mode)
	}
	if s.Fanout != FanoutSerial {
		return fmt.Errorf("service.fanout must be %q, got %q", FanoutSerial, s.Fanout)
	}
	return nil
}

func isFinite(f float64) bool {
	return !math.IsNaN(f) && !math.IsInf(f, 0)
}
