package predicted

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The node config is the one place where a typo silently changes a published
// number: a dropped `median_ms` leaves the median at 0 and the node answers
// instantly while the recording still claims a 20 ms service time. So the
// loader is strict, and these tests pin what "strict" means.

const validNodeYAML = `service:
  name: svc-A
  median_ms: 20.0
  sigma: 0.5
  mode: sleep
  floor_us: 100
  fanout: serial
`

func TestParseNodeConfigValid(t *testing.T) {
	cfg, err := ParseNodeConfig([]byte(validNodeYAML), "node.yaml")
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if cfg.Service.Name != "svc-A" {
		t.Errorf("name = %q, want svc-A", cfg.Service.Name)
	}
	if cfg.Service.MedianMS != 20.0 {
		t.Errorf("median_ms = %v, want 20", cfg.Service.MedianMS)
	}
	if cfg.Service.Sigma != 0.5 {
		t.Errorf("sigma = %v, want 0.5", cfg.Service.Sigma)
	}
	if cfg.Service.FloorUS != 100 {
		t.Errorf("floor_us = %v, want 100", cfg.Service.FloorUS)
	}
}

// The file the campaign actually deploys must load.
func TestParseNodeConfigBenchmarkFile(t *testing.T) {
	// benchmarks/single/node/node.yaml, five directories up from this package.
	path := filepath.Join("..", "..", "..", "..", "..", "..",
		"benchmarks", "single", "node", "node.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("benchmark node.yaml not readable from here: %v", err)
	}
	if _, err := ParseNodeConfig(data, path); err != nil {
		t.Fatalf("benchmarks/single/node/node.yaml does not load: %v", err)
	}
}

func TestParseNodeConfigRejects(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			"unknown key",
			strings.Replace(validNodeYAML, "median_ms: 20.0", "median: 20.0", 1),
			"field median not found",
		},
		{
			"median zero",
			strings.Replace(validNodeYAML, "median_ms: 20.0", "median_ms: 0", 1),
			"median_ms must be > 0",
		},
		{
			"median negative",
			strings.Replace(validNodeYAML, "median_ms: 20.0", "median_ms: -1", 1),
			"median_ms must be > 0",
		},
		{
			"median nan",
			strings.Replace(validNodeYAML, "median_ms: 20.0", "median_ms: .nan", 1),
			"median_ms must be > 0",
		},
		{
			"sigma negative",
			strings.Replace(validNodeYAML, "sigma: 0.5", "sigma: -0.1", 1),
			"sigma must be >= 0",
		},
		{
			"sigma inf",
			strings.Replace(validNodeYAML, "sigma: 0.5", "sigma: .inf", 1),
			"sigma must be >= 0",
		},
		{
			"floor negative",
			strings.Replace(validNodeYAML, "floor_us: 100", "floor_us: -1", 1),
			"floor_us must be >= 0",
		},
		{
			"mode spin",
			strings.Replace(validNodeYAML, "mode: sleep", "mode: spin", 1),
			`mode must be "sleep"`,
		},
		{
			"mode missing",
			strings.Replace(validNodeYAML, "  mode: sleep\n", "", 1),
			`mode must be "sleep"`,
		},
		{
			"fanout concurrent",
			strings.Replace(validNodeYAML, "fanout: serial", "fanout: concurrent", 1),
			`fanout must be "serial"`,
		},
		{
			"two documents",
			validNodeYAML + "---\n" + validNodeYAML,
			"single YAML document",
		},
		{
			"empty",
			"",
			"median_ms must be > 0",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseNodeConfig([]byte(tc.yaml), "node.yaml")
			if err == nil {
				t.Fatalf("expected an error mentioning %q, got none", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.want)
			}
		})
	}
}

func TestLoadNodeConfigEnvOverride(t *testing.T) {
	dir := t.TempDir()
	baked := filepath.Join(dir, "baked.yaml")
	mounted := filepath.Join(dir, "mounted.yaml")
	if err := os.WriteFile(baked, []byte(validNodeYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	override := strings.Replace(validNodeYAML, "median_ms: 20.0", "median_ms: 7.5", 1)
	if err := os.WriteFile(mounted, []byte(override), 0o644); err != nil {
		t.Fatal(err)
	}

	// No override: the baked path wins.
	t.Setenv(EnvNodeConfigPath, "")
	cfg, err := LoadNodeConfig(baked)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Service.MedianMS != 20.0 {
		t.Errorf("baked path: median_ms = %v, want 20", cfg.Service.MedianMS)
	}

	// NODE_CONFIG_PATH set: it wins, which is what lets `harness deploy conf`
	// change a node without recompiling the image.
	t.Setenv(EnvNodeConfigPath, mounted)
	cfg, err = LoadNodeConfig(baked)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Service.MedianMS != 7.5 {
		t.Errorf("%s: median_ms = %v, want 7.5", EnvNodeConfigPath, cfg.Service.MedianMS)
	}
}

func TestLoadNodeConfigMissingPath(t *testing.T) {
	t.Setenv(EnvNodeConfigPath, "")
	if _, err := LoadNodeConfig(""); err == nil {
		t.Fatal("expected an error when no path is configured")
	}
	if _, err := LoadNodeConfig(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected an error when the config file does not exist")
	}
}
