package rpcpolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Registry is one compiled policy file: an immutable snapshot swapped
// atomically by a reload. Policy STATE (breaker windows, budget balances,
// leaky slots) lives inside the profile objects, so a reload that leaves a
// profile's bytes unchanged carries the object across and the state survives.

// Registry is the compiled form of a Config.
type Registry struct {
	cfg    *Config
	sha    string
	def    *profile
	byKey  map[string]*profile // "route|method" and bare "method"
	byName map[string]*profile
	hashes map[string]string // profile name -> sha256 of its serialized block
	routes map[string]string // HTTP handler name -> route key
	server *ServerConfig
	// station is shared across reloads: `server:` is not hot-reloaded, because
	// resizing a live queue mid-recording would change the capacity the
	// calibration recovered.
	station *station
}

// buildRegistry compiles cfg. prev may be nil; when it is not, profiles whose
// serialized block is byte-identical are carried over untouched.
func buildRegistry(cfg *Config, sha string, prev *Registry, clock Clock) (*Registry, error) {
	r := &Registry{
		cfg:    cfg,
		sha:    sha,
		byKey:  make(map[string]*profile, len(cfg.MethodPolicies)),
		byName: make(map[string]*profile, len(cfg.Profiles)),
		hashes: make(map[string]string, len(cfg.Profiles)),
		routes: cfg.Routes,
		server: cfg.Server,
	}
	for name, pc := range cfg.Profiles {
		h, err := profileHash(pc)
		if err != nil {
			return nil, err
		}
		r.hashes[name] = h
		if prev != nil && prev.hashes[name] == h {
			if kept, ok := prev.byName[name]; ok {
				r.byName[name] = kept
				continue
			}
		}
		p, err := buildProfile(name, pc)
		if err != nil {
			return nil, err
		}
		r.byName[name] = p
	}
	def, ok := r.byName[cfg.DefaultPolicy]
	if !ok {
		return nil, fmt.Errorf("default_policy %q does not exist", cfg.DefaultPolicy)
	}
	r.def = def
	for key, profileName := range cfg.MethodPolicies {
		p, ok := r.byName[profileName]
		if !ok {
			return nil, fmt.Errorf("method %q references unknown profile %q", key, profileName)
		}
		r.byKey[key] = p
	}

	switch {
	case prev != nil && prev.station != nil:
		// server: is not hot-reloaded.
		r.station = prev.station
		r.server = prev.server
	case cfg.Server != nil:
		r.station = newStation(cfg.Server, clock)
	}
	return r, nil
}

// profileHash is the identity a reload compares. The struct is re-serialized
// rather than hashing the raw file text so that a whitespace or key-order edit
// which changes nothing semantically also preserves the profile's live state.
func profileHash(pc ProfileConfig) (string, error) {
	b, err := yaml.Marshal(pc)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// Lookup resolves a profile: "route|method", then bare "method", then
// default_policy (CONTRACTS.md §2).
func (r *Registry) Lookup(route, method string) *profile {
	if r == nil {
		return nil
	}
	if route != "" {
		if p, ok := r.byKey[route+"|"+method]; ok {
			return p
		}
	}
	if p, ok := r.byKey[method]; ok {
		return p
	}
	return r.def
}

// RouteFor maps an HTTP path to the route key stamped into metadata: the
// lowercased path without its leading "/", unless `routes:` maps the handler
// name to something else.
func (r *Registry) RouteFor(path string) string {
	key := strings.ToLower(strings.TrimPrefix(path, "/"))
	if r == nil {
		return key
	}
	for handler, mapped := range r.routes {
		if strings.EqualFold(strings.TrimPrefix(path, "/"), handler) {
			return mapped
		}
	}
	return key
}

// Station is the admission station, or nil when `server:` is absent.
func (r *Registry) Station() *station {
	if r == nil {
		return nil
	}
	return r.station
}

// SHA256 is the hash of the file this registry was built from.
func (r *Registry) SHA256() string {
	if r == nil {
		return ""
	}
	return r.sha
}
