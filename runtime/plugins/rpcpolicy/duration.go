package rpcpolicy

import (
	"fmt"
	"time"
)

// Duration is a time.Duration that unmarshals from a YAML scalar through
// time.ParseDuration ("50ms", "1s", "0s"). Ported from
// hotelreservation-experiments/resilience/duration.go, with the yaml.v3 node
// API so a malformed value reports its line.
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var raw string
	if err := unmarshal(&raw); err != nil {
		return fmt.Errorf("duration must be a Go duration string (e.g. \"50ms\"): %w", err)
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML implements yaml.Marshaler so a round trip through the loader is
// lossless (the reload test rewrites files it read).
func (d Duration) MarshalYAML() (interface{}, error) {
	return time.Duration(d).String(), nil
}

// Duration returns the parsed value.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// Nanos returns the parsed value as int64 nanoseconds, the unit the policy
// layer works in.
func (d Duration) Nanos() int64 { return int64(d) }
