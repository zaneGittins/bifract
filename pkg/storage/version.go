package storage

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// ServerVersion is a parsed ClickHouse server version.
type ServerVersion struct {
	Major, Minor, Patch int
	Raw                 string
}

// MinClickHouseVersion is the oldest server the shipped schema, migrations and
// generated SQL are known to work against.
//
// It is pinned to 25.6 by the JSON/Dynamic support the whole log schema depends
// on: the fields column is a JSON type with per-path type hints, queries cast
// Dynamic sub-columns, and every connection sets
// output_format_native_use_flattened_dynamic_and_json_serialization. An older
// server does not fail at startup, it fails midway through a migration or
// returns wrong results for a typed field, which is far harder to diagnose.
var MinClickHouseVersion = ServerVersion{Major: 25, Minor: 6}

// ParseServerVersion reads a ClickHouse version string. It tolerates the build
// and image suffixes that appear in version() output and in image tags
// ("26.6.2.81", "26.6.2.81-alpine").
func ParseServerVersion(s string) (ServerVersion, error) {
	raw := strings.TrimSpace(s)
	if raw == "" {
		return ServerVersion{}, fmt.Errorf("empty version string")
	}
	// Drop any non-numeric suffix (-alpine, -lts, +build).
	trimmed := raw
	if i := strings.IndexAny(trimmed, "-+"); i >= 0 {
		trimmed = trimmed[:i]
	}
	parts := strings.Split(trimmed, ".")
	if len(parts) < 2 {
		return ServerVersion{}, fmt.Errorf("version %q has no minor component", raw)
	}
	v := ServerVersion{Raw: raw}
	for i, dst := range []*int{&v.Major, &v.Minor, &v.Patch} {
		if i >= len(parts) {
			break
		}
		n, err := strconv.Atoi(parts[i])
		if err != nil {
			return ServerVersion{}, fmt.Errorf("version %q is not numeric: %w", raw, err)
		}
		*dst = n
	}
	return v, nil
}

// Compare returns -1, 0 or 1. Only major and minor participate: ClickHouse
// patch releases do not add the features a floor exists to guarantee.
func (v ServerVersion) Compare(o ServerVersion) int {
	switch {
	case v.Major != o.Major:
		if v.Major < o.Major {
			return -1
		}
		return 1
	case v.Minor != o.Minor:
		if v.Minor < o.Minor {
			return -1
		}
		return 1
	}
	return 0
}

func (v ServerVersion) String() string {
	if v.Raw != "" {
		return v.Raw
	}
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

// CheckVersionFloor reads the server version and reports whether it meets
// MinClickHouseVersion. It returns the version even when below the floor, so the
// caller can report it either way.
//
// Enforcement is asymmetric, and deliberately so:
//
//   - Self-managed (single-node, cluster): the operator pins the image and we
//     ship the compose and CHI templates that pin it, so a too-old server is a
//     fixable misconfiguration. The caller makes this fatal.
//   - Cloud: the operator does not control the upgrade schedule, and Cloud runs
//     ahead of any floor we would set. A below-floor reading there means our
//     floor is wrong, not their server, so crashing would be user-hostile. The
//     caller logs and continues.
//
// A failure to read version() is never fatal anywhere: introspection must not
// block boot.
func (c *ClickHouseClient) CheckVersionFloor(ctx context.Context) (ServerVersion, bool, error) {
	var raw string
	if err := c.conn.QueryRow(ctx, "SELECT version()").Scan(&raw); err != nil {
		return ServerVersion{}, true, fmt.Errorf("read ClickHouse version: %w", err)
	}
	v, err := ParseServerVersion(raw)
	if err != nil {
		return ServerVersion{}, true, err
	}
	return v, v.Compare(MinClickHouseVersion) >= 0, nil
}
