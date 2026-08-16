package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// Capabilities are the things this ClickHouse server will actually let Bifract
// do. Unlike Topology, which is derived from configuration and cannot fail, a
// capability is only known by asking the live server.
//
// Two rules govern this file:
//
//  1. A capability is earned by the real operation wherever one exists. Probing
//     "can I CREATE WORKLOAD" separately from actually creating it duplicates the
//     DDL and creates two sources of truth that can disagree. Only facts a system
//     table can answer get a startup probe.
//
//  2. DeploymentKind never gates a capability. We do not encode "managed
//     ClickHouse has no workloads"; we observe the server refusing and report its
//     own message. Hardcoding a vendor's restriction list guarantees it is wrong
//     the moment that list changes.

type CapabilityState uint8

const (
	// CapUnknown means the capability has not been exercised yet.
	CapUnknown CapabilityState = iota
	CapAvailable
	// CapUnavailable means the server refused in a way that will not resolve on
	// its own. A transient failure never reaches this state.
	CapUnavailable
)

func (s CapabilityState) String() string {
	switch s {
	case CapAvailable:
		return "available"
	case CapUnavailable:
		return "unavailable"
	default:
		return "unknown"
	}
}

// MarshalJSON emits the name rather than the ordinal. Clients compare against
// "unavailable"; a bare integer would compare false everywhere and silently
// disable the very reporting this type exists to provide.
func (s CapabilityState) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.String() + `"`), nil
}

// Capability is one capability's current state and why.
type Capability struct {
	State CapabilityState `json:"state"`
	// Reason carries the server's own message. It is operator-facing and may
	// name internal identifiers, so callers show it to admins only.
	Reason    string    `json:"reason,omitempty"`
	CheckedAt time.Time `json:"checked_at,omitempty"`
}

type CapabilityKey string

const (
	// CapWorkloadScheduling backs the admin CPU shares for search and recall.
	CapWorkloadScheduling CapabilityKey = "workload_scheduling"
	// CapQueryIdentity backs the per-class memory ceilings. Without it a class
	// keeps only its per-query ceiling.
	CapQueryIdentity CapabilityKey = "query_identity"
	// CapIngestIdentity is security-relevant: without it the ingest tier runs as
	// the admin user instead of the INSERT-only one.
	CapIngestIdentity CapabilityKey = "ingest_identity"
	// CapMVSecurityDefiner backs least-privilege inserts through materialized views.
	CapMVSecurityDefiner CapabilityKey = "mv_security_definer"
	// CapServerMemoryBudget is whether the server's memory budget is readable at
	// all, which is what percentage-based memory shares are computed from.
	CapServerMemoryBudget CapabilityKey = "server_memory_budget"
	// CapHostCPUMetrics and CapHostDiskMetrics back ingest backpressure. A managed
	// service that autoscales reports host metrics that describe nothing stable.
	CapHostCPUMetrics  CapabilityKey = "host_cpu_metrics"
	CapHostDiskMetrics CapabilityKey = "host_disk_metrics"
)

// Capabilities is the full set, keyed by capability.
type Capabilities map[CapabilityKey]Capability

// Available reports whether a capability is known to work. Unknown counts as
// available: a capability that has not been exercised yet must not read as
// broken, or every feature would appear disabled for the first seconds of boot.
func (c Capabilities) Available(k CapabilityKey) bool {
	return c[k].State != CapUnavailable
}

// capabilityStore is the client's live capability set.
type capabilityStore struct {
	mu   sync.RWMutex
	caps Capabilities
}

func (s *capabilityStore) snapshot() Capabilities {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(Capabilities, len(s.caps))
	for k, v := range s.caps {
		out[k] = v
	}
	return out
}

// record applies an operation's outcome. A nil error marks the capability
// available; an error is classified, and a transient one leaves the previous
// state untouched so a single flaky startup cannot permanently mark a working
// feature broken.
func (s *capabilityStore) record(k CapabilityKey, err error) {
	state, reason := classifyCHError(err)
	if state == CapUnknown {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.caps == nil {
		s.caps = Capabilities{}
	}
	s.caps[k] = Capability{State: state, Reason: reason, CheckedAt: time.Now().UTC()}
}

// Capabilities returns a copy of the current capability set.
func (c *ClickHouseClient) Capabilities() Capabilities { return c.caps.snapshot() }

// recordCapability applies an operation's outcome to a capability.
func (c *ClickHouseClient) recordCapability(k CapabilityKey, err error) { c.caps.record(k, err) }

// permanentCHCodes mean "this server will never allow this", as opposed to
// "this failed right now". Names verified against errorCodeToName() on
// ClickHouse 26.6; do not add a code without checking it the same way.
var permanentCHCodes = map[int32]struct{}{
	48:  {}, // NOT_IMPLEMENTED
	62:  {}, // SYNTAX_ERROR: the server does not know this statement
	80:  {}, // INCORRECT_QUERY
	115: {}, // UNKNOWN_SETTING
	164: {}, // READONLY
	344: {}, // SUPPORT_IS_DISABLED
	392: {}, // QUERY_IS_PROHIBITED
	446: {}, // FUNCTION_NOT_ALLOWED
	472: {}, // READONLY_SETTING
	495: {}, // ACCESS_STORAGE_READONLY: access management exists but is not writable
	497: {}, // ACCESS_DENIED
	508: {}, // UNKNOWN_ACCESS_TYPE: a GRANT this server does not define
	620: {}, // QUERY_NOT_ALLOWED
}

// classifyCHError distinguishes a permanent refusal from a transient failure.
// Only a permanent refusal may set CapUnavailable.
//
// This is the load-bearing judgement in the capability model. Classifying a
// timeout as permanent would disable a working feature until the next restart;
// classifying a privilege error as transient would leave the UI claiming a
// feature is active when the server has refused it outright.
func classifyCHError(err error) (CapabilityState, string) {
	if err == nil {
		return CapAvailable, ""
	}
	// A cancelled or timed-out context says nothing about the server.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return CapUnknown, ""
	}
	var ex *proto.Exception
	if errors.As(err, &ex) {
		if _, permanent := permanentCHCodes[ex.Code]; permanent {
			return CapUnavailable, fmt.Sprintf("ClickHouse refused: %s (code %d)", ex.Message, ex.Code)
		}
		// A known server error that is not in the permanent set (timeouts, memory
		// limits, no free connection) is a condition, not a verdict.
		return CapUnknown, ""
	}
	// Network and driver errors carry no verdict either.
	return CapUnknown, ""
}

// ProbeCapabilities fills in the capabilities that a read-only system-table
// query can answer. Everything else is recorded by the reconcilers that already
// perform the real operation. Never fatal.
func (c *ClickHouseClient) ProbeCapabilities(ctx context.Context) {
	// Host CPU and memory come from the same metrics table and the same helper
	// the ingest backpressure monitor uses, so the probe agrees with the monitor
	// by construction.
	rows, err := c.Query(ctx, SystemCPUMetricsSQL)
	if err != nil {
		c.recordCapability(CapHostCPUMetrics, err)
	} else if _, ok := CPUPercentFromMetrics(MetricRowsToMap(rows)); ok {
		c.recordCapability(CapHostCPUMetrics, nil)
	} else {
		c.caps.set(CapHostCPUMetrics, CapUnavailable,
			"this server reports no usable CPU metrics in system.asynchronous_metrics")
	}

	var total, free uint64
	err = c.conn.QueryRow(ctx,
		"SELECT total_space, free_space FROM system.disks WHERE name = 'default' LIMIT 1").Scan(&total, &free)
	switch {
	case err != nil:
		c.recordCapability(CapHostDiskMetrics, err)
	case total == 0:
		c.caps.set(CapHostDiskMetrics, CapUnavailable,
			"this server reports no local disk in system.disks; storage is managed for you")
	default:
		c.recordCapability(CapHostDiskMetrics, nil)
	}
}

// set records a state directly, for the cases where the server answered
// successfully but the answer itself means the capability is absent.
func (s *capabilityStore) set(k CapabilityKey, state CapabilityState, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.caps == nil {
		s.caps = Capabilities{}
	}
	s.caps[k] = Capability{State: state, Reason: reason, CheckedAt: time.Now().UTC()}
}
