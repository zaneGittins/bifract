package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// classifyCHError decides whether a failure disables a feature until the next
// restart or is forgotten. Getting it wrong in either direction is the failure
// mode this whole model exists to avoid: a timeout classified as permanent
// silently disables a working feature, and a privilege error classified as
// transient leaves the UI claiming a feature is active that the server refused.
func TestClassifyCHErrorPermanent(t *testing.T) {
	// Codes verified against errorCodeToName() on ClickHouse 26.6.
	for _, tc := range []struct {
		code int32
		name string
	}{
		{48, "NOT_IMPLEMENTED"},
		{62, "SYNTAX_ERROR"},
		{80, "INCORRECT_QUERY"},
		{115, "UNKNOWN_SETTING"},
		{164, "READONLY"},
		{344, "SUPPORT_IS_DISABLED"},
		{392, "QUERY_IS_PROHIBITED"},
		{446, "FUNCTION_NOT_ALLOWED"},
		{472, "READONLY_SETTING"},
		{495, "ACCESS_STORAGE_READONLY"},
		{497, "ACCESS_DENIED"},
		{508, "UNKNOWN_ACCESS_TYPE"},
		{620, "QUERY_NOT_ALLOWED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := fmt.Errorf("wrapped: %w", &proto.Exception{Code: tc.code, Message: tc.name})
			state, reason := classifyCHError(err)
			if state != CapUnavailable {
				t.Errorf("state = %v, want CapUnavailable for %s", state, tc.name)
			}
			if reason == "" {
				t.Error("reason is empty; the operator needs the server's own message")
			}
		})
	}
}

func TestClassifyCHErrorTransient(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"timeout exceeded", &proto.Exception{Code: 159, Message: "TIMEOUT_EXCEEDED"}},
		{"memory limit exceeded", &proto.Exception{Code: 241, Message: "MEMORY_LIMIT_EXCEEDED"}},
		{"no free connection", &proto.Exception{Code: 202, Message: "TOO_MANY_SIMULTANEOUS_QUERIES"}},
		{"context cancelled", context.Canceled},
		{"deadline exceeded", context.DeadlineExceeded},
		{"wrapped deadline", fmt.Errorf("exec: %w", context.DeadlineExceeded)},
		{"network error", &net.OpError{Op: "dial", Err: errors.New("connection refused")}},
		{"plain error", errors.New("something went wrong")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if state, _ := classifyCHError(tc.err); state != CapUnknown {
				t.Errorf("state = %v, want CapUnknown: a transient failure must not disable a feature", state)
			}
		})
	}
}

func TestClassifyCHErrorSuccess(t *testing.T) {
	if state, _ := classifyCHError(nil); state != CapAvailable {
		t.Errorf("state = %v, want CapAvailable", state)
	}
}

// A transient failure must leave an earlier verdict alone, so one flaky startup
// cannot permanently mark a working feature broken and one flaky poll cannot
// silently re-enable a feature the server refused.
func TestCapabilityStoreTransientPreservesState(t *testing.T) {
	var s capabilityStore

	s.record(CapWorkloadScheduling, nil)
	if got := s.snapshot()[CapWorkloadScheduling].State; got != CapAvailable {
		t.Fatalf("state = %v, want CapAvailable", got)
	}

	s.record(CapWorkloadScheduling, context.DeadlineExceeded)
	if got := s.snapshot()[CapWorkloadScheduling].State; got != CapAvailable {
		t.Errorf("state = %v after a timeout, want the previous CapAvailable", got)
	}

	s.record(CapWorkloadScheduling, &proto.Exception{Code: 497, Message: "denied"})
	if got := s.snapshot()[CapWorkloadScheduling].State; got != CapUnavailable {
		t.Errorf("state = %v after ACCESS_DENIED, want CapUnavailable", got)
	}

	s.record(CapWorkloadScheduling, context.DeadlineExceeded)
	if got := s.snapshot()[CapWorkloadScheduling].State; got != CapUnavailable {
		t.Errorf("state = %v after a timeout, want the previous CapUnavailable", got)
	}
}

// An unexercised capability must not read as broken, or every feature would
// appear disabled for the first seconds of every boot.
func TestCapabilitiesUnknownIsAvailable(t *testing.T) {
	caps := Capabilities{}
	if !caps.Available(CapWorkloadScheduling) {
		t.Error("an unexercised capability reads as unavailable")
	}
	caps[CapWorkloadScheduling] = Capability{State: CapUnavailable}
	if caps.Available(CapWorkloadScheduling) {
		t.Error("an explicitly unavailable capability reads as available")
	}
}

func TestCapabilitySnapshotIsACopy(t *testing.T) {
	var s capabilityStore
	s.record(CapIngestIdentity, nil)
	snap := s.snapshot()
	snap[CapIngestIdentity] = Capability{State: CapUnavailable}
	if s.snapshot()[CapIngestIdentity].State != CapAvailable {
		t.Error("mutating a snapshot changed the store")
	}
}

// The UI compares state against "unavailable". Emitting the ordinal instead
// would make every comparison false and silently disable the reporting.
func TestCapabilityStateMarshalsAsName(t *testing.T) {
	for _, tc := range []struct {
		state CapabilityState
		want  string
	}{
		{CapUnknown, `"unknown"`},
		{CapAvailable, `"available"`},
		{CapUnavailable, `"unavailable"`},
	} {
		got, err := json.Marshal(tc.state)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if string(got) != tc.want {
			t.Errorf("Marshal(%v) = %s, want %s", tc.state, got, tc.want)
		}
	}

	blob, err := json.Marshal(Capabilities{CapWorkloadScheduling: {State: CapUnavailable, Reason: "denied"}})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !bytes.Contains(blob, []byte(`"state":"unavailable"`)) {
		t.Errorf("Capabilities JSON = %s, want a named state", blob)
	}
}
