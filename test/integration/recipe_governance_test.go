//go:build integration

// Refusal paths for alert governance: history, tests, policies and the review gate.
//
// Every one of these is a way the server says no, and each has to say it in a way a
// client can act on. They are asserted here because the failures they guard against are
// invisible from the success path: a save that answers 500 instead of 400, a gate
// refusal a client cannot tell from a validation error, a proposal in another fractal
// that answers 200.
//
//	go test -tags integration ./test/integration/ -run TestGovernance -v

package integration

import (
	"net/http"
	"testing"
)

// refusal is one call that must be rejected, and how.
type refusal struct {
	name string
	// method and path are the call. body is nil for a bodyless call.
	method string
	path   string
	body   any
	want   int
}

func runRefusals(t *testing.T, c *Client, cases []refusal) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.Status(t, tc.method, tc.path, tc.body); got != tc.want {
				t.Errorf("status = %d, want %d", got, tc.want)
			}
		})
	}
}

const missingID = "00000000-0000-0000-0000-000000000000"

// TestGovernanceTestRefusals covers the alert test runner's input validation. A test
// corpus that asserts nothing is worse than none, so each of these has to be rejected
// rather than quietly accepted.
func TestGovernanceTestRefusals(t *testing.T) {
	c := New(t)
	one := []map[string]any{{"process_name": "cmd.exe"}}

	runRefusals(t, c, []refusal{
		{"no session id", "POST", "/alerts/tests/run",
			map[string]any{"query_string": `a="b"`}, http.StatusBadRequest},
		{"no query", "POST", "/alerts/tests/run",
			map[string]any{"session_id": "s"}, http.StatusBadRequest},
		{"invalid BQL", "POST", "/alerts/tests/run", map[string]any{
			"session_id": "s", "query_string": "| bogus(",
			"tests": []map[string]any{{"name": "t", "expectation": "match", "events": one}},
		}, http.StatusBadRequest},
		{"a test with no events", "POST", "/alerts/tests/run", map[string]any{
			"session_id": "s", "query_string": `a="b"`,
			"tests": []map[string]any{{"name": "t", "expectation": "match", "events": []any{}}},
		}, http.StatusBadRequest},
		{"duplicate test names", "POST", "/alerts/tests/run", map[string]any{
			"session_id": "s", "query_string": `a="b"`,
			"tests": []map[string]any{
				{"name": "d", "expectation": "match", "events": one},
				{"name": "D", "expectation": "no_match", "events": one},
			},
		}, http.StatusBadRequest},
		{"unknown expectation", "POST", "/alerts/tests/run", map[string]any{
			"session_id": "s", "query_string": `a="b"`,
			"tests": []map[string]any{{"name": "t", "expectation": "maybe", "events": one}},
		}, http.StatusBadRequest},
		{"tests for an alert that does not exist", "GET", "/alerts/" + missingID + "/tests", nil, http.StatusNotFound},
	})
}

// TestGovernancePolicyRefusals covers policy authoring. A rule that cannot be evaluated
// must be refused when it is written, not when it silently stops enforcing.
func TestGovernancePolicyRefusals(t *testing.T) {
	c := New(t)
	rule := func(field, operator, value, message, severity string) any {
		return map[string]any{"policies": []map[string]any{{
			"field": field, "operator": operator, "value": value,
			"message": message, "severity": severity, "enabled": true,
		}}}
	}

	runRefusals(t, c, []refusal{
		{"unknown field", "PUT", "/alert-policies",
			rule("nope", "not_empty", "", "m", "warn"), http.StatusBadRequest},
		{"operator that does not apply to the type", "PUT", "/alert-policies",
			rule("labels", "min_length", "3", "m", "warn"), http.StatusBadRequest},
		{"invalid regex", "PUT", "/alert-policies",
			rule("labels", "any_matches", "([", "m", "warn"), http.StatusBadRequest},
		{"no message for the analyst", "PUT", "/alert-policies",
			rule("description", "not_empty", "", "", "warn"), http.StatusBadRequest},
		{"non-numeric threshold", "PUT", "/alert-policies",
			rule("description", "min_length", "forty", "m", "warn"), http.StatusBadRequest},
		{"unknown severity", "PUT", "/alert-policies",
			rule("description", "not_empty", "", "m", "nuke"), http.StatusBadRequest},
		{"malformed policy document", "POST", "/alert-policies/import",
			map[string]any{"content": "rules: [oops", "replace": true}, http.StatusBadRequest},
		{"a document with no rules", "POST", "/alert-policies/import",
			map[string]any{"content": "name: empty\nrules: []", "replace": true}, http.StatusBadRequest},
	})
}

// TestGovernanceRevisionRefusals covers history and rollback.
func TestGovernanceRevisionRefusals(t *testing.T) {
	c := New(t)

	runRefusals(t, c, []refusal{
		{"history for an alert that does not exist", "GET", "/alerts/" + missingID + "/revisions", nil, http.StatusNotFound},
		{"a revision that does not exist", "GET", "/alerts/" + missingID + "/revisions/1", nil, http.StatusNotFound},
		{"restoring a revision that does not exist", "POST", "/alerts/" + missingID + "/revisions/1/restore",
			map[string]any{}, http.StatusNotFound},
	})
}

// TestGovernanceProposalRefusals covers the review gate's input validation. These hold
// whether or not the gate is enabled: a malformed proposal is malformed either way.
func TestGovernanceProposalRefusals(t *testing.T) {
	c := New(t)
	definition := map[string]any{
		"name": "n", "query_string": `a="b"`, "alert_type": "event", "severity": "low",
		"labels": []string{}, "references": []string{},
		"webhook_action_ids": []string{}, "fractal_action_ids": []string{},
		"dictionary_action_ids": []string{}, "email_action_ids": []string{},
	}

	runRefusals(t, c, []refusal{
		{"unknown kind", "POST", "/alert-changes",
			map[string]any{"kind": "rewrite", "alert_id": missingID, "summary": "x"}, http.StatusBadRequest},
		{"an update carrying no definition", "POST", "/alert-changes",
			map[string]any{"kind": "update", "alert_id": missingID}, http.StatusBadRequest},
		{"a delete with no stated reason", "POST", "/alert-changes",
			map[string]any{"kind": "delete", "alert_id": missingID}, http.StatusBadRequest},
		{"a create naming an alert", "POST", "/alert-changes",
			map[string]any{"kind": "create", "alert_id": missingID, "content": definition}, http.StatusBadRequest},
		{"an update naming no alert", "POST", "/alert-changes",
			map[string]any{"kind": "update", "content": definition}, http.StatusBadRequest},
		// The alert is addressed by UUID, so a proposal must not be able to reach one
		// outside its own scope. Not found rather than forbidden: confirming the alert
		// exists elsewhere is itself a leak.
		{"an alert outside the scope", "POST", "/alert-changes",
			map[string]any{"kind": "delete", "alert_id": missingID, "summary": "why"}, http.StatusNotFound},
		{"a proposal that does not exist", "GET", "/alert-changes/" + missingID, nil, http.StatusNotFound},
		{"reviewing a proposal that does not exist", "POST", "/alert-changes/" + missingID + "/review",
			map[string]any{"decision": "approve"}, http.StatusNotFound},
		{"merging a proposal that does not exist", "POST", "/alert-changes/" + missingID + "/merge",
			nil, http.StatusNotFound},
	})
}

// TestGovernanceGateConfigRefusals covers the review settings themselves.
func TestGovernanceGateConfigRefusals(t *testing.T) {
	c := New(t)

	runRefusals(t, c, []refusal{
		{"no approvals required", "PUT", "/alert-gate",
			map[string]any{"enabled": true, "min_approvals": 0}, http.StatusBadRequest},
		{"more approvals than anyone has reviewers", "PUT", "/alert-gate",
			map[string]any{"enabled": true, "min_approvals": 99}, http.StatusBadRequest},
	})
}
