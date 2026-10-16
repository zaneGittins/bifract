package alerts

import (
	"strings"
	"testing"
)

func intPtr(n int) *int       { return &n }
func strPtr(s string) *string { return &s }

func evalOne(p Policy, s PolicySubject) PolicyResult {
	p.Enabled = true
	if p.Severity == "" {
		p.Severity = PolicyBlock
	}
	if p.Message == "" {
		p.Message = "fix it"
	}
	return EvaluatePolicies([]Policy{p}, s)
}

// Every operator, on every field type, in both directions. The detail is part of
// the contract: it is the only place an author learns what was actually found.
func TestPolicyOperators(t *testing.T) {
	cases := []struct {
		name       string
		policy     Policy
		content    RevisionContent
		pass       bool
		wantDetail string
	}{
		// strings
		{"not_empty passes", Policy{Field: "description", Operator: "not_empty"}, RevisionContent{Description: "x"}, true, ""},
		{"not_empty treats whitespace as empty", Policy{Field: "description", Operator: "not_empty"}, RevisionContent{Description: "  \t"}, false, "it is empty"},
		{"min_length counts runes, not bytes", Policy{Field: "name", Operator: "min_length", Value: "3"}, RevisionContent{Name: "äöü"}, true, ""},
		{"min_length fails short", Policy{Field: "name", Operator: "min_length", Value: "5"}, RevisionContent{Name: "abc"}, false, "it is 3 characters, minimum is 5"},
		{"max_length at the bound passes", Policy{Field: "name", Operator: "max_length", Value: "3"}, RevisionContent{Name: "abc"}, true, ""},
		{"max_length fails long", Policy{Field: "name", Operator: "max_length", Value: "2"}, RevisionContent{Name: "abc"}, false, "it is 3 characters, maximum is 2"},
		{"matches passes", Policy{Field: "name", Operator: "matches", Value: `^[A-Z]`}, RevisionContent{Name: "Upper"}, true, ""},
		{"matches fails", Policy{Field: "name", Operator: "matches", Value: `^[A-Z]`}, RevisionContent{Name: "lower"}, false, "it does not match ^[A-Z]"},
		{"not_matches fails on a match", Policy{Field: "query_string", Operator: "not_matches", Value: `^\*$`}, RevisionContent{QueryString: "*"}, false, `it matches ^\*$`},
		{"equals passes", Policy{Field: "alert_type", Operator: "equals", Value: "event"}, RevisionContent{AlertType: "event"}, true, ""},
		{"equals fails and quotes the value", Policy{Field: "alert_type", Operator: "equals", Value: "event"}, RevisionContent{AlertType: "compound"}, false, `it is "compound"`},
		{"not_equals fails", Policy{Field: "severity", Operator: "not_equals", Value: "low"}, RevisionContent{Severity: "low"}, false, `it is "low"`},
		{"one_of trims the list", Policy{Field: "severity", Operator: "one_of", Value: " high , critical ,"}, RevisionContent{Severity: "critical"}, true, ""},
		{"one_of fails", Policy{Field: "severity", Operator: "one_of", Value: "high,critical"}, RevisionContent{Severity: "medium"}, false, `it is "medium"`},
		{"one_of with an empty value matches nothing", Policy{Field: "severity", Operator: "one_of", Value: " , "}, RevisionContent{Severity: ""}, false, `it is ""`},
		{"throttle_field reads", Policy{Field: "throttle_field", Operator: "not_empty"}, RevisionContent{ThrottleField: "host"}, true, ""},
		{"schedule_cron reads the pointer", Policy{Field: "schedule_cron", Operator: "equals", Value: "0 * * * *"}, RevisionContent{ScheduleCron: strPtr("0 * * * *")}, true, ""},
		{"unset schedule_cron is empty", Policy{Field: "schedule_cron", Operator: "not_empty"}, RevisionContent{}, false, "it is empty"},

		// lists
		{"list not_empty fails", Policy{Field: "references", Operator: "not_empty"}, RevisionContent{}, false, "it is empty"},
		{"min_count fails", Policy{Field: "labels", Operator: "min_count", Value: "2"}, RevisionContent{Labels: []string{"a"}}, false, "it has 1, minimum is 2"},
		{"max_count fails", Policy{Field: "labels", Operator: "max_count", Value: "1"}, RevisionContent{Labels: []string{"a", "b"}}, false, "it has 2, maximum is 1"},
		{"max_count at the bound passes", Policy{Field: "labels", Operator: "max_count", Value: "2"}, RevisionContent{Labels: []string{"a", "b"}}, true, ""},
		{"any_matches on an empty list says none", Policy{Field: "labels", Operator: "any_matches", Value: `^x`}, RevisionContent{}, false, "nothing matches ^x (have: none)"},
		{"all_match names the offender", Policy{Field: "references", Operator: "all_match", Value: `^https://`}, RevisionContent{References: []string{"https://a", "http://b"}}, false, `"http://b" does not match ^https://`},
		{"all_match on an empty list passes", Policy{Field: "references", Operator: "all_match", Value: `^https://`}, RevisionContent{}, true, ""},
		{"none_match names the offender", Policy{Field: "labels", Operator: "none_match", Value: `^tmp`}, RevisionContent{Labels: []string{"keep", "tmp-x"}}, false, `"tmp-x" matches ^tmp`},
		{"none_match passes", Policy{Field: "labels", Operator: "none_match", Value: `^tmp`}, RevisionContent{Labels: []string{"keep"}}, true, ""},

		// numbers
		{"gte fails", Policy{Field: "throttle_time_seconds", Operator: "gte", Value: "60"}, RevisionContent{ThrottleTimeSeconds: 30}, false, "it is 30"},
		{"lte fails", Policy{Field: "max_event_lag_seconds", Operator: "lte", Value: "60"}, RevisionContent{MaxEventLagSeconds: 61}, false, "it is 61"},
		{"gt at the bound fails", Policy{Field: "throttle_time_seconds", Operator: "gt", Value: "60"}, RevisionContent{ThrottleTimeSeconds: 60}, false, "it is 60"},
		{"lt at the bound fails", Policy{Field: "throttle_time_seconds", Operator: "lt", Value: "60"}, RevisionContent{ThrottleTimeSeconds: 60}, false, "it is 60"},
		{"lt passes", Policy{Field: "throttle_time_seconds", Operator: "lt", Value: "60"}, RevisionContent{ThrottleTimeSeconds: 59}, true, ""},
		{"number equals fails", Policy{Field: "tests.count", Operator: "equals", Value: "1"}, RevisionContent{}, false, "it is 0"},
		{"number not_equals fails", Policy{Field: "tests.count", Operator: "not_equals", Value: "0"}, RevisionContent{}, false, "it is 0"},
		{"window_duration reads the pointer", Policy{Field: "window_duration", Operator: "gte", Value: "300"}, RevisionContent{WindowDuration: intPtr(600)}, true, ""},
		{"unset window_duration is zero", Policy{Field: "window_duration", Operator: "gt", Value: "0"}, RevisionContent{}, false, "it is 0"},
		{"query_window_seconds reads the pointer", Policy{Field: "query_window_seconds", Operator: "lte", Value: "3600"}, RevisionContent{QueryWindowSeconds: intPtr(7200)}, false, "it is 7200"},
		{"unset query_window_seconds is zero", Policy{Field: "query_window_seconds", Operator: "equals", Value: "0"}, RevisionContent{}, true, ""},
		{"actions.count sums every kind", Policy{Field: "actions.count", Operator: "equals", Value: "4"}, RevisionContent{
			WebhookActionIDs: []string{"w"}, FractalActionIDs: []string{"f"}, DictionaryActionIDs: []string{"d"}, EmailActionIDs: []string{"e"},
		}, true, ""},
		{"fractional thresholds compare as numbers", Policy{Field: "throttle_time_seconds", Operator: "gte", Value: "0.5"}, RevisionContent{ThrottleTimeSeconds: 1}, true, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := (&Policy{Field: tc.policy.Field, Operator: tc.policy.Operator, Value: tc.policy.Value, Severity: PolicyWarn, Message: "m"}).Validate(); err != nil {
				t.Fatalf("the case's policy should be valid: %v", err)
			}
			result := evalOne(tc.policy, subjectWith(tc.content))
			if len(result.Checks) != 1 {
				t.Fatalf("expected one check, got %d", len(result.Checks))
			}
			check := result.Checks[0]
			if check.Passed != tc.pass {
				t.Fatalf("passed = %v, want %v (detail %q)", check.Passed, tc.pass, check.Detail)
			}
			if check.Detail != tc.wantDetail {
				t.Errorf("detail = %q, want %q", check.Detail, tc.wantDetail)
			}
			if !tc.pass && (len(result.Violations) != 1 || result.Violations[0].Detail != tc.wantDetail) {
				t.Errorf("violation should carry the same detail, got %+v", result.Violations)
			}
		})
	}
}

// Boolean operators read the test outcome, and only once tests have run.
func TestPolicyBooleanOperators(t *testing.T) {
	ran := func(passing bool) PolicySubject {
		return PolicySubject{TestsRun: true, TestsPassing: passing}
	}
	if r := evalOne(Policy{Field: "tests.all_passing", Operator: "is_false"}, ran(true)); r.Blocking != 1 || r.Violations[0].Detail != "it is true" {
		t.Errorf("is_false on passing tests should fail with detail, got %+v", r.Violations)
	}
	if r := evalOne(Policy{Field: "tests.all_passing", Operator: "is_false"}, ran(false)); !r.OK() {
		t.Errorf("is_false on failing tests should pass, got %+v", r.Violations)
	}
	if r := evalOne(Policy{Field: "tests.all_passing", Operator: "is_true"}, ran(false)); r.Violations[0].Detail != "it is false" {
		t.Errorf("detail = %q", r.Violations[0].Detail)
	}
}

// Validate refuses a bad regex at authoring time, so an evaluation that still meets
// one (a row edited in the database, a rule from an older version) must not block
// every save in the fractal on a rule nobody can satisfy.
func TestPolicyEvaluationFailsOpenOnABadRegex(t *testing.T) {
	for _, op := range []string{"matches", "not_matches"} {
		if r := evalOne(Policy{Field: "name", Operator: op, Value: "(["}, subjectWith(RevisionContent{Name: "x"})); !r.OK() {
			t.Errorf("string %s with a broken regex blocked: %+v", op, r.Violations)
		}
	}
	for _, op := range []string{"any_matches", "all_match", "none_match"} {
		if r := evalOne(Policy{Field: "labels", Operator: op, Value: "(["}, subjectWith(RevisionContent{Labels: []string{"x"}})); !r.OK() {
			t.Errorf("list %s with a broken regex blocked: %+v", op, r.Violations)
		}
	}
}

func TestPolicyResultCountsAndOrder(t *testing.T) {
	policies := []Policy{
		{Field: "name", Operator: "not_empty", Severity: PolicyBlock, Message: "name it", Enabled: true, ID: "p1"},
		{Field: "description", Operator: "not_empty", Severity: PolicyWarn, Message: "describe it", Enabled: true, ID: "p2"},
		{Field: "labels", Operator: "not_empty", Severity: PolicyBlock, Message: "label it", Enabled: false, ID: "p3"},
		{Field: "references", Operator: "not_empty", Severity: PolicyBlock, Message: "cite it", Enabled: true, ID: "p4"},
	}
	r := EvaluatePolicies(policies, subjectWith(RevisionContent{Name: "n"}))

	if r.Total() != 3 {
		t.Fatalf("disabled rules are not listed: total %d, want 3", r.Total())
	}
	if r.Passed != 1 || r.Warnings != 1 || r.Blocking != 1 || r.OK() {
		t.Errorf("passed=%d warnings=%d blocking=%d ok=%v", r.Passed, r.Warnings, r.Blocking, r.OK())
	}
	var ids []string
	for _, c := range r.Checks {
		ids = append(ids, c.PolicyID)
	}
	if strings.Join(ids, ",") != "p1,p2,p4" {
		t.Errorf("checks must keep author order, got %v", ids)
	}
	if r.Violations[0].PolicyID != "p2" || r.Violations[0].Message != "describe it" || r.Violations[0].Severity != PolicyWarn {
		t.Errorf("violation lost its rule identity: %+v", r.Violations[0])
	}
}

func TestPolicyValidationOperatorsWithoutValue(t *testing.T) {
	// A value-less operator needs no value and must not be held to a regex or number.
	for _, p := range []Policy{
		{Field: "description", Operator: "not_empty", Value: "(["},
		{Field: "tests.all_passing", Operator: "is_true"},
		{Field: "tests.all_passing", Operator: "is_false"},
	} {
		p.Severity, p.Message = PolicyWarn, "m"
		if err := p.Validate(); err != nil {
			t.Errorf("%s/%s rejected: %v", p.Field, p.Operator, err)
		}
	}

	for name, p := range map[string]Policy{
		"unknown field":         {Field: "nope", Operator: "not_empty"},
		"unknown severity":      {Field: "name", Operator: "not_empty", Severity: "fatal"},
		"missing value":         {Field: "name", Operator: "equals", Value: "  "},
		"non-numeric max_count": {Field: "labels", Operator: "max_count", Value: "many"},
		"bad regex on a string": {Field: "name", Operator: "matches", Value: "(["},
	} {
		if p.Severity == "" {
			p.Severity = PolicyWarn
		}
		p.Message = "m"
		if err := p.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestValidatePoliciesBoundsAndNumbering(t *testing.T) {
	good := Policy{Field: "name", Operator: "not_empty", Severity: PolicyWarn, Message: "m"}
	bad := Policy{Field: "name", Operator: "not_empty", Severity: "nope", Message: "m"}

	err := ValidatePolicies([]Policy{good, good, bad})
	if err == nil || !strings.HasPrefix(err.Error(), "rule 3:") {
		t.Errorf("the failing rule should be numbered from one, got %v", err)
	}

	many := make([]Policy, MaxPoliciesPerFractal+1)
	for i := range many {
		many[i] = good
	}
	if err := ValidatePolicies(many); err == nil {
		t.Error("a rule set past the cap must be refused")
	}
	if err := ValidatePolicies(many[:MaxPoliciesPerFractal]); err != nil {
		t.Errorf("a rule set at the cap is allowed: %v", err)
	}
}

func TestJoinForDetailCapsTheList(t *testing.T) {
	if got := joinForDetail([]string{"a", "b"}); got != "a, b" {
		t.Errorf("got %q", got)
	}
	got := joinForDetail([]string{"1", "2", "3", "4", "5", "6", "7"})
	if got != "1, 2, 3, 4, 5 and 2 more" {
		t.Errorf("got %q", got)
	}
}

// The editor builds its pickers from the catalog, so every operator it offers must
// render as a sentence, carry a label, and know whether it takes a value.
func TestCatalogIsSelfConsistent(t *testing.T) {
	c := Catalog()
	if len(c.Fields) != len(policyFields) {
		t.Fatalf("catalog lists %d fields, want %d", len(c.Fields), len(policyFields))
	}
	for i := 1; i < len(c.Fields); i++ {
		if c.Fields[i-1].Name >= c.Fields[i].Name {
			t.Errorf("fields must be sorted for a stable picker: %s before %s", c.Fields[i-1].Name, c.Fields[i].Name)
		}
	}
	for _, f := range c.Fields {
		ops, ok := c.Operators[string(f.Type)]
		if !ok || len(ops) == 0 {
			t.Errorf("field %s has type %s with no operators", f.Name, f.Type)
		}
	}
	for typ, ops := range c.Operators {
		for _, op := range ops {
			if _, ok := operatorPhrases[op]; !ok {
				t.Errorf("%s operator %s has no phrase, so a passing check would read as a raw operator", typ, op)
			}
			if _, ok := c.Labels[op]; !ok {
				t.Errorf("%s operator %s has no picker label", typ, op)
			}
			if _, ok := c.NeedValue[op]; !ok {
				t.Errorf("%s operator %s is missing from operator_needs_value", typ, op)
			}
		}
	}
	if c.NeedValue["not_empty"] || !c.NeedValue["min_length"] {
		t.Error("needs-value flags are wrong for not_empty/min_length")
	}

	names := FieldNames()
	if !strings.HasPrefix(names, "actions.count, alert_type,") || !strings.Contains(names, "tests.all_passing") {
		t.Errorf("FieldNames should list every field sorted, got %q", names)
	}
}

func TestDescribePolicyFallsBackForUnknownOperators(t *testing.T) {
	got := describePolicy(Policy{Field: "custom", Operator: "resembles", Value: "x"}, PolicyField{Name: "custom"})
	if got != "custom resembles x" {
		t.Errorf("got %q", got)
	}
}

func TestPoliciesNeedTests(t *testing.T) {
	needs := Policy{Field: "tests.all_passing", Operator: "is_true", Enabled: true}
	plain := Policy{Field: "name", Operator: "not_empty", Enabled: true}
	if policiesNeedTests([]Policy{plain}) {
		t.Error("no rule reads test outcomes")
	}
	if !policiesNeedTests([]Policy{plain, needs}) {
		t.Error("an enabled outcome rule needs a test run")
	}
	needs.Enabled = false
	if policiesNeedTests([]Policy{needs}) {
		t.Error("a disabled rule must not cost a test run")
	}
	if policiesNeedTests([]Policy{{Field: "retired", Enabled: true}}) {
		t.Error("an unknown field reads nothing")
	}
}

func TestPolicyScope(t *testing.T) {
	cases := []struct{ fractal, prism, col, id string }{
		{"f", "", "fractal_id", "f"},
		{"", "p", "prism_id", "p"},
		{"f", "p", "prism_id", "p"}, // a prism always wins: it is the narrower authority
		{"", "", "", ""},
	}
	for _, tc := range cases {
		col, id := policyScope(tc.fractal, tc.prism)
		if col != tc.col || id != tc.id {
			t.Errorf("policyScope(%q,%q) = (%q,%q), want (%q,%q)", tc.fractal, tc.prism, col, id, tc.col, tc.id)
		}
	}
}

func TestPolicyBlockedError(t *testing.T) {
	one := &PolicyBlockedError{Result: PolicyResult{Blocking: 1, Violations: []Violation{
		{Severity: PolicyBlock, Message: "add an ATT&CK label"},
	}}}
	if one.Error() != "blocked by policy: add an ATT&CK label" {
		t.Errorf("a single violation should be named, got %q", one.Error())
	}

	mixed := &PolicyBlockedError{Result: PolicyResult{Blocking: 2, Warnings: 1, Violations: []Violation{
		{Severity: PolicyBlock, Message: "a"},
		{Severity: PolicyWarn, Message: "b"},
		{Severity: PolicyBlock, Message: "c"},
	}}}
	if mixed.Error() != "blocked by 2 policy rules" {
		t.Errorf("got %q", mixed.Error())
	}
	blocking := mixed.Blocking()
	if len(blocking) != 2 || blocking[0].Message != "a" || blocking[1].Message != "c" {
		t.Errorf("only blocking violations, in order, should be returned: %+v", blocking)
	}
}

// What the editor posts is what a save would judge, field for field.
func TestEvaluatePolicyRequestSubject(t *testing.T) {
	req := EvaluatePolicyRequest{
		Name: "n", Description: "d", QueryString: "q", AlertType: "scheduled", Severity: "high",
		ThrottleTimeSeconds: 1, MaxEventLagSeconds: 2, ThrottleField: "host",
		Labels: []string{"l"}, References: []string{"r"},
		WindowDuration: intPtr(3), ScheduleCron: strPtr("@hourly"), QueryWindowSeconds: intPtr(4),
		WebhookActionIDs: []string{"w"}, FractalActionIDs: []string{"f"},
		DictionaryActionIDs: []string{"d"}, EmailActionIDs: []string{"e"},
		Tests:    []AlertTest{{Name: "t"}},
		TestsRun: true, TestsPassing: true,
	}
	s := req.subject()
	c := s.Content
	if c.Name != "n" || c.Description != "d" || c.QueryString != "q" || c.AlertType != "scheduled" ||
		c.Severity != "high" || c.ThrottleTimeSeconds != 1 || c.MaxEventLagSeconds != 2 || c.ThrottleField != "host" ||
		*c.WindowDuration != 3 || *c.ScheduleCron != "@hourly" || *c.QueryWindowSeconds != 4 {
		t.Errorf("scalar fields not carried: %+v", c)
	}
	if len(c.Labels) != 1 || len(c.References) != 1 || len(c.WebhookActionIDs) != 1 || len(c.FractalActionIDs) != 1 ||
		len(c.DictionaryActionIDs) != 1 || len(c.EmailActionIDs) != 1 {
		t.Errorf("list fields not carried: %+v", c)
	}
	if len(s.Tests) != 1 || !s.TestsRun || !s.TestsPassing {
		t.Errorf("test state not carried: %+v", s)
	}
}
