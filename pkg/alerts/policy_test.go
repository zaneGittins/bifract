package alerts

import "testing"

func subjectWith(content RevisionContent, tests ...AlertTest) PolicySubject {
	return PolicySubject{Content: content, Tests: tests}
}

func mustPolicy(t *testing.T, p Policy) Policy {
	t.Helper()
	p.Enabled = true
	if p.Severity == "" {
		p.Severity = PolicyBlock
	}
	if p.Message == "" {
		p.Message = "fix it"
	}
	if err := p.Validate(); err != nil {
		t.Fatalf("policy %+v rejected: %v", p, err)
	}
	return p
}

func TestMitreLabelPolicy(t *testing.T) {
	policy := mustPolicy(t, Policy{Field: "labels", Operator: "any_matches", Value: `^attack\.t\d{4}`})

	tagged := EvaluatePolicies([]Policy{policy}, subjectWith(RevisionContent{Labels: []string{"noisy", "attack.t1059"}}))
	if !tagged.OK() || len(tagged.Violations) != 0 {
		t.Errorf("an ATT&CK label should satisfy the rule, got %+v", tagged.Violations)
	}

	untagged := EvaluatePolicies([]Policy{policy}, subjectWith(RevisionContent{Labels: []string{"noisy"}}))
	if untagged.OK() {
		t.Fatal("a rule with no ATT&CK label must block")
	}
	if untagged.Blocking != 1 || untagged.Violations[0].Field != "labels" {
		t.Errorf("unexpected result: %+v", untagged)
	}
	if untagged.Violations[0].Detail == "" {
		t.Error("a violation should say what was actually found")
	}
}

func TestDescriptionLengthPolicy(t *testing.T) {
	policy := mustPolicy(t, Policy{Field: "description", Operator: "min_length", Value: "40", Severity: PolicyWarn})

	short := EvaluatePolicies([]Policy{policy}, subjectWith(RevisionContent{Description: "too short"}))
	if short.Warnings != 1 || short.Blocking != 0 {
		t.Errorf("a warn rule must not block: %+v", short)
	}
	if !short.OK() {
		t.Error("warnings do not stop a save")
	}

	long := EvaluatePolicies([]Policy{policy}, subjectWith(RevisionContent{
		Description: "Detects certutil downloading a remote payload, a common LOLBin technique.",
	}))
	if len(long.Violations) != 0 {
		t.Errorf("a long enough description should pass: %+v", long.Violations)
	}
}

func TestBareWildcardPolicy(t *testing.T) {
	policy := mustPolicy(t, Policy{Field: "query_string", Operator: "not_matches", Value: `^\s*\*\s*$`})

	bare := EvaluatePolicies([]Policy{policy}, subjectWith(RevisionContent{QueryString: "*"}))
	if bare.OK() {
		t.Error("a bare wildcard must block")
	}

	real := EvaluatePolicies([]Policy{policy}, subjectWith(RevisionContent{QueryString: `process_name="cmd.exe"`}))
	if len(real.Violations) != 0 {
		t.Errorf("a real query should pass: %+v", real.Violations)
	}
}

func TestTestCountPolicies(t *testing.T) {
	match := mustPolicy(t, Policy{Field: "tests.match_count", Operator: "gte", Value: "1"})
	noMatch := mustPolicy(t, Policy{Field: "tests.no_match_count", Operator: "gte", Value: "1"})
	policies := []Policy{match, noMatch}

	none := EvaluatePolicies(policies, subjectWith(RevisionContent{}))
	if none.Blocking != 2 {
		t.Errorf("an alert with no tests should fail both rules, got %+v", none.Violations)
	}

	onlyMatch := EvaluatePolicies(policies, subjectWith(RevisionContent{},
		AlertTest{Name: "fires", Expectation: "match", Events: []map[string]interface{}{{"a": "b"}}}))
	if onlyMatch.Blocking != 1 || onlyMatch.Violations[0].Field != "tests.no_match_count" {
		t.Errorf("only the no-match rule should fail, got %+v", onlyMatch.Violations)
	}

	both := EvaluatePolicies(policies, subjectWith(RevisionContent{},
		AlertTest{Name: "fires", Expectation: "match", Events: []map[string]interface{}{{"a": "b"}}},
		AlertTest{Name: "quiet", Expectation: "no_match", Events: []map[string]interface{}{{"a": "c"}}}))
	if !both.OK() {
		t.Errorf("both expectations present should pass: %+v", both.Violations)
	}
}

func TestAllPassingDefersUntilTestsRun(t *testing.T) {
	policy := mustPolicy(t, Policy{Field: "tests.all_passing", Operator: "is_true"})
	subject := subjectWith(RevisionContent{},
		AlertTest{Name: "fires", Expectation: "match", Events: []map[string]interface{}{{"a": "b"}}})

	// The editor's live pass has no outcomes: the rule must defer, never accuse.
	live := EvaluatePolicies([]Policy{policy}, subject)
	if len(live.Violations) != 0 {
		t.Errorf("a rule needing test outcomes must not fail before they exist: %+v", live.Violations)
	}
	if len(live.Deferred) != 1 || live.Deferred[0] != "tests.all_passing" {
		t.Errorf("the rule should be reported as deferred, got %+v", live.Deferred)
	}

	subject.TestsRun = true
	subject.TestsPassing = false
	failed := EvaluatePolicies([]Policy{policy}, subject)
	if failed.Blocking != 1 {
		t.Errorf("failing tests should block once they have run: %+v", failed)
	}

	subject.TestsPassing = true
	passed := EvaluatePolicies([]Policy{policy}, subject)
	if !passed.OK() || len(passed.Deferred) != 0 {
		t.Errorf("passing tests should satisfy the rule: %+v", passed)
	}
}

func TestDisabledPolicyIsSkipped(t *testing.T) {
	policy := mustPolicy(t, Policy{Field: "description", Operator: "not_empty"})
	policy.Enabled = false

	result := EvaluatePolicies([]Policy{policy}, subjectWith(RevisionContent{}))
	if len(result.Violations) != 0 {
		t.Error("a disabled rule must not be enforced")
	}
}

func TestPolicyValidationRejectsMismatchedOperator(t *testing.T) {
	p := Policy{Field: "labels", Operator: "min_length", Value: "3", Severity: PolicyWarn, Message: "m"}
	if err := p.Validate(); err == nil {
		t.Error("a string operator on a list field must be rejected")
	}
}

func TestPolicyValidationRejectsBadRegexAndMissingMessage(t *testing.T) {
	bad := Policy{Field: "labels", Operator: "any_matches", Value: "([", Severity: PolicyWarn, Message: "m"}
	if err := bad.Validate(); err == nil {
		t.Error("an invalid regex must be rejected at authoring time, not at save time")
	}

	silent := Policy{Field: "description", Operator: "not_empty", Severity: PolicyWarn}
	if err := silent.Validate(); err == nil {
		t.Error("a rule with no message gives an analyst nothing to act on")
	}
}

func TestPolicyValidationRejectsNonNumericThreshold(t *testing.T) {
	p := Policy{Field: "description", Operator: "min_length", Value: "forty", Severity: PolicyWarn, Message: "m"}
	if err := p.Validate(); err == nil {
		t.Error("a numeric operator needs a number")
	}
}

func TestUnknownFieldStopsBeingEnforced(t *testing.T) {
	// A field dropped from the catalog must not block every save in the fractal.
	policy := Policy{Field: "retired_field", Operator: "not_empty", Severity: PolicyBlock, Message: "m", Enabled: true}
	result := EvaluatePolicies([]Policy{policy}, subjectWith(RevisionContent{}))
	if !result.OK() {
		t.Error("a rule on an unknown field should be ignored, not fatal")
	}
}
