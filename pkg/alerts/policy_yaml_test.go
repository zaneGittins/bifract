package alerts

import (
	"os"
	"path/filepath"
	"testing"
)

// The shipped example is the first thing a team imports, so a rule that no longer
// validates has to fail here rather than in someone's fractal.
func TestExamplePolicyParses(t *testing.T) {
	path := filepath.Join("..", "..", "exchange", "policies", "soc-baseline.yaml")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}

	policies, err := ParsePolicyDocument(string(content))
	if err != nil {
		t.Fatalf("the shipped example policy does not validate: %v", err)
	}
	if len(policies) < 5 {
		t.Errorf("expected a useful baseline, got %d rules", len(policies))
	}

	var blocking int
	for _, p := range policies {
		if p.Severity == PolicyBlock {
			blocking++
		}
		if p.Message == "" {
			t.Errorf("rule on %s has no message", p.Field)
		}
	}
	if blocking == 0 {
		t.Error("a baseline with nothing blocking teaches that policies are advisory")
	}
}

func TestPolicyDocumentRoundTrip(t *testing.T) {
	original := []Policy{
		{Field: "labels", Operator: "any_matches", Value: `^attack\.t\d{4}`, Message: "tag it", Severity: PolicyBlock, Enabled: true},
		{Field: "description", Operator: "min_length", Value: "40", Message: "explain it", Severity: PolicyWarn, Enabled: false},
	}

	rendered, err := RenderPolicyDocument("test set", original)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParsePolicyDocument(rendered)
	if err != nil {
		t.Fatalf("a rendered document must parse back: %v", err)
	}
	if len(parsed) != len(original) {
		t.Fatalf("round trip changed the rule count: %d -> %d", len(original), len(parsed))
	}
	for i := range parsed {
		if parsed[i].Field != original[i].Field || parsed[i].Operator != original[i].Operator ||
			parsed[i].Value != original[i].Value || parsed[i].Severity != original[i].Severity ||
			parsed[i].Enabled != original[i].Enabled {
			t.Errorf("rule %d changed across the round trip:\n  %+v\n  %+v", i, original[i], parsed[i])
		}
	}
}

func TestPolicyDocumentRejectsBadRuleWholesale(t *testing.T) {
	doc := `
name: broken
rules:
  - field: labels
    operator: any_matches
    value: '^attack\.'
    message: fine
  - field: labels
    operator: min_length
    value: '3'
    message: wrong operator for a list
`
	if _, err := ParsePolicyDocument(doc); err == nil {
		t.Error("one bad rule must reject the whole document rather than half-importing it")
	}
}

// Importing the same document twice must not double the rule set. A rule is the
// check it makes, so local changes to its severity or wording are kept.
func TestMergePoliciesSkipsRulesAlreadyPresent(t *testing.T) {
	existing := []Policy{
		{ID: "1", Field: "labels", Operator: "not_empty", Message: "Label it.", Severity: PolicyWarn, Enabled: false},
	}
	imported := []Policy{
		{Field: "labels", Operator: "not_empty", Message: "Add a label.", Severity: PolicyBlock, Enabled: true},
		{Field: "description", Operator: "min_length", Value: "40", Message: "Describe it.", Severity: PolicyWarn, Enabled: true},
		{Field: "description", Operator: "min_length", Value: "40", Message: "Describe it.", Severity: PolicyWarn, Enabled: true},
		{Field: "description", Operator: "min_length", Value: "80", Message: "Describe it.", Severity: PolicyWarn, Enabled: true},
	}

	got := mergePolicies(existing, imported)
	if len(got) != 3 {
		t.Fatalf("got %d rules, want 3: %+v", len(got), got)
	}
	if got[0] != existing[0] {
		t.Errorf("the existing rule must be kept as it was: %+v", got[0])
	}
	if got[1].Value != "40" || got[2].Value != "80" {
		t.Errorf("unexpected merge: %+v", got)
	}

	got[0].Message = "changed"
	if existing[0].Message != "Label it." {
		t.Error("the merge must not write through to the existing slice")
	}
}
