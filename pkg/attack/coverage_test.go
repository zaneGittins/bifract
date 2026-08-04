package attack

import "testing"

func rule(id, severity string, enabled bool, labels ...string) RuleRow {
	return RuleRow{ID: id, Name: "rule " + id, Severity: severity, Enabled: enabled, Labels: labels}
}

func TestComputeDirectAndInherited(t *testing.T) {
	m := MustGet()

	cov := m.Compute([]RuleRow{
		rule("a", "high", true, "attack.t1543.003", "attack.persistence"),
		rule("b", "medium", true, "attack.t1543.003"),
		rule("c", "critical", true, "attack.t1543"),
		rule("d", "low", true, "attack.t1543.001"),
	}, Filter{})

	sub := cov.Techniques["T1543.003"]
	if sub == nil || sub.Direct != 2 || sub.Inherited != 0 || sub.Total != 2 {
		t.Fatalf("T1543.003 = %+v, want direct 2 / inherited 0", sub)
	}
	if sub.MaxSeverity != "high" {
		t.Errorf("T1543.003 max severity = %q, want high", sub.MaxSeverity)
	}

	// The parent gets one direct rule plus three inherited from two sub-techniques.
	parent := cov.Techniques["T1543"]
	if parent == nil || parent.Direct != 1 || parent.Inherited != 3 || parent.Total != 4 {
		t.Fatalf("T1543 = %+v, want direct 1 / inherited 3 / total 4", parent)
	}
	if parent.MaxSeverity != "critical" {
		t.Errorf("T1543 max severity = %q, want critical", parent.MaxSeverity)
	}
	if parent.SubsCovered != 2 {
		t.Errorf("T1543 subs covered = %d, want 2", parent.SubsCovered)
	}
	if parent.SubsTotal < 2 {
		t.Errorf("T1543 subs total = %d, want the full sub-technique count", parent.SubsTotal)
	}
}

// A rule tagged with both a parent and one of its sub-techniques must count once
// against the parent, not twice.
func TestComputeDoesNotDoubleCount(t *testing.T) {
	m := MustGet()

	cov := m.Compute([]RuleRow{
		rule("a", "high", true, "attack.t1543", "attack.t1543.003"),
	}, Filter{})

	parent := cov.Techniques["T1543"]
	if parent.Direct != 1 || parent.Inherited != 0 || parent.Total != 1 {
		t.Errorf("T1543 = %+v, want direct 1 / inherited 0 / total 1", parent)
	}
}

// A rule that names only a tactic says nothing about which technique it detects.
// Counting it as cell coverage would inflate the map, which is exactly the lie
// this feature exists to avoid.
func TestComputeTacticOnlyIsNotCoverage(t *testing.T) {
	m := MustGet()

	cov := m.Compute([]RuleRow{
		rule("a", "high", true, "attack.persistence"),
		rule("b", "high", true, "attack.t1543"),
	}, Filter{})

	if cov.Summary.RulesTacticOnly != 1 {
		t.Errorf("rules_tactic_only = %d, want 1", cov.Summary.RulesTacticOnly)
	}
	if cov.Summary.RulesMapped != 1 {
		t.Errorf("rules_mapped = %d, want 1", cov.Summary.RulesMapped)
	}
	if cov.Summary.RulesUnmapped != 1 {
		t.Errorf("rules_unmapped = %d, want 1", cov.Summary.RulesUnmapped)
	}
	if cov.Summary.TechniquesCovered != 1 {
		t.Errorf("techniques_covered = %d, want 1", cov.Summary.TechniquesCovered)
	}
}

func TestComputeRetiredTags(t *testing.T) {
	m := MustGet()

	cov := m.Compute([]RuleRow{
		rule("a", "high", true, "attack.t9999"),
		rule("b", "high", true, "attack.t1086"), // revoked but resolvable
	}, Filter{})

	if cov.Summary.RulesRetiredTag != 1 {
		t.Errorf("rules_retired_tag = %d, want 1", cov.Summary.RulesRetiredTag)
	}
	if len(cov.Summary.RetiredTags) != 1 || cov.Summary.RetiredTags[0] != "T9999" {
		t.Errorf("retired_tags = %v, want [T9999]", cov.Summary.RetiredTags)
	}
	// The revoked ID still counts as coverage of its replacement.
	if c := cov.Techniques["T1059.001"]; c == nil || c.Direct != 1 {
		t.Errorf("T1059.001 = %+v, want direct 1 via the revoked T1086 tag", c)
	}
}

func TestComputeFilters(t *testing.T) {
	m := MustGet()

	rows := []RuleRow{
		rule("a", "high", true, "attack.t1543"),
		rule("b", "low", false, "attack.t1543"),
		{ID: "c", Name: "c", Severity: "high", Enabled: true, Labels: []string{"attack.t1055"}, FeedID: "feed-1"},
	}

	if cov := m.Compute(rows, Filter{EnabledOnly: true}); cov.Techniques["T1543"].Total != 1 {
		t.Errorf("enabled-only T1543 total = %d, want 1", cov.Techniques["T1543"].Total)
	}
	if cov := m.Compute(rows, Filter{Severity: "low"}); cov.Summary.RulesTotal != 1 {
		t.Errorf("severity=low rules_total = %d, want 1", cov.Summary.RulesTotal)
	}
	if cov := m.Compute(rows, Filter{FeedID: "feed-1"}); cov.Summary.RulesTotal != 1 {
		t.Errorf("feed filter rules_total = %d, want 1", cov.Summary.RulesTotal)
	}
	if cov := m.Compute(rows, Filter{FeedID: "none"}); cov.Summary.RulesTotal != 2 {
		t.Errorf("manual-only rules_total = %d, want 2", cov.Summary.RulesTotal)
	}
}

// The platform filter narrows the technique universe, so denominators must
// shrink with it or the coverage percentage is meaningless.
func TestComputePlatformNarrowsDenominator(t *testing.T) {
	m := MustGet()

	all := m.Compute(nil, Filter{})
	windows := m.Compute(nil, Filter{Platform: "Windows"})

	if windows.Summary.TechniquesTotal == 0 {
		t.Fatal("Windows techniques_total = 0")
	}
	if windows.Summary.TechniquesTotal >= all.Summary.TechniquesTotal {
		t.Errorf("Windows techniques_total = %d, want fewer than the unfiltered %d",
			windows.Summary.TechniquesTotal, all.Summary.TechniquesTotal)
	}
}

func TestSummaryDenominatorsExcludeDeprecated(t *testing.T) {
	m := MustGet()
	cov := m.Compute(nil, Filter{})

	var deprecated int
	for i := range m.Techniques {
		if m.Techniques[i].Deprecated {
			deprecated++
		}
	}
	if deprecated == 0 {
		t.Skip("no deprecated techniques in this matrix")
	}

	total := cov.Summary.TechniquesTotal + cov.Summary.SubTechniquesTotal
	if total != len(m.Techniques)-deprecated {
		t.Errorf("summary totals = %d, want %d (all techniques minus %d deprecated)",
			total, len(m.Techniques)-deprecated, deprecated)
	}
}

func TestPerTacticSummary(t *testing.T) {
	m := MustGet()

	cov := m.Compute([]RuleRow{
		rule("a", "high", true, "attack.t1543"),
	}, Filter{})

	persistence := cov.Summary.PerTactic["persistence"]
	if persistence.Total == 0 {
		t.Fatal("persistence tactic has no techniques")
	}
	if persistence.Covered != 1 {
		t.Errorf("persistence covered = %d, want 1", persistence.Covered)
	}
	// T1543 lives in two columns, so privilege-escalation must count it too.
	if cov.Summary.PerTactic["privilege-escalation"].Covered != 1 {
		t.Errorf("privilege-escalation covered = %d, want 1",
			cov.Summary.PerTactic["privilege-escalation"].Covered)
	}
	if len(cov.Summary.WeakestTactics) != 3 {
		t.Errorf("weakest_tactics = %v, want 3 entries", cov.Summary.WeakestTactics)
	}
}

func TestRulesFor(t *testing.T) {
	m := MustGet()

	rows := []RuleRow{
		rule("a", "low", true, "attack.t1543"),
		rule("b", "critical", true, "attack.t1543.003"),
		rule("c", "high", true, "attack.t1055"),
	}

	direct := m.RulesFor(rows, Filter{}, "T1543", false)
	if len(direct) != 1 || direct[0].ID != "a" {
		t.Errorf("RulesFor(T1543, includeSub=false) = %+v, want just rule a", direct)
	}

	withSubs := m.RulesFor(rows, Filter{}, "T1543", true)
	if len(withSubs) != 2 {
		t.Fatalf("RulesFor(T1543, includeSub=true) returned %d rules, want 2", len(withSubs))
	}
	// Highest severity first, so the drawer leads with what matters.
	if withSubs[0].ID != "b" {
		t.Errorf("first rule = %s, want b (critical)", withSubs[0].ID)
	}
}
