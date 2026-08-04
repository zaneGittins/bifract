package attack

import "testing"

// TestEmbeddedMatrixIsWellFormed is the guardrail against a bad regeneration:
// cmd/bifract-attackgen validates its output, but nothing else would catch a
// corrupt or truncated data file until the UI rendered an empty grid.
func TestEmbeddedMatrixIsWellFormed(t *testing.T) {
	m, err := Get()
	if err != nil {
		t.Fatalf("load embedded matrix: %v", err)
	}

	if m.Version == "" {
		t.Error("matrix has no version")
	}
	if m.Domain != "enterprise-attack" {
		t.Errorf("domain = %q, want enterprise-attack", m.Domain)
	}
	if len(m.Tactics) < 14 {
		t.Errorf("got %d tactics, want at least 14", len(m.Tactics))
	}
	if len(m.Techniques) < 500 {
		t.Errorf("got %d techniques, want at least 500", len(m.Techniques))
	}

	// The first and last columns anchor kill-chain ordering; alphabetical or
	// map-iteration order would scramble the matrix.
	if m.Tactics[0].Short != "reconnaissance" {
		t.Errorf("first tactic = %q, want reconnaissance", m.Tactics[0].Short)
	}
	if last := m.Tactics[len(m.Tactics)-1].Short; last != "impact" {
		t.Errorf("last tactic = %q, want impact", last)
	}

	shortnames := map[string]bool{}
	for _, tactic := range m.Tactics {
		if tactic.ID == "" || tactic.Short == "" || tactic.Name == "" {
			t.Errorf("tactic %+v is missing a field", tactic)
		}
		if shortnames[tactic.Short] {
			t.Errorf("duplicate tactic shortname %q", tactic.Short)
		}
		shortnames[tactic.Short] = true
	}

	var topLevel, sub int
	for i := range m.Techniques {
		tech := &m.Techniques[i]
		if tech.ID == "" || tech.Name == "" {
			t.Errorf("technique %+v is missing a field", tech)
		}
		for _, short := range tech.Tactics {
			if !shortnames[short] {
				t.Errorf("technique %s references unknown tactic %q", tech.ID, short)
			}
		}
		if tech.Sub {
			sub++
			if m.Technique(tech.Parent) == nil {
				t.Errorf("sub-technique %s has unresolvable parent %q", tech.ID, tech.Parent)
			}
		} else {
			topLevel++
			if len(tech.Tactics) == 0 && !tech.Deprecated {
				t.Errorf("technique %s belongs to no tactic", tech.ID)
			}
		}
	}
	if topLevel < 150 || sub < 300 {
		t.Errorf("got %d top-level and %d sub-techniques, want at least 150/300", topLevel, sub)
	}

	// Every column must have techniques, or the grid renders an empty lane.
	for _, tactic := range m.Tactics {
		if len(m.TechniquesForTactic(tactic.Short)) == 0 {
			t.Errorf("tactic %s has no techniques", tactic.Short)
		}
	}
}

func TestRevokedAliasesResolve(t *testing.T) {
	m := MustGet()
	if len(m.RevokedBy) == 0 {
		t.Fatal("no revoked-by aliases in the embedded matrix")
	}
	for from, to := range m.RevokedBy {
		if from == to {
			t.Errorf("%s is revoked by itself", from)
		}
	}
	// T1086 (PowerShell) was folded into T1059.001 and is still all over
	// published Sigma rule sets.
	if got := m.RevokedBy["T1086"]; got != "T1059.001" {
		t.Errorf("RevokedBy[T1086] = %q, want T1059.001", got)
	}
}

func TestLookupHelpers(t *testing.T) {
	m := MustGet()

	tech := m.Technique("T1543.003")
	if tech == nil {
		t.Fatal("T1543.003 not found")
	}
	if !tech.Sub || tech.Parent != "T1543" {
		t.Errorf("T1543.003 = %+v, want sub-technique of T1543", tech)
	}
	if len(m.SubTechniques("T1543")) == 0 {
		t.Error("T1543 reports no sub-techniques")
	}
	if len(m.PlatformNames(tech)) == 0 {
		t.Error("T1543.003 reports no platforms")
	}

	if got := m.Tactic("persistence"); got == nil || got.ID != "TA0003" {
		t.Errorf("Tactic(persistence) = %+v, want TA0003", got)
	}
	if got := m.Tactic("TA0003"); got == nil || got.Short != "persistence" {
		t.Errorf("Tactic(TA0003) = %+v, want persistence", got)
	}
	if got := m.Tactic("not-a-tactic"); got != nil {
		t.Errorf("Tactic(not-a-tactic) = %+v, want nil", got)
	}
}
