package attack

import "testing"

func TestParseLabel(t *testing.T) {
	m := MustGet()

	tests := []struct {
		label string
		kind  Kind
		id    string
	}{
		// Techniques, as Sigma writes them.
		{"attack.t1543.003", KindTechnique, "T1543.003"},
		{"attack.t1068", KindTechnique, "T1068"},
		{"attack.T1068", KindTechnique, "T1068"},
		{"ATTACK.T1068", KindTechnique, "T1068"},

		// Tactics by slug, both separators, plus ID form.
		{"attack.persistence", KindTactic, "persistence"},
		{"attack.privilege-escalation", KindTactic, "privilege-escalation"},
		{"attack.privilege_escalation", KindTactic, "privilege-escalation"},
		{"attack.command-and-control", KindTactic, "command-and-control"},
		{"attack.ta0003", KindTactic, "persistence"},

		// Non-technique ATT&CK objects are recognised but never count as coverage.
		{"attack.g0016", KindGroup, "G0016"},
		{"attack.s0002", KindSoftware, "S0002"},
		{"attack.c0001", KindCampaign, "C0001"},

		// Retired IDs must follow revoked-by, not silently vanish.
		{"attack.t1086", KindTechnique, "T1059.001"},

		// Everything else is not ATT&CK.
		{"sigma:high", KindNone, ""},
		{"product:windows", KindNone, ""},
		{"category:process_creation", KindNone, ""},
		{"cve.2021-44228", KindNone, ""},
		{"attack", KindNone, ""},
		{"attack.", KindNone, ""},
		{"attack.not-a-tactic", KindNone, ""},
		{"", KindNone, ""},
	}

	for _, tc := range tests {
		kind, id := m.ParseLabel(tc.label)
		if kind != tc.kind || id != tc.id {
			t.Errorf("ParseLabel(%q) = (%v, %q), want (%v, %q)", tc.label, kind, id, tc.kind, tc.id)
		}
	}
}

// TestParseLabelTacticAlias covers ATT&CK renaming a tactic out from under the
// rule sets: v19 renamed Defense Evasion to Stealth, but Sigma keeps emitting
// attack.defense-evasion. Both must resolve to whichever one this matrix ships.
func TestParseLabelTacticAlias(t *testing.T) {
	m := MustGet()

	for _, label := range []string{"attack.defense-evasion", "attack.defense_evasion", "attack.stealth"} {
		kind, id := m.ParseLabel(label)
		if kind != KindTactic {
			t.Errorf("ParseLabel(%q) kind = %v, want KindTactic", label, kind)
			continue
		}
		if m.Tactic(id) == nil {
			t.Errorf("ParseLabel(%q) = %q, which is not a tactic in this matrix", label, id)
		}
	}
}

// A technique ID that exists in no ATT&CK version must be reported as retired
// rather than dropped, so operators can find rules that map to nothing.
func TestParseLabelRetiredTechnique(t *testing.T) {
	m := MustGet()

	kind, id := m.ParseLabel("attack.t9999")
	if kind != KindRetired || id != "T9999" {
		t.Errorf("ParseLabel(attack.t9999) = (%v, %q), want (KindRetired, T9999)", kind, id)
	}
}

func TestResolveTechniqueRejectsMalformed(t *testing.T) {
	m := MustGet()

	for _, label := range []string{"attack.tabc", "attack.t", "attack.t1059.abc", "attack.tax"} {
		if kind, id := m.ParseLabel(label); kind != KindNone {
			t.Errorf("ParseLabel(%q) = (%v, %q), want KindNone", label, kind, id)
		}
	}
}
