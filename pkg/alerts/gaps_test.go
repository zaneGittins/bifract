package alerts

import (
	"testing"

	"bifract/pkg/attack"
)

func candidate(feed, path, level, reason string, labels ...string) CandidateRule {
	return CandidateRule{
		FeedID: feed, FeedName: "SigmaHQ", Path: path, Title: path,
		Level: level, SkipReason: reason, Labels: labels,
	}
}

func TestComputeGapsRanksActionableFirst(t *testing.T) {
	m := attack.MustGet()

	// T1055 is covered; T1110 has unimported rules waiting; T1548 has none.
	cov := m.Compute([]attack.RuleRow{
		{ID: "r1", Name: "covered", Severity: "high", Enabled: true, Labels: []string{"attack.t1055"}},
	}, attack.Filter{})

	candidates := []CandidateRule{
		candidate("f1", "a.yml", "high", "min_level", "attack.t1110"),
		candidate("f1", "b.yml", "critical", "min_level", "attack.t1110"),
		candidate("f1", "c.yml", "low", "translate_error", "attack.t1055"),
	}

	gaps := computeGaps(m, cov, candidates, attack.Filter{}, 0)
	if len(gaps) == 0 {
		t.Fatal("no gaps computed")
	}

	// A covered technique is never a gap, whatever the catalog holds for it.
	for _, g := range gaps {
		if g.TechniqueID == "T1055" {
			t.Fatal("T1055 is covered but was reported as a gap")
		}
	}

	if gaps[0].TechniqueID != "T1110" {
		t.Errorf("top gap = %s, want T1110 (the one with rules available today)", gaps[0].TechniqueID)
	}
	if gaps[0].Available != 2 {
		t.Errorf("T1110 available = %d, want 2", gaps[0].Available)
	}
	if gaps[0].ByReason["min_level"] != 2 {
		t.Errorf("T1110 by_reason = %v, want 2 under min_level", gaps[0].ByReason)
	}
	// Highest severity candidate first: the drawer leads with what matters.
	if gaps[0].Candidates[0].Level != "critical" {
		t.Errorf("first candidate level = %q, want critical", gaps[0].Candidates[0].Level)
	}
}

// A parent technique's gap can be closed by a rule tagged with any of its
// sub-techniques, so those must count as candidates.
func TestComputeGapsRollsSubTechniqueCandidatesUp(t *testing.T) {
	m := attack.MustGet()
	cov := m.Compute(nil, attack.Filter{})

	gaps := computeGaps(m, cov, []CandidateRule{
		candidate("f1", "a.yml", "high", "min_level", "attack.t1543.003"),
	}, attack.Filter{}, 0)

	var parent, sub *Gap
	for i := range gaps {
		switch gaps[i].TechniqueID {
		case "T1543":
			parent = &gaps[i]
		case "T1543.003":
			sub = &gaps[i]
		}
	}
	if parent == nil || sub == nil {
		t.Fatal("T1543 and T1543.003 should both be gaps with no rules at all")
	}
	if parent.Available != 1 {
		t.Errorf("T1543 available = %d, want 1 inherited from T1543.003", parent.Available)
	}
	if sub.Available != 1 {
		t.Errorf("T1543.003 available = %d, want 1", sub.Available)
	}
}

// The same rule tagged with both a parent and its sub-technique must not be
// counted twice against the parent.
func TestComputeGapsDeduplicatesCandidates(t *testing.T) {
	m := attack.MustGet()
	cov := m.Compute(nil, attack.Filter{})

	gaps := computeGaps(m, cov, []CandidateRule{
		candidate("f1", "a.yml", "high", "min_level", "attack.t1543", "attack.t1543.003"),
	}, attack.Filter{}, 0)

	for _, g := range gaps {
		if g.TechniqueID == "T1543" {
			if g.Available != 1 {
				t.Errorf("T1543 available = %d, want 1", g.Available)
			}
			return
		}
	}
	t.Fatal("T1543 not found in gaps")
}

func TestComputeGapsRespectsLimitAndPlatform(t *testing.T) {
	m := attack.MustGet()
	cov := m.Compute(nil, attack.Filter{})

	if got := len(computeGaps(m, cov, nil, attack.Filter{}, 5)); got != 5 {
		t.Errorf("limit 5 returned %d gaps", got)
	}

	all := len(computeGaps(m, cov, nil, attack.Filter{}, 0))
	windows := len(computeGaps(m, cov, nil, attack.Filter{Platform: "Windows"}, 0))
	if windows == 0 || windows >= all {
		t.Errorf("Windows gaps = %d, want fewer than the unfiltered %d", windows, all)
	}
}

func TestComputeGapsCapsCandidatesPerTechnique(t *testing.T) {
	m := attack.MustGet()
	cov := m.Compute(nil, attack.Filter{})

	var many []CandidateRule
	for i := 0; i < maxCandidatesPerGap+10; i++ {
		many = append(many, candidate("f1", string(rune('a'+i))+".yml", "high", "min_level", "attack.t1110"))
	}

	gaps := computeGaps(m, cov, many, attack.Filter{}, 0)
	for _, g := range gaps {
		if g.TechniqueID != "T1110" {
			continue
		}
		// The count stays honest even though the list is truncated for display.
		if g.Available != maxCandidatesPerGap+10 {
			t.Errorf("available = %d, want %d", g.Available, maxCandidatesPerGap+10)
		}
		if len(g.Candidates) != maxCandidatesPerGap {
			t.Errorf("candidates rendered = %d, want %d", len(g.Candidates), maxCandidatesPerGap)
		}
		return
	}
	t.Fatal("T1110 not found in gaps")
}
