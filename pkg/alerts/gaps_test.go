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

func gapFor(t *testing.T, m *attack.Matrix, id string, candidates []CandidateRule) Gap {
	t.Helper()
	tech := m.Technique(id)
	if tech == nil {
		t.Fatalf("%s missing from the matrix", id)
	}
	return techniqueGap(m, candidates, tech)
}

func TestTechniqueGapCountsWhatIsWaiting(t *testing.T) {
	m := attack.MustGet()

	gap := gapFor(t, m, "T1110", []CandidateRule{
		candidate("f1", "a.yml", "high", "min_level", "attack.t1110"),
		candidate("f1", "b.yml", "critical", "min_level", "attack.t1110"),
		candidate("f1", "c.yml", "low", "translate_error", "attack.t1055"),
	})

	if gap.Available != 2 {
		t.Errorf("T1110 available = %d, want 2", gap.Available)
	}
	if gap.ByReason["min_level"] != 2 {
		t.Errorf("T1110 by_reason = %v, want 2 under min_level", gap.ByReason)
	}
	// Highest severity candidate first: the drawer leads with what matters.
	if gap.Candidates[0].Level != "critical" {
		t.Errorf("first candidate level = %q, want critical", gap.Candidates[0].Level)
	}
	if len(gap.LogSources) == 0 {
		t.Error("expected MITRE telemetry guidance for T1110")
	}
}

// A parent technique's gap can be closed by a rule tagged with any of its
// sub-techniques, so those must count as candidates.
func TestTechniqueGapRollsSubTechniqueCandidatesUp(t *testing.T) {
	m := attack.MustGet()
	candidates := []CandidateRule{candidate("f1", "a.yml", "high", "min_level", "attack.t1543.003")}

	if got := gapFor(t, m, "T1543", candidates).Available; got != 1 {
		t.Errorf("T1543 available = %d, want 1 inherited from T1543.003", got)
	}
	if got := gapFor(t, m, "T1543.003", candidates).Available; got != 1 {
		t.Errorf("T1543.003 available = %d, want 1", got)
	}
	// A sub-technique never inherits from its siblings.
	if got := gapFor(t, m, "T1543.001", candidates).Available; got != 0 {
		t.Errorf("T1543.001 available = %d, want 0", got)
	}
}

// The same rule tagged with both a parent and its sub-technique must not be
// counted twice against the parent.
func TestTechniqueGapDeduplicatesCandidates(t *testing.T) {
	m := attack.MustGet()

	gap := gapFor(t, m, "T1543", []CandidateRule{
		candidate("f1", "a.yml", "high", "min_level", "attack.t1543", "attack.t1543.003"),
	})
	if gap.Available != 1 {
		t.Errorf("T1543 available = %d, want 1", gap.Available)
	}
}

func TestTechniqueGapCapsCandidatesButNotTheCount(t *testing.T) {
	m := attack.MustGet()

	var many []CandidateRule
	for i := 0; i < maxCandidatesPerGap+10; i++ {
		many = append(many, candidate("f1", string(rune('a'+i))+".yml", "high", "min_level", "attack.t1110"))
	}

	gap := gapFor(t, m, "T1110", many)
	if gap.Available != maxCandidatesPerGap+10 {
		t.Errorf("available = %d, want %d", gap.Available, maxCandidatesPerGap+10)
	}
	if len(gap.Candidates) != maxCandidatesPerGap {
		t.Errorf("candidates rendered = %d, want %d", len(gap.Candidates), maxCandidatesPerGap)
	}
}

// The map only marks gaps: a technique with a rule on it is not closable, however
// many candidates a feed still holds for it.
func TestCandidateCountsSkipCoveredTechniques(t *testing.T) {
	m := attack.MustGet()
	cov := m.Compute([]attack.RuleRow{
		{ID: "r1", Name: "covered", Severity: "high", Enabled: true, Labels: []string{"attack.t1055"}},
	}, attack.Filter{})

	counts := candidateCounts(m, cov, []CandidateRule{
		candidate("f1", "a.yml", "high", "min_level", "attack.t1055"),
		candidate("f1", "b.yml", "high", "min_level", "attack.t1110"),
	}, attack.Filter{})

	if _, ok := counts["T1055"]; ok {
		t.Error("T1055 is covered but was counted as closable")
	}
	if counts["T1110"] != 1 {
		t.Errorf("T1110 count = %d, want 1", counts["T1110"])
	}
	// Nothing waiting is left out entirely rather than reported as zero.
	if _, ok := counts["T1548"]; ok {
		t.Error("T1548 has no candidates and should not appear")
	}
}

func TestCandidateCountsEmptyWithoutCatalog(t *testing.T) {
	m := attack.MustGet()
	if got := candidateCounts(m, m.Compute(nil, attack.Filter{}), nil, attack.Filter{}); len(got) != 0 {
		t.Errorf("counts = %d entries, want 0 with no catalog", len(got))
	}
}

// A platform filter narrows the denominators, so it must narrow the closable gaps
// with them: an amber cell for a technique the filter excluded is a lie.
func TestCandidateCountsRespectPlatformFilter(t *testing.T) {
	m := attack.MustGet()
	cov := m.Compute(nil, attack.Filter{})
	// T1078.004 is cloud-only, so a Windows filter must drop it.
	candidates := []CandidateRule{candidate("f1", "a.yml", "high", "min_level", "attack.t1078.004")}

	if got := candidateCounts(m, cov, candidates, attack.Filter{})["T1078.004"]; got != 1 {
		t.Fatalf("unfiltered count = %d, want 1", got)
	}
	if got := candidateCounts(m, cov, candidates, attack.Filter{Platform: "Windows"})["T1078.004"]; got != 0 {
		t.Errorf("Windows-filtered count = %d, want 0", got)
	}
}
