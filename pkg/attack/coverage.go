package attack

import "sort"

// RuleRow is the compact projection of an alert that coverage needs. Keeping it
// narrow lets the aggregation stay a pure function over a cheap query.
type RuleRow struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Severity string   `json:"severity"`
	Enabled  bool     `json:"enabled"`
	Labels   []string `json:"labels"`
	FeedID   string   `json:"feed_id,omitempty"`
	FeedName string   `json:"feed_name,omitempty"`
}

// Filter narrows which rules count as coverage and which techniques are in scope.
type Filter struct {
	EnabledOnly bool
	Severity    string // exact severity, empty for all
	FeedID      string // "" all, "none" manual rules only, otherwise a feed id
	Platform    string // restrict the technique universe to one ATT&CK platform
}

func (f Filter) matches(r *RuleRow) bool {
	if f.EnabledOnly && !r.Enabled {
		return false
	}
	if f.Severity != "" && r.Severity != f.Severity {
		return false
	}
	switch f.FeedID {
	case "":
	case "none":
		if r.FeedID != "" {
			return false
		}
	default:
		if r.FeedID != f.FeedID {
			return false
		}
	}
	return true
}

// TechniqueCoverage is one cell's tally.
//
// Direct and Inherited are kept apart on purpose: "we have a rule for T1543.003"
// and "we have rules for some sub-technique of T1543" are different claims, and
// collapsing them is how coverage maps end up lying.
type TechniqueCoverage struct {
	Direct      int    `json:"direct"`
	Inherited   int    `json:"inherited"`
	Total       int    `json:"total"`
	Enabled     int    `json:"enabled"`
	MaxSeverity string `json:"max_severity,omitempty"`
	// SubsCovered/SubsTotal are only meaningful on a parent technique.
	SubsCovered int `json:"subs_covered,omitempty"`
	SubsTotal   int `json:"subs_total,omitempty"`
}

// TacticSummary is a matrix column header: covered top-level techniques out of
// the total in that column.
type TacticSummary struct {
	Total       int `json:"total"`
	Covered     int `json:"covered"`
	SubsTotal   int `json:"subs_total"`
	SubsCovered int `json:"subs_covered"`
	RuleCount   int `json:"rule_count"`
}

// Summary is the headline strip above the matrix.
type Summary struct {
	MatrixVersion        string                   `json:"matrix_version"`
	TechniquesTotal      int                      `json:"techniques_total"`
	TechniquesCovered    int                      `json:"techniques_covered"`
	SubTechniquesTotal   int                      `json:"subtechniques_total"`
	SubTechniquesCovered int                      `json:"subtechniques_covered"`
	PerTactic            map[string]TacticSummary `json:"per_tactic"`
	RulesTotal           int                      `json:"rules_total"`
	RulesMapped          int                      `json:"rules_mapped"`
	RulesUnmapped        int                      `json:"rules_unmapped"`
	RulesTacticOnly      int                      `json:"rules_tactic_only"`
	RulesRetiredTag      int                      `json:"rules_retired_tag"`
	RetiredTags          []string                 `json:"retired_tags,omitempty"`
	WeakestTactics       []string                 `json:"weakest_tactics,omitempty"`
}

// Coverage is the full aggregation returned to the UI.
type Coverage struct {
	Techniques map[string]*TechniqueCoverage `json:"techniques"`
	Summary    Summary                       `json:"summary"`
}

var severityRank = map[string]int{"info": 1, "low": 2, "medium": 3, "high": 4, "critical": 5}

// Compute tallies rule coverage across the matrix. It is a pure function so the
// semantics (sub-technique rollup, tactic-only tags, retired IDs) are testable
// without a database.
func (m *Matrix) Compute(rows []RuleRow, f Filter) *Coverage {
	// Rules per technique ID, deduplicated: a rule tagged both attack.t1059 and
	// attack.t1059.001 must not be counted twice against T1059.
	direct := map[string]map[string]*RuleRow{}
	retired := map[string]struct{}{}

	cov := &Coverage{
		Techniques: map[string]*TechniqueCoverage{},
		Summary:    Summary{MatrixVersion: m.Version, PerTactic: map[string]TacticSummary{}},
	}

	for i := range rows {
		r := &rows[i]
		if !f.matches(r) {
			continue
		}
		cov.Summary.RulesTotal++

		var mapped, tacticOnly, hasRetired bool
		for _, label := range r.Labels {
			kind, id := m.ParseLabel(label)
			switch kind {
			case KindTechnique:
				mapped = true
				if direct[id] == nil {
					direct[id] = map[string]*RuleRow{}
				}
				direct[id][r.ID] = r
			case KindTactic:
				tacticOnly = true
			case KindRetired:
				hasRetired = true
				retired[id] = struct{}{}
			}
		}

		switch {
		case mapped:
			cov.Summary.RulesMapped++
		case tacticOnly:
			// A rule that names only a tactic tells us nothing about which
			// technique it detects, so it is never counted as cell coverage.
			cov.Summary.RulesTacticOnly++
			cov.Summary.RulesUnmapped++
		default:
			cov.Summary.RulesUnmapped++
		}
		if hasRetired {
			cov.Summary.RulesRetiredTag++
		}
	}

	for id := range retired {
		cov.Summary.RetiredTags = append(cov.Summary.RetiredTags, id)
	}
	sort.Strings(cov.Summary.RetiredTags)

	// Roll sub-technique hits up to their parent as inherited coverage.
	for id, rules := range direct {
		tech := m.byID[id]
		if tech == nil {
			continue
		}
		cell := cov.cell(id)
		for _, r := range rules {
			cell.Direct++
			cell.Total++
			if r.Enabled {
				cell.Enabled++
			}
			cell.raise(r.Severity)
		}
		if !tech.Sub || tech.Parent == "" {
			continue
		}
		parent := cov.cell(tech.Parent)
		for ruleID, r := range rules {
			if _, alreadyDirect := direct[tech.Parent][ruleID]; alreadyDirect {
				continue
			}
			parent.Inherited++
			parent.Total++
			if r.Enabled {
				parent.Enabled++
			}
			parent.raise(r.Severity)
		}
	}

	m.summarize(cov, f)
	return cov
}

func (c *Coverage) cell(id string) *TechniqueCoverage {
	if t, ok := c.Techniques[id]; ok {
		return t
	}
	t := &TechniqueCoverage{}
	c.Techniques[id] = t
	return t
}

func (t *TechniqueCoverage) raise(severity string) {
	if severityRank[severity] > severityRank[t.MaxSeverity] {
		t.MaxSeverity = severity
	}
}

// summarize walks the matrix (not the coverage map) so denominators reflect
// every in-scope technique, including the ones with no rules at all.
func (m *Matrix) summarize(cov *Coverage, f Filter) {
	for _, tactic := range m.Tactics {
		var ts TacticSummary
		for _, tech := range m.byTactic[tactic.Short] {
			if !m.InScope(tech, f) {
				continue
			}
			ts.Total++
			cell := cov.Techniques[tech.ID]

			var subsTotal, subsCovered int
			for _, sub := range m.subsOf[tech.ID] {
				if !m.InScope(sub, f) {
					continue
				}
				subsTotal++
				if s := cov.Techniques[sub.ID]; s != nil && s.Direct > 0 {
					subsCovered++
				}
			}
			ts.SubsTotal += subsTotal
			ts.SubsCovered += subsCovered

			if cell != nil {
				cell.SubsTotal = subsTotal
				cell.SubsCovered = subsCovered
				ts.RuleCount += cell.Total
				if cell.Total > 0 {
					ts.Covered++
				}
			} else if subsTotal > 0 {
				// Record the denominator even for an untouched parent so the UI
				// can show "0/7 sub-techniques" without a second lookup.
				c := cov.cell(tech.ID)
				c.SubsTotal = subsTotal
				c.SubsCovered = subsCovered
			}
		}
		cov.Summary.PerTactic[tactic.Short] = ts
	}

	// Global technique totals are counted over the matrix once, since a technique
	// can appear in more than one tactic column.
	seen := map[string]bool{}
	for i := range m.Techniques {
		tech := &m.Techniques[i]
		if !m.InScope(tech, f) || seen[tech.ID] {
			continue
		}
		seen[tech.ID] = true
		cell := cov.Techniques[tech.ID]
		if tech.Sub {
			cov.Summary.SubTechniquesTotal++
			if cell != nil && cell.Direct > 0 {
				cov.Summary.SubTechniquesCovered++
			}
			continue
		}
		cov.Summary.TechniquesTotal++
		if cell != nil && cell.Total > 0 {
			cov.Summary.TechniquesCovered++
		}
	}

	cov.Summary.WeakestTactics = weakest(cov.Summary.PerTactic, 3)
}

// InScope excludes deprecated techniques from every denominator and applies the
// platform filter.
func (m *Matrix) InScope(t *Technique, f Filter) bool {
	if t.Deprecated {
		return false
	}
	if f.Platform == "" {
		return true
	}
	for _, name := range m.PlatformNames(t) {
		if name == f.Platform {
			return true
		}
	}
	return false
}

// weakest returns the tactics with the lowest coverage ratio, ignoring empty
// columns (a 0/0 tactic is not a gap).
func weakest(per map[string]TacticSummary, n int) []string {
	type entry struct {
		short string
		ratio float64
	}
	entries := make([]entry, 0, len(per))
	for short, ts := range per {
		if ts.Total == 0 {
			continue
		}
		entries = append(entries, entry{short, float64(ts.Covered) / float64(ts.Total)})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ratio != entries[j].ratio {
			return entries[i].ratio < entries[j].ratio
		}
		return entries[i].short < entries[j].short
	})
	if len(entries) > n {
		entries = entries[:n]
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.short)
	}
	return out
}

// RulesFor returns the rules covering a technique, newest-severity first, for the
// detail drawer. includeSub folds in rules that only tag its sub-techniques.
func (m *Matrix) RulesFor(rows []RuleRow, f Filter, techniqueID string, includeSub bool) []RuleRow {
	wanted := map[string]bool{techniqueID: true}
	if includeSub {
		for _, sub := range m.subsOf[techniqueID] {
			wanted[sub.ID] = true
		}
	}

	seen := map[string]bool{}
	var out []RuleRow
	for i := range rows {
		r := &rows[i]
		if !f.matches(r) || seen[r.ID] {
			continue
		}
		for _, label := range r.Labels {
			kind, id := m.ParseLabel(label)
			if kind == KindTechnique && wanted[id] {
				seen[r.ID] = true
				out = append(out, *r)
				break
			}
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if severityRank[out[i].Severity] != severityRank[out[j].Severity] {
			return severityRank[out[i].Severity] > severityRank[out[j].Severity]
		}
		return out[i].Name < out[j].Name
	})
	return out
}
