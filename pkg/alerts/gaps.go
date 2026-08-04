package alerts

import (
	"context"
	"fmt"
	"sort"

	"github.com/lib/pq"

	"bifract/pkg/attack"
)

// CandidateRule is a rule a configured feed offers that did not become an alert
// in this scope, with the reason it did not.
type CandidateRule struct {
	FeedID     string   `json:"feed_id"`
	FeedName   string   `json:"feed_name"`
	Path       string   `json:"path"`
	Title      string   `json:"title"`
	Level      string   `json:"level"`
	Status     string   `json:"status"`
	SkipReason string   `json:"skip_reason"`
	SkipDetail string   `json:"skip_detail,omitempty"`
	Labels     []string `json:"-"`
}

// Gap is an uncovered technique plus what could be done about it.
type Gap struct {
	TechniqueID string          `json:"technique_id"`
	Name        string          `json:"name"`
	Tactics     []string        `json:"tactics"`
	Available   int             `json:"available"`
	ByReason    map[string]int  `json:"by_reason"`
	Candidates  []CandidateRule `json:"candidates"`
	LogSources  []string        `json:"log_sources,omitempty"`
	Score       int             `json:"score"`
}

// listCandidateRules loads every unimported feed rule in scope. The catalog is a
// few thousand rows per feed at most, so it is read whole and bucketed in Go
// rather than joined per technique.
func (m *Manager) listCandidateRules(ctx context.Context, fractalID, prismID string) ([]CandidateRule, error) {
	query := `
		SELECT c.feed_id::text, f.name, c.path, c.title, c.level, c.status,
		       c.skip_reason, c.skip_detail, c.tags
		FROM feed_rule_catalog c
		JOIN alert_feeds f ON f.id = c.feed_id
		WHERE c.imported = false
	`
	var args []interface{}
	if prismID != "" {
		query += " AND f.prism_id = $1"
		args = append(args, prismID)
	} else if fractalID != "" {
		query += " AND f.fractal_id = $1"
		args = append(args, fractalID)
	}

	rows, err := m.pg.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list candidate rules: %w", err)
	}
	defer rows.Close()

	var out []CandidateRule
	for rows.Next() {
		var c CandidateRule
		if err := rows.Scan(&c.FeedID, &c.FeedName, &c.Path, &c.Title, &c.Level, &c.Status,
			&c.SkipReason, &c.SkipDetail, pq.Array(&c.Labels)); err != nil {
			return nil, fmt.Errorf("failed to scan candidate rule: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// maxCandidatesPerGap bounds what the drawer renders per technique. Sigma ships
// dozens of rules for popular techniques and the list is a starting point, not
// an inventory.
const maxCandidatesPerGap = 12

// computeGaps ranks uncovered techniques by what can actually be done about them
// today: a technique with rules sitting unimported in a configured feed is
// actionable now, one without is a build.
func computeGaps(matrix *attack.Matrix, cov *attack.Coverage, candidates []CandidateRule, f attack.Filter, limit int) []Gap {
	byTechnique := map[string][]CandidateRule{}
	for _, c := range candidates {
		seen := map[string]bool{}
		for _, label := range c.Labels {
			kind, id := matrix.ParseLabel(label)
			if kind != attack.KindTechnique || seen[id] {
				continue
			}
			seen[id] = true
			byTechnique[id] = append(byTechnique[id], c)
		}
	}

	var gaps []Gap
	for i := range matrix.Techniques {
		tech := &matrix.Techniques[i]
		if tech.Deprecated || !matrix.InScope(tech, f) {
			continue
		}
		if cell := cov.Techniques[tech.ID]; cell != nil && cell.Total > 0 {
			continue
		}

		gap := Gap{
			TechniqueID: tech.ID,
			Name:        tech.Name,
			Tactics:     tech.Tactics,
			ByReason:    map[string]int{},
			LogSources:  matrix.LogSourceNames(tech),
		}

		available := byTechnique[tech.ID]
		// A parent technique's gap can be closed by a rule tagged with any of its
		// sub-techniques, so those count as candidates too.
		if !tech.Sub {
			for _, sub := range matrix.SubTechniques(tech.ID) {
				available = append(available, byTechnique[sub.ID]...)
			}
		}

		seen := map[string]bool{}
		for _, c := range available {
			key := c.FeedID + "\x00" + c.Path
			if seen[key] {
				continue
			}
			seen[key] = true
			gap.Available++
			gap.ByReason[c.SkipReason]++
			gap.Candidates = append(gap.Candidates, c)
		}

		// Uncovered techniques with nothing available are still gaps, but they rank
		// below the ones that can be closed today.
		gap.Score = gap.Available*10 + len(gap.LogSources)
		sortCandidates(gap.Candidates)
		if len(gap.Candidates) > maxCandidatesPerGap {
			gap.Candidates = gap.Candidates[:maxCandidatesPerGap]
		}
		gaps = append(gaps, gap)
	}

	sort.Slice(gaps, func(i, j int) bool {
		if gaps[i].Score != gaps[j].Score {
			return gaps[i].Score > gaps[j].Score
		}
		return gaps[i].TechniqueID < gaps[j].TechniqueID
	})
	if limit > 0 && len(gaps) > limit {
		gaps = gaps[:limit]
	}
	return gaps
}

var levelRank = map[string]int{"informational": 1, "low": 2, "medium": 3, "high": 4, "critical": 5}

func sortCandidates(list []CandidateRule) {
	sort.Slice(list, func(i, j int) bool {
		if levelRank[list[i].Level] != levelRank[list[j].Level] {
			return levelRank[list[i].Level] > levelRank[list[j].Level]
		}
		return list[i].Title < list[j].Title
	})
}
