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

// candidatesByTechnique buckets unimported feed rules by the technique each one
// claims. A rule tagged with both a parent and its sub-technique lands under
// both, so callers deduplicate when they roll a parent up.
func candidatesByTechnique(matrix *attack.Matrix, candidates []CandidateRule) map[string][]CandidateRule {
	out := map[string][]CandidateRule{}
	for _, c := range candidates {
		seen := map[string]bool{}
		for _, label := range c.Labels {
			kind, id := matrix.ParseLabel(label)
			if kind != attack.KindTechnique || seen[id] {
				continue
			}
			seen[id] = true
			out[id] = append(out[id], c)
		}
	}
	return out
}

// candidatesFor returns every unimported rule that could close one technique,
// deduplicated. A parent's gap can be closed by a rule tagged with any of its
// sub-techniques, so those count too.
func candidatesFor(matrix *attack.Matrix, byTechnique map[string][]CandidateRule, tech *attack.Technique) []CandidateRule {
	var pool []CandidateRule
	pool = append(pool, byTechnique[tech.ID]...)
	if !tech.Sub {
		for _, sub := range matrix.SubTechniques(tech.ID) {
			pool = append(pool, byTechnique[sub.ID]...)
		}
	}

	seen := make(map[string]bool, len(pool))
	out := pool[:0:0]
	for _, c := range pool {
		key := c.FeedID + "\x00" + c.Path
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, c)
	}
	return out
}

// candidateCounts is how many unimported feed rules could close each uncovered
// technique. The coverage map colours those cells, which is what the ranked gap
// list used to say in prose.
//
// The filter is the one the coverage beside it was computed with, so a technique
// the caller filtered out of the denominators cannot come back as a closable gap.
func candidateCounts(matrix *attack.Matrix, cov *attack.Coverage, candidates []CandidateRule, f attack.Filter) map[string]int {
	byTechnique := candidatesByTechnique(matrix, candidates)
	counts := map[string]int{}
	for i := range matrix.Techniques {
		tech := &matrix.Techniques[i]
		if tech.Deprecated || !matrix.InScope(tech, f) {
			continue
		}
		if cell := cov.Techniques[tech.ID]; cell != nil && cell.Total > 0 {
			continue
		}
		if n := len(candidatesFor(matrix, byTechnique, tech)); n > 0 {
			counts[tech.ID] = n
		}
	}
	return counts
}

// techniqueGap answers the drawer's question about one technique directly.
func techniqueGap(matrix *attack.Matrix, candidates []CandidateRule, tech *attack.Technique) Gap {
	gap := Gap{
		TechniqueID: tech.ID,
		Name:        tech.Name,
		Tactics:     tech.Tactics,
		ByReason:    map[string]int{},
		LogSources:  matrix.LogSourceNames(tech),
	}

	available := candidatesFor(matrix, candidatesByTechnique(matrix, candidates), tech)
	gap.Available = len(available)
	for _, c := range available {
		gap.ByReason[c.SkipReason]++
	}

	sortCandidates(available)
	if len(available) > maxCandidatesPerGap {
		available = available[:maxCandidatesPerGap]
	}
	gap.Candidates = available
	return gap
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
