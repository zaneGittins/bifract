package feeds

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"gopkg.in/yaml.v3"

	"bifract/pkg/sigma"
)

// Skip reasons recorded in feed_rule_catalog. An empty reason means the rule was
// imported as an alert.
const (
	SkipMinLevel       = "min_level"
	SkipMinStatus      = "min_status"
	SkipTranslateError = "translate_error"
	SkipParseError     = "parse_error"
	SkipCreateError    = "create_error"
)

// CatalogEntry is one rule a feed's repository offers, imported or not.
type CatalogEntry struct {
	Path       string
	Title      string
	Level      string
	Status     string
	Tags       []string
	RuleHash   string
	Imported   bool
	SkipReason string
	SkipDetail string
}

// ruleMetadata is the part of a rule that can be read without translating it to
// BQL. Keeping it separate is the point: a rule Bifract cannot translate still
// has ATT&CK tags, and that is exactly the gap worth reporting.
type ruleMetadata struct {
	Title  string
	Level  string
	Status string
	Tags   []string
}

// parseRuleMetadata extracts catalog metadata from a feed rule.
func parseRuleMetadata(content string) (ruleMetadata, error) {
	if sigma.IsSigmaRule(content) {
		rule, err := sigma.ParseSigmaRule(content)
		if err != nil {
			return ruleMetadata{}, fmt.Errorf("parse Sigma rule: %w", err)
		}
		return ruleMetadata{
			Title:  rule.Title,
			Level:  rule.Level,
			Status: rule.Status,
			Tags:   sigma.BuildLabels(rule),
		}, nil
	}

	var native struct {
		Name        string   `yaml:"name"`
		Labels      []string `yaml:"labels"`
		QueryString string   `yaml:"queryString"`
	}
	if err := yaml.Unmarshal([]byte(content), &native); err != nil {
		return ruleMetadata{}, fmt.Errorf("parse rule: %w", err)
	}
	if strings.TrimSpace(native.Name) == "" || strings.TrimSpace(native.QueryString) == "" {
		return ruleMetadata{}, fmt.Errorf("rule has no name or queryString")
	}
	return ruleMetadata{Title: native.Name, Tags: native.Labels}, nil
}

// UpsertCatalog records what a sync found. Written in one statement per batch:
// a large feed is thousands of rules, and a round trip each would dominate the
// sync.
func (m *Manager) UpsertCatalog(ctx context.Context, feedID string, entries []CatalogEntry) error {
	const batchSize = 500
	for start := 0; start < len(entries); start += batchSize {
		end := min(start+batchSize, len(entries))
		if err := m.upsertCatalogBatch(ctx, feedID, entries[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) upsertCatalogBatch(ctx context.Context, feedID string, entries []CatalogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`INSERT INTO feed_rule_catalog
		(feed_id, path, title, level, status, tags, rule_hash, imported, skip_reason, skip_detail, last_seen_at) VALUES `)

	args := make([]interface{}, 0, len(entries)*9+1)
	args = append(args, feedID)
	for i, e := range entries {
		if i > 0 {
			sb.WriteString(", ")
		}
		b := len(args)
		fmt.Fprintf(&sb, "($1, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, NOW())",
			b+1, b+2, b+3, b+4, b+5, b+6, b+7, b+8, b+9)
		args = append(args, e.Path, e.Title, e.Level, e.Status, pq.Array(e.Tags),
			e.RuleHash, e.Imported, e.SkipReason, truncate(e.SkipDetail, 500))
	}

	sb.WriteString(` ON CONFLICT (feed_id, path) DO UPDATE SET
		title = EXCLUDED.title, level = EXCLUDED.level, status = EXCLUDED.status,
		tags = EXCLUDED.tags, rule_hash = EXCLUDED.rule_hash, imported = EXCLUDED.imported,
		skip_reason = EXCLUDED.skip_reason, skip_detail = EXCLUDED.skip_detail,
		last_seen_at = NOW()`)

	_, err := m.pg.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("upsert feed rule catalog: %w", err)
	}
	return nil
}

// DeleteCatalogNotIn removes catalog rows for paths the repository no longer has.
// Callers must only run this after a sync that visited every file.
func (m *Manager) DeleteCatalogNotIn(ctx context.Context, feedID string, paths []string) (int, error) {
	res, err := m.pg.Exec(ctx,
		`DELETE FROM feed_rule_catalog WHERE feed_id = $1 AND NOT (path = ANY($2))`,
		feedID, pq.Array(paths))
	if err != nil {
		return 0, fmt.Errorf("prune feed rule catalog: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
