package dictionaries

import (
	"context"
	"fmt"
	"log"
)

// A REGEXP_TREE runs every (?i) expression through RE2 on every row; a pattern
// list ClickHouse can compile for multiMatchAny is screened with one Hyperscan
// pass first. pattern_prefilter records that it compiled, and pattern_rev,
// bumped by every write, keeps a probe from vouching for rows it did not see.

// patternArraySQL is a scalar subquery for a pattern list's expressions, read
// from the same rows its dictionary loads.
func (m *Manager) patternArraySQL(id, keyCol string) string {
	return fmt.Sprintf("(SELECT groupArray(`%s`) %s)", escCH(keyCol), m.liveRows(id, keyCol))
}

// probeSQL compiles a list's expressions. Where replicas can lag, it reads every
// replica: a query reads any one of them, and any subset of a list that compiles
// also compiles.
func (m *Manager) probeSQL(dict *Dictionary) string {
	if !m.replicated {
		return "SELECT multiMatchAny('', " + m.patternArraySQL(dict.ID, dict.KeyColumn) + ")"
	}
	from := m.ch.Topology().FanoutSystemTableArgs("`"+escCH(m.chDB)+"`", "`"+escCH(chTableName(dict.ID))+"`")
	return fmt.Sprintf("SELECT multiMatchAny('', (SELECT groupUniqArray(`%s`) FROM %s WHERE notEmpty(`%s`)))",
		escCH(dict.KeyColumn), from, escCH(dict.KeyColumn))
}

// invalidatePrefilter withdraws a pattern list's screen before its expressions
// change.
func (m *Manager) invalidatePrefilter(ctx context.Context, dict *Dictionary) error {
	if NormalizeKind(dict.Kind) != KindPattern {
		return nil
	}
	if _, err := m.pg.Exec(ctx, `UPDATE dictionaries SET pattern_prefilter = false, pattern_rev = pattern_rev + 1 WHERE id = $1`, dict.ID); err != nil {
		return fmt.Errorf("failed to withdraw the pattern prefilter: %w", err)
	}
	return nil
}

// refreshPrefilter asks ClickHouse, in the background, to compile a pattern
// list's expressions and records whether it could. A large list takes tens of
// seconds to compile; the rev check makes a late verdict harmless.
func (m *Manager) refreshPrefilter(ctx context.Context, dict *Dictionary) {
	if NormalizeKind(dict.Kind) != KindPattern || dict.KeyColumn == "" {
		return
	}
	d := *dict
	m.background(func() { m.probePrefilter(context.WithoutCancel(ctx), &d) })
}

// probePrefilter is refreshPrefilter's work. Best effort: without a verdict the
// list is only slower.
func (m *Manager) probePrefilter(ctx context.Context, dict *Dictionary) {
	var rev int64
	if err := m.pg.QueryRow(ctx, `SELECT pattern_rev FROM dictionaries WHERE id = $1`, dict.ID).Scan(&rev); err != nil {
		log.Printf("[Dictionaries] prefilter %s: %v", dict.ID, err)
		return
	}
	_, err := m.ch.QuerySchema(ctx, m.probeSQL(dict))
	if err != nil {
		log.Printf("[Dictionaries] %s: no Hyperscan prefilter, lookups use the dictionary alone: %v", dict.ID, err)
	}
	if _, err := m.pg.Exec(ctx, `UPDATE dictionaries SET pattern_prefilter = $2 WHERE id = $1 AND pattern_rev = $3`,
		dict.ID, err == nil, rev); err != nil {
		log.Printf("[Dictionaries] prefilter %s: %v", dict.ID, err)
	}
}
