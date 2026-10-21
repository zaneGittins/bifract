package models

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"bifract/pkg/parser"
)

// netcontext.go loads what the first-seen and threat intel modifiers know about
// a network model's pairs. Neither is in the network state: it holds only the
// window, so how long a destination has been around comes from a linked
// first_seen model, and whether it is a known indicator from a lookup. Both are
// read once per scoring pass for the pass's qualifying pairs, never per pair.

// pairContexts is one pass's modifier context, keyed the way each source is.
type pairContexts struct {
	// firstSeen maps the linked model's key to its first sighting (unix seconds).
	// firstSeenPair says that key is the (source, destination) pair rather than
	// the destination alone.
	firstSeen     map[string]int64
	firstSeenPair bool
	// recentKnown says the linked model has watched for longer than the recent
	// window, so a destination it first saw within that window really is new to
	// it. Until then everything it holds was first seen recently.
	recentKnown bool
	// threat holds the destinations the threat intel lookup lists.
	threat map[string]bool
}

// context is the modifier input for one pair as of now.
func (c pairContexts) context(p PairAgg, prevalence float64, now time.Time, mp ModifierParams) PairContext {
	pc := PairContext{Prevalence: prevalence, ThreatIntelHit: c.threat[p.Dst]}
	key := p.Dst
	if c.firstSeenPair {
		key = p.Src + "\x1e" + p.Dst
	}
	fs, ok := c.firstSeen[key]
	if !ok || fs <= 0 {
		return pc
	}
	// A first sighting after now is a clock problem, not a new destination.
	age := now.Sub(time.Unix(fs, 0))
	if age < 0 || (!c.recentKnown && age <= days(mp.FirstSeenRecentDays)) {
		return pc
	}
	pc.FirstSeenKnown, pc.FirstSeenAge = true, age
	return pc
}

func days(n int) time.Duration { return time.Duration(n) * 24 * time.Hour }

// linkedModel is a resolved first_seen_model link.
type linkedModel struct {
	id   string
	pair bool // keyed on (source, destination) rather than the destination
	// watching is how long the model has recorded: since it was created, or
	// since the start of a completed backfill.
	watching time.Duration
}

// linkedFirstSeen resolves a network model's first_seen_model link in the
// fractal: the oldest model of that name that is active or rebuilding (an edit
// rebuilds it, and the link should outlast that), as modelLookup() resolves
// names. It refuses a model whose key cannot be compared with the network
// state's raw source and destination values, or whose first sightings are
// bounded by a history window.
func (m *Manager) linkedFirstSeen(ctx context.Context, fractalID, name string, netDef ModelDefinition, now time.Time) (linkedModel, error) {
	var (
		lm       linkedModel
		raw      []byte
		created  time.Time
		bfStatus string
		bfFrom   sql.NullTime
	)
	err := m.pg.QueryRow(ctx,
		`SELECT id, definition, created_at, COALESCE(backfill_status, ''), backfill_from FROM analytics_models
		 WHERE fractal_id = $1 AND name = $2 AND model_type = 'first_seen' AND status IN ('active', 'rebuilding')
		 ORDER BY created_at LIMIT 1`, fractalID, name).Scan(&lm.id, &raw, &created, &bfStatus, &bfFrom)
	if errors.Is(err, sql.ErrNoRows) {
		return lm, fmt.Errorf("first_seen_model %q is not a first/last seen model in this fractal", name)
	}
	if err != nil {
		return lm, fmt.Errorf("resolve first_seen_model %q: %w", name, err)
	}
	var def ModelDefinition
	if err := json.Unmarshal(raw, &def); err != nil {
		return lm, fmt.Errorf("first_seen_model %q has an unreadable definition: %w", name, err)
	}

	nf := netFieldMap(netDef)
	switch {
	case slices.Equal(def.KeyFields, []string{nf.DstField}):
	case slices.Equal(def.KeyFields, []string{nf.SrcField, nf.DstField}):
		lm.pair = true
	default:
		return lm, fmt.Errorf("first_seen_model %q is keyed on %v; link one keyed on [%s] (the destination) or [%s, %s] (the pair), in that order",
			name, def.KeyFields, nf.DstField, nf.SrcField, nf.DstField)
	}
	if def.HistoryDaysFor(ModelTypeFirstSeen) > 0 {
		return lm, fmt.Errorf("first_seen_model %q has a %d-day history window, so it cannot say a destination has been known for longer; link one that keeps all history",
			name, def.HistoryDays)
	}
	// The network state holds raw field values, so a key the linked model
	// rewrote (an extraction, a computed column) would never match.
	resolved, err := m.ResolveSource(ctx, def, fractalID, "")
	if err != nil {
		return lm, fmt.Errorf("first_seen_model %q: %w", name, err)
	}
	for _, k := range def.KeyFields {
		if isExtractionOutput(k, def.Extractions) || slices.ContainsFunc(resolved.projections, func(p parser.SourceProjection) bool { return p.Alias == k }) {
			return lm, fmt.Errorf("first_seen_model %q keys on %s as its source query computes it, which the network state never holds; key it on the stored field", name, k)
		}
	}

	start := created
	if bfStatus == "completed" && bfFrom.Valid && bfFrom.Time.Before(start) {
		start = bfFrom.Time
	}
	lm.watching = now.Sub(start)
	return lm, nil
}

// validateModifierLinks checks that what a network model's modifiers link to
// exists, so a typo fails the save rather than leaving a modifier that silently
// never applies. prev is the definition being replaced, nil on create: a link it
// already had is not checked again, so a linked model deleted since does not
// block every later edit of this one.
func (m *Manager) validateModifierLinks(ctx context.Context, fractalID string, mt ModelType, def ModelDefinition, prev *ModelDefinition) error {
	if !mt.IsNetwork() || def.Modifiers == nil {
		return nil
	}
	mp := def.Modifiers
	var was ModifierParams
	if prev != nil && prev.Modifiers != nil {
		was = *prev.Modifiers
	}
	if name := strings.TrimSpace(mp.FirstSeenModel); name != "" && (name != was.FirstSeenModel || prev == nil) {
		if _, err := m.linkedFirstSeen(ctx, fractalID, name, def, time.Now()); err != nil {
			return err
		}
	}
	lookup, column := strings.TrimSpace(mp.ThreatIntelLookup), strings.TrimSpace(mp.ThreatIntelColumn)
	switch {
	case lookup == "" && column == "":
		return nil
	case lookup == "" || column == "":
		return fmt.Errorf("the threat intel modifier needs both a lookup and its key column")
	case prev != nil && lookup == was.ThreatIntelLookup && column == was.ThreatIntelColumn:
		return nil
	}
	opts, err := m.sourceQueryOptionsErr(ctx, fractalID, "")
	if err != nil {
		return fmt.Errorf("resolve threat intel lookup: %w", err)
	}
	_, err = parser.LookupMembership("dst", lookup, column, opts)
	return err
}

// loadPairContexts reads the modifier context for the pairs pairsSQL yields, a
// query with src and dst columns. settings is appended to each read. Nothing
// here fails a pass: a link that no longer resolves, or a context read that
// errors or times out, leaves that modifier at zero and is returned in warnings,
// so the pass still writes its other scores.
func (m *Manager) loadPairContexts(ctx context.Context, fractalID string, def ModelDefinition, pairsSQL, settings string, now time.Time) (pairContexts, []string) {
	var out pairContexts
	var warnings []string
	if def.Modifiers == nil {
		return out, nil
	}
	mp := def.Modifiers.WithDefaults()

	if name := strings.TrimSpace(mp.FirstSeenModel); name != "" {
		if lm, err := m.linkedFirstSeen(ctx, fractalID, name, def, now); err != nil {
			warnings = append(warnings, err.Error())
		} else {
			fs := map[string]int64{}
			table := m.readTableName(&Model{ID: lm.id, CHTableName: chModelTableName(lm.id)})
			err := m.ch.StreamQuery(ctx, "", firstSeenContextSQL(table, fractalID, lm.pair, pairsSQL)+settings, func(row map[string]interface{}) error {
				fs[getString(row, "k")] = int64(getUint64(row, "fs"))
				return nil
			}, nil)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("first seen context: %v", err))
			} else {
				out.firstSeen, out.firstSeenPair = fs, lm.pair
				out.recentKnown = lm.watching > days(mp.FirstSeenRecentDays)
				if !out.recentKnown {
					warnings = append(warnings, fmt.Sprintf("first_seen_model %q has recorded for less than %d days, so no destination counts as recently seen yet; backfill it to start now",
						name, mp.FirstSeenRecentDays))
				}
			}
		}
	}

	if lookup := strings.TrimSpace(mp.ThreatIntelLookup); lookup != "" {
		opts, err := m.sourceQueryOptionsErr(ctx, fractalID, "")
		var hit string
		if err == nil {
			hit, err = parser.LookupMembership("dst", lookup, strings.TrimSpace(mp.ThreatIntelColumn), opts)
		}
		if err != nil {
			warnings = append(warnings, err.Error())
		} else {
			threat := map[string]bool{}
			q := fmt.Sprintf("SELECT DISTINCT dst FROM (%s) WHERE %s", pairsSQL, hit)
			if err := m.ch.StreamQuery(ctx, "", q+settings, func(row map[string]interface{}) error {
				threat[getString(row, "dst")] = true
				return nil
			}, nil); err != nil {
				warnings = append(warnings, fmt.Sprintf("threat intel context: %v", err))
			} else {
				out.threat = threat
			}
		}
	}
	return out, warnings
}

// firstSeenContextSQL reads the linked first_seen model's first sighting of each
// candidate key, in the model's own char(30) composite encoding when it is keyed
// on the pair. No FINAL: min() over the state's min column is already exact,
// whatever has merged. GLOBAL IN because, on a cluster, both sides are
// Distributed tables; on a single node it is an ordinary IN.
func firstSeenContextSQL(table, fractalID string, pair bool, pairsSQL string) string {
	key := "dst"
	if pair {
		key = "concat(src, char(30), dst)"
	}
	return fmt.Sprintf(`SELECT entity_key AS k, toUInt64(toUnixTimestamp(min(first_seen))) AS fs
FROM `+"`%s`"+`
WHERE %s AND entity_key GLOBAL IN (SELECT DISTINCT %s FROM (%s))
GROUP BY entity_key`, table, fractalScopeClause(fractalID), key, pairsSQL)
}
