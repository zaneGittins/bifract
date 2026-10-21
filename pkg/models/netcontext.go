package models

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"bifract/pkg/parser"
)

// The first-seen and threat intel modifiers read what the network state lacks:
// a linked first_seen model's first sightings, and a lookup's indicators.

// firstSeenContext is one pass's first sightings from the linked model.
type firstSeenContext struct {
	byKey map[string]int64 // key -> first sighting, unix seconds
	pair  bool             // keyed on (src, dst) rather than dst
	since int64            // when the linked model's table began recording
}

// context is the modifier input for one pair as of now.
func (c firstSeenContext) context(p PairAgg, prevalence float64, now time.Time, mp ModifierParams) PairContext {
	pc := PairContext{Prevalence: prevalence, ThreatIntelHit: p.ThreatIntelHit}
	key := p.Dst
	if c.pair {
		key = p.Src + "\x1e" + p.Dst
	}
	fs, ok := c.byKey[key]
	if !ok || fs <= 0 {
		return pc
	}
	age := now.Sub(time.Unix(fs, 0))
	recent := days(mp.FirstSeenRecentDays)
	// A sighting after now is a clock problem, and a recent one close to when the
	// model began recording may only be where its history starts.
	if age < 0 || (age <= recent && time.Duration(fs-c.since)*time.Second < recent) {
		return pc
	}
	pc.FirstSeenKnown, pc.FirstSeenAge = true, age
	return pc
}

func days(n int) time.Duration { return time.Duration(n) * 24 * time.Hour }

// errLinkMissing is a first_seen_model that names no such model.
var errLinkMissing = errors.New("is not a first/last seen model in this fractal")

// linkedModel is a resolved first_seen_model link.
type linkedModel struct {
	id    string
	pair  bool
	since time.Time // when its table began recording, or a completed backfill's start
}

// linkedFirstSeen resolves a first_seen_model link, refusing keys the network
// state's raw values cannot match or a windowed history. checkSource also refuses
// a key the model's source query computes, which costs a compile.
func (m *Manager) linkedFirstSeen(ctx context.Context, fractalID, name string, netDef ModelDefinition, checkSource bool) (linkedModel, error) {
	var (
		lm             linkedModel
		raw            []byte
		created        time.Time
		anchor, bfFrom sql.NullTime
		bfStatus       string
	)
	err := m.pg.QueryRow(ctx,
		`SELECT id, definition, created_at, backfill_anchor, COALESCE(backfill_status, ''), backfill_from FROM analytics_models
		 WHERE fractal_id = $1 AND name = $2 AND model_type = 'first_seen' AND status IN ('active', 'rebuilding')
		 ORDER BY created_at LIMIT 1`, fractalID, name).Scan(&lm.id, &raw, &created, &anchor, &bfStatus, &bfFrom)
	if errors.Is(err, sql.ErrNoRows) {
		return lm, fmt.Errorf("first_seen_model %q %w", name, errLinkMissing)
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
	computed := func(k string) bool { return isExtractionOutput(k, def.Extractions) }
	if checkSource {
		resolved, err := m.ResolveSource(ctx, def, fractalID, "")
		if err != nil {
			return lm, fmt.Errorf("first_seen_model %q: %w", name, err)
		}
		computed = func(k string) bool {
			return isExtractionOutput(k, def.Extractions) ||
				slices.ContainsFunc(resolved.projections, func(p parser.SourceProjection) bool { return p.Alias == k })
		}
	}
	for _, k := range def.KeyFields {
		if computed(k) {
			return lm, fmt.Errorf("first_seen_model %q keys on %s as its source query computes it, which the network state never holds; key it on the stored field", name, k)
		}
	}

	lm.since = created
	if anchor.Valid {
		lm.since = anchor.Time
	}
	if bfStatus == "completed" && bfFrom.Valid && bfFrom.Time.Before(lm.since) {
		lm.since = bfFrom.Time
	}
	return lm, nil
}

// validateModifierLinks checks a network model's links on save. prev (nil on
// create) skips links it already had unless the field map changed, and never
// fails an edit for a linked model deleted since.
func (m *Manager) validateModifierLinks(ctx context.Context, fractalID string, mt ModelType, def ModelDefinition, prev *ModelDefinition) error {
	if !mt.IsNetwork() || def.Modifiers == nil {
		return nil
	}
	mp := def.Modifiers
	var was ModifierParams
	if prev != nil && prev.Modifiers != nil {
		was = *prev.Modifiers
	}
	if name := strings.TrimSpace(mp.FirstSeenModel); name != "" {
		renamed := prev == nil || name != was.FirstSeenModel
		if renamed || !reflect.DeepEqual(netFieldMap(def), netFieldMap(*prev)) {
			_, err := m.linkedFirstSeen(ctx, fractalID, name, def, true)
			if err != nil && (renamed || !errors.Is(err, errLinkMissing)) {
				return err
			}
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
	_, err := m.threatIntelHit(ctx, fractalID, lookup, column)
	return err
}

// threatIntelHit renders whether a row's dst is in the lookup, as match() probes it.
func (m *Manager) threatIntelHit(ctx context.Context, fractalID, lookup, column string) (string, error) {
	opts, err := m.sourceQueryOptionsErr(ctx, fractalID, "")
	if err != nil {
		return "", fmt.Errorf("resolve threat intel lookup: %w", err)
	}
	return parser.LookupMembership("dst", lookup, column, opts)
}

// threatIntelColumn is the threat intel expression a pair read selects as
// ti_hit, or "" when none is linked or the link no longer resolves (a warning).
func (m *Manager) threatIntelColumn(ctx context.Context, fractalID string, def ModelDefinition) (string, []string) {
	if def.Modifiers == nil || strings.TrimSpace(def.Modifiers.ThreatIntelLookup) == "" {
		return "", nil
	}
	hit, err := m.threatIntelHit(ctx, fractalID, strings.TrimSpace(def.Modifiers.ThreatIntelLookup), strings.TrimSpace(def.Modifiers.ThreatIntelColumn))
	if err != nil {
		return "", []string{err.Error()}
	}
	return hit, nil
}

// loadFirstSeen reads the linked model's first sightings for pairsSQL's (src, dst)
// pairs; what cannot be read leaves the modifier at zero, with a warning.
func (m *Manager) loadFirstSeen(ctx context.Context, fractalID string, def ModelDefinition, pairsSQL, settings string, now time.Time) (firstSeenContext, []string) {
	var out firstSeenContext
	if def.Modifiers == nil || strings.TrimSpace(def.Modifiers.FirstSeenModel) == "" {
		return out, nil
	}
	mp := def.Modifiers.WithDefaults()
	name := strings.TrimSpace(mp.FirstSeenModel)
	lm, err := m.linkedFirstSeen(ctx, fractalID, name, def, false)
	if err != nil {
		return out, []string{err.Error()}
	}
	var warnings []string
	switch {
	case now.Before(lm.since):
		warnings = append(warnings, fmt.Sprintf("first_seen_model %q began recording after this range, so first-seen adjustments say nothing about it", name))
	case now.Sub(lm.since) < days(mp.FirstSeenRecentDays):
		warnings = append(warnings, fmt.Sprintf("first_seen_model %q began recording %s ago; destinations it first saw within %d days of that do not count as recent, backfill it to start sooner",
			name, humanDuration(now.Sub(lm.since)), mp.FirstSeenRecentDays))
	}
	byKey := map[string]int64{}
	table := m.readTableName(&Model{ID: lm.id, CHTableName: chModelTableName(lm.id)})
	if err := m.ch.StreamQuery(ctx, "", firstSeenContextSQL(table, fractalID, lm.pair, pairsSQL)+settings, func(row map[string]interface{}) error {
		byKey[getString(row, "k")] = int64(getUint64(row, "fs"))
		return nil
	}, nil); err != nil {
		return out, append(warnings, fmt.Sprintf("first seen context: %v", err))
	}
	return firstSeenContext{byKey: byKey, pair: lm.pair, since: lm.since.Unix()}, warnings
}

// firstSeenContextSQL reads the linked model's first sighting of each candidate
// key; GLOBAL IN because on a cluster both sides are Distributed tables.
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
