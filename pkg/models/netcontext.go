package models

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// netcontext.go loads what the first-seen and threat intel modifiers know about
// a network model's pairs. Neither is in the network state: it holds only the
// window, so how long a destination has been around comes from a linked
// first_seen model, and whether it is a known indicator from a lookup. Both are
// read once per scoring pass for the pass's candidate pairs, never per pair.

// pairContexts is one pass's modifier context, keyed the way each source is.
type pairContexts struct {
	// firstSeen maps the linked model's key to its first sighting (unix seconds).
	// firstSeenPair says that key is the (source, destination) pair rather than
	// the destination alone.
	firstSeen     map[string]int64
	firstSeenPair bool
	// threat holds the destinations the threat intel lookup lists.
	threat map[string]bool
}

// context is the modifier input for one pair as of now.
func (c pairContexts) context(p PairAgg, prevalence float64, now time.Time) PairContext {
	pc := PairContext{Prevalence: prevalence, ThreatIntelHit: c.threat[p.Dst]}
	key := p.Dst
	if c.firstSeenPair {
		key = p.Src + "\x1e" + p.Dst
	}
	if fs, ok := c.firstSeen[key]; ok && fs > 0 {
		pc.FirstSeenKnown = true
		pc.FirstSeenAge = now.Sub(time.Unix(fs, 0))
	}
	return pc
}

// errLinkedFirstSeenModel is a first_seen_model that does not name an active
// first_seen model of the fractal.
var errLinkedFirstSeenModel = errors.New("first_seen_model must name an active first/last seen model in this fractal")

// linkedFirstSeen resolves a first_seen_model name in the fractal: the oldest
// active model of that name, as modelLookup() resolves names.
func (m *Manager) linkedFirstSeen(ctx context.Context, fractalID, name string) (string, ModelDefinition, error) {
	var id string
	var raw []byte
	err := m.pg.QueryRow(ctx,
		`SELECT id, definition FROM analytics_models
		 WHERE fractal_id = $1 AND name = $2 AND model_type = 'first_seen' AND status = 'active'
		 ORDER BY created_at LIMIT 1`, fractalID, name).Scan(&id, &raw)
	if err != nil {
		return "", ModelDefinition{}, fmt.Errorf("%w: %q", errLinkedFirstSeenModel, name)
	}
	var def ModelDefinition
	_ = json.Unmarshal(raw, &def)
	if n := len(def.KeyFields); n != 1 && n != 2 {
		return "", ModelDefinition{}, fmt.Errorf("first_seen_model %q is keyed on %d fields; link one keyed on the destination, or on the source and destination", name, n)
	}
	return id, def, nil
}

// validateModifierLinks checks that what a network model's modifiers link to
// exists, so a typo fails the save rather than leaving a modifier that silently
// never applies.
func (m *Manager) validateModifierLinks(ctx context.Context, fractalID, prismID string, mt ModelType, def ModelDefinition) error {
	if !mt.IsNetwork() || def.Modifiers == nil {
		return nil
	}
	mp := def.Modifiers
	if name := strings.TrimSpace(mp.FirstSeenModel); name != "" {
		if _, _, err := m.linkedFirstSeen(ctx, fractalID, name); err != nil {
			return err
		}
	}
	lookup, column := strings.TrimSpace(mp.ThreatIntelLookup), strings.TrimSpace(mp.ThreatIntelColumn)
	switch {
	case lookup == "" && column == "":
		return nil
	case lookup == "" || column == "":
		return fmt.Errorf("the threat intel modifier needs both a lookup and its key column")
	}
	_, err := parser.LookupMembership("dst", lookup, column, m.sourceQueryOptions(ctx, fractalID, prismID))
	return err
}

// loadPairContexts reads the modifier context for the pairs pairsSQL yields, a
// query with src and dst columns. settings is appended to each read. A link that
// no longer resolves (the model or lookup was deleted after the save) leaves its
// modifier at zero rather than failing the pass, and is returned in warnings.
func (m *Manager) loadPairContexts(ctx context.Context, fractalID string, def ModelDefinition, pairsSQL, settings string) (pairContexts, []string, error) {
	var out pairContexts
	var warnings []string
	if def.Modifiers == nil {
		return out, nil, nil
	}
	mp := def.Modifiers

	if name := strings.TrimSpace(mp.FirstSeenModel); name != "" {
		id, fsDef, err := m.linkedFirstSeen(ctx, fractalID, name)
		if err != nil {
			warnings = append(warnings, err.Error())
		} else {
			out.firstSeenPair = len(fsDef.KeyFields) == 2
			table := m.readTableName(&Model{ID: id, CHTableName: chModelTableName(id)})
			q := firstSeenContextSQL(table, fractalID, fsDef.HistoryDaysFor(ModelTypeFirstSeen), out.firstSeenPair, pairsSQL)
			out.firstSeen = map[string]int64{}
			if err := m.ch.StreamQuery(ctx, "", q+settings, func(row map[string]interface{}) error {
				out.firstSeen[getString(row, "k")] = int64(getUint64(row, "fs"))
				return nil
			}, nil); err != nil {
				return out, warnings, fmt.Errorf("first seen context: %w", err)
			}
		}
	}

	if lookup := strings.TrimSpace(mp.ThreatIntelLookup); lookup != "" {
		hit, err := parser.LookupMembership("dst", lookup, strings.TrimSpace(mp.ThreatIntelColumn), m.sourceQueryOptions(ctx, fractalID, ""))
		if err != nil {
			warnings = append(warnings, err.Error())
		} else {
			out.threat = map[string]bool{}
			q := fmt.Sprintf("SELECT DISTINCT dst FROM (%s) WHERE %s", pairsSQL, hit)
			if err := m.ch.StreamQuery(ctx, "", q+settings, func(row map[string]interface{}) error {
				out.threat[getString(row, "dst")] = true
				return nil
			}, nil); err != nil {
				return out, warnings, fmt.Errorf("threat intel context: %w", err)
			}
		}
	}
	return out, warnings, nil
}

// firstSeenContextSQL reads the linked first_seen model's first sighting of each
// candidate key, in the model's own char(30) composite encoding when it is keyed
// on the pair. GLOBAL IN because, on a cluster, both sides are Distributed
// tables; on a single node it is an ordinary IN.
func firstSeenContextSQL(table, fractalID string, historyDays int, pair bool, pairsSQL string) string {
	key := "dst"
	if pair {
		key = "concat(src, char(30), dst)"
	}
	scope := parser.WithHistory("fractal_id = '"+storage.EscCHStr(fractalID)+"'", historyDays)
	return fmt.Sprintf(`SELECT entity_key AS k, toUInt64(toUnixTimestamp(min(first_seen))) AS fs
FROM `+"`%s`"+` FINAL
WHERE %s AND entity_key GLOBAL IN (SELECT DISTINCT %s FROM (%s))
GROUP BY entity_key`, table, scope, key, pairsSQL)
}
