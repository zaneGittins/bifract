package models

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A destination first seen within the recent window is boosted, one known for
// longer than the established window penalized, and one in between, or one the
// linked model has no record of, left alone. A threat intel hit is boosted.
func TestApplyPairModifiers_FirstSeenAndThreatIntel(t *testing.T) {
	mp := (*ModifierParams)(nil).WithDefaults()
	day := 24 * time.Hour
	cases := []struct {
		name  string
		c     PairContext
		fs    float64
		ti    float64
		final float64
	}{
		{"unknown to the model", PairContext{Prevalence: 0.2}, 0, 0, 0.6},
		{"first seen this week", PairContext{Prevalence: 0.2, FirstSeenKnown: true, FirstSeenAge: 2 * day}, 0.15, 0, 0.75},
		{"known for a month", PairContext{Prevalence: 0.2, FirstSeenKnown: true, FirstSeenAge: 30 * day}, 0, 0, 0.6},
		{"known for half a year", PairContext{Prevalence: 0.2, FirstSeenKnown: true, FirstSeenAge: 180 * day}, -0.15, 0, 0.45},
		{"listed in threat intel", PairContext{Prevalence: 0.2, ThreatIntelHit: true}, 0, 0.25, 0.85},
		{"everything at once, clamped", PairContext{Prevalence: 0.01, FirstSeenKnown: true, ThreatIntelHit: true}, 0.15, 0.25, 1},
	}
	for _, c := range cases {
		final, m := ApplyPairModifiers(0.6, c.c, mp)
		if m.FirstSeenScore != c.fs || m.ThreatIntelScore != c.ti || final != c.final {
			t.Errorf("%s: final %v first_seen %v threat %v, want %v %v %v", c.name, final, m.FirstSeenScore, m.ThreatIntelScore, c.final, c.fs, c.ti)
		}
	}

	// A disabled side contributes nothing.
	off := (&ModifierParams{FirstSeenIncrease: -1, ThreatIntelIncrease: -1}).WithDefaults()
	if final, m := ApplyPairModifiers(0.6, PairContext{Prevalence: 0.2, FirstSeenKnown: true, ThreatIntelHit: true}, off); final != 0.6 || m.FirstSeenScore != 0 || m.ThreatIntelScore != 0 {
		t.Errorf("disabled modifiers applied: %v %+v", final, m)
	}
}

// The first-seen context is keyed the way the linked model is: on the
// destination, or on the pair in the model's char(30) encoding.
func TestPairContextsKeying(t *testing.T) {
	now := time.Unix(100*86400, 0)
	p := PairAgg{Src: "10.0.0.1", Dst: "1.2.3.4"}
	byDst := pairContexts{firstSeen: map[string]int64{"1.2.3.4": now.Unix() - 86400}, threat: map[string]bool{"1.2.3.4": true}}
	if c := byDst.context(p, 0.1, now); !c.FirstSeenKnown || c.FirstSeenAge != 24*time.Hour || !c.ThreatIntelHit || c.Prevalence != 0.1 {
		t.Errorf("destination keyed: %+v", c)
	}
	byPair := pairContexts{firstSeenPair: true, firstSeen: map[string]int64{"10.0.0.1\x1e1.2.3.4": now.Unix()}}
	if c := byPair.context(p, 0, now); !c.FirstSeenKnown {
		t.Errorf("pair keyed: %+v", c)
	}
	if c := byPair.context(PairAgg{Src: "10.0.0.2", Dst: "1.2.3.4"}, 0, now); c.FirstSeenKnown {
		t.Errorf("another source's pair matched: %+v", c)
	}
}

func TestFirstSeenContextSQL(t *testing.T) {
	q := firstSeenContextSQL("model_fs", "f'1", 0, false, "SELECT src, dst FROM s")
	mustContain(t, q, "FROM `model_fs` FINAL", "reads the linked model")
	mustContain(t, q, "WHERE fractal_id = 'f\\'1' AND entity_key GLOBAL IN (SELECT DISTINCT dst FROM (SELECT src, dst FROM s))", "candidate destinations")
	mustContain(t, q, "toUInt64(toUnixTimestamp(min(first_seen))) AS fs", "first sighting as seconds")

	pair := firstSeenContextSQL("model_fs", "f1", 90, true, "SELECT src, dst FROM s")
	mustContain(t, pair, "SELECT DISTINCT concat(src, char(30), dst)", "pair keyed in the model's encoding")
	mustContain(t, pair, "day > toDate(now(), 'UTC') - 90", "a windowed model is read over its window")
}

// The threat intel link needs both halves, and a lookup the fractal cannot see
// is refused at save time.
func TestValidateModifierLinksThreatIntel(t *testing.T) {
	m := &Manager{}
	half := ModelDefinition{Modifiers: &ModifierParams{ThreatIntelLookup: "iocs"}}
	if err := m.validateModifierLinks(context.Background(), "f", "", ModelTypeBeacon, half); err == nil || !strings.Contains(err.Error(), "both a lookup and its key column") {
		t.Errorf("lookup without a column: %v", err)
	}
	missing := ModelDefinition{Modifiers: &ModifierParams{ThreatIntelLookup: "iocs", ThreatIntelColumn: "ip"}}
	if err := m.validateModifierLinks(context.Background(), "f", "", ModelTypeBeacon, missing); err == nil || !strings.Contains(err.Error(), `lookup "iocs" not found`) {
		t.Errorf("unknown lookup: %v", err)
	}
	if err := m.validateModifierLinks(context.Background(), "f", "", ModelTypeRarity, missing); err != nil {
		t.Errorf("a non-network model has no modifiers to check: %v", err)
	}
}
