package models

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// A destination first seen within the recent window is boosted, one known for
// longer than the established window penalized, and one in between, or one the
// linked model has no record of, left alone. A threat intel hit is boosted.
func TestApplyPairModifiers_FirstSeenAndThreatIntel(t *testing.T) {
	mp := (*ModifierParams)(nil).WithDefaults()
	cases := []struct {
		name  string
		c     PairContext
		fs    float64
		ti    float64
		final float64
	}{
		{"unknown to the model", PairContext{Prevalence: 0.2}, 0, 0, 0.6},
		{"first seen this week", PairContext{Prevalence: 0.2, FirstSeenKnown: true, FirstSeenAge: days(2)}, 0.15, 0, 0.75},
		{"known for a month", PairContext{Prevalence: 0.2, FirstSeenKnown: true, FirstSeenAge: days(30)}, 0, 0, 0.6},
		{"known for exactly the established window", PairContext{Prevalence: 0.2, FirstSeenKnown: true, FirstSeenAge: days(90)}, -0.15, 0, 0.45},
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
// destination, or on the pair in the model's char(30) encoding. A sighting in
// the future is ignored, and so is a recent one while the model has not yet
// watched for longer than the recent window.
func TestPairContextsContext(t *testing.T) {
	mp := (*ModifierParams)(nil).WithDefaults()
	now := time.Unix(1000*86400, 0)
	p := PairAgg{Src: "10.0.0.1", Dst: "1.2.3.4"}
	byDst := pairContexts{recentKnown: true, firstSeen: map[string]int64{"1.2.3.4": now.Unix() - 86400}, threat: map[string]bool{"1.2.3.4": true}}
	if c := byDst.context(p, 0.1, now, mp); !c.FirstSeenKnown || c.FirstSeenAge != days(1) || !c.ThreatIntelHit || c.Prevalence != 0.1 {
		t.Errorf("destination keyed: %+v", c)
	}
	byPair := pairContexts{recentKnown: true, firstSeenPair: true, firstSeen: map[string]int64{"10.0.0.1\x1e1.2.3.4": now.Unix()}}
	if c := byPair.context(p, 0, now, mp); !c.FirstSeenKnown {
		t.Errorf("pair keyed: %+v", c)
	}
	if c := byPair.context(PairAgg{Src: "10.0.0.2", Dst: "1.2.3.4"}, 0, now, mp); c.FirstSeenKnown {
		t.Errorf("another source's pair matched: %+v", c)
	}
	future := pairContexts{recentKnown: true, firstSeen: map[string]int64{"1.2.3.4": now.Unix() + 3600}}
	if c := future.context(p, 0, now, mp); c.FirstSeenKnown {
		t.Errorf("a first sighting in the future counted: %+v", c)
	}
	young := pairContexts{firstSeen: map[string]int64{"1.2.3.4": now.Unix() - 86400, "5.6.7.8": now.Unix() - 100*86400}}
	if c := young.context(p, 0, now, mp); c.FirstSeenKnown {
		t.Errorf("a young model called a destination recent: %+v", c)
	}
	if c := young.context(PairAgg{Dst: "5.6.7.8"}, 0, now, mp); !c.FirstSeenKnown {
		t.Errorf("a young model's long-known destination was dropped: %+v", c)
	}
}

func TestFirstSeenContextSQL(t *testing.T) {
	q := firstSeenContextSQL("model_fs", "f'1", false, "SELECT src, dst FROM s")
	mustContain(t, q, "FROM `model_fs`\n", "reads the linked model, without FINAL")
	mustContain(t, q, "WHERE fractal_id = 'f\\'1' AND entity_key GLOBAL IN (SELECT DISTINCT dst FROM (SELECT src, dst FROM s))", "candidate destinations")
	mustContain(t, q, "toUInt64(toUnixTimestamp(min(first_seen))) AS fs", "first sighting as seconds")
	mustContain(t, firstSeenContextSQL("model_fs", "f1", true, "x"), "SELECT DISTINCT concat(src, char(30), dst)", "pair keyed in the model's encoding")
}

// The context is read for qualifying pairs only: every pair in the window could
// be many times the set scored.
func TestNetPairsQueriesAreGated(t *testing.T) {
	mustContain(t, BuildNetPairsQuery(ModelDefinition{}, ModelTypeBeacon, "`s`", "f", 7),
		"HAVING sum(conn_count) >= 4 AND sum(conn_count) < 604800", "scorer pairs")
	b := NetPreviewBounds{Start: time.Unix(0, 0), End: time.Unix(86400, 0), Days: 1}
	prev, err := BuildNetPreviewPairs(ModelDefinition{}, ModelTypeLongConnection, "logs", "f", b)
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, prev, "HAVING sum(toFloat64OrZero(fields.`duration`::String)) >= 3600", "preview pairs")
}

// A link is checked against what the network state holds: the linked model's
// keys must be the raw destination, or source and destination in that order, and
// it must keep all its history. Rebuilding models still resolve.
func TestLinkedFirstSeenValidation(t *testing.T) {
	env := newEnv(t)
	ctx := context.Background()
	add := func(name string, def ModelDefinition, status string) {
		seedModel(env, ModelTypeFirstSeen, def, func(r *modelRow) { r.name, r.status = name, status })
	}
	add("dst_seen", ModelDefinition{KeyFields: []string{"dst_ip"}}, "rebuilding")
	add("pair_seen", ModelDefinition{KeyFields: []string{"src_ip", "dst_ip"}}, "active")
	add("reversed", ModelDefinition{KeyFields: []string{"dst_ip", "src_ip"}}, "active")
	add("users", ModelDefinition{KeyFields: []string{"user"}}, "active")
	add("windowed", ModelDefinition{KeyFields: []string{"dst_ip"}, HistoryDays: 30}, "active")
	add("lowered", ModelDefinition{KeyFields: []string{"dst_ip"},
		Extractions: []ExtractionStep{{FromField: "dest", Pattern: "(.*)", OutputField: "dst_ip", Lowercase: true}}}, "active")
	add("broken", ModelDefinition{KeyFields: []string{"dst_ip"}}, "error")

	link := func(name string) ModelDefinition {
		return ModelDefinition{Modifiers: &ModifierParams{FirstSeenModel: name}}
	}
	for name, want := range map[string]string{
		"dst_seen":  "",
		"pair_seen": "",
		"reversed":  "in that order",
		"users":     "keyed on [user]",
		"windowed":  "history window",
		"lowered":   "as its source query computes it",
		"broken":    "is not a first/last seen model",
		"nope":      "is not a first/last seen model",
	} {
		err := env.mgr.validateModifierLinks(ctx, fracA, ModelTypeBeacon, link(name), nil)
		if want == "" && err != nil || want != "" && (err == nil || !strings.Contains(err.Error(), want)) {
			t.Errorf("%s: %v, want %q", name, err, want)
		}
	}

	// An edit that keeps a link it already had does not re-check it, so a
	// linked model deleted since does not block every edit.
	prev := link("nope")
	if err := env.mgr.validateModifierLinks(ctx, fracA, ModelTypeBeacon, link("nope"), &prev); err != nil {
		t.Errorf("an unchanged link was re-checked: %v", err)
	}

	// A network model whose own field map differs links on its own fields.
	custom := link("dst_seen")
	custom.Network = &NetworkFieldMap{DstField: "id.resp_h"}
	if err := env.mgr.validateModifierLinks(ctx, fracA, ModelTypeBeacon, custom, nil); err == nil || !strings.Contains(err.Error(), "[id.resp_h]") {
		t.Errorf("custom field map: %v", err)
	}
}

// The threat intel link needs both halves, and a lookup the fractal cannot see
// is refused at save time.
func TestValidateModifierLinksThreatIntel(t *testing.T) {
	m := &Manager{}
	ctx := context.Background()
	half := ModelDefinition{Modifiers: &ModifierParams{ThreatIntelLookup: "iocs"}}
	if err := m.validateModifierLinks(ctx, "f", ModelTypeBeacon, half, nil); err == nil || !strings.Contains(err.Error(), "both a lookup and its key column") {
		t.Errorf("lookup without a column: %v", err)
	}
	missing := ModelDefinition{Modifiers: &ModifierParams{ThreatIntelLookup: "iocs", ThreatIntelColumn: "ip"}}
	if err := m.validateModifierLinks(ctx, "f", ModelTypeBeacon, missing, nil); err == nil || !strings.Contains(err.Error(), `lookup "iocs" not found`) {
		t.Errorf("unknown lookup: %v", err)
	}
	if err := m.validateModifierLinks(ctx, "f", ModelTypeBeacon, missing, &missing); err != nil {
		t.Errorf("an unchanged lookup was re-checked: %v", err)
	}
	if err := m.validateModifierLinks(ctx, "f", ModelTypeRarity, missing, nil); err != nil {
		t.Errorf("a non-network model has no modifiers to check: %v", err)
	}
}

// A context read that fails leaves that modifier at zero and says why; it never
// fails the pass. A model that has not yet watched for the recent window warns
// that nothing counts as recently seen.
func TestLoadPairContextsDegrades(t *testing.T) {
	def := ModelDefinition{Modifiers: &ModifierParams{FirstSeenModel: "dst_seen"}}
	envWith := func(res fakedb.CHResult) *fakeEnv {
		env := newEnv(t)
		seedModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"dst_ip"}}, func(r *modelRow) {
			r.name, r.createdAt = "dst_seen", time.Now().Add(-48*time.Hour)
		})
		env.ch.On("AS fs\nFROM", res)
		return env
	}

	env := envWith(fakedb.CHResult{IterErr: errors.New("timeout")})
	pc, warnings := env.mgr.loadPairContexts(context.Background(), fracA, def, "SELECT src, dst FROM s", "", time.Now())
	if pc.firstSeen != nil || len(warnings) != 1 || !strings.Contains(warnings[0], "first seen context: ") {
		t.Errorf("failed read: %+v %v", pc, warnings)
	}

	env = envWith(fakedb.CHResult{Cols: cols("k", "String", "fs", "UInt64"), Rows: [][]any{{"1.2.3.4", uint64(1)}}})
	pc, warnings = env.mgr.loadPairContexts(context.Background(), fracA, def, "SELECT src, dst FROM s", "", time.Now())
	if pc.firstSeen["1.2.3.4"] != 1 || pc.recentKnown || len(warnings) != 1 || !strings.Contains(warnings[0], "recorded for less than 7 days") {
		t.Errorf("young model: %+v %v", pc, warnings)
	}
}
