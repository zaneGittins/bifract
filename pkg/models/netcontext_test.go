package models

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

const (
	testDst  = "203.0.113.10"
	testDst2 = "198.51.100.20"
	testSrc  = "10.0.0.1"
)

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

	off := (&ModifierParams{FirstSeenIncrease: -1, ThreatIntelIncrease: -1}).WithDefaults()
	if final, m := ApplyPairModifiers(0.6, PairContext{Prevalence: 0.2, FirstSeenKnown: true, ThreatIntelHit: true}, off); final != 0.6 || m.FirstSeenScore != 0 || m.ThreatIntelScore != 0 {
		t.Errorf("disabled modifiers applied: %v %+v", final, m)
	}
}

// A sighting is keyed the way the linked model is, and ignored when it is in the
// future or recent but close to when the model began recording.
func TestFirstSeenContext(t *testing.T) {
	mp := (*ModifierParams)(nil).WithDefaults()
	now := time.Unix(1000*86400, 0)
	ago := func(d time.Duration) int64 { return now.Add(-d).Unix() }
	old := ago(days(365))
	p := PairAgg{Src: testSrc, Dst: testDst, ThreatIntelHit: true}

	byDst := firstSeenContext{since: old, byKey: map[string]int64{testDst: ago(days(1))}}
	if c := byDst.context(p, 0.1, now, mp); !c.FirstSeenKnown || c.FirstSeenAge != days(1) || !c.ThreatIntelHit || c.Prevalence != 0.1 {
		t.Errorf("destination keyed: %+v", c)
	}
	byPair := firstSeenContext{since: old, pair: true, byKey: map[string]int64{testSrc + "\x1e" + testDst: now.Unix()}}
	if c := byPair.context(p, 0, now, mp); !c.FirstSeenKnown {
		t.Errorf("pair keyed: %+v", c)
	}
	if c := byPair.context(PairAgg{Src: "10.0.0.2", Dst: testDst}, 0, now, mp); c.FirstSeenKnown {
		t.Errorf("another source's pair matched: %+v", c)
	}
	future := firstSeenContext{since: old, byKey: map[string]int64{testDst: now.Unix() + 3600}}
	if c := future.context(p, 0, now, mp); c.FirstSeenKnown {
		t.Errorf("a first sighting in the future counted: %+v", c)
	}

	// The model began recording 8 days ago: a destination first seen 6.9 days ago
	// came only a day into its history, one first seen 2 days ago came 6 days in.
	young := firstSeenContext{since: ago(days(8)), byKey: map[string]int64{
		testDst: ago(days(7) - 2*time.Hour), testDst2: ago(days(2)), "192.0.2.9": ago(days(100)),
	}}
	if c := young.context(p, 0, now, mp); c.FirstSeenKnown {
		t.Errorf("a sighting at the start of the model's history counted as recent: %+v", c)
	}
	if c := young.context(PairAgg{Dst: testDst2}, 0, now, mp); c.FirstSeenKnown {
		t.Errorf("a sighting under the recent window after the start counted: %+v", c)
	}
	if c := young.context(PairAgg{Dst: "192.0.2.9"}, 0, now, mp); !c.FirstSeenKnown {
		t.Errorf("a long-known destination was dropped: %+v", c)
	}
	settled := firstSeenContext{since: ago(days(30)), byKey: map[string]int64{testDst: ago(days(2))}}
	if c := settled.context(p, 0, now, mp); !c.FirstSeenKnown {
		t.Errorf("a recent sighting well into the model's history was dropped: %+v", c)
	}
}

func TestFirstSeenContextSQL(t *testing.T) {
	q := firstSeenContextSQL("model_fs", "f'1", false, "SELECT src, dst FROM s")
	mustContain(t, q, "FROM `model_fs`\n", "reads the linked model, without FINAL")
	mustContain(t, q, "WHERE fractal_id = 'f\\'1' AND entity_key GLOBAL IN (SELECT DISTINCT dst FROM (SELECT src, dst FROM s))", "candidate destinations")
	mustContain(t, firstSeenContextSQL("model_fs", "f1", true, "x"), "SELECT DISTINCT concat(src, char(30), dst)", "pair keyed in the model's encoding")
}

func TestNetPairsQueriesAreGated(t *testing.T) {
	mustContain(t, BuildNetPairsQuery(ModelDefinition{}, ModelTypeBeacon, "`s`", "f", testWindow),
		"HAVING sum(conn_count) >= 4 AND sum(conn_count) < 604800", "scorer pairs")
	b := NetPreviewBounds{Start: time.Unix(0, 0), End: time.Unix(86400, 0), Days: 1}
	prev, err := BuildNetPreviewPairs(ModelDefinition{}, ModelTypeLongConnection, "logs", "f", b)
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, prev, "HAVING sum(toFloat64OrZero(fields.`duration`::String)) >= 3600", "preview pairs")
}

// Threat intel is a column of the pair read itself, never a query of its own.
func TestThreatIntelIsAPairReadColumn(t *testing.T) {
	read := BuildNetScoreReadQuery(ModelDefinition{}, ModelTypeBeacon, "`s`", "f", testWindow, "dictHas('d', dst)")
	mustContain(t, read, "toUInt64(dictHas('d', dst)) AS ti_hit", "score read")
	b := NetPreviewBounds{Start: time.Unix(0, 0), End: time.Unix(86400, 0), Days: 1}
	prev, _ := BuildNetPreviewAgg(ModelDefinition{}, ModelTypeBeacon, "logs", "f", b, "dictHas('d', dst)")
	mustContain(t, prev, "AS ti_hit", "preview read")
	mustNotContain(t, BuildNetScoreReadQuery(ModelDefinition{}, ModelTypeBeacon, "`s`", "f", testWindow, ""), "ti_hit", "no link")
	if p := pairFromRow(map[string]interface{}{"ti_hit": uint64(1), "ts_list": []uint32{1}}, 0); !p.ThreatIntelHit {
		t.Error("ti_hit not read")
	}
}

// A link must key on the raw destination, or source and destination in order,
// and keep all history; rebuilding models still resolve.
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

	// An unchanged link is not re-checked, so a linked model deleted since does
	// not block the edit.
	prev := link("nope")
	if err := env.mgr.validateModifierLinks(ctx, fracA, ModelTypeBeacon, link("nope"), &prev); err != nil {
		t.Errorf("an unchanged link was re-checked: %v", err)
	}
	// It is re-checked when the field map changes under it.
	moved := link("dst_seen")
	moved.Network = &NetworkFieldMap{DstField: "id.resp_h"}
	prev = link("dst_seen")
	if err := env.mgr.validateModifierLinks(ctx, fracA, ModelTypeBeacon, moved, &prev); err == nil || !strings.Contains(err.Error(), "[id.resp_h]") {
		t.Errorf("field map changed under a link: %v", err)
	}
	gone := link("nope")
	gone.Network = &NetworkFieldMap{DstField: "id.resp_h"}
	prev = link("nope")
	if err := env.mgr.validateModifierLinks(ctx, fracA, ModelTypeBeacon, gone, &prev); err != nil {
		t.Errorf("a deleted link blocked a field map edit: %v", err)
	}
}

func TestValidateModifierLinksThreatIntel(t *testing.T) {
	m := &Manager{}
	ctx := context.Background()
	half := ModelDefinition{Modifiers: &ModifierParams{ThreatIntelLookup: "iocs"}}
	if err := m.validateModifierLinks(ctx, "f", ModelTypeBeacon, half, nil); err == nil || !strings.Contains(err.Error(), "both a lookup and its key column") {
		t.Errorf("lookup without a column: %v", err)
	}
	missing := ModelDefinition{Modifiers: &ModifierParams{ThreatIntelLookup: "iocs", ThreatIntelColumn: "ip"}}
	if err := m.validateModifierLinks(ctx, "f", ModelTypeBeacon, missing, nil); err == nil || !strings.Contains(err.Error(), `"iocs" not found`) {
		t.Errorf("unknown lookup: %v", err)
	}
	if err := m.validateModifierLinks(ctx, "f", ModelTypeBeacon, missing, &missing); err != nil {
		t.Errorf("an unchanged lookup was re-checked: %v", err)
	}
	if err := m.validateModifierLinks(ctx, "f", ModelTypeRarity, missing, nil); err != nil {
		t.Errorf("a non-network model has no modifiers to check: %v", err)
	}
}

// The linked model's recording starts when its table was last created, or at a
// completed backfill's start, never at its creation: a rebuild refills it.
func TestLinkedFirstSeenRecordingStart(t *testing.T) {
	env := newEnv(t)
	created := time.Now().Add(-180 * 24 * time.Hour).UTC().Truncate(time.Second)
	rebuilt := time.Now().Add(-2 * 24 * time.Hour).UTC().Truncate(time.Second)
	backfilled := time.Now().Add(-90 * 24 * time.Hour).UTC().Truncate(time.Second)
	seedModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"dst_ip"}}, func(r *modelRow) {
		r.name, r.createdAt, r.bfAnchor = "rebuilt", created, &rebuilt
	})
	seedModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"dst_ip"}}, func(r *modelRow) {
		r.name, r.createdAt, r.bfAnchor, r.bfStatus, r.bfFrom = "seeded", created, &rebuilt, "completed", &backfilled
	})
	for name, want := range map[string]time.Time{"rebuilt": rebuilt, "seeded": backfilled} {
		lm, err := env.mgr.linkedFirstSeen(context.Background(), fracA, name, ModelDefinition{}, false)
		if err != nil || !lm.since.Equal(want) {
			t.Errorf("%s: since %v, %v; want %v", name, lm.since, err, want)
		}
	}
}

// A context read that fails leaves the modifier at zero and says why; a young
// linked model warns that its recent sightings do not count yet.
func TestLoadFirstSeenDegrades(t *testing.T) {
	def := ModelDefinition{Modifiers: &ModifierParams{FirstSeenModel: "dst_seen"}}
	envWith := func(res fakedb.CHResult) *fakeEnv {
		env := newEnv(t)
		anchor := time.Now().Add(-48 * time.Hour)
		seedModel(env, ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"dst_ip"}}, func(r *modelRow) {
			r.name, r.bfAnchor = "dst_seen", &anchor
		})
		env.ch.On("AS fs\nFROM", res)
		return env
	}

	env := envWith(fakedb.CHResult{IterErr: errors.New("timeout")})
	fs, warnings := env.mgr.loadFirstSeen(context.Background(), fracA, def, "SELECT src, dst FROM s", "", time.Now())
	if fs.byKey != nil || len(warnings) != 2 || !strings.Contains(warnings[1], "first seen context: ") {
		t.Errorf("failed read: %+v %v", fs, warnings)
	}

	env = envWith(fakedb.CHResult{Cols: cols("k", "String", "fs", "UInt64"), Rows: [][]any{{testDst, uint64(1)}}})
	fs, warnings = env.mgr.loadFirstSeen(context.Background(), fracA, def, "SELECT src, dst FROM s", "", time.Now())
	if fs.byKey[testDst] != 1 || len(warnings) != 1 || !strings.Contains(warnings[0], "do not count as recent") {
		t.Errorf("young model: %+v %v", fs, warnings)
	}

	_, warnings = env.mgr.loadFirstSeen(context.Background(), fracA, def, "x", "", time.Now().Add(-30*24*time.Hour))
	if len(warnings) != 1 || !strings.Contains(warnings[0], "began recording after this range") {
		t.Errorf("past range: %v", warnings)
	}
}

// A broken link is logged once, not every pass.
func TestScorerWarnsOncePerChange(t *testing.T) {
	e := &ScorerEngine{}
	var logged []string
	for _, w := range [][]string{{"a"}, {"a"}, nil, {"a"}} {
		before := e.warned["m"]
		e.warnModifiers("m", w)
		if e.warned["m"] != before {
			logged = append(logged, e.warned["m"])
		}
	}
	if strings.Join(logged, "|") != "a||a" {
		t.Errorf("changes recorded: %q", logged)
	}
}

func TestPreviewNetworkColumns(t *testing.T) {
	if got := strings.Join(previewNetworkColumns(ModelDefinition{}), ","); strings.Contains(got, "first_seen_score") || strings.Contains(got, "threat_intel_score") {
		t.Errorf("unlinked: %s", got)
	}
	linked := ModelDefinition{Modifiers: &ModifierParams{FirstSeenModel: "m", ThreatIntelLookup: "l"}}
	if got := strings.Join(previewNetworkColumns(linked), ","); !strings.HasSuffix(got, "prevalence,first_seen_score,threat_intel_score,conn_count") {
		t.Errorf("linked: %s", got)
	}
}
