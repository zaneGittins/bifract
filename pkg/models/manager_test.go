package models

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"bifract/pkg/dictionaries"
	"bifract/pkg/storage"
)

// testManager is a Manager with no Postgres and a ClickHouse client that has no
// connection: enough for every path that must decide before any I/O. A test that
// reaches the database panics on the nil pool, which is the point.
func testManager() *Manager {
	return NewManager(nil, &storage.ClickHouseClient{})
}

// Only the parts of a definition that decide what rows a model captures rebuild
// it. Thresholds, min history and scoring parameters are applied at read time;
// rebuilding for them would drop the model's data for nothing.
func TestDetectionChanged(t *testing.T) {
	base := ModelDefinition{
		Filter:       []FilterCondition{{Field: "level", Op: "=", Value: "a"}},
		PartitionKey: "host", ValueKey: "proc", KeyFields: []string{"user"},
		MinSample: 14, TimeBucket: "day", Window: "1d",
		Alert: &AlertConfig{CoverageThreshold: 0.9},
	}
	keep := map[string]func(*ModelDefinition){
		"alert thresholds": func(d *ModelDefinition) { d.Alert = &AlertConfig{CoverageThreshold: 0.5, PercentThreshold: 3} },
		"min sample":       func(d *ModelDefinition) { d.MinSample = 30 },
		"beacon params":    func(d *ModelDefinition) { d.Beacon = &BeaconParams{ScoreThreshold: 0.6} },
		"long-conn params": func(d *ModelDefinition) { d.LongConn = &LongConnParams{BaseSeconds: 60} },
		"modifiers":        func(d *ModelDefinition) { d.Modifiers = &ModifierParams{PrevalenceIncrease: 0.3} },
	}
	rebuild := map[string]func(*ModelDefinition){
		"source query": func(d *ModelDefinition) { d.SourceBQL = `level = "b"` },
		"filter":       func(d *ModelDefinition) { d.Filter = []FilterCondition{{Field: "level", Op: "=", Value: "b"}} },
		"extractions": func(d *ModelDefinition) {
			d.Extractions = []ExtractionStep{{FromField: "x", Pattern: "(.)", OutputField: "y"}}
		},
		"partition key": func(d *ModelDefinition) { d.PartitionKey = "user" },
		"value key":     func(d *ModelDefinition) { d.ValueKey = "cmd" },
		"key fields":    func(d *ModelDefinition) { d.KeyFields = []string{"user", "host"} },
		"time bucket":   func(d *ModelDefinition) { d.TimeBucket = "hour" },
		"network map":   func(d *ModelDefinition) { d.Network = &NetworkFieldMap{SrcField: "a"} },
		"window":        func(d *ModelDefinition) { d.Window = "7d" },
	}
	for name, edit := range keep {
		d := base
		edit(&d)
		if detectionChanged(base, d) {
			t.Errorf("%s: must not rebuild the model's data", name)
		}
	}
	for name, edit := range rebuild {
		d := base
		edit(&d)
		if !detectionChanged(base, d) {
			t.Errorf("%s: changes what the model captures and must rebuild", name)
		}
	}
}

func TestLinkedAlertName(t *testing.T) {
	if got := linkedAlertName("Rare procs", "0123abcd-ef01-2345-6789-abcdef012345"); got != "Rare procs (model 0123abcd)" {
		t.Errorf("got %q", got)
	}
	if got := linkedAlertName("x", "ab-c"); got != "x (model abc)" {
		t.Errorf("short id: %q", got)
	}
}

func TestAlertSpec(t *testing.T) {
	m := testManager()
	mo := &Model{ID: "11111111-2222", Name: "n", Description: "d", FractalID: "f", PrismID: "p", CreatedBy: "u",
		ModelType: ModelTypeVolumeBaseline, Definition: ModelDefinition{KeyFields: []string{"host"}}}
	spec := m.alertSpec(mo, false)
	if spec.Severity != "medium" || spec.Enabled || spec.FractalID != "f" || spec.PrismID != "p" || spec.CreatedBy != "u" {
		t.Errorf("spec: %+v", spec)
	}
	if spec.QueryString != GenerateQuery("n", mo.Definition, mo.ModelType) {
		t.Errorf("the alert must run the model's generated query, got %q", spec.QueryString)
	}
	mo.Definition.Alert = &AlertConfig{Severity: "critical"}
	if got := m.alertSpec(mo, true); got.Severity != "critical" || !got.Enabled {
		t.Errorf("configured severity / enabled: %+v", got)
	}
}

func TestValidateDefinitionShape(t *testing.T) {
	cases := []struct {
		name string
		mt   ModelType
		def  ModelDefinition
		want string // "" = valid
	}{
		{"rarity ok", ModelTypeRarity, ModelDefinition{PartitionKey: "a", ValueKey: "b"}, ""},
		{"rarity no partition", ModelTypeRarity, ModelDefinition{ValueKey: "b"}, "partition_key is required"},
		{"rarity no value", ModelTypeRarity, ModelDefinition{PartitionKey: "a"}, "value_key is required"},
		{"first_seen no keys", ModelTypeFirstSeen, ModelDefinition{}, "key_fields is required for first_seen"},
		{"volume no keys", ModelTypeVolumeBaseline, ModelDefinition{}, "key_fields is required for volume_baseline"},
		{"volume bad bucket", ModelTypeVolumeBaseline, ModelDefinition{KeyFields: []string{"h"}, TimeBucket: "week"}, "invalid time_bucket"},
		{"volume hourly ok", ModelTypeVolumeBaseline, ModelDefinition{KeyFields: []string{"h"}, TimeBucket: "hour", MinSample: 2}, ""},
		{"volume hourly history too long", ModelTypeVolumeBaseline, ModelDefinition{KeyFields: []string{"h"}, TimeBucket: "hour", MinSample: 1000}, "min history for an hourly model"},
		{"beacon defaults ok", ModelTypeBeacon, ModelDefinition{}, ""},
		{"long conn 14d ok", ModelTypeLongConnection, ModelDefinition{Window: "14d"}, ""},
		{"beacon bad window", ModelTypeBeacon, ModelDefinition{Window: "30d"}, "invalid window"},
		{"unknown type", ModelType("nope"), ModelDefinition{}, "invalid model type"},
		{"bad field name", ModelTypeRarity, ModelDefinition{PartitionKey: "a b", ValueKey: "c"}, "is not a field name"},
		{"bad network field", ModelTypeBeacon, ModelDefinition{Network: &NetworkFieldMap{SrcField: "a;b"}}, "src_field"},
		{"bad filter field", ModelTypeFirstSeen, ModelDefinition{KeyFields: []string{"k"}, Filter: []FilterCondition{{Field: "x)"}}}, "filter field"},
	}
	for _, c := range cases {
		err := validateDefinitionShape(c.mt, c.def)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: unexpected error %v", c.name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%s: error %v, want one containing %q", c.name, err, c.want)
		}
	}
}

func TestValidateCreateRequest(t *testing.T) {
	ok := CreateRequest{Name: "n", ModelType: ModelTypeFirstSeen, Definition: ModelDefinition{KeyFields: []string{"u"}}}
	if err := validateCreateRequest(ok); err != nil {
		t.Errorf("an empty alert mode means none: %v", err)
	}
	for _, mode := range []string{"none", "paused", "active"} {
		r := ok
		r.AlertMode = mode
		if err := validateCreateRequest(r); err != nil {
			t.Errorf("mode %q: %v", mode, err)
		}
	}
	bad := ok
	bad.AlertMode = "loud"
	if err := validateCreateRequest(bad); err == nil {
		t.Error("an unknown alert mode was accepted")
	}
	blank := ok
	blank.Name = "   "
	if err := validateCreateRequest(blank); err == nil || !strings.Contains(err.Error(), "name is required") {
		t.Errorf("blank name: %v", err)
	}
}

// Create rejects a request before writing anything: an invalid definition, or a
// source query the translator refuses, must never become a stored model that
// fails later in the background.
func TestCreateRejectsBeforeAnyWrite(t *testing.T) {
	m := testManager()
	ctx := context.Background()
	if _, err := m.Create(ctx, "f", CreateRequest{Name: "n", ModelType: ModelTypeRarity}, "u"); err == nil {
		t.Error("invalid shape accepted")
	}
	req := CreateRequest{Name: "n", ModelType: ModelTypeFirstSeen,
		Definition: ModelDefinition{KeyFields: []string{"u"}, SourceBQL: `level = "a" | groupby(host)`}}
	if _, err := m.Create(ctx, "f", req, "u"); err == nil || !strings.Contains(err.Error(), "collapses rows") {
		t.Errorf("uncompilable source accepted: %v", err)
	}
}

func TestPreviewRejectsBeforeAnyQuery(t *testing.T) {
	m := testManager()
	rng := PreviewRange{Start: time.Now().Add(-time.Hour), End: time.Now()}
	if _, err := m.Preview(context.Background(), "f", ModelTypeVolumeBaseline, ModelDefinition{}, rng); err == nil {
		t.Error("invalid shape previewed")
	}
	def := ModelDefinition{KeyFields: []string{"u"}, SourceBQL: `level = "a" | sort(host)`}
	if _, err := m.Preview(context.Background(), "f", ModelTypeFirstSeen, def, rng); err == nil {
		t.Error("uncompilable source previewed")
	}
}

func TestTestExtractionRequiresAStep(t *testing.T) {
	if _, _, err := testManager().TestExtraction(context.Background(), "f", nil, nil); err == nil {
		t.Error("no extractions accepted")
	}
}

// A type with no read path errors rather than querying a table of the wrong shape,
// and a tlsh index (no alert, no threshold) has no distribution to chart.
func TestReadPathsByType(t *testing.T) {
	m := testManager()
	ctx := context.Background()
	unknown := &Model{ID: "x", ModelType: ModelType("nope"), CHTableName: "t"}
	if _, _, err := m.GetData(ctx, unknown, "f", DataQuery{}); err == nil {
		t.Error("GetData: unknown type")
	}
	if _, err := m.GetStats(ctx, unknown, "f"); err == nil {
		t.Error("GetStats: unknown type")
	}
	if _, err := m.GetHistogram(ctx, unknown, "f"); err == nil {
		t.Error("GetHistogram: unknown type")
	}
	hist, err := m.GetHistogram(ctx, &Model{ID: "x", ModelType: ModelTypeTLSH, CHTableName: "t"}, "f")
	if hist != nil || err != nil {
		t.Errorf("tlsh histogram: %v, %v", hist, err)
	}
	// Single node: reads go to the model's own table.
	if got := m.readTableName(&Model{ID: "x", CHTableName: "model_x"}); got != "model_x" {
		t.Errorf("readTableName = %q", got)
	}
	if got := m.networkStateReadTable("x"); got != chModelStateName("x") {
		t.Errorf("networkStateReadTable = %q", got)
	}
}

// A findings page that the model's rule selects nothing from short-circuits
// without a query (the rule is absent), whatever the type.
func TestFindingsWithoutRuleReadsNothing(t *testing.T) {
	m := testManager()
	rows, total, err := m.GetData(context.Background(),
		&Model{ID: "x", ModelType: ModelTypeRarity, CHTableName: "t"}, "f", DataQuery{View: DataViewFindings})
	if rows != nil || total != 0 || err != nil {
		t.Errorf("got %v %d %v", rows, total, err)
	}
}

func TestNetworkScoreThreshold(t *testing.T) {
	if got := networkScoreThreshold(ModelDefinition{}, ModelTypeBeacon); got != 0.8 {
		t.Errorf("beacon default = %v", got)
	}
	if got := networkScoreThreshold(ModelDefinition{}, ModelTypeLongConnection); got != 0.5 {
		t.Errorf("long connection default = %v", got)
	}
	def := ModelDefinition{Beacon: &BeaconParams{ScoreThreshold: 0.65}, LongConn: &LongConnParams{ScoreThreshold: 0.9}}
	if networkScoreThreshold(def, ModelTypeBeacon) != 0.65 || networkScoreThreshold(def, ModelTypeLongConnection) != 0.9 {
		t.Error("configured thresholds must be used per type")
	}
}

// fillHistogram gives the chart a complete, stable x-axis: missing buckets read 0,
// and a bucket index the labels do not cover is dropped rather than misfiled.
func TestFillHistogram(t *testing.T) {
	rows := []map[string]interface{}{
		{"bucket": uint64(0), "cnt": uint64(3)},
		{"bucket": int64(2), "cnt": float64(4)},
		{"bucket": uint32(2), "cnt": uint32(1)},
		{"bucket": uint64(9), "cnt": uint64(100)},
	}
	got := fillHistogram(rows, []string{"a", "b", "c"})
	want := []histBucket{{"a", 3}, {"b", 0}, {"c", 5}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestNumToUint64(t *testing.T) {
	cases := []struct {
		in   interface{}
		want uint64
	}{
		{uint64(5), 5}, {uint32(5), 5}, {int64(5), 5}, {int64(-5), 0}, {float64(5.7), 5}, {float64(-1), 0}, {"5", 0}, {nil, 0},
	}
	for _, c := range cases {
		if got := numToUint64(c.in); got != c.want {
			t.Errorf("numToUint64(%#v) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestHistogramSQL(t *testing.T) {
	q := histogramQuerySQL("SELECT x FROM t", "least(toUInt64(x), 9)")
	if q != "SELECT least(toUInt64(x), 9) AS bucket, count() AS cnt FROM (SELECT x FROM t) GROUP BY bucket" {
		t.Errorf("got %s", q)
	}
	// Top-level only: the preview appends SETTINGS, which a subquery may not carry.
	if strings.Contains(q, "SETTINGS") {
		t.Error("histogram SQL must not carry its own SETTINGS")
	}
	inner := firstSeenCountInner("`t` FINAL", "f", "entity_key")
	mustContain(t, inner, "WHERE event_count >= 1", "log10 of zero has no bucket")
	mustContain(t, inner, "GROUP BY entity_key", "one count per entity")
}

func TestConvertDaysToStrings(t *testing.T) {
	east := time.FixedZone("UTC+10", 10*3600)
	rows := []map[string]interface{}{
		{"days": []time.Time{time.Date(2026, 3, 2, 5, 0, 0, 0, east), time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)}},
		{"days": []interface{}{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "junk"}},
		{"days": "left alone"},
		{"other": 1},
	}
	convertDaysToStrings(rows)
	// 05:00 at UTC+10 is the previous day in UTC.
	if got := rows[0]["days"]; !reflect.DeepEqual(got, []string{"2026-03-01", "2026-03-03"}) {
		t.Errorf("[]time.Time: %v", got)
	}
	if got := rows[1]["days"]; !reflect.DeepEqual(got, []string{"2026-01-01"}) {
		t.Errorf("[]interface{}: %v", got)
	}
	if rows[2]["days"] != "left alone" {
		t.Errorf("other types untouched: %v", rows[2]["days"])
	}
	if _, ok := rows[3]["days"]; ok {
		t.Error("a row without days gained one")
	}
}

// fakeModelRow scans fixed values into scanModelRow's destinations, and records
// how many it was asked for.
type fakeModelRow struct {
	vals []interface{}
	n    int
}

func (f *fakeModelRow) Scan(dest ...interface{}) error {
	f.n = len(dest)
	if len(dest) != len(f.vals) {
		return fmt.Errorf("scan: %d destinations, %d values", len(dest), len(f.vals))
	}
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(f.vals[i]))
	}
	return nil
}

func modelRowValues(linked string, enabled sql.NullBool, last sql.NullTime, def string) []interface{} {
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	return []interface{}{
		"id1", "f1", "", "name", "desc", ModelTypeRarity,
		[]byte(def), "model_id1", "", "active", "paused", linked, "",
		"u", now, now,
		"none", "", 0, 0,
		(*time.Time)(nil), (*time.Time)(nil), (*time.Time)(nil), "", (*time.Time)(nil),
		enabled, last,
	}
}

// The alert mode a model reports is derived from its linked alert, whichever page
// toggled it. The stored column is only the fallback with no alert at all.
func TestScanModelRowDerivesAlertMode(t *testing.T) {
	fired := time.Date(2026, 9, 30, 8, 0, 0, 0, time.FixedZone("x", 3600))
	cases := []struct {
		name     string
		linked   string
		enabled  sql.NullBool
		last     sql.NullTime
		mode     string
		hasFired bool
	}{
		{"enabled alert", "a1", sql.NullBool{Bool: true, Valid: true}, sql.NullTime{Time: fired, Valid: true}, "active", true},
		{"paused alert", "a1", sql.NullBool{Bool: false, Valid: true}, sql.NullTime{}, "paused", false},
		{"alert row gone", "a1", sql.NullBool{}, sql.NullTime{}, "paused", false},
		{"no linked alert", "", sql.NullBool{Bool: true, Valid: true}, sql.NullTime{Time: fired, Valid: true}, "none", false},
	}
	for _, c := range cases {
		row := &fakeModelRow{vals: modelRowValues(c.linked, c.enabled, c.last, `{"partition_key":"h","value_key":"p"}`)}
		mo, err := scanModelRow(row)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if mo.AlertMode != c.mode {
			t.Errorf("%s: alert mode %q, want %q", c.name, mo.AlertMode, c.mode)
		}
		if (mo.LastAlertAt != nil) != c.hasFired {
			t.Errorf("%s: last alert %v", c.name, mo.LastAlertAt)
		}
		if mo.LastAlertAt != nil && (mo.LastAlertAt.Location() != time.UTC || !mo.LastAlertAt.Equal(fired)) {
			t.Errorf("%s: last alert must be the same instant in UTC: %v", c.name, mo.LastAlertAt)
		}
		if mo.Definition.PartitionKey != "h" {
			t.Errorf("%s: definition not decoded: %+v", c.name, mo.Definition)
		}
	}

	// An unreadable stored definition yields an empty one, not a failed listing.
	mo, err := scanModelRow(&fakeModelRow{vals: modelRowValues("", sql.NullBool{}, sql.NullTime{}, "{not json")})
	if err != nil || !reflect.DeepEqual(mo.Definition, ModelDefinition{}) {
		t.Errorf("bad definition: %+v %v", mo, err)
	}
	if _, err := scanModelRow(&fakeModelRow{vals: nil}); err == nil {
		t.Error("a scan error must surface")
	}
}

// modelColumns and scanModelRow's destinations must stay in step. A variadic Scan
// cannot catch a column added to one and not the other at compile time.
func TestModelColumnsMatchScanDestinations(t *testing.T) {
	depth, cols := 0, 1
	for _, r := range modelColumns {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				cols++
			}
		}
	}
	row := &fakeModelRow{vals: modelRowValues("", sql.NullBool{}, sql.NullTime{}, "{}")}
	if _, err := scanModelRow(row); err != nil {
		t.Fatal(err)
	}
	if row.n != cols {
		t.Errorf("modelColumns selects %d columns, scanModelRow scans %d", cols, row.n)
	}
}

type fakeDicts struct {
	fractal, prism string
	calls          int
	scope          dictionaries.Scope
	err            error
}

func (f *fakeDicts) ListDictionaryMappings(_ context.Context, fractalID, prismID string) (dictionaries.Scope, error) {
	f.calls++
	f.fractal, f.prism = fractalID, prismID
	return f.scope, f.err
}

// A source query compiles against the dictionaries of the model's own scope, and
// nothing else. No scope means no lookup; a failed lookup compiles without lists
// rather than failing the model.
func TestSourceQueryOptionsUseTheModelScope(t *testing.T) {
	m := testManager()
	if opts := m.sourceQueryOptions(context.Background(), "f", ""); opts.DictionaryDatabase != "logs" || opts.Dictionaries != nil {
		t.Errorf("no resolver: %+v", opts)
	}

	d := &fakeDicts{scope: dictionaries.Scope{
		Mappings:        map[string]map[string]string{"bad_ips": {"ip": "dict_bad_ips"}},
		CaseInsensitive: map[string]bool{"bad_ips": true},
		Network:         map[string]bool{"dict_bad_ips": true},
		Pattern:         map[string]bool{"dict_p": true},
	}}
	m.SetDictionaryResolver(d)
	if m.sourceQueryOptions(context.Background(), "", ""); d.calls != 0 {
		t.Error("an unscoped compile must not list dictionaries")
	}
	opts := m.sourceQueryOptions(context.Background(), "f1", "p1")
	if d.fractal != "f1" || d.prism != "p1" {
		t.Errorf("resolver asked for %q/%q", d.fractal, d.prism)
	}
	if !reflect.DeepEqual(opts.Dictionaries, d.scope.Mappings) || !opts.CaseInsensitiveDicts["bad_ips"] ||
		!opts.NetworkDicts["dict_bad_ips"] || !opts.PatternDicts["dict_p"] {
		t.Errorf("scope not carried: %+v", opts)
	}

	d.err = errors.New("pg down")
	if opts := m.sourceQueryOptions(context.Background(), "f1", ""); opts.Dictionaries != nil || opts.DictionaryDatabase != "logs" {
		t.Errorf("failed lookup: %+v", opts)
	}
}

func TestResolveSource(t *testing.T) {
	m := testManager()
	plain := ModelDefinition{KeyFields: []string{"u"}}
	got, err := m.ResolveSource(context.Background(), plain, "f", "")
	if err != nil || got.compiledSet {
		t.Errorf("a definition without a source query compiles nothing: %+v %v", got, err)
	}
	src := ModelDefinition{KeyFields: []string{"u"}, SourceBQL: `level = "warn"`}
	got, err = m.ResolveSource(context.Background(), src, "f", "")
	if err != nil {
		t.Fatal(err)
	}
	if !got.compiledSet || len(got.compiled) == 0 {
		t.Fatalf("source not compiled: %+v", got)
	}
	if !strings.Contains(strings.Join(got.compiled, " "), "warn") {
		t.Errorf("predicates %v do not carry the filter", got.compiled)
	}
	if _, err := m.ResolveSource(context.Background(), ModelDefinition{SourceBQL: `level = `}, "f", ""); err == nil {
		t.Error("a malformed source query compiled")
	}
}

func TestExtractionTestSQLChainsSteps(t *testing.T) {
	q, err := buildExtractionTestSQL("logs", "f'1",
		[]FilterCondition{{Field: "level", Op: "=", Value: "dns"}},
		[]ExtractionStep{
			{FromField: "query", Pattern: `(?<dom>[a-z.]+)`, OutputField: "dom", MinLength: 4},
			{FromField: "dom", Pattern: `\.([a-z]+)$`, OutputField: "tld"},
		})
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, q, "WHERE fractal_id = 'f\\'1'", "scoped to the fractal")
	mustContain(t, q, "AND fields.`level`::String = 'dns'", "filter")
	mustContain(t, q, "fields.`query`::String AS `query`", "first input projected")
	// The second step reads the first step's output, so it is not projected from logs.
	mustNotContain(t, q, "fields.`dom`::String", "an extraction output is not a log field")
	mustContain(t, q, "extract(`query`, '([a-z.]+)')", "named group stripped")
	mustContain(t, q, "length(extract(`query`, '([a-z.]+)')) >= 4", "min length")
	mustContain(t, q, "FROM e0", "steps chain")
	mustContain(t, q, "SELECT `tld`, count() AS cnt FROM e1 GROUP BY `tld`", "samples the last output")
	mustContain(t, q, "LIMIT 1000", "bounded probe")
}

// Nothing to resolve is an empty answer, not a query with an empty IN list.
func TestModelResolutionWithNoScope(t *testing.T) {
	m := testManager()
	infos, err := m.ListModelInfosForFractals(context.Background(), nil)
	if err != nil || len(infos) != 0 {
		t.Errorf("no fractals: %v %v", infos, err)
	}
	idx, filtered, err := m.ListTLSHIndexes(context.Background(), nil, "tlsh")
	if err != nil || len(idx) != 0 || len(filtered) != 0 {
		t.Errorf("no fractals: %v %v %v", idx, filtered, err)
	}
	idx, _, err = m.ListTLSHIndexes(context.Background(), []string{"f"}, "")
	if err != nil || len(idx) != 0 {
		t.Errorf("no key field: %v %v", idx, err)
	}
}
