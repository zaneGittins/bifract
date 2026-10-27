package models

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// testWindow is a fixed 7-day scorer window.
var testWindow = netWindowAt(time.Date(2026, 10, 27, 15, 30, 0, 0, time.UTC), 7)

// testWindowSQL is testWindow's bound on a state read.
const testWindowSQL = "hour >= toDateTime('2026-10-20 15:00:00', 'UTC') AND hour < toDateTime('2026-10-27 15:00:00', 'UTC')"

// The state table keeps two days past the model's window, buckets by hour, and
// keeps each point's timestamp and size together.
func TestNetStateTableTTLFollowsWindow(t *testing.T) {
	for _, days := range []int{1, 7, 14} {
		ddl := BuildNetStateTableDDL("`s`", days)
		mustContain(t, ddl, fmt.Sprintf("TTL hour + INTERVAL %d DAY", days+2), "state TTL")
		mustContain(t, ddl, "points     AggregateFunction(groupArraySorted(84), Tuple(UInt32, Float64))", "points state")
		mustContain(t, ddl, "ORDER BY (fractal_id, src, dst, port, hour)", "state key")
	}
	res := BuildNetResultsTableDDL("`r`")
	mustContain(t, res, "ReplacingMergeTree(scored_at)", "results keep the latest score")
	mustContain(t, res, "ORDER BY (fractal_id, src_ip, dst_ip, dst_port)", "results key")
}

// The scorer's read merges the parametric groupArraySorted state, and ClickHouse
// rejects a Merge whose parameter differs from the stored state's (code 43).
// Both sides are pinned to the same constant.
func TestNetScoreReadMergesWithTheStateParameter(t *testing.T) {
	read := BuildNetScoreReadQuery(ModelDefinition{}, ModelTypeBeacon, "`s`", "f1", testWindow, "")
	state, _ := BuildNetStateInsert(ModelDefinition{}, "`s`", "logs", "", "f1")
	cap := fmt.Sprintf("(%d)", netStateHourCap)
	mustContain(t, state, "toStartOfHour(timestamp) AS hour", "state writer buckets by hour")
	mustContain(t, state, "groupArraySortedState"+cap+"((toUnixTimestamp(timestamp), toFloat64OrZero(fields.`orig_bytes`::String))) AS points", "state writer")
	mustContain(t, read, "groupArraySortedMerge"+cap+"(points)", "scorer read")
}

// The read merges each hour on its own and concatenates the hours in time order,
// so every hour of the window keeps its earliest points (see
// TestCHBeaconStateCoversEveryHour). Points go out once, as two parallel lists.
func TestNetReadsCapPointsPerHour(t *testing.T) {
	read := BuildNetScoreReadQuery(ModelDefinition{}, ModelTypeBeacon, "`s`", "f1", testWindow, "")
	b := netPreviewWindow(PreviewRange{Start: time.Unix(0, 0), End: time.Unix(7*86400, 0)})
	prev, err := BuildNetPreviewAgg(ModelDefinition{}, ModelTypeBeacon, "logs", "f", b, "")
	if err != nil {
		t.Fatal(err)
	}
	for name, q := range map[string]string{"read": read, "preview": prev} {
		mustContain(t, q, "GROUP BY src, dst, port, hour", name+": one row per pair and hour")
		mustContain(t, q, "arraySort(arrayFlatten(groupArray(hour_points))) AS points", name+": hours concatenated in time order")
		mustContain(t, q, "toUnixTimestamp(min(hour_first)) AS first_ts", name+": first seen")
		mustContain(t, q, "toUnixTimestamp(max(hour_last)) AS last_ts", name+": last seen")
		mustContain(t, q, "SELECT src, dst, port, cnt,\n    arrayMap(p -> p.1, points) AS ts_list,\n    arrayMap(p -> p.2, points) AS size_list,\n    total_duration, first_ts, last_ts\nFROM (", name+": points returned once")
	}
	mustContain(t, read, "groupArraySortedMerge(84)(points) AS hour_points", "merged within the hour")
	mustContain(t, read, "min(first_ts) AS hour_first,\n        max(last_ts) AS hour_last", "the state's own first and last seen")
	mustContain(t, prev, "groupArraySorted(84)((toUnixTimestamp(timestamp), toFloat64OrZero(fields.`orig_bytes`::String))) AS hour_points", "preview caps each hour as the state does")
	mustContain(t, prev, "GROUP BY src, dst, port\nHAVING cnt >= 4 AND cnt < 604800\n)", "preview gates the whole pair")
}

// A preview up to the longest window keeps the state's hourly cap; a longer one
// caps each hour lower to stay within the point budget.
func TestNetPreviewHourCap(t *testing.T) {
	for _, c := range []struct{ hours, want int }{{1, 84}, {336, 84}, {337, 83}, {360, 78}, {720, 39}, {2160, 13}} {
		if got := netPreviewHourCap(c.hours); got != c.want {
			t.Errorf("%d hours: cap %d, want %d", c.hours, got, c.want)
		}
		if netPreviewHourCap(c.hours)*c.hours > netPairPointBudget {
			t.Errorf("%d hours exceeds the point budget", c.hours)
		}
	}
	b := netPreviewWindow(PreviewRange{Start: time.Unix(0, 0), End: time.Unix(30*86400, 0)})
	prev, _ := BuildNetPreviewAgg(ModelDefinition{}, ModelTypeBeacon, "logs", "f", b, "")
	mustContain(t, prev, "groupArraySorted(39)(", "30-day preview")
}

// The scorer's window is the model's days of complete hours before the current
// one, so every hour of the day counts equally and the gate counts what is scored.
func TestNetWindowAt(t *testing.T) {
	now := time.Date(2026, 10, 27, 14, 25, 10, 0, time.FixedZone("x", -5*3600))
	end := time.Date(2026, 10, 27, 19, 0, 0, 0, time.UTC)
	for _, days := range []int{1, 7} {
		w := netWindowAt(now, days)
		want := NetWindow{Start: end.Add(-time.Duration(days) * 24 * time.Hour), End: end, Days: days}
		if w != want || w.hours() != days*24 {
			t.Errorf("%d days: %+v, want %+v", days, w, want)
		}
	}
}

// A preview scores its range's complete hours, so a preset preview ending now
// scores exactly the window the model scores.
func TestNetPreviewWindow(t *testing.T) {
	now := time.Date(2026, 10, 27, 14, 25, 10, 0, time.UTC)
	for _, days := range []int{1, 7, 30} {
		rng := PreviewRange{Start: now.Add(-time.Duration(days) * 24 * time.Hour), End: now}
		if got, want := netPreviewWindow(rng), netWindowAt(now, days); got != want {
			t.Errorf("%d days: preview %+v, model %+v", days, got, want)
		}
	}
	at := func(h, m int) time.Time { return time.Date(2026, 10, 27, h, m, 0, 0, time.UTC) }
	for _, c := range []struct {
		start, end, wantStart, wantEnd time.Time
		days                           int
	}{
		{at(10, 20), at(11, 50), at(10, 0), at(11, 0), 1},
		{at(10, 20), at(10, 50), at(10, 0), at(11, 0), 1}, // inside one hour: that hour
		{at(0, 0).Add(-36 * time.Hour), at(0, 0), at(0, 0).Add(-36 * time.Hour), at(0, 0), 2},
	} {
		got := netPreviewWindow(PreviewRange{Start: c.start, End: c.end})
		if want := (NetWindow{Start: c.wantStart, End: c.wantEnd, Days: c.days}); got != want {
			t.Errorf("%v to %v: %+v, want %+v", c.start, c.end, got, want)
		}
	}
}

func TestScorerReadSettings(t *testing.T) {
	mustContain(t, scorerReadSettings(), "max_block_size = 256, max_bytes_before_external_group_by = 2000000000", "defaults")
	t.Setenv("BIFRACT_MODEL_SCORER_MAX_GROUPBY_BYTES", "0")
	mustContain(t, scorerReadSettings(), "max_bytes_before_external_group_by = 0", "spill turned off")
}

func TestNetScoreReadQuery(t *testing.T) {
	// Beacon: a pair qualifies on connection count, between min_connections and
	// the strobe limit (one connection per second over the window by default),
	// and qualifies before any timestamp state is merged.
	beacon := BuildNetScoreReadQuery(ModelDefinition{}, ModelTypeBeacon, "`s`", "f'1", testWindow, "")
	mustContain(t, beacon, "WHERE fractal_id = 'f\\'1' AND "+testWindowSQL, "scoped to fractal and window")
	mustContain(t, beacon, "AND (src, dst, port) GLOBAL IN (SELECT src, dst, port", "gated before the merge")
	mustContain(t, beacon, "HAVING sum(conn_count) >= 4 AND sum(conn_count) < 604800", "default beacon gate")

	custom := ModelDefinition{Beacon: &BeaconParams{MinConnections: 10, StrobeLimit: 5000}}
	mustContain(t, BuildNetScoreReadQuery(custom, ModelTypeBeacon, "`s`", "f", testWindow, ""), "HAVING sum(conn_count) >= 10 AND sum(conn_count) < 5000", "custom gate")

	// Long connection: a pair qualifies on total duration, never on count, and a
	// large threshold renders without exponent notation.
	lc := ModelDefinition{LongConn: &LongConnParams{BaseSeconds: 2_000_000}}
	q := BuildNetScoreReadQuery(lc, ModelTypeLongConnection, "`s`", "f", testWindow, "")
	mustContain(t, q, "HAVING sum(dur_sum) >= 2000000", "long-connection gate")
	mustNotContain(t, q, "sum(conn_count) >=", "long connections are not gated on count")
	mustNotContain(t, q, "e+", "float literal")
}

func TestNetPrevalenceQueries(t *testing.T) {
	prev := BuildNetPrevalenceQuery("`s`", "f1", testWindow)
	mustContain(t, prev, "SELECT dst, uniqExact(src) AS prev_total", "per-destination sources")
	mustContain(t, prev, "fractal_id = 'f1' AND "+testWindowSQL, "prevalence scope")
	mustContain(t, prev, "GROUP BY dst", "prevalence grouping")
	size := BuildNetNetworkSizeQuery("`s`", "f1", testWindow)
	mustContain(t, size, "uniqExact(src) AS network_size", "network size")
	mustContain(t, size, "fractal_id = 'f1' AND "+testWindowSQL, "network size scope")
}

// Preview prevalence counts every source in the window that passes the model's
// filter, not only the pairs that qualify for scoring, so it carries the filter
// but none of the pair gates.
func TestNetPreviewPrevalenceAndSize(t *testing.T) {
	def := ModelDefinition{
		Network: &NetworkFieldMap{SrcField: "id.orig_h", DstField: "id.resp_h"},
		Filter:  []FilterCondition{{Field: "proto", Op: "=", Value: "tcp"}},
	}
	b := NetWindow{Start: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), Days: 1}
	prev, err := BuildNetPreviewPrevalence(def, "logs", "f1", b)
	if err != nil {
		t.Fatal(err)
	}
	size, err := BuildNetPreviewNetworkSize(def, "logs", "f1", b)
	if err != nil {
		t.Fatal(err)
	}
	for name, q := range map[string]string{"prevalence": prev, "network size": size} {
		mustContain(t, q, "fractal_id = 'f1'", name+" scope")
		mustContain(t, q, "fields.`proto`::String = 'tcp'", name+" filter")
		mustContain(t, q, b.timestampSQL(), name+" window")
		mustContain(t, q, "fields.`id.orig_h`::String != '' AND fields.`id.resp_h`::String != ''", name+" field map")
		mustNotContain(t, q, "HAVING", name+" must not gate on pair qualification")
	}
	mustContain(t, prev, "GROUP BY dst", "prevalence grouping")
}

// A network preview with an unresolved source query refuses to render rather than
// silently falling back to the structured filter.
func TestNetPreviewRequiresResolvedSource(t *testing.T) {
	def := ModelDefinition{SourceBQL: `proto = "tcp"`}
	b := NetWindow{Start: time.Unix(0, 0), End: time.Unix(86400, 0), Days: 1}
	if _, err := BuildNetPreviewAgg(def, ModelTypeBeacon, "logs", "f", b, ""); err == nil {
		t.Error("agg rendered an unresolved source")
	}
	if _, err := BuildNetPreviewPrevalence(def, "logs", "f", b); err == nil {
		t.Error("prevalence rendered an unresolved source")
	}
	if _, err := BuildNetPreviewNetworkSize(def, "logs", "f", b); err == nil {
		t.Error("network size rendered an unresolved source")
	}
	if _, err := BuildNetStateInsert(def, "s", "logs", "", "f"); err == nil {
		t.Error("state insert rendered an unresolved source")
	}
}

func TestNetPreviewAggLongConnection(t *testing.T) {
	b := NetWindow{Start: time.Unix(0, 0), End: time.Unix(86400, 0), Days: 1}
	q, err := BuildNetPreviewAgg(ModelDefinition{Filter: []FilterCondition{{Field: "proto", Value: "udp"}}}, ModelTypeLongConnection, "logs", "f", b, "")
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, q, "HAVING total_duration >= 3600", "default long-connection gate")
	mustContain(t, q, "sum(toFloat64OrZero(fields.`duration`::String)) AS hour_dur", "duration coerced to a number")
	mustContain(t, q, "AND fields.`proto`::String = 'udp'", "an op-less filter is equality")
}

func TestFilterConditionToSQLOperators(t *testing.T) {
	ref := "fields.`f`::String"
	cases := []struct {
		fc   FilterCondition
		want string
	}{
		{FilterCondition{Field: "f", Op: "=", Value: "a"}, ref + " = 'a'"},
		{FilterCondition{Field: "f", Op: "!=", Value: "a"}, ref + " != 'a'"},
		{FilterCondition{Field: "f", Op: "~", Value: "^a.*"}, "match(" + ref + ", '^a.*')"},
		{FilterCondition{Field: "f", Op: "!~", Value: "x"}, "NOT match(" + ref + ", 'x')"},
		{FilterCondition{Field: "f", Op: "cidr", Value: "10.0.0.0/8"}, "(isIPAddressInRange(if((isIPv4String(" + ref + ") OR isIPv6String(" + ref + ")), " + ref + ", '0.0.0.0'), '10.0.0.0/8') AND (isIPv4String(" + ref + ") OR isIPv6String(" + ref + ")))"},
		{FilterCondition{Field: "f", Op: "bogus", Value: "a"}, ref + " = 'a'"},
		// Quotes and backslashes in a value stay inside the literal.
		{FilterCondition{Field: "f", Op: "=", Value: `it's \x`}, ref + ` = 'it\'s \\x'`},
		// A known log column is referenced directly, not as a JSON sub-column.
		{FilterCondition{Field: "log_id", Op: "=", Value: "1"}, "log_id = '1'"},
	}
	for _, c := range cases {
		if got := filterConditionToSQL(c.fc); got != c.want {
			t.Errorf("%+v:\n got  %s\n want %s", c.fc, got, c.want)
		}
	}
	// !cidr negates the whole guarded expression, so a non-IP value (which the
	// guard already excludes) is not counted as "outside the range".
	neg := filterConditionToSQL(FilterCondition{Field: "f", Op: "!cidr", Value: "10.0.0.0/8"})
	if !strings.HasPrefix(neg, "NOT (isIPAddressInRange(") {
		t.Errorf("!cidr: %s", neg)
	}
}

func TestGenerateTableDDLPerType(t *testing.T) {
	if _, err := GenerateDDL(ModelDefinition{}, ModelTypeBeacon, "`t`"); err == nil {
		t.Error("a network model has no aggregate table of this shape")
	}
	day, _ := GenerateDDL(ModelDefinition{}, ModelTypeVolumeBaseline, "`t`")
	hour, _ := GenerateDDL(ModelDefinition{TimeBucket: "hour"}, ModelTypeVolumeBaseline, "`t`")
	mustContain(t, day, "bucket      Date", "daily bucket column")
	mustContain(t, hour, "bucket      DateTime('UTC')", "hourly bucket column")
	fs, _ := GenerateDDL(ModelDefinition{}, ModelTypeFirstSeen, "`t`")
	mustContain(t, fs, FirstRecordedColumn+" "+FirstRecordedDefinition, "first_seen records when the model learned an entity")
}

// The hourly bucket expression has to match the hourly column type, or every
// insert fails to convert.
func TestVolumeHourlySelectBucketsByHour(t *testing.T) {
	def := ModelDefinition{KeyFields: []string{"host", "user"}, TimeBucket: "hour"}
	q, err := BuildStateInsert(def, ModelTypeVolumeBaseline, "`t`", "logs", "", "f")
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, q, "toStartOfHour(timestamp) AS bucket", "hourly bucket")
	mustContain(t, q, "concat(fields.`host`::String, char(30), fields.`user`::String) AS entity_val", "composite key separator")
}

// A first_seen entity is the value of its key fields, so a log missing any of
// them is not an entity. Every other path already skipped it; the direct select
// recorded a single empty entity that every keyless log then joined.
func TestFirstSeenSkipsLogsWithoutTheKey(t *testing.T) {
	direct, err := BuildStateInsert(ModelDefinition{KeyFields: []string{"user", "host"}}, ModelTypeFirstSeen, "`t`", "logs", "", "f")
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, direct, "AND fields.`user`::String != ''", "direct select guards user")
	mustContain(t, direct, "AND fields.`host`::String != ''", "direct select guards host")

	withExt := ModelDefinition{
		KeyFields:   []string{"tld"},
		Extractions: []ExtractionStep{{FromField: "norm_log", Pattern: "([a-z]+)", OutputField: "tld"}},
	}
	cte, err := BuildStateInsert(withExt, ModelTypeFirstSeen, "`t`", "logs", "", "f")
	if err != nil {
		t.Fatal(err)
	}
	mustContain(t, cte, "WHERE tld != ''", "extraction path guards the key")
}

// Extraction-derived keys: several keys join with the record separator, and a
// lowercase extraction is applied in the projection, not in the CTE that filters.
func TestFinalSelectCompositeAndLowercasedKeys(t *testing.T) {
	exts := []ExtractionStep{{FromField: "norm_log", Pattern: "(?<dom>[A-Za-z.]+)", OutputField: "dom", Lowercase: true, MinLength: 3}}
	for _, mt := range []ModelType{ModelTypeFirstSeen, ModelTypeVolumeBaseline} {
		def := ModelDefinition{KeyFields: []string{"dom", "user"}, Extractions: exts}
		q, err := BuildStateInsert(def, mt, "`t`", "logs", "", "f")
		if err != nil {
			t.Fatalf("%s: %v", mt, err)
		}
		mustContain(t, q, "concat(lower(dom), char(30), user)", string(mt)+" composite key")
		mustContain(t, q, "extract(norm_log, '([A-Za-z.]+)')", string(mt)+" named group stripped for extract()")
		mustContain(t, q, "AND length(extract(norm_log, '([A-Za-z.]+)')) >= 3", string(mt)+" min length")
		mustContain(t, q, "fields.`user`::String AS user", string(mt)+" plain key projected by the base CTE")
	}
}

func TestSourceFilterRequiresResolution(t *testing.T) {
	for _, mt := range []ModelType{ModelTypeRarity, ModelTypeFirstSeen, ModelTypeTLSH, ModelTypeVolumeBaseline} {
		def := ModelDefinition{SourceBQL: `x = "1"`, PartitionKey: "a", ValueKey: "b", KeyFields: []string{"k"}}
		if _, err := BuildStateInsert(def, mt, "`t`", "logs", "", "f"); err == nil || !strings.Contains(err.Error(), "not resolved") {
			t.Errorf("%s: unresolved source rendered: %v", mt, err)
		}
	}
}
