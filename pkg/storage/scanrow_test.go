package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// scanOne runs scanRowMap over a single scripted row.
func scanOne(t *testing.T, cols []fakedb.Col, row []any) (map[string]interface{}, error) {
	t.Helper()
	rows, err := fakedb.NewCH().On("", fakedb.CHResult{Cols: cols, Rows: [][]any{row}}).Query(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("no row")
	}
	return scanRowMap(rows.ColumnTypes(), rows)
}

// The row map is encoded straight to JSON, so its value types are the API
// contract. Narrow integer and float columns are widened, so a column's storage
// width (a system table's Int32 vs a log table's Int64) never leaks into the shape
// a client sees, and every column type a system table hands out must scan rather
// than fail the whole query on the first row.
func TestScanRowMapValueShapes(t *testing.T) {
	ts := time.Date(2026, 7, 1, 12, 30, 45, 123456789, time.UTC)
	cases := []struct {
		typ  string
		in   any
		want any
	}{
		{"String", "x", "x"},
		{"Nullable(String)", "x", "x"},
		{"LowCardinality(String)", "lc", "lc"},
		{"LowCardinality(Nullable(String))", "lcn", "lcn"},
		{"UInt64", uint64(7), uint64(7)},
		{"Int64", int64(-7), int64(-7)},
		{"Float64", 1.5, 1.5},
		{"Int8", int8(-1), int64(-1)},
		{"Int16", int16(-2), int64(-2)},
		{"Int32", int32(-3), int64(-3)},
		{"Nullable(Int32)", int32(4), int64(4)},
		{"UInt8", uint8(1), uint64(1)},
		{"UInt16", uint16(2), uint64(2)},
		{"UInt32", uint32(3), uint64(3)},
		{"LowCardinality(UInt32)", uint32(9), uint64(9)},
		{"Float32", float32(0.5), float64(0.5)},
		{"SimpleAggregateFunction(max, Int32)", int32(11), int64(11)},
		// Datetimes become millisecond strings, never time.Time (see RowTime).
		{"DateTime64(3, 'UTC')", ts, "2026-07-01 12:30:45.123"},
		{"Nullable(DateTime)", ts, "2026-07-01 12:30:45.123"},
		{"Date", ts, "2026-07-01 12:30:45.123"},
		{"SimpleAggregateFunction(max, DateTime64(3, 'UTC'))", ts, "2026-07-01 12:30:45.123"},
		{"Array(String)", []string{"a", "b"}, []string{"a", "b"}},
		{"Array(Float64)", []float64{1, 2}, []float64{1, 2}},
		{"Array(UInt64)", []uint64{1}, []uint64{1}},
		{"Array(Int64)", []int64{-1}, []int64{-1}},
		// Complex arrays and tuples pass through whatever the driver produced.
		{"Array(Tuple(String, UInt64))", []any{"t"}, []any{"t"}},
		{"Tuple(String, UInt64)", []any{"a", uint64(1)}, []any{"a", uint64(1)}},
		// Unknown types fall back to a string scan.
		{"JSON", `{"a":1}`, `{"a":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.typ, func(t *testing.T) {
			got, err := scanOne(t, []fakedb.Col{{Column: "v", Type: tc.typ}}, []any{tc.in})
			if err != nil {
				t.Fatalf("scanRowMap: %v", err)
			}
			if !reflect.DeepEqual(got["v"], tc.want) {
				t.Errorf("v = %#v (%T), want %#v (%T)", got["v"], got["v"], tc.want, tc.want)
			}
		})
	}
}

// toString(fields) arrives as a JSON string and must reach the client as an
// object. Only the two field columns get that treatment: a user column that
// happens to hold JSON text stays text, and unparseable field text is passed
// through rather than dropped.
func TestScanRowMapParsesFieldColumnsOnly(t *testing.T) {
	cols := []fakedb.Col{{Column: "fields", Type: "String"}, {Column: "_all_fields", Type: "String"}, {Column: "note", Type: "String"}}
	got, err := scanOne(t, cols, []any{`{"user":"alice"}`, `{"n":1}`, `{"x":1}`})
	if err != nil {
		t.Fatal(err)
	}
	if m, ok := got["fields"].(map[string]interface{}); !ok || m["user"] != "alice" {
		t.Errorf("fields = %#v, want a parsed object", got["fields"])
	}
	if m, ok := got["_all_fields"].(map[string]interface{}); !ok || m["n"] != float64(1) {
		t.Errorf("_all_fields = %#v, want a parsed object", got["_all_fields"])
	}
	if got["note"] != `{"x":1}` {
		t.Errorf("note = %#v, want the raw string", got["note"])
	}

	got, err = scanOne(t, []fakedb.Col{{Column: "fields", Type: "String"}}, []any{"not json"})
	if err != nil {
		t.Fatal(err)
	}
	if got["fields"] != "not json" {
		t.Errorf("unparseable fields = %#v, want it kept as a string", got["fields"])
	}
}

func TestScanRowMapReportsScanFailure(t *testing.T) {
	// A UInt64 value delivered for a String column: the driver refuses it, and
	// the error must say it was the scan rather than the query that failed.
	_, err := scanOne(t, []fakedb.Col{{Column: "v", Type: "String"}}, []any{uint64(1)})
	if err == nil || !strings.Contains(err.Error(), "failed to scan row") {
		t.Fatalf("err = %v, want a scan failure", err)
	}
}

func TestIsCHDateTimeType(t *testing.T) {
	for typ, want := range map[string]bool{
		"DateTime":                       true,
		"DateTime('UTC')":                true,
		"DateTime64(3, 'UTC')":           true,
		"Nullable(DateTime64(3, 'UTC'))": true,
		"Date":                           true,
		"Nullable(Date)":                 true,
		"Date32":                         false, // scanned as a string, not a time
		"Array(DateTime)":                false,
		"String":                         false,
	} {
		if got := isCHDateTimeType(typ); got != want {
			t.Errorf("isCHDateTimeType(%q) = %v, want %v", typ, got, want)
		}
	}
}

func TestQueryConnCollectsRowsAndSurfacesIterationErrors(t *testing.T) {
	f := fakedb.NewCH()
	f.On("SELECT a", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "a", Type: "String"}, {Column: "n", Type: "UInt32"}},
		Rows: [][]any{{"x", uint32(1)}, {"y", uint32(2)}},
	})
	rows, err := QueryConn(context.Background(), f, "SELECT a, n")
	if err != nil {
		t.Fatal(err)
	}
	want := []map[string]interface{}{{"a": "x", "n": uint64(1)}, {"a": "y", "n": uint64(2)}}
	if !reflect.DeepEqual(rows, want) {
		t.Errorf("rows = %#v, want %#v", rows, want)
	}

	broken := errors.New("connection reset")
	f.On("SELECT broken", fakedb.CHResult{Cols: []fakedb.Col{{Column: "a", Type: "String"}}, IterErr: broken})
	if _, err := QueryConn(context.Background(), f, "SELECT broken"); !errors.Is(err, broken) {
		t.Errorf("err = %v, want the iteration error", err)
	}
	f.On("SELECT refused", fakedb.CHResult{Err: broken})
	if _, err := QueryConn(context.Background(), f, "SELECT refused"); !errors.Is(err, broken) {
		t.Errorf("err = %v, want the query error", err)
	}
}

// Tagged queries carry their attribution in log_comment so the activity view
// reads as "who did what". A log_comment a caller already set wins, and a
// caller's settings map is extended rather than replaced.
func TestTaggedSettings(t *testing.T) {
	c := &ClickHouseClient{}
	ctx := context.Background()
	if got := c.taggedSettings(ctx, nil); got != nil {
		t.Errorf("untagged ctx produced settings %v", got)
	}

	tagged := TagContext(ctx, QueryTag{Source: SourceSearch, User: "alice"})
	got := c.taggedSettings(tagged, nil)
	if !strings.Contains(got["log_comment"].(string), `"user":"alice"`) {
		t.Errorf("log_comment = %v, want the tag", got["log_comment"])
	}

	settings := map[string]any{"priority": 5}
	got = c.taggedSettings(tagged, settings)
	if got["priority"] != 5 || got["log_comment"] == nil {
		t.Errorf("settings = %v, want the caller's settings plus the tag", got)
	}

	got = c.taggedSettings(tagged, map[string]any{"log_comment": "explicit"})
	if got["log_comment"] != "explicit" {
		t.Errorf("log_comment = %v, an explicit comment must win", got["log_comment"])
	}
}

// A search tagged with its workload must run on the class identity's connection,
// and an onRow error must stop the read and come back unchanged, which is how
// callers cap the rows they read.
func TestStreamQueryRoutesAndStopsOnCallbackError(t *testing.T) {
	c, def := newFakeClient()
	search := fakedb.NewCH()
	search.On("SELECT", fakedb.CHResult{Cols: []fakedb.Col{{Column: "n", Type: "UInt64"}}, Rows: [][]any{{uint64(1)}, {uint64(2)}, {uint64(3)}}})
	c.queryConns.Store(&map[string]*queryIdentity{QuerySearchWorkload: {conn: search}})

	stop := errors.New("enough")
	var seen []uint64
	err := c.StreamQuery(UserSearchContext(context.Background()), "q1", "SELECT n", func(row map[string]interface{}) error {
		seen = append(seen, row["n"].(uint64))
		if len(seen) == 2 {
			return stop
		}
		return nil
	}, nil)
	if !errors.Is(err, stop) {
		t.Fatalf("err = %v, want the callback's error", err)
	}
	if !reflect.DeepEqual(seen, []uint64{1, 2}) {
		t.Errorf("rows delivered = %v, want the read to stop after the error", seen)
	}
	if len(def.Queried()) != 0 {
		t.Error("a tagged search must not run on the privileged default connection")
	}
}

func TestQueryStreamDeliversRows(t *testing.T) {
	c, f := newFakeClient()
	f.On("SELECT", fakedb.CHResult{Cols: []fakedb.Col{{Column: "n", Type: "Int32"}}, Rows: [][]any{{int32(1)}, {int32(2)}}})
	var got []int64
	stats, err := c.QueryStream(context.Background(), "id", "SELECT n", func(row map[string]interface{}) {
		got = append(got, row["n"].(int64))
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Errorf("rows = %v", got)
	}
	if stats != (QueryStats{}) {
		t.Errorf("stats = %+v, want zero without progress packets", stats)
	}

	f.On("FAIL", fakedb.CHResult{Err: errors.New("boom")})
	if _, err := c.QueryStream(context.Background(), "id", "FAIL", nil, nil); err == nil {
		t.Error("expected the query error")
	}
}

// Point lookups pin the primary key with the exact timestamp and prune with the
// fractal only when given one; the args must line up with the placeholders.
func TestGetLogByTimestampBuildsPinnedLookup(t *testing.T) {
	c, f := newFakeClient()
	f.On("FROM logs WHERE log_id = ?", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "timestamp", Type: "DateTime64(3, 'UTC')"}, {Column: "log_id", Type: "String"}, {Column: "fields", Type: "String"}, {Column: "fractal_id", Type: "LowCardinality(String)"}, {Column: "ingest_timestamp", Type: "DateTime64(3, 'UTC')"}},
		Rows: [][]any{{time.Date(2026, 1, 2, 3, 4, 5, 6e6, time.UTC), "L1", `{"user":"bob"}`, "F1", time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC)}},
	})

	ts := time.Date(2026, 1, 2, 3, 4, 5, 6e6, time.FixedZone("x", 3600))
	row, err := c.GetLogByTimestamp(context.Background(), ts, "L1", "F1")
	if err != nil {
		t.Fatal(err)
	}
	q := f.Queried()[0]
	if !strings.Contains(q, "AND timestamp = toDateTime64(?, 3, 'UTC') AND fractal_id = ? LIMIT 1") {
		t.Errorf("query = %q, want timestamp and fractal predicates", q)
	}
	if row["log_id"] != "L1" || row["timestamp"] != "2026-01-02 03:04:05.006" {
		t.Errorf("row = %v", row)
	}
	if m, _ := row["fields"].(map[string]interface{}); m["user"] != "bob" {
		t.Errorf("fields = %v, want parsed norm_log", row["fields"])
	}

	// No timestamp, no fractal: neither predicate is emitted.
	c2, f2 := newFakeClient()
	if row, err := c2.GetLogByTimestamp(context.Background(), time.Time{}, "L2", ""); err != nil || row != nil {
		t.Fatalf("missing row = %v, %v; want nil, nil", row, err)
	}
	q = f2.Queried()[0]
	if strings.Contains(q, "timestamp =") || strings.Contains(q, "fractal_id =") {
		t.Errorf("query = %q, want no optional predicates", q)
	}

	if _, err := c.GetLogByTimestamp(context.Background(), ts, "", ""); err == nil {
		t.Error("an empty log_id must be rejected before querying")
	}
}

// Declared type hints materialize as "" even for logs that never carried them, so
// the detail view drops empty hinted fields. A genuinely empty dynamic field was
// in the log and stays.
func TestParseLogFieldsDropsOnlyEmptyHintedFields(t *testing.T) {
	c, f := newFakeClient()
	f.On("name = 'fields'", fakedb.Strs("type", "JSON(max_dynamic_paths=1024, `src_ip` String, user String, ts DateTime64(3, 'UTC'))"))

	got := c.parseLogFields(context.Background(), `{"src_ip":"","user":"alice","dyn":"","ts":""}`)
	want := map[string]interface{}{"user": "alice", "dyn": ""}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("fields = %v, want %v", got, want)
	}

	if got := c.parseLogFields(context.Background(), "not json"); len(got) != 0 {
		t.Errorf("unparseable norm_log = %v, want an empty map", got)
	}

	// The hint set is cached; the second parse issued no new query.
	if n := len(f.Queried()); n != 1 {
		t.Errorf("type hints read %d times, want once (cached)", n)
	}
	c.invalidateTypeHintCache()
	c.parseLogFields(context.Background(), `{}`)
	if n := len(f.Queried()); n != 2 {
		t.Errorf("type hints read %d times after invalidation, want 2", n)
	}
}

// When the hint list cannot be read, nothing is stripped: a little noise is
// better than hiding data.
func TestTypeHintFieldSetFailsOpen(t *testing.T) {
	c, f := newFakeClient()
	f.On("system.columns", fakedb.CHResult{Err: errors.New("denied")})
	if got := c.parseLogFields(context.Background(), `{"user":""}`); got["user"] != "" || len(got) != 1 {
		t.Errorf("fields = %v, want the empty field kept", got)
	}
	if set := (&ClickHouseClient{}).typeHintFieldSet(context.Background()); len(set) != 0 {
		t.Errorf("nil-conn hint set = %v, want empty", set)
	}
}

func TestGetLogFieldsByID(t *testing.T) {
	c, f := newFakeClient()
	ingest := time.Date(2026, 3, 4, 5, 6, 7, 8e6, time.UTC)
	f.On("FROM logs PREWHERE", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "log_id", Type: "String"}, {Column: "fractal_id", Type: "String"}, {Column: "fields", Type: "String"}, {Column: "normalizer", Type: "String"}, {Column: "ingest_timestamp", Type: "DateTime64(3, 'UTC')"}},
		Rows: [][]any{{"L1", "F1", `{"a":"b"}`, "sysmon@2", ingest}},
	})
	f.On("FROM logs_raw", fakedb.Strs("raw_log", "<raw>"))

	ts := time.Date(2026, 3, 4, 5, 6, 0, 0, time.UTC)
	got, err := c.GetLogFieldsByID(context.Background(), "L1", ts, "F1")
	if err != nil {
		t.Fatal(err)
	}
	fields := got["fields"].(map[string]interface{})
	if fields["a"] != "b" || fields["_normalizer"] != "sysmon@2" {
		t.Errorf("fields = %v, want parsed fields plus the normalizer stamp", fields)
	}
	if got["raw_log"] != "<raw>" || got["ingest_timestamp"] != "2026-03-04 05:06:07.008" {
		t.Errorf("entry = %v", got)
	}
	for _, q := range f.Queried() {
		if strings.Contains(q, "PREWHERE") || strings.Contains(q, "logs_raw") {
			if !strings.Contains(q, "AND fractal_id = ? LIMIT 1") {
				t.Errorf("query %q must carry the fractal scope", q)
			}
		}
	}

	if _, err := c.GetLogFieldsByID(context.Background(), "L1", time.Time{}, ""); err == nil {
		t.Error("a zero timestamp must be rejected: it is the single deterministic lookup key")
	}
	if _, err := c.GetLogFieldsByID(context.Background(), "", ts, ""); err == nil {
		t.Error("an empty log_id must be rejected")
	}
}

// The raw tab is best-effort: a missing or unreadable logs_raw row yields "" and
// never fails the detail lookup.
func TestGetLogFieldsByIDRawLogIsBestEffort(t *testing.T) {
	c, f := newFakeClient()
	f.On("FROM logs PREWHERE", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "log_id", Type: "String"}, {Column: "fractal_id", Type: "String"}, {Column: "fields", Type: "String"}, {Column: "normalizer", Type: "String"}, {Column: "ingest_timestamp", Type: "DateTime64(3, 'UTC')"}},
		Rows: [][]any{{"L1", "", `{}`, "", time.Unix(0, 0)}},
	})
	f.On("FROM logs_raw", fakedb.CHResult{Err: errors.New("TTL'd away")})
	got, err := c.GetLogFieldsByID(context.Background(), "L1", time.Unix(1, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if got["raw_log"] != "" {
		t.Errorf("raw_log = %v, want empty", got["raw_log"])
	}
	if _, ok := got["fields"].(map[string]interface{})["_normalizer"]; ok {
		t.Error("an unnormalized log must not carry a _normalizer field")
	}
}

func TestGetLogFieldsByIDsScopesAndBounds(t *testing.T) {
	c, f := newFakeClient()
	f.On("log_id IN (?)", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "log_id", Type: "String"}, {Column: "fractal_id", Type: "String"}, {Column: "fields", Type: "String"}},
		Rows: [][]any{{"L1", "F1", `{"a":1}`}},
	})
	got, err := c.GetLogFieldsByIDs(context.Background(), []string{"L1"}, []string{"F1"})
	if err != nil || len(got) != 1 || got[0]["log_id"] != "L1" {
		t.Fatalf("got %v, %v", got, err)
	}
	c.GetLogFieldsByIDs(context.Background(), []string{"L1"}, nil)
	var lookups []string
	for _, q := range f.Queried() {
		if strings.Contains(q, "log_id IN (?)") {
			lookups = append(lookups, q)
		}
	}
	if len(lookups) != 2 || !strings.Contains(lookups[0], "fractal_id IN (?)") || strings.Contains(lookups[1], "fractal_id IN") {
		t.Errorf("lookups = %q, want the fractal scope only when one is given", lookups)
	}

	if got, err := c.GetLogFieldsByIDs(context.Background(), nil, nil); got != nil || err != nil {
		t.Errorf("empty input = %v, %v", got, err)
	}
	if _, err := c.GetLogFieldsByIDs(context.Background(), make([]string, 501), nil); err == nil {
		t.Error("more than 500 ids must be refused")
	}
}

// Off a cluster (or without a shard number) the direct path is the distributed one.
func TestGetLogFieldsByIDDirectFallsBackWithoutShardRouting(t *testing.T) {
	c, f := newFakeClient()
	c.GetLogFieldsByIDDirect(context.Background(), "L1", time.Unix(1, 0), "", 3)
	c.topo.ShardRouting = true
	c.GetLogFieldsByIDDirect(context.Background(), "L1", time.Unix(1, 0), "", 0)
	lookups := 0
	for _, q := range f.Queried() {
		if strings.Contains(q, "system.clusters") {
			t.Errorf("query = %q: no shard lookup may happen without routing and a shard number", q)
		}
		if strings.Contains(q, "PREWHERE") {
			lookups++
		}
	}
	if lookups != 2 {
		t.Errorf("%d pooled lookups, want 2", lookups)
	}
}

func TestCountLogs(t *testing.T) {
	c, f := newFakeClient()
	f.On("SELECT count()", fakedb.Count(42))
	n, err := c.CountLogs(context.Background(), time.Unix(0, 0), time.Unix(10, 0))
	if err != nil || n != 42 {
		t.Fatalf("CountLogs = %d, %v", n, err)
	}
	f2 := fakedb.NewCH()
	f2.On("SELECT count()", fakedb.CHResult{Err: errors.New("down")})
	c.conn = f2
	if _, err := c.CountLogs(context.Background(), time.Unix(0, 0), time.Unix(10, 0)); err == nil {
		t.Error("expected the error to surface")
	}
}

// Every interactive read helper runs on the class identity its ctx names, so a
// search's privileges and memory ceiling are the class's, not the admin's; the same
// helpers on an untagged ctx (alerts, field stats) stay on the default connection.
func TestQueryHelpersRouteByWorkload(t *testing.T) {
	ctx := context.Background()
	helpers := map[string]func(c *ClickHouseClient, ctx context.Context) error{
		"QueryUserSearchWithID": func(c *ClickHouseClient, ctx context.Context) error {
			_, err := c.QueryUserSearchWithID(ctx, "id", "SELECT 1")
			return err
		},
		"QueryLowPriority": func(c *ClickHouseClient, ctx context.Context) error {
			_, err := c.QueryLowPriority(ctx, "SELECT 1")
			return err
		},
		"QueryLowPriorityBounded": func(c *ClickHouseClient, ctx context.Context) error {
			_, err := c.QueryLowPriorityBounded(ctx, "SELECT 1", 1<<20)
			return err
		},
		"QueryProvenance": func(c *ClickHouseClient, ctx context.Context) error {
			_, err := c.QueryProvenance(ctx, "SELECT 1")
			return err
		},
		"StreamLowPriority": func(c *ClickHouseClient, ctx context.Context) error {
			return c.StreamLowPriority(ctx, "id", "SELECT 1", func(map[string]interface{}) error { return nil })
		},
	}
	for name, run := range helpers {
		c, def := newFakeClient()
		recall := fakedb.NewCH()
		c.queryConns.Store(&map[string]*queryIdentity{QueryRecallWorkload: {conn: recall}})

		if err := run(c, RecallContext(ctx)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(recall.Queried()) != 1 || len(def.Queried()) != 0 {
			t.Errorf("%s: a recall ctx must run on the recall identity", name)
		}
		if err := run(c, ctx); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(def.Queried()) != 1 {
			t.Errorf("%s: untagged work must run on the default connection", name)
		}

		def.On("SELECT", fakedb.CHResult{Err: errors.New("down")})
		if err := run(c, ctx); err == nil {
			t.Errorf("%s: query error swallowed", name)
		}
	}
}
