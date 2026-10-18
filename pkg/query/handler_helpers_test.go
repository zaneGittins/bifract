package query

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bifract/pkg/parser"
	"bifract/pkg/storage"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// The source a request names reaches ClickHouse's query log, so it is mapped
// through an allowlist; anything else is attributed as an ordinary search.
func TestRequestTag(t *testing.T) {
	r := testRequest(http.MethodPost, "", ctxVals{"user": &storage.User{Username: "ana"}})
	cases := map[string]string{
		"notebook":       storage.SourceNotebook,
		"  Dashboard ":   storage.SourceDashboard,
		"ALERT":          storage.SourceAlert,
		"":               storage.SourceSearch,
		"export":         storage.SourceSearch, // export tags itself; a client cannot claim it
		"'); DROP TABLE": storage.SourceSearch,
	}
	for in, want := range cases {
		tag := requestTag(r, QueryRequest{Source: in, FractalID: "f1", Query: "level=error"})
		if tag.Source != want {
			t.Errorf("source %q tagged as %q, want %q", in, tag.Source, want)
		}
		if tag.User != "ana" || tag.Fractal != "f1" || tag.BQL != "level=error" {
			t.Errorf("tag lost attribution: %+v", tag)
		}
	}

	anon := requestTag(testRequest(http.MethodPost, "", ctxVals{}), QueryRequest{})
	if anon.User != "" || anon.Source != storage.SourceSearch {
		t.Errorf("anonymous tag = %+v", anon)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	want := time.Date(2026, 10, 1, 12, 30, 45, 123_000_000, time.UTC)
	for name, ts := range map[string]interface{}{
		"driver time":        want,
		"clickhouse string":  "2026-10-01 12:30:45.123",
		"rfc3339 string":     "2026-10-01T14:30:45.123+02:00",
		"rfc3339 nano (UTC)": "2026-10-01T12:30:45.123Z",
	} {
		t.Run(name, func(t *testing.T) {
			c, err := encodeCursor(map[string]interface{}{"timestamp": ts, "log_id": "L1"})
			if err != nil {
				t.Fatal(err)
			}
			tok, err := decodeCursor(c)
			if err != nil {
				t.Fatal(err)
			}
			if tok.TSMilli != want.UnixMilli() || tok.LID != "L1" {
				t.Errorf("decoded %+v, want ts=%d lid=L1", tok, want.UnixMilli())
			}
		})
	}

	// A row the cursor cannot describe must not produce a token, or the next page
	// would start from the epoch and repeat everything.
	for name, ts := range map[string]interface{}{"garbage string": "soon", "unexpected type": 12345, "missing": nil} {
		if _, err := encodeCursor(map[string]interface{}{"timestamp": ts, "log_id": "L1"}); err == nil {
			t.Errorf("%s: encodeCursor accepted %v", name, ts)
		}
	}
	for _, bad := range []string{"%%%", "bm90IGpzb24="} { // not base64; base64 of "not json"
		if _, err := decodeCursor(bad); err == nil {
			t.Errorf("decodeCursor(%q) succeeded", bad)
		}
	}
}

func TestEscCHStr(t *testing.T) {
	cases := map[string]string{
		"plain":   "plain",
		"o'brien": `o\'brien`,
		`a\b`:     `a\\b`,
		// The backslash must be doubled before quotes are escaped, or a trailing
		// backslash would escape the closing quote of the literal.
		`x\'`: `x\\\'`,
	}
	for in, want := range cases {
		if got := escCHStr(in); got != want {
			t.Errorf("escCHStr(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFractalScopeSQL(t *testing.T) {
	cases := []struct {
		name string
		opts parser.QueryOptions
		want string
	}{
		{"unscoped", parser.QueryOptions{}, ""},
		{"one fractal", parser.QueryOptions{FractalID: "f1"}, "fractal_id = 'f1'"},
		{"prism members win over a single id", parser.QueryOptions{FractalID: "x", FractalIDs: []string{"f1", "f'2"}}, `fractal_id IN ('f1', 'f\'2')`},
		{"default fractal owns legacy rows", parser.QueryOptions{FractalID: "f1", IncludeEmptyFractalID: true}, "(fractal_id = 'f1' OR fractal_id = '')"},
		{"legacy rows need a scope to attach to", parser.QueryOptions{IncludeEmptyFractalID: true}, ""},
	}
	for _, tc := range cases {
		if got := fractalScopeSQL(tc.opts); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// The rollup's upper bound is exclusive: end is a bucket boundary, and the minute
// on it belongs to the next bucket. An inclusive bound double counts it across
// chunks.
func TestHistogramRollupSQL(t *testing.T) {
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.FixedZone("x", 3600))
	sql := histogramRollupSQL("logs_histogram", parser.QueryOptions{FractalID: "f1"}, start, start.Add(time.Hour), 900)
	for _, want := range []string{
		"minute >= '2026-09-30 23:00:00'", // converted to UTC
		"minute < '2026-10-01 00:00:00'",
		"AND fractal_id = 'f1'",
		"INTERVAL 900 SECOND",
		"sum(cnt) AS cnt FROM logs_histogram",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %q in:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "minute <=") {
		t.Errorf("inclusive upper bound:\n%s", sql)
	}
}

// Histogram chunks behind the newest one may use the ClickHouse query cache; the
// newest may not, or a live tail would freeze.
func TestBuildHistogramSQLCachesOnlyWhenAsked(t *testing.T) {
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(6 * time.Hour)

	pipeline, err := parser.ParseQuery("level=error")
	if err != nil {
		t.Fatal(err)
	}
	scanned := &preparedQuery{pipeline: pipeline, histBucketSec: 900, translationOpts: parser.QueryOptions{FractalID: "f1", TableName: "logs"}}
	rollup := &preparedQuery{histRollupTable: "logs_histogram", histBucketSec: 900, translationOpts: parser.QueryOptions{FractalID: "f1"}}

	for name, p := range map[string]*preparedQuery{"scan": scanned, "rollup": rollup} {
		t.Run(name, func(t *testing.T) {
			fresh, err := p.buildHistogramSQL(start, end, false)
			if err != nil {
				t.Fatal(err)
			}
			cached, err := p.buildHistogramSQL(start, end, true)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(fresh, "use_query_cache") {
				t.Errorf("uncached chunk carries cache settings:\n%s", fresh)
			}
			if cached != fresh+" SETTINGS "+histogramCacheSettings {
				t.Errorf("cached chunk is not the fresh SQL plus cache settings:\n%s", cached)
			}
			if !strings.Contains(fresh, "2026-10-01 06:00:00") {
				t.Errorf("chunk does not carry its own bounds:\n%s", fresh)
			}
		})
	}
}

// Each stream chunk re-translates the query over its own slice with the
// remaining row budget; older chunks use an exclusive upper bound so the slices
// abut without overlap.
func TestBuildChunkSQL(t *testing.T) {
	pipeline, err := parser.ParseQuery("level=error")
	if err != nil {
		t.Fatal(err)
	}
	p := &preparedQuery{pipeline: pipeline, translationOpts: parser.QueryOptions{FractalID: "f1", TableName: "logs", MaxRows: 1000}}
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	newest, err := p.buildChunkSQL(streamChunk{start: start, end: start.Add(time.Hour)}, 37)
	if err != nil {
		t.Fatal(err)
	}
	older, err := p.buildChunkSQL(streamChunk{start: start, end: start.Add(time.Hour), endExclusive: true}, 37)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(newest, "timestamp <= '2026-10-01 01:00:00") || !strings.Contains(older, "timestamp < '2026-10-01 01:00:00") {
		t.Errorf("upper bounds wrong:\nnewest: %s\nolder:  %s", newest, older)
	}
	if !strings.Contains(newest, "LIMIT 37") {
		t.Errorf("remaining budget not applied:\n%s", newest)
	}
	if p.translationOpts.MaxRows != 1000 || !p.translationOpts.EndTime.IsZero() {
		t.Error("buildChunkSQL mutated the shared translation options")
	}
}

func TestIsRawLogRegexQuery(t *testing.T) {
	cases := map[string]bool{
		"/denied/":                      true,
		`"access denied"`:               true,
		"level=error | /denied/":        true,
		"level=error AND /denied/":      true,
		"(level=error OR /denied/)":     true,
		"* | (level=error OR /denied/)": true,
		"msg=/denied/":                  false, // a field regex is index-assisted, not a full-text scan
		"level=error":                   false,
		"level=error | groupby(host)":   false,
	}
	for q, want := range cases {
		p, err := parser.ParseQuery(q)
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		if got := isRawLogRegexQuery(p); got != want {
			t.Errorf("isRawLogRegexQuery(%q) = %v, want %v", q, got, want)
		}
	}
}

func TestProfileValueCoercion(t *testing.T) {
	if profStr(nil) != "" || profStr("x") != "x" || profStr(uint64(7)) != "7" {
		t.Error("profStr")
	}
	u64 := []struct {
		in   interface{}
		want uint64
	}{
		{uint64(5), 5}, {uint8(1), 1}, {int64(9), 9}, {int64(-3), 0}, {float64(2.9), 2}, {float64(-1), 0},
		{"42", 42}, {"n/a", 0}, {nil, 0}, {true, 0},
	}
	for _, tc := range u64 {
		if got := profU64(tc.in); got != tc.want {
			t.Errorf("profU64(%#v) = %d, want %d", tc.in, got, tc.want)
		}
	}
	if profF64(12.5) != 12.5 || profF64(uint64(3)) != 3 || profF64(int64(-2)) != -2 || profF64("1") != 0 {
		t.Error("profF64")
	}
}

// encoding/json cannot marshal NaN or Inf, and one such value in one row would
// fail the whole response. Every container shape a result can take is walked.
func TestSanitizeFloats(t *testing.T) {
	row := map[string]interface{}{
		"nan":    math.NaN(),
		"inf":    math.Inf(1),
		"f32":    float32(math.Inf(-1)),
		"ok":     1.5,
		"nested": map[string]interface{}{"x": math.NaN()},
		"list":   []interface{}{math.NaN(), 2.0, float32(math.NaN()), map[string]interface{}{"y": math.Inf(1)}},
		"rows":   []map[string]interface{}{{"z": math.NaN()}},
	}
	rec := httptest.NewRecorder()
	respondJSON(rec, http.StatusOK, row)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"nan":null`) {
		t.Fatalf("response not encodable: %d %s", rec.Code, rec.Body.String())
	}
	if row["ok"] != 1.5 || row["inf"] != nil || row["f32"] != nil {
		t.Errorf("top level: %v", row)
	}
	if row["nested"].(map[string]interface{})["x"] != nil {
		t.Error("nested map not sanitized")
	}
	list := row["list"].([]interface{})
	if list[0] != nil || list[1] != 2.0 || list[2] != nil || list[3].(map[string]interface{})["y"] != nil {
		t.Errorf("list not sanitized: %v", list)
	}
	if row["rows"].([]map[string]interface{})[0]["z"] != nil {
		t.Error("row slice not sanitized")
	}
}

func TestHistogramBucketSeconds(t *testing.T) {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		span time.Duration
		want int
	}{
		{time.Hour, 30},
		{time.Hour + time.Second, 900},
		{24 * time.Hour, 900},
		{7 * 24 * time.Hour, 3600},
		{30 * 24 * time.Hour, 10800},
		{365 * 24 * time.Hour, 86400},
		{2 * 365 * 24 * time.Hour, 604800},
	}
	for _, tc := range cases {
		if got := histogramBucketSeconds(base, base.Add(tc.span)); got != tc.want {
			t.Errorf("%v: %ds, want %ds", tc.span, got, tc.want)
		}
	}
	if sec, n, _, _ := histogramWindow(base, base); sec != 0 || n != 0 {
		t.Errorf("empty range produced %d buckets of %ds", n, sec)
	}
}

func TestBucketHistogram(t *testing.T) {
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	rows := []map[string]interface{}{
		{"bucket": start, "cnt": uint64(3)},
		{"bucket": "2026-10-01 00:01:00", "cnt": int64(4)}, // string from the HTTP driver
		{"bucket": start.Add(2 * time.Minute), "cnt": float64(5)},
		{"bucket": start.Add(3 * time.Minute), "cnt": 6},
		{"bucket": start.Add(10 * time.Minute), "cnt": uint64(99)}, // past the window
		{"bucket": start.Add(-time.Minute), "cnt": uint64(99)},     // before it
		{"bucket": "not a time", "cnt": uint64(99)},
		{"cnt": uint64(99)}, // no bucket
		{"bucket": start},   // no count
	}
	got := bucketHistogram(rows, start, 60, 4)
	want := []int{3, 4, 5, 6}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if bucketHistogram(rows, start, 0, 4) != nil || bucketHistogram(rows, start, 60, 0) != nil {
		t.Error("a degenerate window must yield no histogram, not a panic or an empty array")
	}
}

func TestParseTimeRange(t *testing.T) {
	var h QueryHandler
	before := time.Now()
	start, end, err := h.parseTimeRange("", "")
	if err != nil {
		t.Fatal(err)
	}
	if d := end.Sub(start); d < 24*time.Hour-time.Second || d > 24*time.Hour+time.Second {
		t.Errorf("default range is %v, want 24h", d)
	}
	if end.Before(before) || start.Location() != time.UTC {
		t.Errorf("default range %v..%v not ending now in UTC", start, end)
	}

	for _, tc := range [][2]string{{"bad", ""}, {"", "bad"}, {"2026-10-02T00:00:00Z", "2026-10-01T00:00:00Z"}} {
		if _, _, err := h.parseTimeRange(tc[0], tc[1]); err == nil {
			t.Errorf("parseTimeRange(%q, %q) accepted", tc[0], tc[1])
		}
	}
	// A zero-length range is a point lookup, not an error.
	if _, _, err := h.parseTimeRange("2026-10-01T00:00:00Z", "2026-10-01T00:00:00Z"); err != nil {
		t.Errorf("equal bounds rejected: %v", err)
	}
}

// The query timeout must reach ClickHouse as well as Go: a Go deadline alone
// stops this process reading while the server keeps scanning.
func TestSearchContext(t *testing.T) {
	ctx, cancel := searchContext(context.Background(), 30)
	defer cancel()
	dl, ok := ctx.Deadline()
	if !ok || time.Until(dl) > 30*time.Second || time.Until(dl) < 29*time.Second {
		t.Errorf("deadline = %v (set=%v), want ~30s", dl, ok)
	}

	ctx, cancel = searchContext(context.Background(), 0)
	if _, ok := ctx.Deadline(); ok {
		t.Error("timeout 0 means unbounded, but a deadline was set")
	}
	cancel()
	if ctx.Err() == nil {
		t.Error("an unbounded search context must still be cancellable")
	}
}

func TestTableNamesWithoutClient(t *testing.T) {
	var h QueryHandler
	if h.queryTableName() != "logs" || h.procLineageTableName() != "proc_lineage" ||
		h.procFreqTableName() != "proc_freq" || h.procEdgesTableName() != "process_edges" || h.dictDatabase() != "" {
		t.Error("nil client defaults changed")
	}
	h.db = &storage.ClickHouseClient{}
	if h.queryTableName() != "logs" || h.procLineageTableName() != "proc_lineage" ||
		h.procFreqTableName() != "proc_freq" || h.procEdgesTableName() != "process_edges" {
		t.Error("single-node client must read the local tables")
	}
	if h.dictDatabase() != "logs" {
		t.Errorf("dictDatabase = %q, want the default logs database so dictGet is qualified", h.dictDatabase())
	}
}

func TestErrorSpanFrom(t *testing.T) {
	_, perr := parser.ParseQuery("level=error | eval(x:=)")
	if perr == nil {
		t.Fatal("expected a parse error")
	}
	span := errorSpanFrom(perr)
	if span == nil || span.Start < 0 || span.End <= span.Start {
		t.Errorf("span = %+v", span)
	}
	if errorSpanFrom(errors.New("no position")) != nil {
		t.Error("an error without a position produced a span")
	}
}

// Raw ClickHouse exceptions can carry column names and sampled values, so only
// mapped codes produce a message; everything else exposes just the number.
func TestClickHouseErrorMapping(t *testing.T) {
	mapped := map[int32]string{
		241: "memory limits", 159: "timed out", 160: "timed out", 158: "too much data", 396: "too much data",
		53: "Type mismatch", 43: "Type mismatch", 70: "Type mismatch", 42: "wrong number of arguments",
		6: "parse a value", 27: "parse a value", 72: "parse a value", 47: "unknown field",
	}
	for code, want := range mapped {
		err := fmt.Errorf("query: %w", &proto.Exception{Code: code, Message: "secret_column = 'sampled value'"})
		msg, ok := clickhouseUserMessage(err)
		if !ok || !strings.Contains(msg, want) {
			t.Errorf("code %d: %q (ok=%v), want it to mention %q", code, msg, ok, want)
		}
		if strings.Contains(msg, "secret_column") {
			t.Errorf("code %d leaked the server message", code)
		}
		if got := clickhouseErrorCode(err); got != code {
			t.Errorf("clickhouseErrorCode = %d, want %d", got, code)
		}
	}
	if _, ok := clickhouseUserMessage(context.DeadlineExceeded); ok {
		t.Error("a non-ClickHouse error was mapped")
	}
	if clickhouseErrorCode(context.DeadlineExceeded) != 0 {
		t.Error("a non-ClickHouse error has no code")
	}
}

// With no audit fractal configured the audit write is skipped entirely; it must
// not reach for a client that may not exist.
func TestLogQueryAuditWithoutAuditFractal(t *testing.T) {
	h := &QueryHandler{} // nil client: a write attempt would panic
	h.logQueryAudit("level=error", "bql", "f1", "ana", 12, 3, nil)
	if h.getAuditFractalID(context.Background()) != "" {
		t.Error("audit fractal resolved without a fractal manager")
	}
	h.abandonSearch("bif-search-x") // nil client: must be a no-op too
}
