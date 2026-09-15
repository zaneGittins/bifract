package parser

import (
	"strings"
	"testing"
	"time"
)

// Every query here compiled to SQL that ClickHouse refused to run. They are
// grouped by the defect they pin, and each one was verified against a live
// server after the fix.

func serverOpts() QueryOptions {
	return QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
		FractalID: "f1",
	}
}

func translateForServer(t *testing.T, query string) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %s: %v", query, err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, serverOpts())
	if err != nil {
		t.Fatalf("translate %s: %v", query, err)
	}
	return result.SQL
}

// A window function is evaluated after HAVING, so a filter on a window output
// has to move to a stage above it. Leaving it in HAVING, or letting a stage push
// downgrade the column back to a log field so it sank into the scan's WHERE,
// both reached the server as a reference to a column that does not exist yet.
func TestWindowOutputFiltersSitAboveTheWindow(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | frequency(status_code) | _percentage > 1.0`, "WHERE _percentage > 1.0"},
		{`* | headTail(src_ip) | _segment = "head"`, "WHERE _segment = 'head'"},
		{`* | mad(latency) | _mad > 50`, "WHERE _mad > 50"},
		{`* | groupby(host) | count() | mad(_count) | _mad > 50`, "WHERE _mad > 50"},
		{`* | groupby(user) | count() | madOutlier(_count, 3.5) | _is_outlier = "1"`, "WHERE _is_outlier = '1'"},
	}
	for _, tc := range cases {
		sql := translateForServer(t, tc.query)
		if !strings.HasSuffix(sql, tc.want) {
			t.Errorf("%s: want the filter last as %q, got: %s", tc.query, tc.want, sql)
		}
		if strings.Contains(sql, "HAVING") {
			t.Errorf("%s: a window output cannot be filtered in HAVING: %s", tc.query, sql)
		}
	}
}

func TestWindowOutputSortSitsAboveTheWindow(t *testing.T) {
	sql := translateForServer(t, `* | groupby(user) | count() | modifiedZScore(_count) | sort(_modified_z, desc)`)
	if !strings.HasSuffix(sql, "ORDER BY _modified_z DESC LIMIT 1000") {
		t.Errorf("want the sort in the outer stage, got: %s", sql)
	}
}

// These commands need one value per row. After a GROUP BY the rows are groups
// and a log field is gone, which the server reported as "not under aggregate
// function and not in GROUP BY" (code 215).
func TestMeasuringACollapsedFieldIsRejected(t *testing.T) {
	for _, query := range []string{
		`* | groupby(host) | mad(latency)`,
		`* | groupby(host) | modifiedZScore(latency)`,
		`* | groupby(host) | madOutlier(latency, 3.5)`,
		`* | groupby(host) | histogram(latency)`,
	} {
		pipeline, err := ParseQuery(query)
		if err != nil {
			t.Fatalf("parse %s: %v", query, err)
		}
		if _, err := TranslateToSQLWithOrder(pipeline, serverOpts()); err == nil {
			t.Errorf("%s: expected a rejection, the column does not survive the aggregation", query)
		}
	}
}

// The bucketing layers read one numeric column, and which expression produces it
// is only knowable while the registry still holds the field. Rebuilding it later
// emitted a bare identifier the scan never projected (code 47).
func TestHistogramReadsAProjectedValue(t *testing.T) {
	cases := []struct{ query, want, reject string }{
		{`* | histogram(bytes)`, "toFloat64OrNull(fields.`bytes`::String) AS _hist_val", "toFloat64OrNull(toString(bytes))"},
		{`* | histogram(bytes, buckets=30)`, "_hist_val AS _val", "toFloat64OrNull(toString(bytes))"},
		{`* | groupby(user) | count() | histogram(_count)`, "toFloat64(_count) AS _val", "toString(_count)"},
	}
	for _, tc := range cases {
		sql := translateForServer(t, tc.query)
		if !strings.Contains(sql, tc.want) {
			t.Errorf("%s: want %q, got: %s", tc.query, tc.want, sql)
		}
		if strings.Contains(sql, tc.reject) {
			t.Errorf("%s: still emits %q: %s", tc.query, tc.reject, sql)
		}
	}
}

// A single distinct value makes max = min, and dividing by that range produced
// inf, which toUInt32 refused to convert.
func TestHistogramBucketExpressionHandlesAFlatRange(t *testing.T) {
	sql := translateForServer(t, `* | histogram(bytes)`)
	if !strings.Contains(sql, "if(_max_val = _min_val, 0,") {
		t.Errorf("want a guarded bucket expression, got: %s", sql)
	}
	if !strings.Contains(sql, "least(toUInt32(") {
		t.Errorf("want the top bucket clamped, got: %s", sql)
	}
}

// topKWeightedWithCount exists in no ClickHouse release.
func TestTopPercentUsesAFunctionThatExists(t *testing.T) {
	for _, query := range []string{
		`* | top(user, percent=true)`,
		`* | groupby(user) | multi(top(field=event_id, percent=true, as=t))`,
	} {
		sql := translateForServer(t, query)
		if !strings.Contains(sql, "approx_top_k(") {
			t.Errorf("%s: want approx_top_k, got: %s", query, sql)
		}
		if strings.Contains(sql, "topKWeightedWithCount") {
			t.Errorf("%s: still emits a function the server does not have: %s", query, sql)
		}
	}
}

// toFloat64OrZero and its siblings take a String. Applying one to an expression
// that already compiles to a number was rejected as an illegal argument type.
func TestNumericExpressionsAreNotCoercedFromString(t *testing.T) {
	sql := translateForServer(t, `* | n := len(commandline) | n > 500`)
	if !strings.Contains(sql, "length(fields.`commandline`::String) > 500") {
		t.Errorf("want the length compared directly, got: %s", sql)
	}
	if strings.Contains(sql, "toFloat64OrZero(length(") {
		t.Errorf("still casts a number as if it were a string: %s", sql)
	}
}

// A second eval of the same name reads the column the first one made.
func TestEvalRedefinesItsOwnColumnOnce(t *testing.T) {
	sql := translateForServer(t, `* | eval(total = bytes * 2) | eval(total = total * 3)`)
	if !strings.Contains(sql, "(toFloat64OrNull(fields.`bytes`::String) * 2) * 3 AS total") {
		t.Errorf("want the prior assignment folded in, got: %s", sql)
	}
	if strings.Count(sql, "AS total") != 1 {
		t.Errorf("want one total column, got: %s", sql)
	}
}

// A dotted field is aliased with backticks, and comparing the quoted alias
// against the unquoted name made the aggregate miss the column it grouped by.
func TestDottedFieldSurvivesAnAggregation(t *testing.T) {
	sql := translateForServer(t, `* | groupby(a.b) | count() | percentile(a.b)`)
	if !strings.Contains(sql, "quantiles(0.5, 0.75, 0.99)(toFloat64OrNull(a.b))") {
		t.Errorf("want the grouped alias measured, got: %s", sql)
	}
}

// An expression filter used to be readable only as a whole stage head. As one
// leaf of a boolean chain it fell through to the field-comparison parser, which
// read lower(image) as a column name.
func TestExpressionFilterWorksAsAnyChainLeaf(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | (a="x" OR lower(image)="y")`, "lower(fields.`image`::String) = 'y'"},
		{`* | lower(a)="x" AND b=/foo/`, "match(fields.`b`::String, 'foo')"},
		{`* | lower(image) =~ "cmd","ps"`, "multiSearchAnyCaseInsensitive(lower(fields.`image`::String), ['cmd', 'ps'])"},
	}
	for _, tc := range cases {
		sql := translateForServer(t, tc.query)
		if !strings.Contains(sql, tc.want) {
			t.Errorf("%s: want %q, got: %s", tc.query, tc.want, sql)
		}
		if !strings.Contains(sql, "fractal_id = 'f1' AND ") {
			t.Errorf("%s: the fractal guard must stay joined to the rest: %s", tc.query, sql)
		}
	}
}

// An argument written with no value used to be re-parsed as the whole query,
// which put a fragment of the user's own text into the SQL.
func TestAnArgumentWithNoValueIsAParseError(t *testing.T) {
	for _, query := range []string{`user="x" | sort(field=)`, `* | table(fields=)`} {
		if _, err := ParseQuery(query); err == nil {
			t.Errorf("%s: expected a parse error", query)
		}
	}
}

func TestCIDRNeedsARange(t *testing.T) {
	pipeline, err := ParseQuery(`* | cidr(src_ip, "")`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, err := TranslateToSQLWithOrder(pipeline, serverOpts()); err == nil {
		t.Error("expected a rejection; isIPAddressInRange needs a '/'")
	}
}

// as= renames the column, so the registry has to know it by that name. Declaring
// the default left mad(x, as=spread) | _mad > 5 filtering on a column no stage
// projects.
func TestAggregateAliasIsWhatTheRegistryKnows(t *testing.T) {
	sql := translateForServer(t, `* | mad(latency, as=spread) | spread > 5`)
	if !strings.HasSuffix(sql, "WHERE spread > 5") {
		t.Errorf("want the filter on the aliased column, got: %s", sql)
	}
}

func TestCIDRRangeMustParse(t *testing.T) {
	for _, arg := range []string{`""`, `"10.0.0.0"`, `"1.2.3.4/40"`, `"notanip/8"`, `"/8"`} {
		query := `* | cidr(src_ip, ` + arg + `)`
		pipeline, err := ParseQuery(query)
		if err != nil {
			continue
		}
		if _, err := TranslateToSQLWithOrder(pipeline, serverOpts()); err == nil {
			t.Errorf("%s: expected a rejection", query)
		}
	}
	if _, err := ParseQuery(`* | cidr(src_ip, "10.0.0.0/8")`); err != nil {
		t.Fatalf("a valid range must still parse: %v", err)
	}
	sql := translateForServer(t, `* | cidr(src_ip, "2001:db8::/32")`)
	if !strings.Contains(sql, "isIPAddressInRange") {
		t.Errorf("an IPv6 range must still compile, got: %s", sql)
	}
}
