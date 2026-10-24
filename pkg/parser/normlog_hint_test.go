package parser

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func hintOpts() QueryOptions {
	return QueryOptions{
		StartTime:         time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:           time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		FractalID:         "f1",
		MaxRows:           100,
		NormLogIndexHints: true,
	}
}

// A field filter whose value must appear in norm_log carries an indexHint on that
// value, attached to the filter itself. indexHint is always true for a row, so the
// rows returned cannot change; ClickHouse only uses it to skip blocks whose n-gram
// index proves the value absent.
func TestNormLogHintAttachedToEligibleFilters(t *testing.T) {
	cases := []struct {
		query string
		want  string
	}{
		{`computer_name=~"MAvi01"`,
			"(multiSearchAnyCaseInsensitive(fields.`computer_name`::String, ['MAvi01']) AND indexHint(match(lower(norm_log), 'mavi01')))"},
		{`user="Alice.Smith"`,
			"(fields.`user`::String = 'Alice.Smith' AND indexHint(match(lower(norm_log), 'alice\\\\.smith')))"},
		{`client_id="C.ab1e0b780f51aa20"`,
			"(fields.`client_id`::String = 'C.ab1e0b780f51aa20' AND indexHint(match(lower(norm_log), 'c\\\\.ab1e0b780f51aa20')))"},
		{`image=^"cmd"`,
			"(startsWith(lower(fields.`image`::String), 'cmd') AND indexHint(match(lower(norm_log), 'cmd')))"},
		{`image=$"cmd.exe"`,
			"(endsWith(lower(fields.`image`::String), 'cmd.exe') AND indexHint(match(lower(norm_log), 'cmd\\\\.exe')))"},
		// One hint per value: a block may be skipped only when every value is absent.
		{`user="alice","bob-1"`,
			"(fields.`user`::String IN ('alice', 'bob-1') AND (indexHint(match(lower(norm_log), 'alice')) OR indexHint(match(lower(norm_log), 'bob-1'))))"},
		{`computer_name=~"mavi","host:9"`,
			"(multiSearchAnyCaseInsensitive(fields.`computer_name`::String, ['mavi', 'host:9']) AND (indexHint(match(lower(norm_log), 'mavi')) OR indexHint(match(lower(norm_log), 'host:9'))))"},
		{`proc.name="svc@x"`,
			"(fields.`proc`.`name`::String = 'svc@x' AND indexHint(match(lower(norm_log), 'svc@x')))"},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			res, err := TranslateToSQLWithOrder(mustParse(t, tc.query), hintOpts())
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(res.SourceWhere, " AND "), tc.want) {
				t.Errorf("source scan lacks %s\n%s", tc.want, res.SQL)
			}
			if n := strings.Count(res.SQL, "indexHint("); n != strings.Count(tc.want, "indexHint(") {
				t.Errorf("%d hints in the SQL, want them only on the scan:\n%s", n, res.SQL)
			}
		})
	}
}

// A hint stays on its own filter, so OR and NOT keep their meaning: a block is
// skipped only if no branch can match in it.
func TestNormLogHintStaysWithItsFilter(t *testing.T) {
	sql := mustTranslate(t, `user="alice" OR image=~"cmd.exe"`, hintOpts())
	want := "((fields.`user`::String = 'alice' AND indexHint(match(lower(norm_log), 'alice'))) OR " +
		"(multiSearchAnyCaseInsensitive(fields.`image`::String, ['cmd.exe']) AND indexHint(match(lower(norm_log), 'cmd\\\\.exe'))))"
	if !strings.Contains(sql, want) {
		t.Errorf("want %s\n%s", want, sql)
	}
	sql = mustTranslate(t, `!(user="alice" AND image=~"cmd.exe")`, hintOpts())
	if !strings.Contains(sql, "NOT ((fields.`user`::String = 'alice' AND indexHint(") {
		t.Errorf("negated group lost its shape:\n%s", sql)
	}
}

// Anything the hint's premise does not cover is left exactly as it was: the value
// must reach norm_log verbatim, the filter must require it, and it must run on
// the logs scan.
func TestNormLogHintSkipsIneligibleFilters(t *testing.T) {
	iceberg := hintOpts()
	iceberg.SourceMode = SourceIceberg
	pgr := hintOpts()
	pgr.SourceSubquery = "SELECT 'a' AS user"
	pgr.SourceColumns = []string{"user"}
	off := hintOpts()
	off.NormLogIndexHints = false

	cases := []struct {
		name  string
		query string
		opts  QueryOptions
	}{
		{"disabled", `user="alice"`, off},
		{"iceberg archive", `user="alice"`, iceberg},
		{"subquery source", `user="alice"`, pgr},
		{"not equal", `user!="alice"`, hintOpts()},
		{"negated contains", `!user=~"alice"`, hintOpts()},
		{"presence", `user=*`, hintOpts()},
		{"too short", `user="ab"`, hintOpts()},
		{"number", `dst_port="443"`, hintOpts()},
		{"float", `ratio="1.5e3"`, hintOpts()},
		{"float word", `ratio="inf"`, hintOpts()},
		{"nan", `ratio="NaN"`, hintOpts()},
		{"space", `user="alice smith"`, hintOpts()},
		{"slash", `image=~"bin/sh"`, hintOpts()},
		{"backslash", `image=~"c:\\win"`, hintOpts()},
		{"quote", `user="o'neil"`, hintOpts()},
		{"non-ascii", `user="café1"`, hintOpts()},
		{"one ineligible value", `user="alice","ab"`, hintOpts()},
		{"regex", `user=/alice/`, hintOpts()},
		{"compare to field", `user=field(image)`, hintOpts()},
		{"bare term", `"alice"`, hintOpts()},
		{"assigned field", `x := lower(user) | x="alice"`, hintOpts()},
		{"after a row cap", `* | head(10) | user="alice"`, hintOpts()},
		{"after dedup", `* | dedup(user) | user="alice"`, hintOpts()},
		{"case branch", `* | case { kind="abcq" | k := "yes" ; * | k := "no" ; } | k="yes"`, hintOpts()},
		{"chain step", `* | chain([user], within=5m) { kind="abcq"; image=~"defq" }`, hintOpts()},
		{"too many values", `user=` + quotedList(17), hintOpts()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql := mustTranslate(t, tc.query, tc.opts)
			if strings.Contains(sql, "indexHint(") {
				t.Errorf("unexpected hint:\n%s", sql)
			}
		})
	}
}

// The histogram, field statistics and every chunk of a search share the scan
// filter, hints included.
func TestNormLogHintReachesHistogram(t *testing.T) {
	sql, err := BuildHistogramSQL(mustParse(t, `computer_name=~"MAvi01"`), hintOpts(), 60)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "AND indexHint(match(lower(norm_log), 'mavi01'))") {
		t.Errorf("histogram lacks the hint:\n%s", sql)
	}
}

func quotedList(n int) string {
	vals := make([]string, n)
	for i := range vals {
		vals[i] = fmt.Sprintf("%q", fmt.Sprintf("user%02d", i))
	}
	return strings.Join(vals, ",")
}

// Each scan of the logs table hints its own filters: a join's subquery, a filter
// on a group key the translator pushes into the scan, and lists up to the cap.
func TestNormLogHintCoversEveryLogsScan(t *testing.T) {
	cases := []struct {
		query string
		hints []string
	}{
		{`user="alice" | join(user) { user_type="admin" | groupby(user) }`, []string{"'alice'", "'admin'"}},
		{`* | groupby(user) | user="alice"`, []string{"'alice'"}},
		{`user=` + quotedList(16), []string{"'user00'", "'user15'"}},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			sql := mustTranslate(t, tc.query, hintOpts())
			for _, h := range tc.hints {
				if !strings.Contains(sql, "indexHint(match(lower(norm_log), "+h+"))") {
					t.Errorf("no hint on %s:\n%s", h, sql)
				}
			}
		})
	}
}
