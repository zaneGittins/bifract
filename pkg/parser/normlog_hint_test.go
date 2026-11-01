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
		// Every run of characters JSON never escapes must be in norm_log; any other
		// character only separates runs, and each run is required.
		{`user="alice smith"`,
			"(fields.`user`::String = 'alice smith' AND (indexHint(match(lower(norm_log), 'alice')) AND indexHint(match(lower(norm_log), 'smith'))))"},
		{`image=~"bin/sh.x"`,
			"(multiSearchAnyCaseInsensitive(fields.`image`::String, ['bin/sh.x']) AND (indexHint(match(lower(norm_log), 'sh\\\\.x')) AND indexHint(match(lower(norm_log), 'bin'))))"},
		{`image=~"c:\\win"`,
			"(multiSearchAnyCaseInsensitive(fields.`image`::String, ['c:\\\\win']) AND indexHint(match(lower(norm_log), 'win')))"},
		{`user="o'neil"`,
			"(fields.`user`::String = 'o\\'neil' AND indexHint(match(lower(norm_log), 'neil')))"},
		{`user="café1"`,
			"(fields.`user`::String = 'café1' AND indexHint(match(lower(norm_log), 'caf')))"},
		// A number-like run may be stored typed, so only the others are hinted.
		{`cmd=~"port 443 open"`,
			"(multiSearchAnyCaseInsensitive(fields.`cmd`::String, ['port 443 open']) AND (indexHint(match(lower(norm_log), 'port')) AND indexHint(match(lower(norm_log), 'open'))))"},
		// An IPv4 address cannot be read as a number.
		{`answers="209.146.1.1"`,
			"(fields.`answers`::String = '209.146.1.1' AND indexHint(match(lower(norm_log), '209\\\\.146\\\\.1\\\\.1')))"},
		// Only the longest runs are kept; dropping one only weakens the hint.
		{`cmd=~"aaaa bbbbb ccc dddddd"`,
			"(multiSearchAnyCaseInsensitive(fields.`cmd`::String, ['aaaa bbbbb ccc dddddd']) AND (indexHint(match(lower(norm_log), 'dddddd')) AND indexHint(match(lower(norm_log), 'bbbbb')) AND indexHint(match(lower(norm_log), 'aaaa'))))"},
		// A regex is hinted by the literals every match contains.
		{`user=/alice/`,
			"(match(fields.`user`::String, 'alice') AND indexHint(match(lower(norm_log), 'alice')))"},
		{`commandline=/sekurlsa::\w+ (privilege::debug)+/`,
			"(match(fields.`commandline`::String, 'sekurlsa::\\\\w+ (privilege::debug)+') AND (indexHint(match(lower(norm_log), 'privilege::debug')) AND indexHint(match(lower(norm_log), 'sekurlsa::'))))"},
		// Under /i a letter folding outside ASCII (k: the Kelvin sign, s: long s)
		// ends a run, since lower() does not fold it back.
		{`image=/mimikatz/i`,
			"(match(fields.`image`::String, '(?i)mimikatz') AND (indexHint(match(lower(norm_log), 'mimi')) AND indexHint(match(lower(norm_log), 'atz'))))"},
		// A repeated run is looked up once.
		{`cmd=~"abc xyz abc"`,
			"(multiSearchAnyCaseInsensitive(fields.`cmd`::String, ['abc xyz abc']) AND (indexHint(match(lower(norm_log), 'abc')) AND indexHint(match(lower(norm_log), 'xyz'))))"},
		{`commandline=/(sekurlsa){2,}::logon/`,
			"(match(fields.`commandline`::String, '(sekurlsa){2,}::logon') AND (indexHint(match(lower(norm_log), 'sekurlsa')) AND indexHint(match(lower(norm_log), '::logon'))))"},
		// Only the (?i:) scope folds, so the k outside it stays in its run.
		{`user=/kqv21(?i:kit)/`,
			"(match(fields.`user`::String, 'kqv21(?i:kit)') AND indexHint(match(lower(norm_log), 'kqv21')))"},
		{`user=/kqv21/i`,
			"(match(fields.`user`::String, '(?i)kqv21') AND indexHint(match(lower(norm_log), 'qv21')))"},
		{`user=/sqw22x/i`,
			"(match(fields.`user`::String, '(?i)sqw22x') AND indexHint(match(lower(norm_log), 'qw22x')))"},
		{`image=/Invoke-Mimikatz/`,
			"(match(fields.`image`::String, 'Invoke-Mimikatz') AND indexHint(match(lower(norm_log), 'invoke-mimikatz')))"},
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

// A list's values share one lookup budget, so 8 values keep their 2 longest
// runs each rather than 3.
func TestNormLogHintSharesLookupBudget(t *testing.T) {
	vals := make([]string, 8)
	for i := range vals {
		vals[i] = fmt.Sprintf(`"aaa%[1]d bbbb%[1]d ccccc%[1]d"`, i)
	}
	sql := mustTranslate(t, "cmd=~"+strings.Join(vals, ","), hintOpts())
	if n := strings.Count(sql, "indexHint("); n != maxHintValues {
		t.Errorf("%d lookups, want %d:\n%s", n, maxHintValues, sql)
	}
	if strings.Contains(sql, "norm_log), 'aaa") {
		t.Errorf("kept the shortest run:\n%s", sql)
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
		{"one ineligible value", `user="alice","ab"`, hintOpts()},
		{"no run long enough", `user="a b/c d"`, hintOpts()},
		{"only numeric runs", `cmd=~"443 1.5"`, hintOpts()},
		{"dotted but not an address", `v="1.2.3"`, hintOpts()},
		{"negated regex", `user!=/alice/`, hintOpts()},
		{"regex alternation", `user=/alice|bob/`, hintOpts()},
		{"optional regex part", `user=/(alice)?x/`, hintOpts()},
		{"regex without a literal", `user=/[a-z]+\d{3}/`, hintOpts()},
		{"unparsable regex", `user=/(alice/`, hintOpts()},
		{"folded k and s split every run", `user=/kits/i`, hintOpts()},
		{"regex on norm_log", `norm_log=/alice/`, hintOpts()},
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

// A positive =~, =^ or =$ on norm_log itself (how Sigma keyword rules compile)
// hints each value in the form the index answers: lowercased, serialization
// escaped as the hot store writes it ('/' as '\/'), regex quoted.
func TestNormLogHintOnNormLogFilters(t *testing.T) {
	cases := []struct {
		query string
		want  string
	}{
		{`norm_log=~"Mimikatz"`,
			"(multiSearchAnyCaseInsensitive(norm_log, ['Mimikatz']) AND indexHint(match(lower(norm_log), 'mimikatz')))"},
		{`norm_log=~"sekurlsa::logonpasswords","Invoke-Mimikatz"`,
			"(multiSearchAnyCaseInsensitive(norm_log, ['sekurlsa::logonpasswords', 'Invoke-Mimikatz']) AND " +
				"(indexHint(match(lower(norm_log), 'sekurlsa::logonpasswords')) OR indexHint(match(lower(norm_log), 'invoke-mimikatz'))))"},
		{`norm_log=~"/tmp/x.sh"`,
			`(multiSearchAnyCaseInsensitive(norm_log, ['\\/tmp\\/x.sh']) AND indexHint(match(lower(norm_log), '\\\\/tmp\\\\/x\\.sh')))`},
		{`norm_log=~"C:\\Windows"`,
			`(multiSearchAnyCaseInsensitive(norm_log, ['C:\\\\Windows']) AND indexHint(match(lower(norm_log), 'c:\\\\\\\\windows')))`},
		{`norm_log=~"say \"hi\""`,
			`(multiSearchAnyCaseInsensitive(norm_log, ['say \\"hi\\"']) AND indexHint(match(lower(norm_log), 'say \\\\"hi\\\\"')))`},
		{`norm_log=^"{\"abc"`,
			`(startsWith(lower(norm_log), '{\\"abc') AND indexHint(match(lower(norm_log), '\\{\\\\"abc')))`},
		{`norm_log=$"xyz\"}"`,
			`(endsWith(lower(norm_log), 'xyz\\"}') AND indexHint(match(lower(norm_log), 'xyz\\\\"\\}')))`},
		// =^ and =$ hint the needle they search for, lowered the same way.
		{`norm_log=^"İstanbul"`,
			"(startsWith(lower(norm_log), 'istanbul') AND indexHint(match(lower(norm_log), 'istanbul')))"},
		{`norm_log=~"日本語"`,
			"(multiSearchAnyCaseInsensitive(norm_log, ['日本語']) AND indexHint(match(lower(norm_log), '日本語')))"},
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
		})
	}
}

// norm_log filters the index cannot vouch for keep their exact SQL.
func TestNormLogHintSkipsIneligibleNormLogFilters(t *testing.T) {
	iceberg := hintOpts()
	iceberg.SourceMode = SourceIceberg
	cases := []struct {
		name  string
		query string
		opts  QueryOptions
	}{
		{"iceberg archive", `norm_log=~"mimikatz"`, iceberg},
		{"negated", `!norm_log=~"mimikatz"`, hintOpts()},
		{"too short", `norm_log=~"ab"`, hintOpts()},
		{"one short value", `norm_log=~"mimikatz","ab"`, hintOpts()},
		{"letter with other cases", `norm_log=~"Café"`, hintOpts()},
		{"equality compares the raw literal", `norm_log="a/bc"`, hintOpts()},
		{"too many values", `norm_log=~` + quotedList(17), hintOpts()},
		{"after a row cap", `* | head(10) | norm_log=~"mimikatz"`, hintOpts()},
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
