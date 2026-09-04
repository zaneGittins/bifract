package parser

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// regex(... as=NAME) without a named capture group must alias the extracted
// value to NAME (not the legacy "regex_match"), so downstream references and
// live previews see the intended column.
func TestRegexAsAliasesOutputColumn(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}
	pipeline, err := ParseQuery(`level="a" | regex(field=norm_log, regex="(\\d+)", as=num)`)
	if err != nil {
		t.Fatalf("ParseQuery failed: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	if !strings.Contains(result.SQL, "AS num") {
		t.Fatalf("expected column aliased AS num, got SQL:\n%s", result.SQL)
	}
	if strings.Contains(result.SQL, "regex_match") {
		t.Fatalf("did not expect regex_match when as= is provided, got SQL:\n%s", result.SQL)
	}
}

// len(x, as=name) must produce an independently-named length column so multiple
// length filters in one pipeline do not collide on the shared _len field.
func TestLenAsAvoidsCollision(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}
	pipeline, err := ParseQuery(`level="a" | regex(field=norm_log, regex="(\\S+)", as=a) | len(a, as=alen) | alen >= 2 | regex(field=norm_log, regex="(\\d+)", as=b) | len(b, as=blen) | blen >= 3`)
	if err != nil {
		t.Fatalf("ParseQuery failed: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	// Each length filter must reference its own extraction, not a shared field.
	if !strings.Contains(result.SQL, `length(extract(norm_log, '(\\S+)')) >= 2`) {
		t.Fatalf("alen filter missing or wrong:\n%s", result.SQL)
	}
	if !strings.Contains(result.SQL, `length(extract(norm_log, '(\\d+)')) >= 3`) {
		t.Fatalf("blen filter missing or wrong:\n%s", result.SQL)
	}
}

// Named capture groups continue to take precedence and produce their own columns.
func TestRegexNamedGroupStillWorks(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
	}
	pipeline, err := ParseQuery(`level="a" | regex(field=norm_log, regex="(?<word>[a-z]+)")`)
	if err != nil {
		t.Fatalf("ParseQuery failed: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	if !strings.Contains(result.SQL, "AS word") {
		t.Fatalf("expected column aliased AS word, got SQL:\n%s", result.SQL)
	}
}

// Named capture groups must be recognized in every flavor users write. Only
// (?<name>...) was detected before, so a Go/Python style (?P<name>...) pattern
// silently produced a regex_match array instead of the named columns.
func TestRegexNamedGroupFlavors(t *testing.T) {
	opts := QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   100,
	}
	cases := []struct{ name, pattern string }{
		{"angle", `<Command>(?<binary>.*?)</Command>\\s*<Arguments>(?<args>.*?)</Arguments>`},
		{"perl_p", `<Command>(?P<binary>.*?)</Command>\\s*<Arguments>(?P<args>.*?)</Arguments>`},
		{"quoted_p", `<Command>(?P'binary'.*?)</Command>\\s*<Arguments>(?P'args'.*?)</Arguments>`},
		{"quoted", `<Command>(?'binary'.*?)</Command>\\s*<Arguments>(?'args'.*?)</Arguments>`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pipeline, err := ParseQuery(`event_id=4698 | regex(pattern="(?is)` + tc.pattern + `", field=task_content) | table(binary, args)`)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			result, err := TranslateToSQLWithOrder(pipeline, opts)
			if err != nil {
				t.Fatalf("translate: %v", err)
			}
			for i, col := range []string{"binary", "args"} {
				want := fmt.Sprintf("[1][%d] AS %s", i+1, col)
				if !strings.Contains(result.SQL, want) {
					t.Fatalf("missing %q in SQL:\n%s", want, result.SQL)
				}
			}
			if strings.Contains(result.SQL, "regex_match") {
				t.Fatalf("named groups should not fall back to regex_match:\n%s", result.SQL)
			}
			// Named-group syntax is stripped so ClickHouse's RE2 sees plain groups.
			if strings.Contains(result.SQL, "?P<") || strings.Contains(result.SQL, "?<binary>") || strings.Contains(result.SQL, "?'") {
				t.Fatalf("named-group syntax leaked into SQL:\n%s", result.SQL)
			}
		})
	}
}

// A pattern with no capture group cannot feed extractAllGroups; the user gets a
// clear parse-time message instead of a ClickHouse error.
func TestRegexWithoutCaptureGroupErrors(t *testing.T) {
	pipeline, err := ParseQuery(`* | regex("powershell", field=image)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = TranslateToSQL(pipeline, QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   100,
	})
	if err == nil || !strings.Contains(err.Error(), "no capture group") {
		t.Fatalf("expected a no-capture-group error, got %v", err)
	}
}
