package parser

import (
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
	pipeline, err := ParseQuery(`level="a" | regex(field=raw_log, regex="(\\d+)", as=num)`)
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
	pipeline, err := ParseQuery(`level="a" | regex(field=raw_log, regex="(\\S+)", as=a) | len(a, as=alen) | alen >= 2 | regex(field=raw_log, regex="(\\d+)", as=b) | len(b, as=blen) | blen >= 3`)
	if err != nil {
		t.Fatalf("ParseQuery failed: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		t.Fatalf("translate failed: %v", err)
	}
	// Each length filter must reference its own extraction, not a shared field.
	if !strings.Contains(result.SQL, `length(extract(raw_log, '(\\S+)')) >= 2`) {
		t.Fatalf("alen filter missing or wrong:\n%s", result.SQL)
	}
	if !strings.Contains(result.SQL, `length(extract(raw_log, '(\\d+)')) >= 3`) {
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
	pipeline, err := ParseQuery(`level="a" | regex(field=raw_log, regex="(?<word>[a-z]+)")`)
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
