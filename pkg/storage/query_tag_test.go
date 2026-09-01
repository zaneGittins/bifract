package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestQueryTagRendering(t *testing.T) {
	if got := (QueryTag{}).String(); got != "" {
		t.Errorf("a tag with no source should render empty, got %q", got)
	}

	tag := QueryTag{Source: SourceSearch, User: "zane", Fractal: "f-1", Label: "ad hoc"}
	var back QueryTag
	if err := json.Unmarshal([]byte(tag.String()), &back); err != nil {
		t.Fatalf("rendered tag is not valid JSON: %v", err)
	}
	if back != tag {
		t.Errorf("tag did not round-trip: %+v != %+v", back, tag)
	}
}

// The tag is written into system.query_log for every query, so an oversized label
// must be trimmed rather than stored forever.
func TestQueryTagIsBounded(t *testing.T) {
	tag := QueryTag{Source: SourceDashboard, Label: strings.Repeat("wide", 400)}
	rendered := tag.String()
	if len(rendered) > maxQueryTagLen {
		t.Fatalf("rendered tag is %d bytes, over the %d cap", len(rendered), maxQueryTagLen)
	}
	if !strings.Contains(rendered, SourceDashboard) {
		t.Error("truncation dropped the source, which is the field that must survive")
	}
	if !json.Valid([]byte(rendered)) {
		t.Error("truncated tag is not valid JSON")
	}
}

func TestQueryTagTruncationKeepsValidUTF8(t *testing.T) {
	tag := QueryTag{Source: SourceSearch, Label: strings.Repeat("日本語", 100)}
	rendered := tag.String()
	if !json.Valid([]byte(rendered)) {
		t.Fatalf("multibyte label produced invalid JSON: %q", rendered)
	}
	var back QueryTag
	if err := json.Unmarshal([]byte(rendered), &back); err != nil {
		t.Fatalf("multibyte label did not round-trip: %v", err)
	}
}

// The BQL a search carries is the reason the tag can be large, so the cap has to
// shed it before the fields that identify who ran the query.
func TestQueryTagShedsBQLBeforeIdentity(t *testing.T) {
	tag := QueryTag{
		Source:  SourceSearch,
		User:    "zane",
		Fractal: "f-1",
		Label:   strings.Repeat("label ", 40),
		BQL:     strings.Repeat("host=web-01 | stats count() by user ", 60),
	}
	rendered := tag.String()
	if len(rendered) > maxQueryTagLen {
		t.Fatalf("rendered tag is %d bytes, over the %d cap", len(rendered), maxQueryTagLen)
	}
	var back QueryTag
	if err := json.Unmarshal([]byte(rendered), &back); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if back.Source != SourceSearch || back.User != "zane" || back.Fractal != "f-1" {
		t.Errorf("truncation dropped an identifying field: %+v", back)
	}
	if len(back.BQL) > maxTagBQLLen {
		t.Errorf("BQL is %d runes, over its own cap", len(back.BQL))
	}
}

// A multi-line query is read back in a table cell, so the newlines only cost bytes.
func TestQueryTagFlattensBQL(t *testing.T) {
	tag := QueryTag{Source: SourceSearch, BQL: "host=web-01\n  | stats count()\n  | sort -count"}
	var back QueryTag
	if err := json.Unmarshal([]byte(tag.String()), &back); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if strings.ContainsAny(back.BQL, "\n\t") {
		t.Errorf("BQL kept its line breaks: %q", back.BQL)
	}
	if back.BQL != "host=web-01 | stats count() | sort -count" {
		t.Errorf("BQL did not flatten cleanly: %q", back.BQL)
	}
}

// src must stay the first key: the activity view classifies rows with
// startsWith over the rendered tag rather than parsing JSON per row.
func TestQueryTagKeepsSourceFirst(t *testing.T) {
	tag := QueryTag{Source: SourceSearch, User: "zane", Fractal: "f", Label: "l", BQL: "*"}
	if got := tag.String(); !strings.HasPrefix(got, `{"src":"`+SourceSearch+`"`) {
		t.Fatalf("a new field displaced src: %s", got)
	}
}

func TestTagContext(t *testing.T) {
	ctx := context.Background()
	if got := ContextTag(ctx); got.Source != "" {
		t.Errorf("an untagged context should carry nothing, got %+v", got)
	}
	ctx = TagContext(ctx, QueryTag{Source: SourceAlert, Label: "rare parent"})
	if got := ContextTag(ctx); got.Source != SourceAlert || got.Label != "rare parent" {
		t.Errorf("tag did not survive the context: %+v", got)
	}
	// A tag set closer to the query wins.
	ctx = TagContext(ctx, QueryTag{Source: SourceRecall})
	if got := ContextTag(ctx); got.Source != SourceRecall {
		t.Errorf("nested tag did not override: %+v", got)
	}
	if got := ContextTag(TagContext(ctx, QueryTag{})); got.Source != SourceRecall {
		t.Error("an empty tag should not clear the one already set")
	}
}

// The row scanner's default case falls back to *string, so any column type it
// does not name outright fails the whole query with "converting X to *string is
// unsupported" the moment there is a row to scan. System tables hand out Int32
// and UInt8 columns routinely, and LowCardinality(String) is an encoding rather
// than a type, so both have to resolve.
func TestScanRowMapTypeCoverage(t *testing.T) {
	covered := []string{
		"String", "Nullable(String)",
		"Int8", "Int16", "Int32", "Int64",
		"UInt8", "UInt16", "UInt32", "UInt64",
		"Float32", "Float64",
		"LowCardinality(String)", "LowCardinality(UInt32)",
		"SimpleAggregateFunction(max, Int32)",
	}
	for _, name := range covered {
		unwrapped := unwrapLowCardinality(unwrapSimpleAggregateFunction(name))
		switch unwrapped {
		case "String", "Nullable(String)",
			"Int8", "Int16", "Int32", "Int64",
			"UInt8", "UInt16", "UInt32", "UInt64",
			"Float32", "Float64":
		default:
			t.Errorf("%q unwraps to %q, which the scanner does not name and would scan as a string", name, unwrapped)
		}
	}
}

func TestUnwrapLowCardinality(t *testing.T) {
	cases := map[string]string{
		"LowCardinality(String)":           "String",
		"LowCardinality(Nullable(String))": "Nullable(String)",
		"String":                           "String",
		"Array(LowCardinality(String))":    "Array(LowCardinality(String))", // only the outer wrapper
		"LowCardinality":                   "LowCardinality",
	}
	for in, want := range cases {
		if got := unwrapLowCardinality(in); got != want {
			t.Errorf("unwrapLowCardinality(%q) = %q, want %q", in, got, want)
		}
	}
}
