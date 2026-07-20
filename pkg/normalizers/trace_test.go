package normalizers

import (
	"encoding/json"
	"testing"
)

func mustObj(t *testing.T, raw string) map[string]interface{} {
	t.Helper()
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		t.Fatalf("bad sample json: %v", err)
	}
	return obj
}

// resolve collapses a trace back to the name->value map ingestion would store.
func (r TraceResult) resolve() map[string]string {
	out := make(map[string]string, len(r.Fields))
	for _, f := range r.Fields {
		if _, seen := out[f.Name]; seen {
			continue
		}
		out[f.Name] = f.Value
	}
	return out
}

// TestTraceMatchesHotPath is the guard that keeps the editor preview honest: if
// anyone changes ApplyTransformsWithNested without updating Trace, the preview
// would start describing something ingestion does not do. Collision cases are
// excluded because the hot path drops the loser nondeterministically.
func TestTraceMatchesHotPath(t *testing.T) {
	sample := mustObj(t, `{
		"Computer": "WS01",
		"EventID": 1,
		"User": {"Name": "CORP\\jdoe", "Domain": "CORP"},
		"SourceIp": "10.0.0.5",
		"CommandLine": "powershell.exe -enc AAA",
		"Nested": {"Deep": {"QueryName": "example.com"}}
	}`)

	cases := []struct {
		name string
		n    Normalizer
	}{
		{"no transforms", Normalizer{}},
		{"flatten leaf only", Normalizer{Transforms: []Transform{TransformFlattenLeaf}}},
		{"flatten full only", Normalizer{Transforms: []Transform{TransformFlattenFull}}},
		{"leaf snake lower", Normalizer{Transforms: []Transform{TransformFlattenLeaf, TransformSnakeCase, TransformLowercase}}},
		{"full pascal", Normalizer{Transforms: []Transform{TransformFlattenFull, TransformPascalCase}}},
		{"dedot camel", Normalizer{Transforms: []Transform{TransformDedot, TransformCamelCase}}},
		{
			"with mappings",
			Normalizer{
				Transforms: []Transform{TransformFlattenLeaf, TransformSnakeCase, TransformLowercase},
				FieldMappings: []FieldMapping{
					{Sources: []string{"computer", "host"}, Target: "computer_name"},
					{Sources: []string{"source_ip", "orig_h"}, Target: "src_ip"},
					{Sources: []string{"command_line"}, Target: "commandline"},
				},
			},
		},
		{
			"with derived field",
			Normalizer{
				Transforms: []Transform{TransformFlattenLeaf, TransformSnakeCase, TransformLowercase},
				FieldMappings: []FieldMapping{
					{Sources: []string{"computer"}, Target: "computer_name"},
				},
				ValueMappings: []ValueMapping{
					{FromField: "event_id", ToField: "bifract_category", Map: map[string]string{"1": "process_creation"}},
				},
			},
		},
		{
			"derived with default",
			Normalizer{
				Transforms: []Transform{TransformFlattenLeaf, TransformLowercase},
				ValueMappings: []ValueMapping{
					{FromField: "eventid", ToField: "category", Map: map[string]string{"99": "other"}, Default: "unknown"},
				},
			},
		},
		{
			// No default and no match means the target must not appear at all.
			"derived unmatched without default",
			Normalizer{
				Transforms: []Transform{TransformFlattenLeaf, TransformLowercase},
				ValueMappings: []ValueMapping{
					{FromField: "eventid", ToField: "category", Map: map[string]string{"99": "other"}},
				},
			},
		},
		{
			// Source field absent entirely: the rule must be a no-op.
			"derived from missing source",
			Normalizer{
				Transforms: []Transform{TransformFlattenLeaf, TransformLowercase},
				ValueMappings: []ValueMapping{
					{FromField: "not_present", ToField: "category", Map: map[string]string{"1": "x"}, Default: "fallback"},
				},
			},
		},
		{
			"derived overriding an existing field",
			Normalizer{
				Transforms: []Transform{TransformFlattenLeaf, TransformLowercase},
				ValueMappings: []ValueMapping{
					{FromField: "eventid", ToField: "computer", Map: map[string]string{"1": "OVERRIDDEN"}},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trace := tc.n.Trace(sample)
			if len(trace.Collisions) > 0 {
				t.Fatalf("fixture unexpectedly collided: %v", trace.Collisions)
			}

			built := BuildFieldsWithNested(sample)
			want := tc.n.Compile().ApplyTransformsWithNested(built.Fields, built.NestedKeys)
			got := trace.resolve()

			if len(got) != len(want) {
				t.Fatalf("field count: trace %d, hot path %d\ntrace: %v\nhot:   %v", len(got), len(want), got, want)
			}
			for k, wv := range want {
				gv, ok := got[k]
				if !ok {
					t.Errorf("trace missing field %q (hot path value %q)", k, wv)
					continue
				}
				if gv != wv {
					t.Errorf("field %q: trace %q, hot path %q", k, gv, wv)
				}
			}
		})
	}
}

// TestTraceDetectsMappingCollision covers the case the hot path silently loses:
// two distinct fields mapped onto one target. Ingestion keeps an arbitrary one,
// so the editor has to warn.
func TestTraceDetectsMappingCollision(t *testing.T) {
	sample := mustObj(t, `{"Computer": "WS01", "host": "ws01.corp.local"}`)
	n := Normalizer{
		Transforms: []Transform{TransformLowercase},
		FieldMappings: []FieldMapping{
			{Sources: []string{"computer", "host"}, Target: "computer_name"},
		},
	}

	trace := n.Trace(sample)
	srcs, ok := trace.Collisions["computer_name"]
	if !ok {
		t.Fatalf("expected a collision on computer_name, got %v", trace.Collisions)
	}
	if len(srcs) != 2 {
		t.Fatalf("expected 2 competing sources, got %v", srcs)
	}

	var flagged int
	for _, f := range trace.Fields {
		if f.Name == "computer_name" {
			if !f.Collision {
				t.Errorf("field from %q not flagged as colliding", f.Source)
			}
			flagged++
		}
	}
	if flagged != 2 {
		t.Fatalf("expected both colliding fields retained, got %d", flagged)
	}
}

// TestTraceFlattenCollisionResolvedByPipeline confirms flatten's own full-path
// fallback is reported as-is, not double-counted as a mapping collision.
func TestTraceFlattenCollisionResolvedByPipeline(t *testing.T) {
	sample := mustObj(t, `{"Computer": "WS01", "Event": {"System": {"Computer": "WS01"}}}`)
	n := Normalizer{Transforms: []Transform{TransformFlattenLeaf, TransformLowercase}}

	trace := n.Trace(sample)
	if len(trace.Collisions) > 0 {
		t.Fatalf("flatten should resolve leaf collisions via full paths, got %v", trace.Collisions)
	}

	built := BuildFieldsWithNested(sample)
	want := n.Compile().ApplyTransformsWithNested(built.Fields, built.NestedKeys)
	if got := trace.resolve(); len(got) != len(want) {
		t.Fatalf("trace %v does not match hot path %v", got, want)
	}
}

// TestTraceAttributesAlias checks the editor can highlight which alias fired.
func TestTraceAttributesAlias(t *testing.T) {
	sample := mustObj(t, `{"id.orig_h": "10.0.0.5"}`)
	n := Normalizer{
		Transforms: []Transform{TransformFlattenLeaf},
		FieldMappings: []FieldMapping{
			{Sources: []string{"source_ip"}, Target: "src_ip"},
			{Sources: []string{"id_orig_h", "orig_h"}, Target: "src_ip"},
		},
	}

	trace := n.Trace(sample)
	var found bool
	for _, f := range trace.Fields {
		if f.Name != "src_ip" {
			continue
		}
		found = true
		if f.MappingIndex != 1 {
			t.Errorf("expected mapping index 1, got %d", f.MappingIndex)
		}
		if f.MatchedAlias != "id_orig_h" {
			t.Errorf("expected alias id_orig_h, got %q", f.MatchedAlias)
		}
	}
	if !found {
		t.Fatalf("src_ip not produced: %+v", trace.Fields)
	}
}
