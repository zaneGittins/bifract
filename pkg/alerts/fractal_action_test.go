package alerts

import (
	"context"
	"encoding/json"
	"testing"

	"bifract/pkg/storage"
)

// TestMergeRowFieldsIsDeterministic guards the bug hydration would otherwise
// expose: the old merge walked a single Go map range, so when a field appeared
// both top-level (the projection's String cast) and inside the nested map, the
// winner flipped between runs.
func TestMergeRowFieldsIsDeterministic(t *testing.T) {
	f := &FractalActionClient{}
	row := map[string]interface{}{
		"timestamp":  "2026-08-03 10:00:00.000",
		"log_id":     "abc",
		"fractal_id": "f1",
		"event_id":   "4624",
		"user":       "zane",
		"fields": map[string]interface{}{
			"event_id":      float64(4624),
			"user":          "ZANE",
			"computer_name": "host1",
		},
	}

	first := f.mergeRowFields(context.Background(), row)
	for i := 0; i < 200; i++ {
		got := f.mergeRowFields(context.Background(), row)
		for k, v := range first {
			if got[k] != v {
				t.Fatalf("iteration %d: key %q = %v, want %v", i, k, got[k], v)
			}
		}
	}
}

// TestMergeRowFieldsTopLevelWins mirrors the precedence in ResolveTemplateName
// and dictionaries.getLogField, and keeps projected values byte-identical to
// what they were before hydration existed.
func TestMergeRowFieldsTopLevelWins(t *testing.T) {
	f := &FractalActionClient{}
	got := f.mergeRowFields(context.Background(), map[string]interface{}{
		"log_id":   "abc",
		"event_id": "4624",
		"fields": map[string]interface{}{
			"event_id":      float64(4624),
			"computer_name": "host1",
		},
	})

	if got["event_id"] != "4624" {
		t.Errorf("event_id = %#v, want the top-level String cast", got["event_id"])
	}
	if got["computer_name"] != "host1" {
		t.Errorf("computer_name = %#v, want the hydrated value", got["computer_name"])
	}
	if got["log_id"] != "abc" {
		t.Errorf("log_id = %#v, want abc", got["log_id"])
	}
	if _, ok := got["fields"]; ok {
		t.Error("the nested map should be promoted, not carried through")
	}
}

// TestMergeRowFieldsParsesNormLog covers the unpruned shape: a pipeline command
// keeps the full projection, which lands norm_log as a raw JSON string.
func TestMergeRowFieldsParsesNormLog(t *testing.T) {
	f := &FractalActionClient{ch: &storage.ClickHouseClient{}}
	got := f.mergeRowFields(context.Background(), map[string]interface{}{
		"log_id":   "abc",
		"norm_log": `{"user":"zane","event_id":"4624"}`,
	})

	if got["user"] != "zane" {
		t.Errorf("user = %#v, want zane", got["user"])
	}
	if _, ok := got["norm_log"]; ok {
		t.Error("norm_log should be promoted, not forwarded as a JSON blob")
	}
}

// TestFieldString covers the value types hydration newly introduces. The pruned
// projection only ever produced String casts, so these paths were unreachable.
func TestFieldString(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{"string", "zane", "zane"},
		{"nil", nil, ""},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"int", 42, "42"},
		{"small float", float64(1.5), "1.5"},
		{"integral float", float64(4624), "4624"},
		{"nanosecond epoch", float64(1700000000000000000), "1700000000000000000"},
		{"nested object", map[string]interface{}{"a": "1"}, `{"a":"1"}`},
		{"array", []interface{}{"a", "b"}, `["a","b"]`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := fieldString(tc.value); got != tc.want {
				t.Errorf("fieldString(%#v) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}

// TestTransformLogsForFractalCarriesHydratedFields is the end of the chain: a
// hydrated row must land in the forwarded log's raw JSON and its indexed fields.
func TestTransformLogsForFractalCarriesHydratedFields(t *testing.T) {
	f := &FractalActionClient{}
	rows := []map[string]interface{}{{
		"timestamp":  "2026-08-03 10:00:00.000",
		"log_id":     "src-1",
		"fractal_id": "f1",
		"event_id":   "4624",
		"fields": map[string]interface{}{
			"event_id":      "4624",
			"user":          "zane",
			"computer_name": "host1",
		},
	}}
	action := FractalAction{TargetFractalID: "f2", PreserveTimestamp: true, FieldMappings: map[string]string{"user": "actor"}}

	entries, err := f.transformLogsForFractal(context.Background(), action, &Alert{Name: "test"}, "test", rows)
	if err != nil {
		t.Fatalf("transform failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(entries[0].RawLog), &raw); err != nil {
		t.Fatalf("raw log is not valid JSON: %v", err)
	}
	for _, key := range []string{"user", "computer_name", "event_id"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("raw log is missing hydrated field %q: %v", key, raw)
		}
	}
	if raw["source_log_id"] != "src-1" {
		t.Errorf("source_log_id = %v, want src-1", raw["source_log_id"])
	}
	if entries[0].Fields["user"] != "zane" {
		t.Errorf("indexed user = %q, want zane", entries[0].Fields["user"])
	}
	// A mapping over a hydrated field resolves now that mappings read merged data.
	if entries[0].Fields["actor"] != "zane" {
		t.Errorf("mapped actor = %q, want zane", entries[0].Fields["actor"])
	}
	if entries[0].Timestamp.IsZero() {
		t.Error("PreserveTimestamp did not carry the row timestamp")
	}
}

// A forwarded detection has to carry the rule's own attack.* tags, or mitre()
// sees Bifract-native detections as untagged noise while it maps every external
// EDR's detections fine.
func TestTransformLogsForFractalCarriesAlertLabels(t *testing.T) {
	f := &FractalActionClient{}
	rows := []map[string]interface{}{{
		"timestamp": "2026-08-03 10:00:00.000",
		"log_id":    "src-1",
	}}
	alert := &Alert{
		ID:       "a1",
		Name:     "Encoded PowerShell",
		Severity: "high",
		Labels:   []string{"attack.t1059.001", "attack.execution", "product:windows"},
	}

	entries, err := f.transformLogsForFractal(context.Background(),
		FractalAction{TargetFractalID: "f2", AddAlertContext: true}, alert, alert.Name, rows)
	if err != nil {
		t.Fatalf("transform failed: %v", err)
	}

	got := entries[0].Fields["alert_labels"]
	want := `["attack.t1059.001","attack.execution","product:windows"]`
	if got != want {
		t.Errorf("alert_labels = %q, want %q", got, want)
	}
	if entries[0].Fields["alert_severity"] != "high" {
		t.Errorf("alert_severity = %q, want high", entries[0].Fields["alert_severity"])
	}

	// Without alert context the forwarded log stays exactly as it was.
	plain, err := f.transformLogsForFractal(context.Background(),
		FractalAction{TargetFractalID: "f2"}, alert, alert.Name, rows)
	if err != nil {
		t.Fatalf("transform failed: %v", err)
	}
	if _, ok := plain[0].Fields["alert_labels"]; ok {
		t.Error("alert_labels leaked into a forward with alert context disabled")
	}
}

// Nested values must reach the target fractal as JSON. Go's default formatting
// renders them as map[k:v] text that no consumer can parse.
func TestFieldStringSerializesCollectionsAsJSON(t *testing.T) {
	cases := map[string]struct {
		in   interface{}
		want string
	}{
		"events":     {[]map[string]interface{}{{"step": 1, "log_id": "a"}}, `[{"log_id":"a","step":1}]`},
		"uint slice": {[]uint64{10, 20}, `[10,20]`},
		"strings":    {[]string{"a", "b"}, `["a","b"]`},
		"map":        {map[string]interface{}{"k": "v"}, `{"k":"v"}`},
	}
	for name, tc := range cases {
		if got := fieldString(tc.in); got != tc.want {
			t.Errorf("%s: got %s, want %s", name, got, tc.want)
		}
	}
	// Scalars keep their existing rendering.
	if got := fieldString(int64(7)); got != "7" {
		t.Errorf("int64: got %s, want 7", got)
	}
	if got := fieldString("plain"); got != "plain" {
		t.Errorf("string: got %s, want plain", got)
	}
}
