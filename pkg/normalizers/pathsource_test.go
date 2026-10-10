package normalizers

import (
	"reflect"
	"strings"
	"testing"
)

var leafSnakeLower = []Transform{TransformFlattenLeaf, TransformSnakeCase, TransformLowercase}

// velociraptorEvent mirrors the shape Velociraptor ships: System.Execution.ProcessID
// is the event log service and collides with EventData.ProcessId after snake_case.
func velociraptorEvent(withEventDataPID bool) map[string]interface{} {
	eventData := map[string]interface{}{"Image": "a.exe"}
	if withEventDataPID {
		eventData["ProcessId"] = float64(6964)
	}
	return map[string]interface{}{
		"System": map[string]interface{}{
			"EventID":   map[string]interface{}{"Value": float64(1)},
			"Execution": map[string]interface{}{"ProcessID": float64(1152), "ThreadID": float64(7036)},
		},
		"EventData": eventData,
	}
}

func apply(n *Normalizer, obj map[string]interface{}) map[string]string {
	built := BuildFieldsWithNested(obj)
	return n.Compile().ApplyTransformsWithNested(built.Fields, built.NestedKeys)
}

func TestPathSource_NoPathsMatchesPlainNormalizer(t *testing.T) {
	plain := &Normalizer{Transforms: leafSnakeLower, FieldMappings: []FieldMapping{{Sources: []string{"image"}, Target: "img"}}}
	if plain.Compile().paths != nil {
		t.Fatal("a normalizer without path sources must not build a path table")
	}
	got := apply(plain, velociraptorEvent(true))
	if got["img"] != "a.exe" || got["system_execution_process_id"] != "1152" || got["event_data_process_id"] != "6964" {
		t.Fatalf("plain normalizer output changed: %v", got)
	}
}

func TestPathSource_ClaimsLeafBeforeCollision(t *testing.T) {
	n := &Normalizer{Transforms: leafSnakeLower, FieldMappings: []FieldMapping{
		{Sources: []string{"$.System.Execution.ProcessID"}, Target: "execution_process_id"},
	}}
	for i := 0; i < 50; i++ {
		got := apply(n, velociraptorEvent(true))
		if got["execution_process_id"] != "1152" || got["process_id"] != "6964" {
			t.Fatalf("expected the claimed leaf out of the way of EventData.ProcessId, got %v", got)
		}
	}
	got := apply(n, velociraptorEvent(false))
	if _, ok := got["process_id"]; ok {
		t.Fatalf("an event without EventData.ProcessId must have no process_id, got %v", got)
	}
}

func TestPathSource_TargetIsReservedAndExemptFromRenames(t *testing.T) {
	n := &Normalizer{Transforms: leafSnakeLower, FieldMappings: []FieldMapping{
		{Sources: []string{"$.System.Execution.ProcessID"}, Target: "ProcessId"},
	}}
	got := apply(n, velociraptorEvent(true))
	if got["ProcessId"] != "1152" {
		t.Fatalf("target must be used verbatim, without rename transforms: %v", got)
	}
	// A leaf whose renamed name equals a target falls back to its full path.
	n.FieldMappings[0].Target = "process_id"
	got = apply(n, velociraptorEvent(true))
	if got["process_id"] != "1152" || got["event_data_process_id"] != "6964" {
		t.Fatalf("a leaf named like a path target must fall back to its full path: %v", got)
	}
}

func TestPathSource_WinsOverLeafMappingToSameTarget(t *testing.T) {
	n := &Normalizer{Transforms: leafSnakeLower, FieldMappings: []FieldMapping{
		{Sources: []string{"image"}, Target: "x"},
		{Sources: []string{"$.System.Execution.ThreadID"}, Target: "x"},
	}}
	if got := apply(n, velociraptorEvent(true)); got["x"] != "7036" {
		t.Fatalf("path source must take precedence over a leaf mapping: %v", got)
	}
}

func TestPathSource_SharedTargetLowestRankWins(t *testing.T) {
	n := &Normalizer{Transforms: leafSnakeLower, FieldMappings: []FieldMapping{
		{Sources: []string{"$.System.Execution.ThreadID", "$.System.Execution.ProcessID"}, Target: "pid"},
	}}
	for i := 0; i < 50; i++ {
		got := apply(n, velociraptorEvent(true))
		// The losing ProcessID is named as if unmatched, so it collides with
		// EventData.ProcessId and both take their full paths.
		if got["pid"] != "7036" || got["system_execution_process_id"] != "1152" || got["event_data_process_id"] != "6964" {
			t.Fatalf("first listed path must win and the other must keep its value: %v", got)
		}
	}
}

func TestPathSource_FlattenFull(t *testing.T) {
	n := &Normalizer{Transforms: []Transform{TransformFlattenFull, TransformSnakeCase}, FieldMappings: []FieldMapping{
		{Sources: []string{"$.EventData.Image"}, Target: "image"},
	}}
	got := apply(n, velociraptorEvent(true))
	if got["image"] != "a.exe" {
		t.Fatalf("path source must work under flatten_full: %v", got)
	}
	if _, ok := got["event_data_image"]; ok {
		t.Fatalf("claimed leaf must not also appear under its full path: %v", got)
	}
}

func TestPathSource_ArrayIndex(t *testing.T) {
	obj := map[string]interface{}{"kprobe": map[string]interface{}{"args": []interface{}{
		map[string]interface{}{"string_arg": "a"},
		map[string]interface{}{"string_arg": "b"},
	}}}
	n := &Normalizer{Transforms: leafSnakeLower, FieldMappings: []FieldMapping{
		{Sources: []string{"$.kprobe.args.1.string_arg"}, Target: "second_arg"},
	}}
	got := apply(n, obj)
	if got["second_arg"] != "b" || got["string_arg"] != "a" {
		t.Fatalf("numeric segment must select the array element: %v", got)
	}
}

func TestPathSource_NoFlattenTopLevel(t *testing.T) {
	n := &Normalizer{Transforms: []Transform{TransformLowercase}, FieldMappings: []FieldMapping{
		{Sources: []string{"$.EventID"}, Target: "event_id"},
	}}
	got := apply(n, map[string]interface{}{"EventID": float64(4), "Other": "x"})
	if got["event_id"] != "4" || got["other"] != "x" {
		t.Fatalf("top-level path must match before renames: %v", got)
	}
	if _, ok := got["eventid"]; ok {
		t.Fatalf("claimed key must not also be renamed: %v", got)
	}
}

func TestPathSource_ValueMappingSeesTarget(t *testing.T) {
	n := &Normalizer{Transforms: leafSnakeLower,
		FieldMappings: []FieldMapping{{Sources: []string{"$.System.EventID.Value"}, Target: "event_id"}},
		ValueMappings: []ValueMapping{{FromField: "event_id", ToField: "category", Map: map[string]string{"1": "process_creation"}}},
	}
	if got := apply(n, velociraptorEvent(true)); got["category"] != "process_creation" {
		t.Fatalf("value mapping must see the path target: %v", got)
	}
}

func TestPathSource_TraceMatchesIngest(t *testing.T) {
	n := &Normalizer{Transforms: leafSnakeLower, FieldMappings: []FieldMapping{
		{Sources: []string{"$.System.Execution.ProcessID"}, Target: "execution_process_id"},
		{Sources: []string{"image"}, Target: "img"},
	}}
	obj := velociraptorEvent(true)
	want := apply(n, obj)
	res := n.Trace(obj)
	got := map[string]string{}
	var claimed *TracedField
	for i, f := range res.Fields {
		got[f.Name] = f.Value
		if f.Name == "execution_process_id" {
			claimed = &res.Fields[i]
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("trace disagrees with ingest:\ntrace  %v\ningest %v", got, want)
	}
	if claimed == nil || claimed.MappingIndex != 0 || claimed.MatchedAlias != "$.System.Execution.ProcessID" {
		t.Fatalf("path field must be attributed to its mapping: %+v", claimed)
	}
}

func TestPathSource_IgnoredBySigmaFieldNames(t *testing.T) {
	c := (&Normalizer{Transforms: leafSnakeLower, FieldMappings: []FieldMapping{
		{Sources: []string{"$.System.Execution.ProcessID", "pid"}, Target: "process_id"},
	}}).Compile()
	if _, ok := c.FieldMappingMap["$.System.Execution.ProcessID"]; ok {
		t.Fatal("path sources must not enter the leaf mapping map")
	}
	if got := c.ApplyFieldName("ProcessID"); got != "process_id" {
		t.Fatalf("Sigma field translation must be unchanged, got %q", got)
	}
}

func TestValidateFieldMappings(t *testing.T) {
	cases := []struct {
		transforms []Transform
		sources    []string
		wantErr    string
	}{
		{leafSnakeLower, []string{"$.a.b"}, ""},
		{nil, []string{"$.a"}, ""},
		{leafSnakeLower, []string{"$."}, "empty segment"},
		{leafSnakeLower, []string{"$.a..b"}, "empty segment"},
		{leafSnakeLower, []string{"$.a."}, "empty segment"},
		{nil, []string{"$.a.b"}, "needs a flatten transform"},
		{[]Transform{TransformSnakeCase, TransformFlattenLeaf}, []string{"$.a"}, "before any rename"},
		{nil, []string{"plain.dotted.name"}, ""},
	}
	for _, c := range cases {
		err := ValidateFieldMappings(c.transforms, []FieldMapping{{Sources: c.sources, Target: "t"}})
		if c.wantErr == "" && err != nil {
			t.Errorf("%v %v: unexpected error %v", c.transforms, c.sources, err)
		}
		if c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)) {
			t.Errorf("%v %v: want error containing %q, got %v", c.transforms, c.sources, c.wantErr, err)
		}
	}
	err := ValidateFieldMappings(leafSnakeLower, []FieldMapping{
		{Sources: []string{"$.a"}, Target: "x"},
		{Sources: []string{"$.a"}, Target: "y"},
	})
	if err == nil || !strings.Contains(err.Error(), "maps to both") {
		t.Errorf("one path claimed by two targets must be rejected, got %v", err)
	}
}
