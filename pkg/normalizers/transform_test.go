package normalizers

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage/storagetest"
)

// ApplyFieldName is how Sigma field names reach stored names, so it must apply
// the same per-name transforms, in the same order, as ingestion, and skip the
// structural flatten steps that only make sense on a whole event.
func TestApplyFieldName(t *testing.T) {
	cases := []struct {
		name       string
		transforms []Transform
		mappings   []FieldMapping
		in, want   string
	}{
		{"no transforms", nil, nil, "CommandLine", "CommandLine"},
		{"flatten is skipped", []Transform{TransformFlattenLeaf}, nil, "Parent.Image", "Parent.Image"},
		{"snake then lower", []Transform{TransformFlattenLeaf, TransformSnakeCase, TransformLowercase}, nil, "ParentCommandLine", "parent_command_line"},
		{"dedot", []Transform{TransformDedot}, nil, "process.parent.name", "process_parent_name"},
		{"upper", []Transform{TransformUppercase}, nil, "Image", "IMAGE"},
		{"camel from snake", []Transform{TransformCamelCase}, nil, "parent_image", "parentImage"},
		{"pascal from camel", []Transform{TransformPascalCase}, nil, "parentImage", "ParentImage"},
		// Mappings match the post-transform name, not the Sigma spelling.
		{"mapping after transforms", []Transform{TransformSnakeCase}, []FieldMapping{{Sources: []string{"image"}, Target: "process_path"}}, "Image", "process_path"},
		{"mapping on raw name misses after rename", []Transform{TransformSnakeCase}, []FieldMapping{{Sources: []string{"Image"}, Target: "process_path"}}, "Image", "image"},
		// A transform this build does not know (an older or newer row) is a no-op,
		// never a panic or an empty name.
		{"unknown transform passes through", []Transform{"title_case"}, nil, "Image", "Image"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := (&Normalizer{Transforms: tc.transforms, FieldMappings: tc.mappings}).Compile()
			if got := c.ApplyFieldName(tc.in); got != tc.want {
				t.Fatalf("ApplyFieldName(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestCaseConversions(t *testing.T) {
	camel := map[string]string{
		"":              "",
		"QueryName":     "queryName",
		"query_name":    "queryName",
		"QUERY_NAME":    "queryName",
		"a__b":          "aB", // empty segments are dropped
		"x":             "x",
		"Événement_id":  "événementId",
		"already_Camel": "alreadyCamel",
	}
	for in, want := range camel {
		if got := toCamelCase(in); got != want {
			t.Errorf("toCamelCase(%q) = %q, want %q", in, got, want)
		}
	}
	pascal := map[string]string{
		"":           "",
		"queryName":  "QueryName",
		"query_name": "QueryName",
		"a__b":       "AB",
		"x":          "X",
	}
	for in, want := range pascal {
		if got := toPascalCase(in); got != want {
			t.Errorf("toPascalCase(%q) = %q, want %q", in, got, want)
		}
	}
}

// Value maps compare the stored string form of a value. Numbers are stored as
// plain decimals, so a map keyed on "5000000" matches a JSON 5000000 (which the
// decoder hands over as float64 5e+06); a key in exponential form does not.
func TestValueMapping_NumericSourceUsesPlainDecimalForm(t *testing.T) {
	n := &Normalizer{ValueMappings: []ValueMapping{
		{FromField: "bytes", ToField: "size_class", Map: map[string]string{"5000000": "large", "5e+06": "exponential"}},
		{FromField: "ratio", ToField: "ratio_class", Map: map[string]string{"0.5": "half"}},
		{FromField: "ok", ToField: "status", Map: map[string]string{"true": "success"}},
	}}
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(`{"bytes":5000000,"ratio":0.5,"ok":true}`), &obj); err != nil {
		t.Fatal(err)
	}
	got := apply(n, obj)
	if got["bytes"] != "5000000" || got["size_class"] != "large" {
		t.Errorf("bytes=%q size_class=%q", got["bytes"], got["size_class"])
	}
	if got["ratio_class"] != "half" || got["status"] != "success" {
		t.Errorf("ratio_class=%q status=%q", got["ratio_class"], got["status"])
	}
}

// Value mappings run in order on the progressively derived map: a later mapping
// can read a field an earlier one derived.
func TestValueMapping_ChainsInOrder(t *testing.T) {
	n := &Normalizer{ValueMappings: []ValueMapping{
		{FromField: "event_id", ToField: "category", Map: map[string]string{"1": "process_creation"}},
		{FromField: "category", ToField: "family", Map: map[string]string{"process_creation": "process"}},
	}}
	got := n.Compile().ApplyTransforms(map[string]string{"event_id": "1"})
	if got["family"] != "process" {
		t.Fatalf("second mapping must see the first one's output, got %v", got)
	}
	// Reversed order: the second mapping runs before its source exists.
	n.ValueMappings[0], n.ValueMappings[1] = n.ValueMappings[1], n.ValueMappings[0]
	got = n.Compile().ApplyTransforms(map[string]string{"event_id": "1"})
	if _, ok := got["family"]; ok {
		t.Fatalf("a mapping must not see fields derived after it, got %v", got)
	}
}

func TestFlatten_DepthLimitMarksTruncation(t *testing.T) {
	// MaxFlattenDepth+5 levels of nesting with a leaf at the bottom and one near the top.
	inner := map[string]interface{}{"deep": "bottom"}
	for i := 0; i < MaxFlattenDepth+5; i++ {
		inner = map[string]interface{}{"l": inner}
	}
	inner["shallow"] = "top"
	obj := map[string]interface{}{"root": inner}

	got := apply(&Normalizer{Transforms: []Transform{TransformFlattenLeaf}}, obj)
	if got["_bifract_truncated"] != "true" || got["_bifract_truncation_reason"] != "max_depth" {
		t.Fatalf("expected a max_depth truncation marker, got %v", got)
	}
	if got["shallow"] != "top" {
		t.Errorf("leaves above the limit must survive, got %v", got)
	}
	for _, v := range got {
		if v == "bottom" {
			t.Errorf("a leaf past the depth limit was emitted: %v", got)
		}
	}
}

func TestFlatten_FieldLimitInsideNestedObject(t *testing.T) {
	big := map[string]interface{}{}
	for i := 0; i < MaxFlattenFields+50; i++ {
		big["k"+strconv.Itoa(i)] = float64(i)
	}
	got := apply(&Normalizer{Transforms: []Transform{TransformFlattenFull}}, map[string]interface{}{"data": big})
	if got["_bifract_truncated"] != "true" || got["_bifract_truncation_reason"] != "max_fields" {
		t.Fatalf("expected a max_fields marker, got reason %q", got["_bifract_truncation_reason"])
	}
	if len(got) != MaxFlattenFields+2 {
		t.Fatalf("expected %d leaves plus 2 markers, got %d fields", MaxFlattenFields, len(got))
	}
}

// A path source can name a top-level scalar under a flatten transform; it is
// claimed before renames, so snake_case does not touch its target.
func TestPathSource_TopLevelScalarUnderFlatten(t *testing.T) {
	n := &Normalizer{
		Transforms:    []Transform{TransformFlattenLeaf, TransformSnakeCase},
		FieldMappings: []FieldMapping{{Sources: []string{"$.Computer"}, Target: "HostName"}},
	}
	got := apply(n, map[string]interface{}{"Computer": "WS01", "UserName": "bob"})
	want := map[string]string{"HostName": "WS01", "user_name": "bob"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// Without a flatten, two single-segment paths sharing a target: the first listed
// wins and the other field keeps its own name rather than vanishing.
func TestPathSource_TopLevelSharedTargetDemotesLoser(t *testing.T) {
	n := &Normalizer{FieldMappings: []FieldMapping{
		{Sources: []string{"$.hostname"}, Target: "host"},
		{Sources: []string{"$.Computer"}, Target: "host"},
	}}
	for i := 0; i < 20; i++ {
		got := apply(n, map[string]interface{}{"Computer": "from-computer", "hostname": "from-hostname"})
		want := map[string]string{"host": "from-hostname", "Computer": "from-computer"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// Contested mapping targets pick the same winner whatever other fields are present.
func TestFieldMappingWinnerIgnoresUnrelatedFields(t *testing.T) {
	c := (&Normalizer{FieldMappings: []FieldMapping{{Sources: []string{"host", "computer"}, Target: "computer_name"}}}).Compile()
	for i := 0; i < 20; i++ {
		got := c.ApplyTransforms(map[string]string{"computer": "c", "host": "h", "other": "o", "zz": "z"})
		want := map[string]string{"computer_name": "h", "other": "o", "zz": "z"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v (first-listed source wins)", got, want)
		}
	}
}

func TestTraceSkipsTargetlessMappingAndUnchangedDerivation(t *testing.T) {
	n := &Normalizer{
		FieldMappings: []FieldMapping{{Sources: []string{"Image"}}}, // no target: ignored
		ValueMappings: []ValueMapping{
			// Writes the value the field already holds: not a derivation worth badging.
			{FromField: "level", ToField: "severity", Map: map[string]string{"4": "info"}},
		},
	}
	tr := n.Trace(map[string]interface{}{"Image": "a.exe", "level": "4", "severity": "info"})
	for _, f := range tr.Fields {
		switch f.Name {
		case "Image":
			if f.MappingIndex != -1 {
				t.Errorf("a mapping without a target must not claim a field: %+v", f)
			}
		case "severity":
			if f.Derived || f.Override {
				t.Errorf("an unchanged value must not be badged as derived: %+v", f)
			}
		case "":
			t.Errorf("empty field name in trace: %+v", tr.Fields)
		}
	}
}

func TestManagerScanErrorsSurface(t *testing.T) {
	m, fake := newManager()
	fake.On("FROM normalizers ORDER BY", fakedb.Row("only", "three", "cols"))
	if _, err := m.List(t.Context()); err == nil || !strings.Contains(err.Error(), "scan normalizer") {
		t.Fatalf("List with a short row: %v", err)
	}

	m, fake = newManager()
	fake.On("FROM ingest_tokens", fakedb.Row("t-1"))
	if _, err := m.GetTokenUsage(t.Context(), "n-1"); err == nil || !strings.Contains(err.Error(), "scan token usage") {
		t.Fatalf("GetTokenUsage with a short row: %v", err)
	}
}

func TestHandleCreateWithoutUserRecordsNoCreator(t *testing.T) {
	h, fake := newTestHandler(nil)
	fake.On("INSERT INTO normalizers", fakedb.Row(sysmonRow()...))
	r := request("POST", "/normalizers", `{"name":"x"}`, "")
	r = r.WithContext(t.Context()) // drop the user
	if code, _ := serve(t, h.HandleCreate, r); code != http.StatusOK {
		t.Fatalf("code = %d", code)
	}
	if got := fake.Matching("INSERT")[0].Args[6]; got != "" {
		t.Fatalf("created_by = %v, want empty", got)
	}
}

func TestHandleSamplesAllUnusable(t *testing.T) {
	ch, fake := storagetest.ClickHouse()
	h, _ := newTestHandler(ch)
	fake.On("FROM logs_raw", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "raw_log", Type: "String"}, {Column: "ts", Type: "DateTime"}},
		Rows: [][]any{{"not json", time.Unix(1, 0)}, {"[1,2]", time.Unix(2, 0)}},
	})
	code, env := serve(t, h.HandleSamples, request("GET", "/?fractal_id=f-1", "", ""))
	if code != http.StatusOK || string(env.Data) != "[]" {
		t.Fatalf("no usable sample must give []: %d %s", code, env.Data)
	}

	// A timestamp column that does not scan into time.Time fails the request rather
	// than returning samples with zero timestamps.
	ch, fake = storagetest.ClickHouse()
	h, _ = newTestHandler(ch)
	fake.On("FROM logs_raw", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "raw_log", Type: "String"}, {Column: "ts", Type: "String"}},
		Rows: [][]any{{`{"a":1}`, "2026-01-01"}},
	})
	if code, _ := serve(t, h.HandleSamples, request("GET", "/?fractal_id=f-1", "", "")); code != http.StatusInternalServerError {
		t.Fatalf("scan failure: code = %d", code)
	}
}
