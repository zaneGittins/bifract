package normalizers

import (
	"testing"
)

// helper: compile a normalizer with a single value mapping and given transforms.
func compileWithValueMap(transforms []Transform, mappings []FieldMapping, vm ValueMapping) *CompiledNormalizer {
	n := &Normalizer{
		Transforms:    transforms,
		FieldMappings: mappings,
		ValueMappings: []ValueMapping{vm},
	}
	return n.Compile()
}

// (a) matched value -> derived field written, source retained.
func TestValueMapping_MatchWritesDerivedKeepsSource(t *testing.T) {
	c := compileWithValueMap(nil, nil, ValueMapping{
		FromField: "event_id",
		ToField:   "category",
		Map:       map[string]string{"1": "process_creation", "3": "network_connect"},
	})
	out := c.ApplyTransforms(map[string]string{"event_id": "1", "other": "x"})

	if got := out["category"]; got != "process_creation" {
		t.Errorf("category = %q, want process_creation", got)
	}
	if got := out["event_id"]; got != "1" {
		t.Errorf("event_id = %q, want 1 (source must be retained)", got)
	}
	if got := out["other"]; got != "x" {
		t.Errorf("other = %q, want x (untouched)", got)
	}
}

// (b) unmatched value + Default set -> fallback written.
func TestValueMapping_UnmatchedUsesDefault(t *testing.T) {
	c := compileWithValueMap(nil, nil, ValueMapping{
		FromField: "event_id",
		ToField:   "category",
		Map:       map[string]string{"1": "process_creation"},
		Default:   "other",
	})
	out := c.ApplyTransforms(map[string]string{"event_id": "999"})

	if got := out["category"]; got != "other" {
		t.Errorf("category = %q, want other (default)", got)
	}
}

// (c) unmatched value, no default -> ToField absent.
func TestValueMapping_UnmatchedNoDefaultAbsent(t *testing.T) {
	c := compileWithValueMap(nil, nil, ValueMapping{
		FromField: "event_id",
		ToField:   "category",
		Map:       map[string]string{"1": "process_creation"},
	})
	out := c.ApplyTransforms(map[string]string{"event_id": "999"})

	if _, ok := out["category"]; ok {
		t.Errorf("category should be absent for unmatched value with no default, got %q", out["category"])
	}
}

// source field missing entirely -> no derived field, no panic.
func TestValueMapping_MissingSourceNoop(t *testing.T) {
	c := compileWithValueMap(nil, nil, ValueMapping{
		FromField: "event_id",
		ToField:   "category",
		Map:       map[string]string{"1": "process_creation"},
		Default:   "other",
	})
	out := c.ApplyTransforms(map[string]string{"unrelated": "x"})

	if _, ok := out["category"]; ok {
		t.Errorf("category should be absent when source field missing, got %q", out["category"])
	}
}

// (d) ordering: the value map reads the field AFTER field mappings run, so it must
// key on the renamed target, not the raw source. Raw "EventID" is renamed to
// "event_id" by the field mapping; the value map keyed on "event_id" then derives.
func TestValueMapping_ReadsPostMappingKey(t *testing.T) {
	c := compileWithValueMap(
		nil,
		[]FieldMapping{{Sources: []string{"EventID"}, Target: "event_id"}},
		ValueMapping{
			FromField: "event_id",
			ToField:   "category",
			Map:       map[string]string{"11": "file_write"},
		},
	)
	out := c.ApplyTransforms(map[string]string{"EventID": "11"})

	if got := out["category"]; got != "file_write" {
		t.Errorf("category = %q, want file_write (value map must read post-mapping key)", got)
	}
	// Keying on the pre-mapping name must NOT work, proving ordering.
	c2 := compileWithValueMap(
		nil,
		[]FieldMapping{{Sources: []string{"EventID"}, Target: "event_id"}},
		ValueMapping{
			FromField: "EventID",
			ToField:   "category",
			Map:       map[string]string{"11": "file_write"},
		},
	)
	out2 := c2.ApplyTransforms(map[string]string{"EventID": "11"})
	if _, ok := out2["category"]; ok {
		t.Errorf("category should be absent when keyed on pre-mapping name, got %q", out2["category"])
	}
}

// Compile drops value mappings missing a source or target field.
func TestValueMapping_CompileSkipsIncomplete(t *testing.T) {
	n := &Normalizer{
		ValueMappings: []ValueMapping{
			{FromField: "", ToField: "category", Map: map[string]string{"1": "a"}},
			{FromField: "event_id", ToField: "", Map: map[string]string{"1": "a"}},
			{FromField: "event_id", ToField: "category", Map: map[string]string{"1": "a"}},
		},
	}
	c := n.Compile()
	if len(c.ValueMappings) != 1 {
		t.Fatalf("compiled value mappings = %d, want 1", len(c.ValueMappings))
	}
	if c.ValueMappings[0].FromField != "event_id" || c.ValueMappings[0].ToField != "category" {
		t.Errorf("unexpected compiled mapping: %+v", c.ValueMappings[0])
	}
}

// Compile trims whitespace around from/to so runtime lookups match real keys.
func TestValueMapping_CompileTrimsFields(t *testing.T) {
	c := compileWithValueMap(nil, nil, ValueMapping{
		FromField: " event_id ",
		ToField:   " category ",
		Map:       map[string]string{"1": "process_creation"},
	})
	if len(c.ValueMappings) != 1 {
		t.Fatalf("compiled value mappings = %d, want 1", len(c.ValueMappings))
	}
	out := c.ApplyTransforms(map[string]string{"event_id": "1"})
	if got := out["category"]; got != "process_creation" {
		t.Errorf("category = %q, want process_creation (from/to must be trimmed)", got)
	}
}
