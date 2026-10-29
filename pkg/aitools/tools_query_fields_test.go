package aitools

import (
	"context"
	"reflect"
	"testing"
)

// get_fields reports the fields this scope's logs actually carry, most common
// first, with the share of rows carrying each.
func TestGetFieldsListsObservedFields(t *testing.T) {
	c := serve(t, 200, `{"success":true,"rows":200,"fields":[{"name":"src_ip","rows":150},{"name":"process_guid","rows":20},{"name":"dst_ip","rows":2}]}`)
	got, err := getFields(context.Background(), c, getFieldsArgs{Filter: "IP"})
	if err != nil {
		t.Fatal(err)
	}
	if c.last.Path != "/query/fields/observed" {
		t.Errorf("called %s", c.last.Path)
	}
	want := map[string]any{
		"count": 2,
		"rows":  float64(200),
		"fields": []map[string]any{
			{"name": "src_ip", "coverage": 0.75},
			{"name": "dst_ip", "coverage": 0.01},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v\nwant %#v", got, want)
	}
}

// Before anything has been counted (a new install, an empty fractal), the
// configured fields are still worth knowing, and are labelled as such.
func TestGetFieldsFallsBackToConfiguredFields(t *testing.T) {
	c := serve(t, 200, "")
	c.byPath = map[string]string{
		"/query/fields/observed": `{"success":true,"rows":0,"fields":[]}`,
		"/query/fields":          `{"success":true,"data":["src_ip","user"]}`,
	}
	got, err := getFields(context.Background(), c, getFieldsArgs{})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := got.(map[string]any)
	if m["count"] != 2 || !reflect.DeepEqual(m["fields"], []any{"src_ip", "user"}) || m["note"] == nil {
		t.Errorf("got %#v", got)
	}
}

// The filter applies to the fallback list too.
func TestGetFieldsFallbackHonoursTheFilter(t *testing.T) {
	c := serve(t, 200, "")
	c.byPath = map[string]string{
		"/query/fields/observed": `{"success":true,"rows":0,"fields":[]}`,
		"/query/fields":          `{"success":true,"data":["src_ip","user","dst_ip"]}`,
	}
	got, err := getFields(context.Background(), c, getFieldsArgs{Filter: "ip"})
	if err != nil {
		t.Fatal(err)
	}
	if m, _ := got.(map[string]any); !reflect.DeepEqual(m["fields"], []any{"src_ip", "dst_ip"}) {
		t.Errorf("got %#v", got)
	}
}
