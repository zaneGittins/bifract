package openapi

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

type probeInner struct {
	ID   string    `json:"id"`
	When time.Time `json:"when,omitempty"`
}

type probeOuter struct {
	Name    string            `json:"name"`
	Inner   *probeInner       `json:"inner,omitempty"`
	Items   []probeInner      `json:"items"`
	Tags    map[string]string `json:"tags,omitempty"`
	Any     interface{}       `json:"any,omitempty"`
	Skipped string            `json:"-"`
	hidden  string
}

type probeSelf struct {
	Child *probeSelf `json:"child,omitempty"`
}

func TestSchemaShapes(t *testing.T) {
	g := newSchemaGen()
	s := g.schemaFor(reflect.TypeOf(probeOuter{}))
	if s.Ref != "#/components/schemas/openapi.probeOuter" {
		t.Fatalf("ref = %q", s.Ref)
	}
	body := g.Components()["openapi.probeOuter"]
	b, _ := json.Marshal(body)
	t.Logf("probeOuter: %s", b)
	if body.Properties["items"].Type != "array" {
		t.Error("items should be an array")
	}
	if body.Properties["tags"].AdditionalProperties.Type != "string" {
		t.Error("tags should be a string map")
	}
	if _, ok := body.Properties["hidden"]; ok {
		t.Error("unexported field leaked")
	}
	if _, ok := body.Properties["-"]; ok {
		t.Error("json:\"-\" field leaked")
	}
	req := body.Required
	if len(req) != 2 {
		t.Errorf("required = %v, want name and items", req)
	}
	inner := g.Components()["openapi.probeInner"]
	if inner.Properties["when"].Format != "date-time" {
		t.Error("time.Time should be date-time")
	}
}

func TestSchemaSelfReference(t *testing.T) {
	g := newSchemaGen()
	done := make(chan struct{})
	go func() { g.schemaFor(reflect.TypeOf(probeSelf{})); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("self-referencing type did not terminate")
	}
}
