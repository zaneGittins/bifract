package openapi

import (
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"bifract/pkg/api"
)

// Schema is the subset of JSON Schema the generated document uses.
type Schema struct {
	Ref                  string             `json:"$ref,omitempty"`
	Type                 string             `json:"type,omitempty"`
	Format               string             `json:"format,omitempty"`
	Description          string             `json:"description,omitempty"`
	Properties           map[string]*Schema `json:"properties,omitempty"`
	Required             []string           `json:"required,omitempty"`
	Items                *Schema            `json:"items,omitempty"`
	AdditionalProperties *Schema            `json:"additionalProperties,omitempty"`
	Nullable             bool               `json:"nullable,omitempty"`
	Enum                 []string           `json:"enum,omitempty"`
}

var (
	timeType    = reflect.TypeOf(time.Time{})
	rawJSONType = reflect.TypeOf(json.RawMessage{})

	enumeratorType = reflect.TypeOf((*api.Enumerator)(nil)).Elem()
)

// schemaGen turns Go types into JSON Schema, naming every named struct as a
// reusable component so a payload that appears on many routes is described once.
type schemaGen struct {
	components map[string]*Schema
	building   map[string]bool
}

func newSchemaGen() *schemaGen {
	return &schemaGen{components: map[string]*Schema{}, building: map[string]bool{}}
}

// componentName is the name a named type is published under. Package-qualified,
// because Alert, Handler and Response all exist in more than one package.
func componentName(t reflect.Type) string {
	if t.Name() == "" {
		return ""
	}
	pkg := t.PkgPath()
	if i := strings.LastIndex(pkg, "/"); i >= 0 {
		pkg = pkg[i+1:]
	}
	name := t.Name()
	// Generic instantiations carry their argument in the name; the envelope
	// types are handled separately, so anything reaching here is sanitised.
	name = strings.NewReplacer("[", "_", "]", "", " ", "", "*", "", "/", ".").Replace(name)
	if pkg == "" {
		return name
	}
	return pkg + "." + name
}

// schemaFor returns a schema for t, registering a component and returning a $ref
// when t is a named struct.
func (g *schemaGen) schemaFor(t reflect.Type) *Schema {
	if t == nil {
		return &Schema{}
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t {
	case timeType:
		return &Schema{Type: "string", Format: "date-time"}
	case rawJSONType:
		return &Schema{Description: "Arbitrary JSON."}
	}

	// A type that knows its own values describes them, so a caller does not have
	// to guess what a field like alert severity accepts.
	if t.Implements(enumeratorType) {
		values := reflect.Zero(t).Interface().(api.Enumerator).EnumValues()
		if len(values) > 0 {
			return &Schema{Type: "string", Enum: values}
		}
	}

	switch t.Kind() {
	case reflect.Bool:
		return &Schema{Type: "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &Schema{Type: "integer"}
	case reflect.Float32, reflect.Float64:
		return &Schema{Type: "number"}
	case reflect.String:
		return &Schema{Type: "string"}
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 && t.Name() != "" {
			return &Schema{Type: "string", Format: "byte"}
		}
		return &Schema{Type: "array", Items: g.schemaFor(t.Elem())}
	case reflect.Map:
		return &Schema{Type: "object", AdditionalProperties: g.schemaFor(t.Elem())}
	case reflect.Interface:
		return &Schema{Description: "Any JSON value."}
	case reflect.Struct:
		return g.structSchema(t)
	}
	return &Schema{}
}

func (g *schemaGen) structSchema(t reflect.Type) *Schema {
	name := componentName(t)
	if name == "" {
		return g.structBody(t)
	}
	// A self-referencing type would recurse forever; the $ref closes the loop.
	if _, done := g.components[name]; !done && !g.building[name] {
		g.building[name] = true
		g.components[name] = g.structBody(t)
		delete(g.building, name)
	}
	return &Schema{Ref: "#/components/schemas/" + name}
}

func (g *schemaGen) structBody(t reflect.Type) *Schema {
	out := &Schema{Type: "object", Properties: map[string]*Schema{}}
	g.addFields(out, t)
	if len(out.Properties) == 0 {
		out.Properties = nil
	}
	return out
}

func (g *schemaGen) addFields(out *Schema, t reflect.Type) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, opts, _ := strings.Cut(tag, ",")
		// An embedded struct with no name of its own contributes its fields.
		if f.Anonymous && name == "" {
			ft := f.Type
			for ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				g.addFields(out, ft)
				continue
			}
		}
		if name == "" {
			name = f.Name
		}
		out.Properties[name] = g.schemaFor(f.Type)
		if !strings.Contains(opts, "omitempty") {
			out.Required = append(out.Required, name)
		}
	}
}

// Components returns every named schema the generator produced.
func (g *schemaGen) Components() map[string]*Schema { return g.components }
