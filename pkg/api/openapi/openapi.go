package openapi

import (
	"reflect"
	"regexp"
	"sort"
	"strings"

	"bifract/pkg/api"
)

// OpenAPI document types. Only the subset Bifract actually emits is modelled;
// anything absent is absent from the output too, which keeps the generated file
// readable and its diffs meaningful.
type Document struct {
	OpenAPI    string               `json:"openapi"`
	Info       Info                 `json:"info"`
	Paths      map[string]*PathItem `json:"paths"`
	Components Components           `json:"components"`
}

type Info struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

type Components struct {
	Schemas map[string]*Schema `json:"schemas,omitempty"`
}

type PathItem struct {
	Get    *Operation `json:"get,omitempty"`
	Post   *Operation `json:"post,omitempty"`
	Put    *Operation `json:"put,omitempty"`
	Patch  *Operation `json:"patch,omitempty"`
	Delete *Operation `json:"delete,omitempty"`
}

type Operation struct {
	OperationID string               `json:"operationId"`
	Summary     string               `json:"summary,omitempty"`
	Description string               `json:"description,omitempty"`
	Tags        []string             `json:"tags,omitempty"`
	Parameters  []*Parameter         `json:"parameters,omitempty"`
	RequestBody *RequestBody         `json:"requestBody,omitempty"`
	Responses   map[string]*Response `json:"responses"`
}

type Parameter struct {
	Name        string  `json:"name"`
	In          string  `json:"in"`
	Required    bool    `json:"required,omitempty"`
	Description string  `json:"description,omitempty"`
	Schema      *Schema `json:"schema,omitempty"`
}

type RequestBody struct {
	Required bool                  `json:"required,omitempty"`
	Content  map[string]*MediaType `json:"content"`
}

type Response struct {
	Description string                `json:"description"`
	Content     map[string]*MediaType `json:"content,omitempty"`
}

type MediaType struct {
	Schema *Schema `json:"schema,omitempty"`
}

var pathParamRe = regexp.MustCompile(`\{([^}]+)\}`)

// accessDescription explains, in the document, who may call an operation. The
// access level is the authorization contract, so it belongs in the spec rather
// than only in the code.
var accessDescription = map[api.Access]string{
	api.AccessPublic:        "No authentication required.",
	api.AccessIngestToken:   "Authenticated by an ingest token, not a session.",
	api.AccessInternal:      "Reachable only from the private network.",
	api.AccessAuthenticated: "Any authenticated principal.",
	api.AccessViewer:        "Requires the viewer role on the fractal or prism in scope.",
	api.AccessAnalyst:       "Requires the analyst role on the fractal or prism in scope.",
	api.AccessFractalAdmin:  "Requires the admin role on the fractal or prism in scope.",
	api.AccessTenantAdmin:   "Requires instance-wide administration.",
}

// OpenAPI renders the registry as an OpenAPI 3.1 document. It reads the routes
// the server actually mounts, so the document cannot describe an API the binary
// does not serve.
func Generate(reg *api.Registry, version string) *Document {
	gen := newSchemaGen()
	doc := &Document{
		OpenAPI: "3.1.0",
		Info: Info{
			Title:   "Bifract API",
			Version: version,
			Description: "Every operation Bifract serves. Generated from the running " +
				"router, so it describes exactly what this build mounts.",
		},
		Paths: map[string]*PathItem{},
	}

	routes := reg.Routes()
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Path != routes[j].Path {
			return routes[i].Path < routes[j].Path
		}
		return routes[i].Method < routes[j].Method
	})

	for _, route := range routes {
		if route.Path == "/*" {
			continue // the SPA file server is not an API operation
		}
		item := doc.Paths[route.Path]
		if item == nil {
			item = &PathItem{}
			doc.Paths[route.Path] = item
		}
		op := gen.operation(route)
		switch strings.ToUpper(route.Method) {
		case "GET":
			item.Get = op
		case "POST":
			item.Post = op
		case "PUT":
			item.Put = op
		case "PATCH":
			item.Patch = op
		case "DELETE":
			item.Delete = op
		}
	}

	doc.Components.Schemas = gen.Components()
	return doc
}

func (g *schemaGen) operation(route api.Route) *Operation {
	op := &Operation{
		OperationID: operationID(route),
		Summary:     route.Summary,
		Description: accessDescription[route.Access],
		Tags:        []string{tagFor(route.Path)},
		Responses:   map[string]*Response{},
	}

	for _, m := range pathParamRe.FindAllStringSubmatch(route.Path, -1) {
		op.Parameters = append(op.Parameters, &Parameter{
			Name:     m[1],
			In:       "path",
			Required: true,
			Schema:   &Schema{Type: "string"},
		})
	}

	if route.Request != nil {
		op.RequestBody = &RequestBody{
			Required: true,
			Content:  map[string]*MediaType{"application/json": {Schema: g.envelopeAware(route.Request)}},
		}
	}

	switch {
	case route.Produces != "":
		op.Responses["200"] = &Response{
			Description: mediaDescription(route.Produces),
			Content:     map[string]*MediaType{route.Produces: {}},
		}
	case route.Response != nil:
		op.Responses["200"] = &Response{
			Description: "Success.",
			Content:     map[string]*MediaType{"application/json": {Schema: g.envelopeAware(route.Response)}},
		}
	default:
		op.Responses["200"] = &Response{Description: "Success."}
	}

	// Every authenticated operation can answer these, and a client has to
	// handle them, so they belong on every operation rather than in prose.
	if route.Access != api.AccessPublic {
		op.Responses["401"] = &Response{
			Description: "Not authenticated.",
			Content:     map[string]*MediaType{"application/json": {Schema: g.schemaFor(reflect.TypeOf(api.Response[any]{}))}},
		}
		op.Responses["403"] = &Response{
			Description: "Authenticated, but not permitted.",
			Content:     map[string]*MediaType{"application/json": {Schema: g.schemaFor(reflect.TypeOf(api.Response[any]{}))}},
		}
	}
	return op
}

// envelopeAware keeps the generic envelopes readable: rather than a component
// per instantiation (Response_alerts.Alert and so on) it inlines the envelope
// and refers to the payload type by name.
func (g *schemaGen) envelopeAware(v any) *Schema {
	t := reflect.TypeOf(v)
	if t == nil {
		return &Schema{}
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || t.PkgPath() != "bifract/pkg/api" {
		return g.schemaFor(t)
	}
	name := t.Name()
	if !strings.HasPrefix(name, "Response[") && !strings.HasPrefix(name, "ListResponse[") {
		return g.schemaFor(t)
	}
	return g.structBody(t)
}

// mediaDescription says what a non-JSON body is, since a media type alone does
// not tell a reader that the response never ends.
func mediaDescription(media string) string {
	switch media {
	case "text/event-stream":
		return "An open server-sent event stream; the response does not complete."
	case "application/x-ndjson":
		return "Newline-delimited JSON, streamed as rows arrive."
	case "text/csv":
		return "A CSV file download."
	case "text/yaml":
		return "A YAML document download."
	}
	return "Success."
}

// operationID is stable across regenerations so a generated client's method
// names do not churn.
func operationID(route api.Route) string {
	parts := []string{strings.ToLower(route.Method)}
	for _, seg := range strings.Split(strings.Trim(route.Path, "/"), "/") {
		if seg == "" {
			continue
		}
		if strings.HasPrefix(seg, "{") {
			seg = "by" + strings.Title(strings.Trim(seg, "{}"))
		}
		parts = append(parts, seg)
	}
	id := strings.Join(parts, "_")
	return strings.NewReplacer("-", "_", ".", "_").Replace(id)
}

// tagFor groups operations by the resource they act on, which is what a reader
// navigates by.
func tagFor(path string) string {
	segs := strings.Split(strings.Trim(strings.TrimPrefix(path, "/api/v1"), "/"), "/")
	if len(segs) == 0 || segs[0] == "" {
		return "root"
	}
	return segs[0]
}
