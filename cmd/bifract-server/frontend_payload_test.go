package main

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"bifract/pkg/api"
)

// fetchRe finds the literal prefix of an /api/v1 path the frontend fetches.
var fetchRe = regexp.MustCompile(`(?:safeFetch|fetch)\(\s*["'` + "`" + `](/api/v1/[a-zA-Z0-9_/\-]*)`)

// readRe finds a field read off the response envelope: data.data.name,
// res.data?.items, and so on. The holder is matched by shape rather than by a
// list of names, so a reader that calls it something new is still checked.
var readRe = regexp.MustCompile(`\b[A-Za-z_$][A-Za-z0-9_$]*\s*\.\s*data\s*\??\.\s*([a-zA-Z_][a-zA-Z0-9_]*)`)

// arrayMembers are legitimate on a list payload, so reading one is not a claim
// that the payload is an object.
var arrayMembers = map[string]bool{
	"forEach": true, "map": true, "filter": true, "find": true, "length": true,
	"slice": true, "some": true, "every": true, "sort": true, "reduce": true,
	"includes": true, "indexOf": true, "join": true, "flatMap": true, "at": true,
}

// methodRe finds the verb in a fetch's options object, which sits a line or two
// after the path. Absent means GET.
var methodRe = regexp.MustCompile(`method:\s*["'` + "`" + `](GET|POST|PUT|DELETE|PATCH)`)

// readWindow is how many lines after a fetch a field read still plausibly
// belongs to it.
const readWindow = 14

// TestFrontendReadsFieldsTheResponseCarries checks every field the frontend
// pulls off an API response against the payload the route declares.
//
// TestFrontendAPIPathsHaveRoutes covers the path; nothing covered the shape, and
// that is the gap the typed-payload refactor fell through: dropping a wrapper key
// turned data.data.normalizers into undefined, which is not an error, so the
// ingest token form silently offered no normalizer at all. Eight other reads
// broke the same way and stayed broken.
//
// A fetch whose URL is built by a helper rather than written inline cannot be
// resolved to a route, so it is not checked.
func TestFrontendReadsFieldsTheResponseCarries(t *testing.T) {
	_, registry := buildRouter(testDeps())

	for _, file := range frontendFiles(t) {
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		lines := strings.Split(string(source), "\n")

		for i, line := range lines {
			match := fetchRe.FindStringSubmatch(line)
			if match == nil {
				continue
			}
			method := methodAt(lines, i)
			candidates := payloadsFor(registry, method, match[1])
			if len(candidates) == 0 {
				continue
			}

			for j := i; j < min(i+readWindow, len(lines)); j++ {
				for _, read := range readRe.FindAllStringSubmatch(lines[j], -1) {
					name := read[1]
					if accepted(candidates, name) {
						continue
					}
					t.Errorf("%s:%d reads .%s off %s %s, which does not carry it",
						filepath.Base(file), j+1, name, method, match[1])
				}
			}
		}
	}
}

type payloadKind int

const (
	payloadUnknown payloadKind = iota
	payloadList
	payloadObject
)

// methodAt reads the verb of the fetch starting at line i. A create posting to
// the same path as a list answers one object, not the list, so the verb decides
// which route's payload applies.
func methodAt(lines []string, i int) string {
	for j := i; j < min(i+4, len(lines)); j++ {
		if m := methodRe.FindStringSubmatch(lines[j]); m != nil {
			return m[1]
		}
	}
	return "GET"
}

type payload struct {
	fields map[string]bool
	kind   payloadKind
}

// accepted reports whether any route the path could reach carries the field. A
// read is only wrong when no candidate would answer it, which keeps a path built
// at runtime checkable without having to resolve exactly which route it hits.
func accepted(candidates []payload, name string) bool {
	for _, c := range candidates {
		if c.kind == payloadList && arrayMembers[name] {
			return true
		}
		if c.kind == payloadObject && c.fields[name] {
			return true
		}
	}
	return false
}

// payloadsFor resolves a path to the payloads it could answer with: the exact
// route if the frontend wrote the path in full, otherwise every route the
// partial path prefixes.
func payloadsFor(registry *api.Registry, method, path string) []payload {
	if route, ok := registry.Lookup(method, path); ok {
		if p, ok := payloadOf(route); ok {
			return []payload{p}
		}
		return nil
	}

	var candidates []payload
	for _, route := range registry.Routes() {
		if route.Method != method || !strings.HasPrefix(route.Path, path) {
			continue
		}
		p, ok := payloadOf(route)
		if !ok {
			// One unknowable candidate makes the whole set unknowable.
			return nil
		}
		candidates = append(candidates, p)
	}
	return candidates
}

func payloadOf(route api.Route) (payload, bool) {
	if route.Response == nil || route.Produces != "" {
		return payload{}, false
	}
	envelope := reflect.TypeOf(route.Response)
	if envelope.Kind() != reflect.Struct {
		return payload{}, false
	}
	data, found := envelope.FieldByName("Data")
	if !found {
		return payload{}, false
	}
	fields, kind := fieldsOf(data.Type)
	if kind == payloadUnknown {
		return payload{}, false
	}
	return payload{fields: fields, kind: kind}, true
}

func fieldsOf(t reflect.Type) (map[string]bool, payloadKind) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		return nil, payloadList
	case reflect.Interface, reflect.Map:
		// any or map[string]any promises nothing, so there is nothing to check.
		return nil, payloadUnknown
	case reflect.Struct:
		fields := map[string]bool{}
		collectJSONFields(t, fields)
		return fields, payloadObject
	}
	return nil, payloadUnknown
}

func collectJSONFields(t reflect.Type, into map[string]bool) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if field.Anonymous && name == "" {
			embedded := field.Type
			for embedded.Kind() == reflect.Ptr {
				embedded = embedded.Elem()
			}
			if embedded.Kind() == reflect.Struct {
				collectJSONFields(embedded, into)
			}
			continue
		}
		if name == "" {
			name = field.Name
		}
		into[name] = true
	}
}
