package main

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"

	"bifract/pkg/alerts"
	"bifract/pkg/api"
	"bifract/pkg/models"
	"bifract/pkg/normalizers"
	"bifract/pkg/rbac"
	"bifract/pkg/schemafields"
)

// enumTypes are the named types that describe their own values to the API
// description, paired with the file their constants live in.
var enumTypes = []struct {
	name   string
	values api.Enumerator
	source string
}{
	{"alerts.AlertType", alerts.AlertType(""), "pkg/alerts/alerttype.go"},
	{"models.ModelType", models.ModelType(""), "pkg/models/models.go"},
	{"rbac.Role", rbac.Role(""), "pkg/rbac/rbac.go"},
	{"normalizers.Transform", normalizers.Transform(""), "pkg/normalizers/models.go"},
	{"schemafields.IndexType", schemafields.IndexType(""), "pkg/schemafields/models.go"},
}

// TestEnumValuesCoverEveryConstant keeps the description honest. Adding a
// constant without adding it to EnumValues would silently leave it out of the
// API description and the explorer, which is the drift this whole registry
// exists to prevent.
func TestEnumValuesCoverEveryConstant(t *testing.T) {
	for _, e := range enumTypes {
		t.Run(e.name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", "..", e.source))
			if err != nil {
				t.Fatalf("reading %s: %v", e.source, err)
			}

			// Constants of the form:  NameOfConst  TypeName = "value"
			shortType := e.name[len(e.name)-len(e.name):]
			if i := lastDot(e.name); i >= 0 {
				shortType = e.name[i+1:]
			}
			re := regexp.MustCompile(`(?m)^\s*\w+\s+` + shortType + `\s*=\s*"([^"]*)"`)

			var declared []string
			for _, m := range re.FindAllStringSubmatch(string(data), -1) {
				if m[1] != "" { // the zero value is absence, not a choice
					declared = append(declared, m[1])
				}
			}
			if len(declared) == 0 {
				t.Fatalf("found no constants of %s in %s; the pattern this test relies on changed", shortType, e.source)
			}

			listed := map[string]bool{}
			for _, v := range e.values.EnumValues() {
				listed[v] = true
			}
			var missing []string
			for _, v := range declared {
				if !listed[v] {
					missing = append(missing, v)
				}
			}
			if len(missing) > 0 {
				sort.Strings(missing)
				t.Errorf("%s declares constants its EnumValues() omits: %v\n"+
					"They would not appear in the API description.", e.name, missing)
			}
		})
	}
}

func lastDot(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '.' {
			return i
		}
	}
	return -1
}
