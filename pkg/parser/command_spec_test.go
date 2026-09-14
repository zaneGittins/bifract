package parser

import (
	"sort"
	"strings"
	"testing"
)

// Every command a user can type must declare its arguments, so validation,
// completion and the built-in reference all read one source of truth instead of
// each re-deriving it. The list is taken from the registries rather than
// hardcoded, so a new command fails this until it is described.
func TestEveryCommandHasArgumentSchema(t *testing.T) {
	var missing []string
	for _, name := range RegisteredCommandNames() {
		if _, ok := CommandSpecFor(name); !ok {
			missing = append(missing, name)
		}
	}
	for _, name := range SourceCommandNames() {
		if _, ok := CommandSpecFor(name); !ok {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("%d command(s) without an argument schema: %s",
			len(missing), strings.Join(missing, ", "))
	}
}

// The reverse guard: a schema for a name nothing dispatches is documentation of
// a command that does not exist.
func TestNoSchemaWithoutCommand(t *testing.T) {
	dispatchable := map[string]bool{}
	for _, n := range RegisteredCommandNames() {
		dispatchable[strings.ToLower(n)] = true
	}
	for _, n := range SourceCommandNames() {
		dispatchable[strings.ToLower(n)] = true
	}

	var orphans []string
	for _, n := range CommandSpecNames() {
		if !dispatchable[n] {
			orphans = append(orphans, n)
		}
	}
	sort.Strings(orphans)
	if len(orphans) > 0 {
		t.Errorf("schema declared for %d name(s) no command registers: %s",
			len(orphans), strings.Join(orphans, ", "))
	}
}

// A spec must be internally coherent, or the binder's guarantees are hollow.
func TestCommandSpecsAreWellFormed(t *testing.T) {
	seen := map[*CommandSpec]bool{}
	for _, name := range CommandSpecNames() {
		spec, _ := CommandSpecFor(name)
		if seen[spec] {
			continue
		}
		seen[spec] = true

		if spec.Name == "" {
			t.Errorf("%s: spec has no name", name)
		}
		names := map[string]bool{}
		for i, p := range spec.Params {
			if p.Name == "" {
				t.Errorf("%s: parameter %d has no name", spec.Name, i)
			}
			if names[strings.ToLower(p.Name)] {
				t.Errorf("%s: duplicate parameter %s", spec.Name, p.Name)
			}
			names[strings.ToLower(p.Name)] = true
			if p.Variadic && !p.Positional {
				t.Errorf("%s: %s is variadic but not positional", spec.Name, p.Name)
			}
		}
		positional := spec.positionalParams()
		for i, p := range positional {
			if p.Variadic && i != len(positional)-1 {
				t.Errorf("%s: variadic %s is not the last positional parameter", spec.Name, p.Name)
			}
		}
	}
}

// Validation must accept what the docs and the corpus already use, and reject
// what a handler would otherwise ignore.
func TestValidateCommandArgs(t *testing.T) {
	accept := []string{
		`* | groupby(user)`,
		`* | groupby(image, user)`,
		`* | groupby(user, function=count())`,
		`* | groupby(computer_name, function=count(field=user, unique=true))`,
		`* | groupby(src_ip, dst_ip, image, computer_name, limit=100)`,
		`* | table(timestamp, image, user)`,
		`* | table([timestamp,user,image])`,
		`* | table(timestamp, image, user, limit=5)`,
		`* | sort(bytes, order=desc)`,
		`* | limit(100)`,
		`* | head(50)`,
		`* | dedup(src_ip, dst_ip)`,
	}
	for _, q := range accept {
		pipeline, err := ParseQuery(q)
		if err != nil {
			t.Errorf("parse %q: %v", q, err)
			continue
		}
		for _, cmd := range pipeline.Commands {
			if err := ValidateCommandArgs(cmd); err != nil {
				t.Errorf("%q rejected: %v", q, err)
			}
		}
	}

	reject := []struct{ query, want string }{
		{`* | groupby(user, nope=1)`, "unknown parameter nope"},
		{`* | sort(bytes, direction=desc)`, "unknown parameter direction"},
		{`* | dedup(a, bogus=1)`, "unknown parameter bogus"},
	}
	for _, c := range reject {
		pipeline, err := ParseQuery(c.query)
		if err != nil {
			t.Errorf("parse %q: %v", c.query, err)
			continue
		}
		var got error
		for _, cmd := range pipeline.Commands {
			if err := ValidateCommandArgs(cmd); err != nil {
				got = err
			}
		}
		if got == nil {
			t.Errorf("%q: expected a rejection, got none", c.query)
			continue
		}
		if !strings.Contains(got.Error(), c.want) {
			t.Errorf("%q\n  want %q, got: %v", c.query, c.want, got)
		}
	}
}

// Every other named list in BQL is written with brackets. comment(tags=a,b)
// works only because the named argument ends at the comma and the handler folds
// the trailing bare arguments back in, so both spellings must agree.
func TestCommentTagSpellingsAgree(t *testing.T) {
	bracket, _, okBracket := ExtractCommentParams(mustParseSpec(t, `* | comment(tags=[security,critical])`))
	bare, _, okBare := ExtractCommentParams(mustParseSpec(t, `* | comment(tags=security,critical)`))
	if !okBracket || !okBare {
		t.Fatalf("comment() not detected: bracket=%v bare=%v", okBracket, okBare)
	}
	if len(bracket) != 2 || len(bare) != 2 {
		t.Fatalf("expected two tags each, got bracket=%q bare=%q", bracket, bare)
	}
	for i := range bracket {
		if bracket[i] != bare[i] {
			t.Errorf("spellings disagree: bracket=%q bare=%q", bracket, bare)
			break
		}
	}
}

func mustParseSpec(t *testing.T, query string) *PipelineNode {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	return pipeline
}
