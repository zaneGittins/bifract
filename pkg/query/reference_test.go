package query

import (
	"sort"
	"strings"
	"testing"

	"bifract/pkg/parser"
)

// referenceNames returns every name the built-in reference advertises, primary
// names and declared aliases alike, lowercased. Command lookup is case-insensitive,
// so the comparison is too.
func referenceNames() map[string]bool {
	names := make(map[string]bool, len(bqlFunctionDocs)*2)
	for _, fn := range bqlFunctionDocs {
		names[strings.ToLower(fn.Name)] = true
		for _, a := range fn.Aliases {
			names[strings.ToLower(a)] = true
		}
	}
	return names
}

// registeredCommands is every name the parser will dispatch: ordinary pipeline
// commands plus source commands, which live in their own registry because they
// generate the pipeline's source rather than transforming it.
func registeredCommands() map[string]bool {
	out := make(map[string]bool)
	for _, n := range parser.RegisteredCommandNames() {
		out[strings.ToLower(n)] = true
	}
	for _, n := range parser.SourceCommandNames() {
		out[strings.ToLower(n)] = true
	}
	return out
}

// statsOnlyFunctions are documented as functions but dispatched inside the stats /
// groupby(function=...) evaluator rather than the command registry, so they are
// legitimately absent from both command registries.
var statsOnlyFunctions = map[string]bool{
	"collect": true,
}

// Every command the parser accepts must appear in the built-in reference, because
// that reference is how the command is discovered: it drives the query UI's
// documentation panel and is the description an agent reads.
//
// This exists because tlsh() shipped without an entry and nothing noticed. The
// reference used to be a literal inside HandleReference, unreachable from a test.
func TestEveryRegisteredCommandIsDocumented(t *testing.T) {
	documented := referenceNames()

	var missing []string
	for name := range registeredCommands() {
		if !documented[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("commands registered in pkg/parser but absent from the built-in reference: %s\n"+
			"Add a FunctionDoc to bqlFunctionDocs, or list the name in an existing entry's Aliases.",
			strings.Join(missing, ", "))
	}
}

// The reverse guard: an entry naming a command the parser does not accept is a
// documented function nobody can run, which is worse than an undocumented one.
func TestReferenceDocumentsNoUnknownCommands(t *testing.T) {
	registered := registeredCommands()

	var unknown []string
	for _, fn := range bqlFunctionDocs {
		for _, n := range append([]string{fn.Name}, fn.Aliases...) {
			lower := strings.ToLower(n)
			if !registered[lower] && !statsOnlyFunctions[lower] {
				unknown = append(unknown, n)
			}
		}
	}
	sort.Strings(unknown)
	if len(unknown) > 0 {
		t.Errorf("the built-in reference documents commands the parser does not register: %s",
			strings.Join(unknown, ", "))
	}
}

// Each entry needs the fields the UI renders; a blank one shows as an empty panel.
func TestReferenceEntriesAreComplete(t *testing.T) {
	seen := make(map[string]bool)
	for _, fn := range bqlFunctionDocs {
		if fn.Name == "" {
			t.Error("a reference entry has no name")
			continue
		}
		key := strings.ToLower(fn.Name)
		if seen[key] {
			t.Errorf("%s: duplicate reference entry", fn.Name)
		}
		seen[key] = true

		if strings.TrimSpace(fn.Category) == "" {
			t.Errorf("%s: missing category, so it lands in no section of the reference", fn.Name)
		}
		if strings.TrimSpace(fn.Description) == "" {
			t.Errorf("%s: missing description", fn.Name)
		}
		if strings.TrimSpace(fn.Syntax) == "" {
			t.Errorf("%s: missing syntax", fn.Name)
		}
		if len(fn.Examples) == 0 {
			t.Errorf("%s: no examples", fn.Name)
		}
	}
}

// Examples are copied straight into the query bar, so a broken one is a broken
// promise. Parsing is the check: translation needs per-command server-side context
// (resolved models, dictionaries, comment ids) that a unit test has no way to supply.
func TestReferenceExamplesParse(t *testing.T) {
	for _, fn := range bqlFunctionDocs {
		for _, ex := range fn.Examples {
			ex = strings.TrimSpace(ex)
			if ex == "" || strings.HasPrefix(ex, "|") {
				// A bare fragment is a syntax illustration, not a runnable query.
				continue
			}
			if _, err := parser.ParseQuery(ex); err != nil {
				t.Errorf("%s: example does not parse: %q\n  %v", fn.Name, ex, err)
			}
		}
	}
}
