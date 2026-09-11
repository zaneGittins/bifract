package query

import (
	"sort"
	"strings"
	"testing"

	"bifract/pkg/parser"
	"bifract/pkg/tlshresolve"
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

// syntaxFunctions are function-shaped syntax the parser handles inline rather than
// through the command registry. They are documented as functions because that is how
// they are typed, and because completion and the ? hint read only the function list.
var syntaxFunctions = map[string]bool{
	"field": true, // right-hand operand of a comparison: src_port = field(dst_port)
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
			if !registered[lower] && !statsOnlyFunctions[lower] && !syntaxFunctions[lower] {
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

// The two skip reasons must be worded differently: telling someone "no TLSH index"
// about a fractal whose model is merely filtered sends them looking for something
// that is already there.
func TestTLSHSkipWarningsDistinguishReasons(t *testing.T) {
	ws := tlshSkipWarnings("tlsh", []tlshresolve.SkippedFractal{
		{FractalID: "f-bare"},
		{FractalID: "f-filtered", FilteredModel: "partial_idx"},
	})
	if len(ws) != 2 {
		t.Fatalf("expected one warning per reason, got %d: %v", len(ws), ws)
	}
	joined := strings.Join(ws, "\n")
	for _, want := range []string{"f-bare", "no TLSH index", "f-filtered", "partial_idx", "definition filter"} {
		if !strings.Contains(joined, want) {
			t.Errorf("warnings should mention %q:\n%s", want, joined)
		}
	}
	// A fractal with a filtered model must not be described as having no index.
	for _, w := range ws {
		if strings.Contains(w, "f-filtered") && strings.Contains(w, "no TLSH index") {
			t.Errorf("filtered fractal wrongly reported as unindexed: %s", w)
		}
	}
	if len(tlshSkipWarnings("tlsh", nil)) != 0 {
		t.Error("no skips should produce no warnings")
	}
}

// A description may embed a runnable snippet after "Example: ". Those are copied
// into the query bar like any other, but TestReferenceExamplesParse only walks the
// Examples slice, so a broken one shipped in both the docs and the in-product help.
func TestReferenceDescriptionExamplesParse(t *testing.T) {
	for _, fn := range bqlFunctionDocs {
		for _, seg := range strings.Split(fn.Description, "Example: ")[1:] {
			// The snippet runs to the end of the sentence.
			snippet := strings.TrimSpace(strings.SplitN(seg, ". ", 2)[0])
			snippet = strings.TrimSuffix(snippet, ".")
			if snippet == "" {
				continue
			}
			// A leading pipe makes it a fragment; give it a source to hang off.
			q := snippet
			if strings.HasPrefix(q, "|") {
				q = "*" + " " + q
			}
			if _, err := parser.ParseQuery(q); err != nil {
				t.Errorf("%s: embedded example does not parse: %q\n  %v", fn.Name, snippet, err)
			}
		}
	}
}
