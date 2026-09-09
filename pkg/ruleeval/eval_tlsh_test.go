package ruleeval

import (
	"context"
	"strings"
	"testing"

	"bifract/pkg/parser"
)

// A rule using tlsh() must not fail inside the translator with the opaque
// "requires server-side pre-processing". A test runner with no dictionary access
// cannot evaluate it, and should say that rather than leaking an internal contract.
func TestEvaluateExplainsWhyTLSHCannotRun(t *testing.T) {
	pipeline, err := parser.ParseQuery(`* | tlsh(field=tlsh, dict="known_bad")`)
	if err != nil {
		t.Fatal(err)
	}
	s := &Scratch{table: "scratch_x"} // no resolver wired

	var opts parser.QueryOptions
	err = s.resolveTLSH(context.Background(), pipeline, &opts, Unit{FractalID: "f1"})
	if err == nil {
		t.Fatal("expected an error explaining that tlsh() cannot be evaluated here")
	}
	if strings.Contains(err.Error(), "server-side pre-processing") {
		t.Errorf("error leaks the translator's internal contract instead of explaining the runner: %v", err)
	}
	for _, want := range []string{"tlsh()", "dictionaries"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q, got: %v", want, err)
		}
	}
}

// A rule with no tlsh() must be left completely alone.
func TestResolveTLSHIsNoOpWithoutTheCommand(t *testing.T) {
	pipeline, err := parser.ParseQuery(`event_id=1 | groupby(image)`)
	if err != nil {
		t.Fatal(err)
	}
	s := &Scratch{table: "scratch_x"}
	var opts parser.QueryOptions
	if err := s.resolveTLSH(context.Background(), pipeline, &opts, Unit{FractalID: "f1"}); err != nil {
		t.Fatalf("unexpected error for a rule without tlsh(): %v", err)
	}
	if opts.HasTLSHFilter {
		t.Error("HasTLSHFilter set for a rule that does not use tlsh()")
	}
}

// A malformed tlsh() must surface its own argument error, not the missing-resolver one.
func TestResolveTLSHReportsArgumentErrorsFirst(t *testing.T) {
	pipeline, err := parser.ParseQuery(`* | tlsh(field=tlsh)`)
	if err != nil {
		t.Fatal(err)
	}
	s := &Scratch{table: "scratch_x"}
	var opts parser.QueryOptions
	err = s.resolveTLSH(context.Background(), pipeline, &opts, Unit{FractalID: "f1"})
	if err == nil || !strings.Contains(err.Error(), "hash=") {
		t.Fatalf("expected the argument error, got: %v", err)
	}
}

// tlsh(hash="...") compares against literals and needs no dictionary, so a runner
// without dictionary access must still be able to evaluate it. Only dict= depends
// on the reader, and that reports its own absence.
func TestResolveTLSHHashOnlyNeedsNoDictionary(t *testing.T) {
	pipeline, err := parser.ParseQuery(`* | tlsh(field=tlsh, hash="` + strings.Repeat("a", 70) + `")`)
	if err != nil {
		t.Fatal(err)
	}
	p, found, err := parser.ExtractTLSHParams(pipeline)
	if err != nil || !found {
		t.Fatalf("extract: found=%v err=%v", found, err)
	}
	if p.Dict != "" {
		t.Fatalf("expected a literal-needle rule, got dict=%q", p.Dict)
	}
	if len(p.Hashes) != 1 {
		t.Errorf("expected one literal needle, got %d", len(p.Hashes))
	}
}
