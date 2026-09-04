package alerts

import (
	"context"
	"errors"
	"testing"
)

type stubResolver struct {
	mappings map[string]map[string]string
	err      error
	calls    int
	gotScope [2]string
}

func (s *stubResolver) ListDictionaryMappings(_ context.Context, fractalID, prismID string) (map[string]map[string]string, error) {
	s.calls++
	s.gotScope = [2]string{fractalID, prismID}
	return s.mappings, s.err
}

// A rule under test must see the dictionaries of the scope it belongs to, not the
// per-case scratch fractal, which exists only inside the scratch table.
func TestDictionariesForScope(t *testing.T) {
	want := map[string]map[string]string{"sensitive_groups": {"group_name": "lookup_abc"}}
	stub := &stubResolver{mappings: want}
	r := &TestRunner{dicts: stub}

	got := r.dictionariesFor(context.Background(), "", "prism-1")
	if len(got) != 1 || got["sensitive_groups"]["group_name"] != "lookup_abc" {
		t.Fatalf("got %v", got)
	}
	if stub.gotScope != [2]string{"", "prism-1"} {
		t.Fatalf("resolver saw scope %v", stub.gotScope)
	}
}

// No resolver wired, or no scope: match() reports the dictionary as unavailable
// instead of the run failing.
func TestDictionariesWithoutResolverOrScope(t *testing.T) {
	if got := (&TestRunner{}).dictionariesFor(context.Background(), "f1", ""); got != nil {
		t.Fatalf("no resolver must yield nil, got %v", got)
	}
	stub := &stubResolver{mappings: map[string]map[string]string{"x": nil}}
	if got := (&TestRunner{dicts: stub}).dictionariesFor(context.Background(), "", ""); got != nil {
		t.Fatalf("no scope must yield nil, got %v", got)
	}
	if stub.calls != 0 {
		t.Fatal("an unscoped run must not query for mappings")
	}
}

// A lookup failure degrades to an unresolvable match(), which names the dictionary,
// rather than erroring the whole run and hiding every other test's outcome.
func TestDictionaryResolveFailureIsNotFatal(t *testing.T) {
	r := &TestRunner{dicts: &stubResolver{err: errors.New("postgres down")}}
	if got := r.dictionariesFor(context.Background(), "f1", ""); got != nil {
		t.Fatalf("got %v", got)
	}
}
