package alerts

import (
	"context"
	"errors"
	"testing"

	"bifract/pkg/dictionaries"
)

type stubResolver struct {
	mappings        map[string]map[string]string
	caseInsensitive map[string]bool
	network         map[string]bool
	err             error
	calls           int
	gotScope        [2]string
}

func (s *stubResolver) ListDictionaryMappings(_ context.Context, fractalID, prismID string) (dictionaries.Scope, error) {
	s.calls++
	s.gotScope = [2]string{fractalID, prismID}
	return dictionaries.Scope{
		Mappings:        s.mappings,
		CaseInsensitive: s.caseInsensitive,
		Network:         s.network,
	}, s.err
}

// A rule under test must see the dictionaries of the scope it belongs to, not the
// per-case scratch fractal, which exists only inside the scratch table.
func TestDictionariesForScope(t *testing.T) {
	want := map[string]map[string]string{"sensitive_groups": {"group_name": "lookup_abc"}}
	stub := &stubResolver{mappings: want}
	r := &TestRunner{dicts: stub}

	got := r.dictionariesFor(context.Background(), "", "prism-1").Mappings
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
	if got := (&TestRunner{}).dictionariesFor(context.Background(), "f1", "").Mappings; got != nil {
		t.Fatalf("no resolver must yield nil, got %v", got)
	}
	stub := &stubResolver{mappings: map[string]map[string]string{"x": nil}}
	if got := (&TestRunner{dicts: stub}).dictionariesFor(context.Background(), "", "").Mappings; got != nil {
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
	if got := r.dictionariesFor(context.Background(), "f1", "").Mappings; got != nil {
		t.Fatalf("got %v", got)
	}
}

// The rule tester resolves case-insensitivity alongside the mappings: a rule tested
// against a case-insensitive dictionary must probe it the same way the live alert does.
func TestDictionariesForCarriesCaseInsensitivity(t *testing.T) {
	stub := &stubResolver{
		mappings:        map[string]map[string]string{"iocs": {"indicator": "lookup_abc"}},
		caseInsensitive: map[string]bool{"iocs": true},
	}
	r := &TestRunner{dicts: stub}
	if ci := r.dictionariesFor(context.Background(), "f1", "").CaseInsensitive; !ci["iocs"] {
		t.Fatalf("case-insensitive flag lost, got %v", ci)
	}
}

// A network list is probed with an address, not a string, so the kind has to reach
// the translator the same way case-insensitivity does.
func TestDictionariesForCarriesNetworkKind(t *testing.T) {
	stub := &stubResolver{
		mappings: map[string]map[string]string{"corp": {"network": "lookup_net"}},
		network:  map[string]bool{"corp": true},
	}
	r := &TestRunner{dicts: stub}
	if net := r.dictionariesFor(context.Background(), "f1", "").Network; !net["corp"] {
		t.Fatalf("network kind lost, got %v", net)
	}
}
