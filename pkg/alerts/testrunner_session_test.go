package alerts

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"bifract/pkg/dictionaries"
	"bifract/pkg/storage"
)

func runnerCorpus() []AlertTest {
	return []AlertTest{
		{Name: "fires", Expectation: "match", Events: []map[string]interface{}{{"process": "certutil.exe"}}},
		{Name: "quiet", Expectation: "no_match", Events: []map[string]interface{}{{"process": "notepad.exe"}}},
	}
}

func TestRunnerAvailability(t *testing.T) {
	var none *TestRunner
	if none.Available() {
		t.Error("a nil runner cannot run tests")
	}
	if (&TestRunner{}).Available() {
		t.Error("a runner without ClickHouse cannot run tests")
	}
	if _, err := (&TestRunner{}).Run(context.Background(), "s", `a=1`, runnerCorpus(), "f1", ""); err == nil {
		t.Error("Run must refuse without a connection rather than fail obscurely")
	}
	// Release and dictionary wiring are safe to call on a runner that was never built.
	none.Release(context.Background(), "s")
	none.SetDictionaryResolver(&stubResolver{})
}

// A corpus that has not changed reuses its loaded scratch table: iterating on a query
// must not reload the events every run.
func TestSessionReusesAnUnchangedCorpus(t *testing.T) {
	tests := runnerCorpus()
	hash, err := corpusHash(tests)
	if err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-time.Hour)
	loaded := &testSession{corpus: hash, lastUsed: stale}
	r := &TestRunner{ch: new(storage.ClickHouseClient), sessions: map[string]*testSession{"alice:editor": loaded}}

	// Renaming a test changes nothing that was inserted.
	tests[0].Name = "renamed"
	got, err := r.session(context.Background(), "alice:editor", tests)
	if err != nil {
		t.Fatal(err)
	}
	if got != loaded {
		t.Fatal("an unchanged corpus must reuse the loaded session")
	}
	if !got.lastUsed.After(stale) {
		t.Error("reuse must keep the session from being swept as idle")
	}
}

// Sessions are bounded: a new editor is turned away rather than creating scratch
// tables without limit. An editor that already holds one is not.
func TestSessionCapacityIsBounded(t *testing.T) {
	tests := runnerCorpus()
	hash, _ := corpusHash(tests)
	r := &TestRunner{ch: new(storage.ClickHouseClient), sessions: map[string]*testSession{}}
	for i := 0; i < maxOpenSessions; i++ {
		r.sessions[fmt.Sprintf("u:%d", i)] = &testSession{corpus: hash, lastUsed: time.Now()}
	}

	if _, err := r.session(context.Background(), "u:new", tests); err == nil || !strings.Contains(err.Error(), "too many editors") {
		t.Errorf("a new session past the cap: got %v", err)
	}
	if _, err := r.session(context.Background(), "u:0", tests); err != nil {
		t.Errorf("an existing session is served at capacity: %v", err)
	}
}

func TestRunValidatesBeforeLoading(t *testing.T) {
	r := &TestRunner{ch: new(storage.ClickHouseClient), sessions: map[string]*testSession{}}
	ctx := context.Background()

	// Nothing to run is an empty result, not an error.
	res, err := r.Run(ctx, "s", `a=1`, nil, "f1", "")
	if err != nil || res.Passed != 0 || res.Failed != 0 || len(res.Outcomes) != 0 || res.SessionID != "s" {
		t.Errorf("empty corpus: %+v %v", res, err)
	}

	bad := runnerCorpus()
	bad[1].Name = bad[0].Name
	if _, err := r.Run(ctx, "s", `a=1`, bad, "f1", ""); err == nil || !strings.Contains(err.Error(), "duplicate test name") {
		t.Errorf("an invalid corpus: %v", err)
	}
	if _, err := r.Run(ctx, "s", `a=(`, runnerCorpus(), "f1", ""); err == nil || !strings.Contains(err.Error(), "invalid query syntax") {
		t.Errorf("an unparseable query: %v", err)
	}
	if len(r.sessions) != 0 {
		t.Error("a refused run must not create a session")
	}
}

// tlsh() reads candidate digests from the scratch table, so it must use the pinned
// connection that table lives on. Any other node reads as empty and fails every
// "should match" case for the wrong reason.
func TestTLSHResolverUsesThePinnedConnection(t *testing.T) {
	var none *TestRunner
	if none.tlshResolver() != nil {
		t.Error("a nil runner has no resolver")
	}
	if (&TestRunner{}).tlshResolver() != nil {
		t.Error("no connection, no resolver")
	}

	shared := new(storage.ClickHouseClient)
	single := (&TestRunner{ch: shared}).tlshResolver()
	if single == nil || single.DB != shared {
		t.Error("a single node uses the shared client")
	}

	pinned := new(storage.ClickHouseClient)
	multi := (&TestRunner{ch: shared, pinned: pinned}).tlshResolver()
	if multi.DB != pinned {
		t.Error("a pinned node must be preferred over the load-balanced client")
	}
	if multi.Models != nil {
		t.Error("the tester never reads a model index")
	}

	// A resolver that only lists mappings cannot read needle lists: literal-only
	// tlsh() still works, so it is left unset rather than failing the run.
	withDicts := (&TestRunner{ch: shared, dicts: &stubResolver{}}).tlshResolver()
	if withDicts.Dicts != nil {
		t.Error("a mappings-only resolver must not be offered as a dictionary reader")
	}
}

// A resolver that can also read needle lists is passed through, so tlsh(dict=...)
// resolves in a test the way it does in the live alert.
func TestTLSHResolverWithFullDictionaryReader(t *testing.T) {
	r := &TestRunner{ch: new(storage.ClickHouseClient), dicts: &fullDictReader{}}
	if r.tlshResolver().Dicts == nil {
		t.Error("a reader that can list needles must be passed through")
	}
}

type fullDictReader struct{ stubResolver }

func (*fullDictReader) GetDictionaryByName(context.Context, string, string, string) (*dictionaries.Dictionary, error) {
	return nil, nil
}
func (*fullDictReader) GetKeys(context.Context, string, int) ([]string, error) { return nil, nil }
