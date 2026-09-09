package tlshresolve

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"bifract/pkg/dictionaries"
	"bifract/pkg/models"
	"bifract/pkg/parser"
	"bifract/pkg/tlsh"
)

// A and B are unrelated digests from the pkg/tlsh reference corpus. A2 is A with
// its final nibble changed, giving a digest a short distance from A without
// needing a second fixture.
const (
	digestA  = "8aa32957b3e520f9e1b28a3884954a49d775f8361b219fef03b442961f237e48d3ab31"
	digestA2 = "8aa32957b3e520f9e1b28a3884954a49d775f8361b219fef03b442961f237e48d3ab30"
	digestB  = "c1716de2793a163c2cb95403ff9c33dae415c8c44f192262786270eb9136a0c8b1d549"
)

// mustMatch runs the sweep with a live context and fails on error, keeping the
// assertions below focused on the matching itself.
func mustMatch(t *testing.T, candidates []tlshCandidate, needles []tlshNeedle, threshold int) []parser.TLSHMatch {
	t.Helper()
	got, err := matchTLSH(context.Background(), candidates, needles, threshold)
	if err != nil {
		t.Fatalf("matchTLSH: %v", err)
	}
	return got
}

func mustCandidate(t *testing.T, raw string) tlshCandidate {
	t.Helper()
	d, err := tlsh.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return tlshCandidate{raw: raw, digest: d}
}

func mustNeedle(t *testing.T, label, raw string) tlshNeedle {
	t.Helper()
	d, err := tlsh.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return tlshNeedle{label: label, digest: d}
}

func TestMatchTLSHRespectsThreshold(t *testing.T) {
	candidates := []tlshCandidate{mustCandidate(t, digestA), mustCandidate(t, digestB)}
	needles := []tlshNeedle{mustNeedle(t, "needle-a", digestA)}

	// Threshold 0 admits only the exact digest.
	got := mustMatch(t, candidates, needles, 0)
	if len(got) != 1 || got[0].Digest != digestA || got[0].Distance != 0 {
		t.Fatalf("threshold 0 should match only the identical digest, got %+v", got)
	}

	// The unrelated digest is far away, so no sane threshold pulls it in.
	exact, _ := tlsh.Parse(digestA)
	other, _ := tlsh.Parse(digestB)
	if d := exact.Distance(other); d <= 50 {
		t.Skipf("corpus digests unexpectedly similar (%d); threshold assertion not meaningful", d)
	}
	if got := mustMatch(t, candidates, needles, 50); len(got) != 1 {
		t.Fatalf("threshold 50 should still exclude the unrelated digest, got %+v", got)
	}
}

// The reported distance and needle must be the closest one, not merely the first
// within threshold: a hunt ranks on this.
func TestMatchTLSHReportsClosestNeedle(t *testing.T) {
	candidates := []tlshCandidate{mustCandidate(t, digestA)}
	needles := []tlshNeedle{
		mustNeedle(t, "far", digestB),
		mustNeedle(t, "exact", digestA),
	}

	got := mustMatch(t, candidates, needles, 500)
	if len(got) != 1 {
		t.Fatalf("expected one match, got %d", len(got))
	}
	if got[0].Needle != "exact" || got[0].Distance != 0 {
		t.Errorf("expected the nearest needle, got needle=%q distance=%d", got[0].Needle, got[0].Distance)
	}
}

// Results are ordered closest-first so the match cap keeps the strongest hits.
func TestMatchTLSHSortsByDistance(t *testing.T) {
	candidates := []tlshCandidate{
		mustCandidate(t, digestB),
		mustCandidate(t, digestA2),
		mustCandidate(t, digestA),
	}
	needles := []tlshNeedle{mustNeedle(t, "n", digestA)}

	got := mustMatch(t, candidates, needles, 500)
	if len(got) < 2 {
		t.Fatalf("expected several matches, got %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].Distance > got[i].Distance {
			t.Fatalf("results not sorted by distance: %+v", got)
		}
	}
	if got[0].Digest != digestA || got[0].Distance != 0 {
		t.Errorf("closest match should be the identical digest, got %+v", got[0])
	}
}

// The matcher shards candidates across goroutines, so a large set must produce the
// same answer as a single-threaded pass and must not drop or duplicate rows.
func TestMatchTLSHParallelIsComplete(t *testing.T) {
	var candidates []tlshCandidate
	seen := map[string]bool{}
	// Vary the final hex character to produce many distinct, closely-related digests.
	for i := 0; i < 16; i++ {
		raw := digestA[:len(digestA)-1] + fmt.Sprintf("%x", i)
		if seen[raw] {
			continue
		}
		seen[raw] = true
		candidates = append(candidates, mustCandidate(t, raw))
	}
	needles := []tlshNeedle{mustNeedle(t, "n", digestA)}

	got := mustMatch(t, candidates, needles, 500)
	if len(got) != len(candidates) {
		t.Fatalf("expected every candidate within a wide threshold, got %d of %d", len(got), len(candidates))
	}
	uniq := map[string]bool{}
	for _, m := range got {
		if uniq[m.Digest] {
			t.Fatalf("duplicate digest in results: %s", m.Digest)
		}
		uniq[m.Digest] = true
	}
}

func TestMatchTLSHEmptyInputs(t *testing.T) {
	needles := []tlshNeedle{mustNeedle(t, "n", digestA)}
	if got := mustMatch(t, nil, needles, 30); got != nil {
		t.Errorf("no candidates should yield no matches, got %+v", got)
	}
	if got := mustMatch(t, []tlshCandidate{mustCandidate(t, digestA)}, nil, 30); got != nil {
		t.Errorf("no needles should yield no matches, got %+v", got)
	}
}

// The lower bound prunes candidates before the full comparison, so a pruned
// candidate must never have been a real match.
func TestMatchTLSHLowerBoundPruningIsSafe(t *testing.T) {
	candidates := []tlshCandidate{
		mustCandidate(t, digestA),
		mustCandidate(t, digestA2),
		mustCandidate(t, digestB),
	}
	needles := []tlshNeedle{mustNeedle(t, "n", digestA)}

	for _, threshold := range []int{0, 1, 10, 30, 50, 100, 300} {
		got := mustMatch(t, candidates, needles, threshold)
		found := map[string]bool{}
		for _, m := range got {
			found[m.Digest] = true
		}
		// Brute force the same answer with no pruning.
		for _, c := range candidates {
			want := c.digest.Distance(needles[0].digest) <= threshold
			if want != found[c.raw] {
				t.Errorf("threshold %d: digest %s pruned incorrectly (want match=%v)", threshold, c.raw, want)
			}
		}
	}
}

// An unindexed fractal is skipped, not scanned and not fatal: the query still runs
// over the indexed members. Scanning it is not cheap (nothing prunes a
// distinct-value read), and blocking the whole prism over a fractal that may hold
// no digests at all is worse than covering less.
func TestNoIndexErrorNamesTheModelToCreate(t *testing.T) {
	// Single fractal: an empty result would read as "no matches" rather than
	// "not searched", so this must be an error.
	err := tlshNoIndexError("tlsh", []string{"f1"}, nil, "")
	for _, want := range []string{"tlsh", "backfill", "Analytics Models"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("single-fractal error should mention %q, got: %v", want, err)
		}
	}

	// Prism where nothing at all is indexed.
	prismErr := tlshNoIndexError("tlsh", []string{"f1", "f2"}, nil, "p1")
	if !strings.Contains(prismErr.Error(), "prism") {
		t.Errorf("prism error should say so, got: %v", prismErr)
	}

	// A filtered model is a different fix and must not be reported as "no index".
	filteredErr := tlshNoIndexError("tlsh", []string{"f1"}, map[string]string{"f1": "partial_idx"}, "")
	if !strings.Contains(filteredErr.Error(), "partial_idx") {
		t.Errorf("filtered-model error should name the model, got: %v", filteredErr)
	}
	if strings.Contains(filteredErr.Error(), "no TLSH index") {
		t.Errorf("a filtered model exists; the error must not claim there is none: %v", filteredErr)
	}
}

// fakeDB returns a fixed digest list for any index probe.
type fakeDB struct {
	digests []string
	queries []string
}

func (f *fakeDB) Query(_ context.Context, q string) ([]map[string]interface{}, error) {
	f.queries = append(f.queries, q)
	rows := make([]map[string]interface{}, 0, len(f.digests))
	for _, d := range f.digests {
		rows = append(rows, map[string]interface{}{"digest": d})
	}
	return rows, nil
}

// The whole design turns on telling "searched and found nothing" apart from "not
// searched". An unindexed member is skipped so it never blocks the query, but the
// skip is reported; only when nothing at all was indexed is it an error, because
// then an empty result would be indistinguishable from a clean miss.
func TestProbeSkipsUnindexedWithoutScanningThem(t *testing.T) {
	db := &fakeDB{digests: []string{digestA}}
	r := &Resolver{DB: db}
	indexes := map[string]models.TLSHIndex{
		"f-indexed": {Name: "idx", TableName: "model_idx", FractalID: "f-indexed"},
	}

	candidates, skipped, err := r.probeTLSHIndex(context.Background(), indexes, nil,
		[]string{"f-indexed", "f-bare-1", "f-bare-2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 {
		t.Errorf("expected the indexed fractal's digest, got %d candidates", len(candidates))
	}
	if len(skipped) != 2 {
		t.Fatalf("expected both unindexed fractals reported, got %v", skipped)
	}
	for _, sk := range skipped {
		if sk.FilteredModel != "" {
			t.Errorf("no filtered model was supplied, so none should be reported: %+v", sk)
		}
	}
	// One query only: an unindexed fractal must not be scanned.
	if len(db.queries) != 1 {
		t.Errorf("expected exactly one probe query (the index), got %d:\n%v", len(db.queries), db.queries)
	}
	for _, q := range db.queries {
		if strings.Contains(q, "f-bare") {
			t.Errorf("an unindexed fractal was queried:\n%s", q)
		}
	}
}

// An index that exists but is empty means the fractal WAS searched, so it must not
// be confused with an unindexed one.
func TestEmptyIndexIsSearchedNotSkipped(t *testing.T) {
	db := &fakeDB{digests: nil}
	r := &Resolver{DB: db}
	indexes := map[string]models.TLSHIndex{"f1": {Name: "idx", TableName: "model_idx", FractalID: "f1"}}

	candidates, skipped, err := r.probeTLSHIndex(context.Background(), indexes, nil,
		[]string{"f1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 0 {
		t.Errorf("expected no candidates, got %d", len(candidates))
	}
	if len(skipped) != 0 {
		t.Errorf("an empty index is not a skip; got skipped=%v", skipped)
	}
}

// The same digest present in two fractals is compared once.
func TestProbeDeduplicatesAcrossFractals(t *testing.T) {
	db := &fakeDB{digests: []string{digestA, digestA}}
	r := &Resolver{DB: db}
	indexes := map[string]models.TLSHIndex{
		"f1": {Name: "i1", TableName: "m1", FractalID: "f1"},
		"f2": {Name: "i2", TableName: "m2", FractalID: "f2"},
	}
	candidates, _, err := r.probeTLSHIndex(context.Background(), indexes, nil,
		[]string{"f1", "f2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 {
		t.Errorf("expected the shared digest once, got %d", len(candidates))
	}
}

// A fractal whose tlsh model exists but is filtered is skipped for a different
// reason than one with no model at all, and the caller words the two differently:
// "no index" is actively misleading when a model is sitting in the models list.
func TestSkipCarriesTheFilteredModelReason(t *testing.T) {
	db := &fakeDB{digests: []string{digestA}}
	r := &Resolver{DB: db}
	indexes := map[string]models.TLSHIndex{"f-ok": {Name: "idx", TableName: "m", FractalID: "f-ok"}}
	filtered := map[string]string{"f-filtered": "partial_idx"}

	_, skipped, err := r.probeTLSHIndex(context.Background(), indexes, filtered,
		[]string{"f-ok", "f-filtered", "f-bare"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, s := range skipped {
		got[s.FractalID] = s.FilteredModel
	}
	if len(got) != 2 {
		t.Fatalf("expected two skipped fractals, got %v", got)
	}
	if got["f-filtered"] != "partial_idx" {
		t.Errorf("filtered fractal should name its model, got %q", got["f-filtered"])
	}
	if got["f-bare"] != "" {
		t.Errorf("fractal with no model should name none, got %q", got["f-bare"])
	}
}

// fakeDicts records which scope a dictionary was resolved against.
type fakeDicts struct {
	gotFractalID string
	gotPrismID   string
}

func (f *fakeDicts) GetDictionaryByName(_ context.Context, fractalID, prismID, _ string) (*dictionaries.Dictionary, error) {
	f.gotFractalID, f.gotPrismID = fractalID, prismID
	return &dictionaries.Dictionary{ID: "d1", KeyColumn: "key"}, nil
}

func (f *fakeDicts) GetKeys(_ context.Context, _ string, _ int) ([]string, error) {
	return []string{digestA}, nil
}

// The table holding the rows and the scope owning the needle dictionary are two
// different things. In the rule tester the rows sit under a synthetic per-case
// fractal that exists only inside the scratch table and owns no dictionaries, so
// resolving needles against it would find nothing, or silently fall through to a
// same-named global and compare against the wrong list.
func TestResolveForTableSeparatesDataScopeFromDictionaryScope(t *testing.T) {
	dicts := &fakeDicts{}
	db := &fakeDB{digests: []string{digestA}}
	r := &Resolver{Dicts: dicts, DB: db}

	res, err := r.ResolveForTable(context.Background(),
		parser.TLSHParams{Field: "tlsh", Dict: "known_bad", Threshold: 30},
		TableSource{
			Table:         "scratch_abc",
			FractalID:     "synthetic-per-case-uuid",
			DictFractalID: "real-rule-fractal",
			DictPrismID:   "",
		})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(res.Matches) != 1 {
		t.Errorf("expected the identical digest to match, got %d", len(res.Matches))
	}

	if dicts.gotFractalID != "real-rule-fractal" {
		t.Errorf("needles resolved against %q; must use the rule's scope, not the synthetic data fractal",
			dicts.gotFractalID)
	}
	// The scan, by contrast, must be scoped to the rows actually under test.
	if len(db.queries) != 1 || !strings.Contains(db.queries[0], "synthetic-per-case-uuid") {
		t.Errorf("digest scan should be scoped to the unit's rows:\n%v", db.queries)
	}
	if !strings.Contains(db.queries[0], "scratch_abc") {
		t.Errorf("digest scan should read the scratch table:\n%v", db.queries)
	}
}
