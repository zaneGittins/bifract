package tlshresolve

import (
	"context"
	"fmt"
	"testing"

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
