package tlsh

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"testing"
)

// testdata/reference_pairs.tsv holds digest pairs and their distances as computed
// by github.com/glaslos/tlsh, the implementation that produces the digests Bifract
// ingests (Velociraptor's tlsh_hash() is a thin wrapper over it). Agreeing with it
// is what makes a match against real data correct, so this is the gate on any
// change to the scoring below.
//
// Regenerate with a program that hashes a corpus and emits `a<TAB>b<TAB>Diff(a,b)`.
func loadPairs(t *testing.T) [][3]string {
	t.Helper()
	f, err := os.Open("testdata/reference_pairs.tsv")
	if err != nil {
		t.Fatalf("open reference pairs: %v", err)
	}
	defer f.Close()

	var pairs [][3]string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 3 {
			t.Fatalf("malformed fixture line: %q", line)
		}
		pairs = append(pairs, [3]string{parts[0], parts[1], parts[2]})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if len(pairs) == 0 {
		t.Fatal("reference pair fixture is empty")
	}
	return pairs
}

func TestDistanceMatchesReference(t *testing.T) {
	pairs := loadPairs(t)
	for _, p := range pairs {
		a, err := Parse(p[0])
		if err != nil {
			t.Fatalf("parse %q: %v", p[0], err)
		}
		b, err := Parse(p[1])
		if err != nil {
			t.Fatalf("parse %q: %v", p[1], err)
		}
		want, err := strconv.Atoi(p[2])
		if err != nil {
			t.Fatal(err)
		}
		if got := a.Distance(b); got != want {
			t.Fatalf("Distance(%s, %s) = %d, reference says %d", p[0], p[1], got, want)
		}
	}
	t.Logf("verified %d reference pairs", len(pairs))
}

func TestDistanceIsSymmetricAndReflexive(t *testing.T) {
	pairs := loadPairs(t)
	for _, p := range pairs {
		a, _ := Parse(p[0])
		b, _ := Parse(p[1])
		if fwd, rev := a.Distance(b), b.Distance(a); fwd != rev {
			t.Fatalf("asymmetric: %d vs %d for %s / %s", fwd, rev, p[0], p[1])
		}
		if d := a.Distance(a); d != 0 {
			t.Fatalf("self-distance %d, want 0, for %s", d, p[0])
		}
	}
}

// The lower bound gates the expensive comparison, so it must never exceed the true
// distance: a bound that overshoots would drop real matches.
func TestLowerBoundNeverExceedsDistance(t *testing.T) {
	pairs := loadPairs(t)
	for _, p := range pairs {
		a, _ := Parse(p[0])
		b, _ := Parse(p[1])
		if lb, d := a.LowerBound(b), a.Distance(b); lb > d {
			t.Fatalf("lower bound %d exceeds distance %d for %s / %s", lb, d, p[0], p[1])
		}
	}
}

// Digests reach Bifract in whatever form the producer emits. The Go implementation
// upstream writes bare lowercase; public corpora such as MalwareBazaar publish the
// uppercase T1-prefixed form. All four spellings of one digest must compare equal.
func TestParseAcceptsPrefixAndCase(t *testing.T) {
	bare := loadPairs(t)[0][0]
	variants := []string{
		bare,
		strings.ToUpper(bare),
		"T1" + bare,
		"T1" + strings.ToUpper(bare),
	}
	base, err := Parse(bare)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range variants {
		d, err := Parse(v)
		if err != nil {
			t.Fatalf("parse %q: %v", v, err)
		}
		if got := base.Distance(d); got != 0 {
			t.Fatalf("variant %q differs from base by %d, want 0", v, got)
		}
	}
}

// TLSH produces nothing for inputs under 50 bytes, so absent and malformed values
// are routine in log data. Two of them parse to the same zero value and compare at
// distance 0, which matches everything: rejecting them is what keeps a similarity
// filter from returning the entire table.
func TestValidRejectsUnusableDigests(t *testing.T) {
	good := loadPairs(t)[0][0]
	bad := []struct{ name, in string }{
		{"empty", ""},
		{"whitespace", "   "},
		{"too short", good[:68]},
		{"too long", good + "ab"},
		{"non-hex", strings.Repeat("z", 70)},
		{"non-hex tail", good[:68] + "zz"},
		{"prefix only", "T1"},
		{"wrong prefix", "T2" + good},
		{"sha256", strings.Repeat("a", 64)},
	}
	for _, c := range bad {
		if Valid(c.in) {
			t.Errorf("%s: %q accepted, want rejected", c.name, c.in)
		}
	}
	if !Valid(good) {
		t.Errorf("valid digest %q rejected", good)
	}
	if !Valid("T1" + good) {
		t.Errorf("valid prefixed digest rejected")
	}
}

func TestChunkDiffBucketCosts(t *testing.T) {
	// One bucket per case, in the low 2 bits: value difference to expected cost.
	cases := []struct {
		a, b uint64
		want int
	}{
		{0, 0, 0}, {1, 1, 0}, {2, 2, 0}, {3, 3, 0},
		{0, 1, 1}, {1, 2, 1}, {2, 3, 1},
		{1, 0, 1}, {2, 1, 1}, {3, 2, 1},
		{0, 2, 2}, {1, 3, 2}, {2, 0, 2}, {3, 1, 2},
		{0, 3, 6}, {3, 0, 6},
	}
	for _, c := range cases {
		if got := chunkDiff(c.a, c.b); got != c.want {
			t.Errorf("chunkDiff(%d, %d) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestModDiffWraps(t *testing.T) {
	if got := modDiff(0, 255, rangeLValue); got != 1 {
		t.Errorf("modDiff(0, 255, 256) = %d, want 1", got)
	}
	if got := modDiff(0, 15, rangeQRatio); got != 1 {
		t.Errorf("modDiff(0, 15, 16) = %d, want 1", got)
	}
	if got := modDiff(3, 7, rangeQRatio); got != 4 {
		t.Errorf("modDiff(3, 7, 16) = %d, want 4", got)
	}
}

// benchDigests loads distinct digests from the fixture.
func benchDigests(tb testing.TB, max int) []Digest {
	tb.Helper()
	f, err := os.Open("testdata/reference_pairs.tsv")
	if err != nil {
		tb.Skip("fixture unavailable")
	}
	defer f.Close()

	seen := map[string]bool{}
	var out []Digest
	sc := bufio.NewScanner(f)
	for sc.Scan() && len(out) < max {
		parts := strings.Split(sc.Text(), "\t")
		if len(parts) != 3 {
			continue
		}
		for _, s := range parts[:2] {
			if seen[s] || len(out) >= max {
				continue
			}
			seen[s] = true
			if d, err := Parse(s); err == nil {
				out = append(out, d)
			}
		}
	}
	return out
}

// The real hot loop is one needle scanned against the whole candidate set, so the
// benchmark mirrors that rather than walking unrelated pairs: it is the shape that
// determines whether `distinct x needles` fits in a query's time budget.
func BenchmarkDistanceScan(b *testing.B) {
	candidates := benchDigests(b, 512)
	if len(candidates) == 0 {
		b.Skip("no digests in fixture")
	}
	needle := candidates[0]

	// ns/op covers one full pass over candidates; divide by len(candidates) for the
	// per-comparison cost.
	b.SetBytes(int64(len(candidates)))
	b.ResetTimer()
	b.ReportAllocs()
	sink := 0
	for i := 0; i < b.N; i++ {
		for j := range candidates {
			sink += needle.Distance(candidates[j])
		}
	}
	_ = sink
}

// The lower bound exists to reject most candidates before the full comparison, so
// it must be materially cheaper than the thing it guards.
func BenchmarkLowerBoundScan(b *testing.B) {
	candidates := benchDigests(b, 512)
	if len(candidates) == 0 {
		b.Skip("no digests in fixture")
	}
	needle := candidates[0]

	b.ResetTimer()
	sink := 0
	for i := 0; i < b.N; i++ {
		for j := range candidates {
			sink += needle.LowerBound(candidates[j])
		}
	}
	_ = sink
}

// Parsing happens once per distinct digest per probe, so its cost sets the floor
// on a probe that hands the index to the matcher as strings.
func BenchmarkParse(b *testing.B) {
	f, err := os.Open("testdata/reference_pairs.tsv")
	if err != nil {
		b.Skip("fixture unavailable")
	}
	sc := bufio.NewScanner(f)
	var strs []string
	for sc.Scan() && len(strs) < 512 {
		parts := strings.Split(sc.Text(), "\t")
		if len(parts) == 3 {
			strs = append(strs, parts[0])
		}
	}
	f.Close()
	if len(strs) == 0 {
		b.Skip("no digests")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range strs {
			if _, err := Parse(strs[j]); err != nil {
				b.Fatal(err)
			}
		}
	}
}
