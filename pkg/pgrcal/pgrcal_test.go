package pgrcal

import "testing"

// pick is the percentile walk Cutoffs uses; exercised directly so the derivation is covered
// without a database.
func pickFrom(hist map[int]int64, total int64, share float64) float64 {
	want := int64(float64(total) * share / 100)
	if want < 1 {
		want = 1
	}
	var seen int64
	for b := buckets - 1; b >= 0; b-- {
		seen += hist[b]
		if seen >= want {
			return float64(b) / 1000
		}
	}
	return 0
}

func TestCutoffTracksRequestedShare(t *testing.T) {
	// Uniform over the full range: the top N% must cut at 1 - N/100.
	hist := map[int]int64{}
	var total int64
	for b := 0; b < buckets; b++ {
		hist[b] = 10
		total += 10
	}
	if got := pickFrom(hist, total, 2); got < 0.979 || got > 0.981 {
		t.Errorf("top 2%% of a uniform distribution should cut near 0.98, got %v", got)
	}
	if got := pickFrom(hist, total, 10); got < 0.899 || got > 0.901 {
		t.Errorf("top 10%% should cut near 0.90, got %v", got)
	}
}

// The saturated case is the one the fixed 0.9 cutoff got wrong: when nearly every edge scores
// above 0.99, an absolute cutoff flags everything while a share-based one still flags a share.
func TestCutoffHandlesSaturatedDistribution(t *testing.T) {
	hist := map[int]int64{999: 9800, 1000: 200}
	var total int64 = 10000
	got := pickFrom(hist, total, 2)
	if got != 1.0 {
		t.Errorf("top 2%% of a saturated distribution should land in the top bucket, got %v", got)
	}
	// A fixed 0.9 would have flagged all 10000; the derived cutoff flags 200.
	var flagged int64
	for b, n := range hist {
		if float64(b)/1000 >= got {
			flagged += n
		}
	}
	if flagged > total/40 {
		t.Errorf("derived cutoff flagged %d of %d edges, want about 2%%", flagged, total)
	}
}

func TestClampBucket(t *testing.T) {
	r := NewRecorder(nil)
	r.Observe([]float64{-5, 0.5, 42})
	if r.pending[0] != 1 || r.pending[500] != 1 || r.pending[buckets-1] != 1 {
		t.Errorf("out-of-range scores must clamp into the end buckets, got %v", r.pending)
	}
}
