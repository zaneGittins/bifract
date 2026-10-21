package models

import (
	"math"
	"testing"
)

const testWindowSecs = 86400 // 1 day

// buildSeries returns count timestamps starting at t0, each interval seconds apart,
// with a per-step jitter added deterministically. jitterFn(i) is added to the ideal
// timestamp for point i (i from 0).
func buildSeries(t0 int64, interval int64, count int, jitterFn func(i int) int64) []int64 {
	ts := make([]int64, count)
	for i := 0; i < count; i++ {
		v := t0 + int64(i)*interval
		if jitterFn != nil {
			v += jitterFn(i)
		}
		ts[i] = v
	}
	return ts
}

func constSizes(v float64, n int) []float64 {
	s := make([]float64, n)
	for i := range s {
		s[i] = v
	}
	return s
}

// regularPair builds a perfectly regular beacon spanning a full day: 288 points at a
// 5-minute interval with constant size. This exercises all four subscores.
func regularPair() PairAgg {
	const interval = 300
	const count = 288 // 24h / 5min
	ts := buildSeries(0, interval, count, nil)
	return PairAgg{
		Count:         count,
		TsList:        ts,
		SizeList:      constSizes(1024, count),
		FirstTs:       ts[0],
		LastTs:        ts[count-1],
		TotalDuration: 0,
	}
}

func defaultBeaconParams() BeaconParams {
	var bp *BeaconParams
	return bp.WithDefaults(testWindowSecs)
}

func TestScoreBeacon_PerfectlyRegular(t *testing.T) {
	got := ScoreBeacon(regularPair(), defaultBeaconParams(), testWindowSecs)
	if got.Score < 0.95 {
		t.Fatalf("perfectly regular beacon should score ~1.0, got %+v", got)
	}
	if got.TsScore < 0.99 || got.DsScore < 0.99 || got.HistScore < 0.99 || got.DurScore < 0.99 {
		t.Fatalf("expected all subscores ~1.0, got %+v", got)
	}
}

func TestScoreBeacon_JitteredScoresLower(t *testing.T) {
	regular := ScoreBeacon(regularPair(), defaultBeaconParams(), testWindowSecs)

	// Same cadence but with substantial deterministic jitter on each interval.
	const interval = 300
	const count = 288
	jitter := func(i int) int64 {
		// Triangle-ish wave in [-140, 140], varying the interval a lot.
		return int64((i*97)%281) - 140
	}
	ts := buildSeries(0, interval, count, jitter)
	p := PairAgg{Count: count, TsList: ts, SizeList: constSizes(1024, count), FirstTs: ts[0], LastTs: ts[count-1]}
	jittered := ScoreBeacon(p, defaultBeaconParams(), testWindowSecs)

	if jittered.TsScore >= regular.TsScore {
		t.Fatalf("jittered timing should reduce ts_score: regular=%.3f jittered=%.3f", regular.TsScore, jittered.TsScore)
	}
	if jittered.Score >= regular.Score {
		t.Fatalf("jittered beacon should score below regular: regular=%.3f jittered=%.3f", regular.Score, jittered.Score)
	}
}

func TestScoreBeacon_BurstyScoresLow(t *testing.T) {
	// Realistic non-beacon traffic is bursty / heavy-tailed: mostly short gaps with
	// occasional long idle periods, and widely varying payload sizes. Such a skewed
	// interval distribution must score well below the default flag threshold (0.8).
	const count = 200
	var seed uint64 = 0x9E3779B97F4A7C15
	next := func() uint64 { seed = seed*6364136223846793005 + 1442695040888963407; return seed >> 33 }
	ts := make([]int64, count)
	sizes := make([]float64, count)
	var cur int64 = 0
	for i := 0; i < count; i++ {
		// Product of two small randoms -> heavy-tailed, highly skewed intervals.
		cur += 1 + int64(next()%40)*int64(next()%40)
		ts[i] = cur
		sizes[i] = float64(next() % 100000)
	}
	p := PairAgg{Count: count, TsList: ts, SizeList: sizes, FirstTs: ts[0], LastTs: ts[count-1]}
	bp := defaultBeaconParams()
	got := ScoreBeacon(p, bp, testWindowSecs)
	// The timing-regularity signal must correctly read as poor...
	if got.TsScore > 0.6 {
		t.Fatalf("bursty timing should yield a low ts_score, got %+v", got)
	}
	// ...and the composite must stay under the default flag threshold with margin.
	if got.Score >= bp.ScoreThreshold {
		t.Fatalf("bursty traffic must not reach the flag threshold %.2f, got %+v", bp.ScoreThreshold, got)
	}
}

// TestScoreBeacon_QuantizedTightBeacon guards the Bowley-skew regression: a tight
// beacon whose second-resolution intervals take only a few adjacent integer values
// (e.g. Merlin d90/j3: ~92s median, ~1s MAD) collapses the quartiles onto adjacent
// ticks. Without relative-IQR weighting, Bowley skew saturates to 1 and halves the
// timing score to ~0.5 for a textbook beacon. It must score high instead.
func TestScoreBeacon_QuantizedTightBeacon(t *testing.T) {
	// Deterministic deltas drawn from {90,91,92,93,94} clustered near 92, matching
	// the observed distribution of a real low-jitter beacon at second resolution.
	pattern := []int64{92, 91, 93, 92, 91, 92, 93, 92, 90, 92, 93, 91, 92, 94, 92}
	ts := make([]int64, 0, 300)
	var cur int64 = 0
	ts = append(ts, cur)
	for i := 0; len(ts) < 300; i++ {
		cur += pattern[i%len(pattern)]
		ts = append(ts, cur)
	}
	p := PairAgg{Count: uint64(len(ts)), TsList: ts, SizeList: constSizes(1024, len(ts)), FirstTs: ts[0], LastTs: ts[len(ts)-1]}
	got := ScoreBeacon(p, defaultBeaconParams(), testWindowSecs)
	if got.TsScore < 0.85 {
		t.Fatalf("tight quantized beacon must score high on timing, got ts_score=%.3f (%+v)", got.TsScore, got)
	}
}

func TestScoreBeacon_TooFewPoints(t *testing.T) {
	ts := []int64{0, 300, 600} // only 3 points (< 4)
	p := PairAgg{Count: 3, TsList: ts, SizeList: constSizes(1024, 3), FirstTs: 0, LastTs: 600}
	got := ScoreBeacon(p, defaultBeaconParams(), testWindowSecs)
	if got.Score != 0 {
		t.Fatalf("fewer than 4 points must score 0, got %+v", got)
	}
}

func TestScoreLongConn_BucketBoundaries(t *testing.T) {
	var lc *LongConnParams
	p := lc.WithDefaults() // base 3600, low 14400, med 28800, high 43200

	cases := []struct {
		dur      float64
		wantLow  float64
		wantHigh float64
	}{
		{dur: 1800, wantLow: 0, wantHigh: 0},        // below base
		{dur: 3600, wantLow: 0, wantHigh: 0.001},    // exactly base -> ~0
		{dur: 14400, wantLow: 0.25, wantHigh: 0.25}, // low boundary
		{dur: 28800, wantLow: 0.5, wantHigh: 0.5},   // med boundary
		{dur: 43200, wantLow: 0.75, wantHigh: 0.75}, // high boundary
		{dur: 86400, wantLow: 1.0, wantHigh: 1.0},   // saturates at 2x high
		{dur: 500000, wantLow: 1.0, wantHigh: 1.0},  // clamped
	}
	for _, c := range cases {
		got := ScoreLongConn(c.dur, p)
		if got < c.wantLow-1e-9 || got > c.wantHigh+1e-9 {
			t.Errorf("ScoreLongConn(%.0f) = %.3f, want in [%.3f, %.3f]", c.dur, got, c.wantLow, c.wantHigh)
		}
	}
}

func TestApplyPairModifiers_Prevalence(t *testing.T) {
	var mp *ModifierParams
	p := mp.WithDefaults() // low 0.02 (+0.15), high 0.5 (-0.15)

	// Rare destination: boosted.
	rareFinal, rareMods := ApplyPairModifiers(0.6, PairContext{Prevalence: 0.01}, p)
	if rareMods.PrevalenceScore <= 0 {
		t.Fatalf("rare destination should be boosted, got %+v", rareMods)
	}
	if math.Abs(rareFinal-0.75) > 1e-9 {
		t.Fatalf("rare final expected 0.75, got %.3f", rareFinal)
	}

	// Ubiquitous destination: penalized.
	commonFinal, commonMods := ApplyPairModifiers(0.6, PairContext{Prevalence: 0.9}, p)
	if commonMods.PrevalenceScore >= 0 {
		t.Fatalf("ubiquitous destination should be penalized, got %+v", commonMods)
	}
	if math.Abs(commonFinal-0.45) > 1e-9 {
		t.Fatalf("common final expected 0.45, got %.3f", commonFinal)
	}

	// Mid prevalence: unchanged.
	midFinal, midMods := ApplyPairModifiers(0.6, PairContext{Prevalence: 0.2}, p)
	if midMods.PrevalenceScore != 0 || math.Abs(midFinal-0.6) > 1e-9 {
		t.Fatalf("mid prevalence should not change score, got final=%.3f mods=%+v", midFinal, midMods)
	}

	// Clamp: boost cannot exceed 1.0.
	capFinal, _ := ApplyPairModifiers(0.95, PairContext{Prevalence: 0.01}, p)
	if capFinal > 1.0 {
		t.Fatalf("final must be clamped to 1.0, got %.3f", capFinal)
	}
}

// dispersionScore averages a Bowley-skew term and a MAD/median term. These are
// hand-computed from the definition (linear-interpolated quartiles) so a change
// to either term, or to the quartile method, shows up as a number that moved.
func TestDispersionScore_HandComputed(t *testing.T) {
	cases := []struct {
		name   string
		values []float64
		want   float64
	}{
		// Under three values there is no spread to measure.
		{"two values", []float64{5, 5}, 0},
		// Identical values: q1 == q3 (skew term 1) and MAD 0 (MAD term 1).
		{"constant", []float64{10, 10, 10}, 1},
		// q1=1.5 q2=2 q3=2.5: symmetric, skew term 1; MAD 1 over median 2 = 0.5.
		{"symmetric spread", []float64{1, 2, 3}, 0.75},
		// q1=11 q2=12 q3=20: skew 7/9 (term 2/9); MAD 2 over median 12 (term 5/6).
		{"right skewed", []float64{40, 10, 20, 12, 11}, 19.0 / 36.0},
		// One retry far off the cadence moves neither quartiles nor MAD.
		{"single outlier", []float64{10, 10, 10, 10, 100}, 1},
		// q1=q2=100 q3=101: |skew| 1, but the IQR is 1% of the median, a tenth of
		// skewSpreadFloor, so the penalty is damped to 0.1. Undamped this is 0.5.
		{"quantized tick", []float64{100, 100, 100, 101, 101}, 0.95},
	}
	for _, c := range cases {
		if got := dispersionScore(c.values); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("%s: dispersionScore(%v) = %.6f, want %.6f", c.name, c.values, got, c.want)
		}
	}
}

// dispersionScore must not reorder its caller's slice: the same slice is the
// pair's size list, which stays parallel to its timestamps.
func TestDispersionScore_DoesNotMutateInput(t *testing.T) {
	in := []float64{3, 1, 2}
	dispersionScore(in)
	if in[0] != 3 || in[1] != 1 || in[2] != 2 {
		t.Fatalf("input reordered: %v", in)
	}
}

// Zero-gap duplicates (two logs of one connection) carry no interval and must not
// read as a burst of zero-length intervals.
func TestTimestampRegularity_IgnoresDuplicateTimestamps(t *testing.T) {
	if got := scoreTimestampRegularity([]int64{0, 0, 300, 300, 600, 900}); got != 1 {
		t.Fatalf("duplicates must be dropped before measuring intervals, got %.3f", got)
	}
}

// Timing regularity only falls as the same cadence gets noisier. The noise is one
// fixed pseudo-random sequence scaled by the amplitude, so each step is the same
// series with wider jitter. (A sawtooth jitter would not do: it makes the
// intervals two-valued, which the robust statistics rightly read as regular.)
func TestTimestampRegularity_MonotoneInJitter(t *testing.T) {
	const count = 288
	noise := make([]float64, count)
	var seed uint64 = 0x2545F4914F6CDD1D
	for i := range noise {
		seed = seed*6364136223846793005 + 1442695040888963407
		noise[i] = float64(seed>>11)/float64(1<<53)*2 - 1 // uniform in [-1, 1)
	}
	prev := 2.0
	for _, amp := range []float64{0, 10, 40, 100, 140} {
		jitter := func(i int) int64 { return int64(math.Round(amp * noise[i])) }
		got := scoreTimestampRegularity(buildSeries(0, 300, count, jitter))
		if got > prev+1e-9 {
			t.Fatalf("jitter +/-%.0fs scored %.3f, above the less jittered %.3f", amp, got, prev)
		}
		if got < 0 || got > 1 {
			t.Fatalf("score out of [0,1]: %.3f", got)
		}
		prev = got
	}
}

func TestScoreDuration(t *testing.T) {
	// Hourly for 12 hours of a one-day window: span 11h (coverage 0.458) and 12 of
	// 24 hours touched (consistency 0.5). The better of the two wins.
	ts := buildSeries(0, 3600, 12, nil)
	p := PairAgg{TsList: ts, FirstTs: ts[0], LastTs: ts[len(ts)-1]}
	if got := scoreDuration(p, 86400); math.Abs(got-0.5) > 1e-9 {
		t.Errorf("12 hourly connections = %.4f, want 0.5", got)
	}
	// Four connections at the window's edges, all in hour 0: coverage carries it.
	edge := []int64{0, 60, 86400 * 7, 86400*7 + 60}
	p = PairAgg{TsList: edge, FirstTs: edge[0], LastTs: edge[3]}
	if got := scoreDuration(p, 86400*7); got < 0.99 {
		t.Errorf("a pair spanning the whole window = %.4f, want ~1", got)
	}
	// A non-positive window cannot be covered.
	if got := scoreDuration(p, 0); got != 0 {
		t.Errorf("zero window = %.4f, want 0", got)
	}
	// An inverted range is no span at all, never negative coverage.
	p = PairAgg{TsList: []int64{100}, FirstTs: 500, LastTs: 100}
	if got := scoreDuration(p, 86400); got < 0 || got > 1.0/24+1e-9 {
		t.Errorf("inverted range = %.4f, want only the one touched hour", got)
	}
}

// scoreHistogram is 1 - CV of the 24 hourly bins, zero once CV reaches 1.
func TestScoreHistogram_HandComputed(t *testing.T) {
	hours := func(n int) []int64 {
		var ts []int64
		for h := 0; h < n; h++ {
			for k := 0; k < 4; k++ {
				ts = append(ts, int64(h*3600+k*60))
			}
		}
		return ts
	}
	cases := []struct {
		name string
		ts   []int64
		want float64
	}{
		{"no connections", nil, 0},
		{"every hour evenly", hours(24), 1},
		// 18 equal bins, 6 empty: CV = 1/sqrt(3).
		{"18 of 24 hours", hours(18), 1 - 1/math.Sqrt(3)},
		// Half the bins empty: CV is exactly 1.
		{"12 of 24 hours", hours(12), 0},
		{"one hour", hours(1), 0},
	}
	for _, c := range cases {
		if got := scoreHistogram(c.ts); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("%s: %.6f, want %.6f", c.name, got, c.want)
		}
	}
}

func TestHourlyHistogramBinsByUTCHour(t *testing.T) {
	h := hourlyHistogram([]int64{5*3600 + 59, 86400 + 5*3600, 23*3600 + 3599})
	if h[5] != 2 || h[23] != 1 {
		t.Fatalf("bins: %v", h)
	}
}

func TestQuantile(t *testing.T) {
	if got := quantile(nil, 0.5); got != 0 {
		t.Errorf("empty = %v", got)
	}
	if got := quantile([]float64{7}, 0.9); got != 7 {
		t.Errorf("single = %v", got)
	}
	if got := quantile([]float64{0, 10}, 0.25); got != 2.5 {
		t.Errorf("interpolated = %v, want 2.5", got)
	}
	if got := quantile([]float64{1, 2, 3, 4}, 1); got != 4 {
		t.Errorf("max = %v", got)
	}
}

// Between tier boundaries the score is a straight line, and over any sweep it
// never decreases and never leaves [0, 1].
func TestScoreLongConn_InterpolatesAndIsMonotone(t *testing.T) {
	var lc *LongConnParams
	p := lc.WithDefaults()
	mid := map[float64]float64{
		9000:  0.125, // halfway base(3600)..low(14400)
		21600: 0.375, // halfway low..med(28800)
		36000: 0.625, // halfway med..high(43200)
		64800: 0.875, // halfway high..2*high
	}
	for d, want := range mid {
		if got := ScoreLongConn(d, p); math.Abs(got-want) > 1e-9 {
			t.Errorf("ScoreLongConn(%.0f) = %.3f, want %.3f", d, got, want)
		}
	}
	prev := -1.0
	for d := 0.0; d <= 120000; d += 500 {
		got := ScoreLongConn(d, p)
		if got < prev || got < 0 || got > 1 {
			t.Fatalf("ScoreLongConn(%.0f) = %.3f after %.3f", d, got, prev)
		}
		prev = got
	}
}

// Misordered tiers (an API-supplied definition) still score inside [0, 1].
func TestScoreLongConn_MisorderedTiersStayBounded(t *testing.T) {
	p := LongConnParams{BaseSeconds: 3600, LowSeconds: 1000, MedSeconds: 2000, HighSeconds: 3000}
	for _, d := range []float64{0, 3600, 5000, 1e9} {
		if got := ScoreLongConn(d, p); got < 0 || got > 1 {
			t.Errorf("ScoreLongConn(%.0f) = %.3f out of range", d, got)
		}
	}
}

func TestApplyPairModifiers_BoundariesAndBounds(t *testing.T) {
	var mp *ModifierParams
	p := mp.WithDefaults()

	// Both thresholds are inclusive.
	if _, m := ApplyPairModifiers(0.5, PairContext{Prevalence: p.PrevalenceLowThreshold}, p); m.PrevalenceScore != p.PrevalenceIncrease {
		t.Errorf("prevalence at the low threshold must be boosted: %+v", m)
	}
	if _, m := ApplyPairModifiers(0.5, PairContext{Prevalence: p.PrevalenceHighThreshold}, p); m.PrevalenceScore != -p.PrevalenceDecrease {
		t.Errorf("prevalence at the high threshold must be penalized: %+v", m)
	}
	// A destination with no recorded sources is unknown, not rare: no boost.
	if final, m := ApplyPairModifiers(0.5, PairContext{Prevalence: 0}, p); m.PrevalenceScore != 0 || final != 0.5 {
		t.Errorf("zero prevalence: final %.3f mods %+v", final, m)
	}
	// The penalty never drives a score negative.
	if final, _ := ApplyPairModifiers(0.05, PairContext{Prevalence: 1}, p); final != 0 {
		t.Errorf("penalized low score = %.3f, want 0", final)
	}
	// For a fixed prevalence the final score never falls as regularity rises, and
	// for a fixed regularity it never rises as the destination gets commoner.
	for _, prev := range []float64{0.01, 0.2, 0.9} {
		last := -1.0
		for r := 0.0; r <= 1.0; r += 0.05 {
			final, _ := ApplyPairModifiers(r, PairContext{Prevalence: prev}, p)
			if final < last || final < 0 || final > 1 {
				t.Fatalf("prevalence %.2f: regularity %.2f -> %.3f after %.3f", prev, r, final, last)
			}
			last = final
		}
	}
	last := 2.0
	for _, prev := range []float64{0.001, 0.02, 0.1, 0.49, 0.5, 1} {
		final, _ := ApplyPairModifiers(0.6, PairContext{Prevalence: prev}, p)
		if final > last {
			t.Fatalf("prevalence %.3f scored %.3f, above a rarer destination's %.3f", prev, final, last)
		}
		last = final
	}
}

func TestScoreBeacon_RequiresMinConnections(t *testing.T) {
	p := regularPair()
	bp := defaultBeaconParams()
	bp.MinConnections = int(p.Count) + 1
	if got := ScoreBeacon(p, bp, testWindowSecs); got != (RegularityScore{}) {
		t.Fatalf("a pair under min_connections must not score, got %+v", got)
	}
}

// Caller weights are trusted verbatim, and the total is clamped even when they
// sum past 1.
func TestScoreBeacon_WeightsAndClamp(t *testing.T) {
	p := regularPair()
	bp := BeaconParams{TsWeight: 1}
	bp = (&bp).WithDefaults(testWindowSecs)
	got := ScoreBeacon(p, bp, testWindowSecs)
	if got.Score != got.TsScore {
		t.Errorf("timing-only weights: score %.3f, ts_score %.3f", got.Score, got.TsScore)
	}
	heavy := BeaconParams{TsWeight: 1, DsWeight: 1, DurWeight: 1, HistWeight: 1}
	heavy = (&heavy).WithDefaults(testWindowSecs)
	if got := ScoreBeacon(p, heavy, testWindowSecs); got.Score != 1 {
		t.Errorf("weights summing to 4 must clamp to 1, got %.3f", got.Score)
	}
}

func TestBeaconParamsDefaults(t *testing.T) {
	var nilParams *BeaconParams
	d := nilParams.WithDefaults(7 * 86400)
	if d.MinConnections != 4 || d.ScoreThreshold != 0.8 || d.StrobeLimit != 7*86400 {
		t.Errorf("defaults: %+v", d)
	}
	if d.TsWeight+d.DsWeight+d.DurWeight+d.HistWeight != 1 {
		t.Errorf("default weights must split evenly: %+v", d)
	}
	partial := &BeaconParams{DsWeight: 0.5, StrobeLimit: 100}
	got := partial.WithDefaults(86400)
	if got.TsWeight != 0 || got.DsWeight != 0.5 || got.StrobeLimit != 100 {
		t.Errorf("a set weight must be kept verbatim with the rest at 0: %+v", got)
	}
}
