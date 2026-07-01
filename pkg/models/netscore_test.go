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
		{dur: 1800, wantLow: 0, wantHigh: 0},            // below base
		{dur: 3600, wantLow: 0, wantHigh: 0.001},        // exactly base -> ~0
		{dur: 14400, wantLow: 0.25, wantHigh: 0.25},     // low boundary
		{dur: 28800, wantLow: 0.5, wantHigh: 0.5},       // med boundary
		{dur: 43200, wantLow: 0.75, wantHigh: 0.75},     // high boundary
		{dur: 86400, wantLow: 1.0, wantHigh: 1.0},       // saturates at 2x high
		{dur: 500000, wantLow: 1.0, wantHigh: 1.0},      // clamped
	}
	for _, c := range cases {
		got := ScoreLongConn(c.dur, p)
		if got < c.wantLow-1e-9 || got > c.wantHigh+1e-9 {
			t.Errorf("ScoreLongConn(%.0f) = %.3f, want in [%.3f, %.3f]", c.dur, got, c.wantLow, c.wantHigh)
		}
	}
}

func TestApplyModifiers_Prevalence(t *testing.T) {
	var mp *ModifierParams
	p := mp.WithDefaults() // low 0.02 (+0.15), high 0.5 (-0.15)

	// Rare destination: boosted.
	rareFinal, rareMods := ApplyModifiers(0.6, 0.01, p)
	if rareMods.PrevalenceScore <= 0 {
		t.Fatalf("rare destination should be boosted, got %+v", rareMods)
	}
	if math.Abs(rareFinal-0.75) > 1e-9 {
		t.Fatalf("rare final expected 0.75, got %.3f", rareFinal)
	}

	// Ubiquitous destination: penalized.
	commonFinal, commonMods := ApplyModifiers(0.6, 0.9, p)
	if commonMods.PrevalenceScore >= 0 {
		t.Fatalf("ubiquitous destination should be penalized, got %+v", commonMods)
	}
	if math.Abs(commonFinal-0.45) > 1e-9 {
		t.Fatalf("common final expected 0.45, got %.3f", commonFinal)
	}

	// Mid prevalence: unchanged.
	midFinal, midMods := ApplyModifiers(0.6, 0.2, p)
	if midMods.PrevalenceScore != 0 || math.Abs(midFinal-0.6) > 1e-9 {
		t.Fatalf("mid prevalence should not change score, got final=%.3f mods=%+v", midFinal, midMods)
	}

	// Clamp: boost cannot exceed 1.0.
	capFinal, _ := ApplyModifiers(0.95, 0.01, p)
	if capFinal > 1.0 {
		t.Fatalf("final must be clamped to 1.0, got %.3f", capFinal)
	}
}

func TestSeverityTier(t *testing.T) {
	cases := map[float64]string{0.1: "none", 0.3: "low", 0.5: "medium", 0.7: "high", 0.9: "critical"}
	for score, want := range cases {
		if got := SeverityTier(score); got != want {
			t.Errorf("SeverityTier(%.1f) = %q, want %q", score, got, want)
		}
	}
}
