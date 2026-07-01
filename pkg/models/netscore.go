package models

import (
	"math"
	"sort"
)

// netscore.go holds the pure scoring core for network analysis models. It has no
// ClickHouse dependency so it is fully unit-testable and is shared verbatim by the
// scheduled scorer (over rolling state) and the preview path (over raw logs). All
// scores are in [0, 1].

// PairAgg is one qualifying (src, dst, port), merged from the rolling state buckets
// (or a preview aggregation). It is the sole input to scoring.
type PairAgg struct {
	Src, Dst, Port string
	Count          uint64
	TsList         []int64   // unix seconds; sorted and window-trimmed before scoring
	SizeList       []float64 // per-connection byte sizes
	FirstTs        int64
	LastTs         int64
	TotalDuration  float64
}

// RegularityScore is the base beacon signal: timing/size/duration/histogram
// regularity only. It is deliberately not the final verdict; the modifier layer
// (ApplyModifiers) reranks it into a final score.
type RegularityScore struct {
	Score     float64 `json:"score"`
	TsScore   float64 `json:"ts_score"`
	DsScore   float64 `json:"ds_score"`
	DurScore  float64 `json:"dur_score"`
	HistScore float64 `json:"hist_score"`
}

// Modifiers holds the contextual score adjustments applied on top of regularity.
// Each is roughly +/- 0.15 and additive. Only PrevalenceScore is wired in v1;
// the others are reserved so the results schema and breakdown stay stable.
type Modifiers struct {
	PrevalenceScore  float64 `json:"prevalence_score"`
	FirstSeenScore   float64 `json:"first_seen_score"`
	ThreatIntelScore float64 `json:"threat_intel_score"`
}

// ScoreBeacon computes the base regularity score for a pair over a window of
// windowSecs seconds. It expects p.TsList sorted ascending and already trimmed to
// the window. Too few data points yield a zero score (not a beacon).
func ScoreBeacon(p PairAgg, bp BeaconParams, windowSecs int64) RegularityScore {
	// Need at least 4 connections (3 intervals) to say anything about regularity.
	if len(p.TsList) < 4 || int(p.Count) < bp.MinConnections {
		return RegularityScore{}
	}

	tsScore := scoreTimestampRegularity(p.TsList)
	dsScore := dispersionScore(p.SizeList)
	durScore := scoreDuration(p, windowSecs)
	histScore := scoreHistogram(p.TsList)

	total := tsScore*bp.TsWeight + dsScore*bp.DsWeight + durScore*bp.DurWeight + histScore*bp.HistWeight
	return RegularityScore{
		Score:     round3(clamp01(total)),
		TsScore:   round3(tsScore),
		DsScore:   round3(dsScore),
		DurScore:  round3(durScore),
		HistScore: round3(histScore),
	}
}

// scoreTimestampRegularity measures how consistent the inter-connection intervals
// are, via Bowley (quartile) skewness and median absolute deviation over the deltas.
func scoreTimestampRegularity(ts []int64) float64 {
	deltas := make([]float64, 0, len(ts))
	for i := 1; i < len(ts); i++ {
		d := ts[i] - ts[i-1]
		if d > 0 { // drop zero-gap duplicates; they carry no interval information
			deltas = append(deltas, float64(d))
		}
	}
	return dispersionScore(deltas)
}

// dispersionScore returns a [0,1] consistency score for a set of values: high when
// the values are tightly and symmetrically clustered (a hallmark of automation).
// It combines a skewness score and a MAD score. Needs at least 3 values.
func dispersionScore(values []float64) float64 {
	if len(values) < 3 {
		return 0
	}
	v := append([]float64(nil), values...)
	sort.Float64s(v)

	q1 := quantile(v, 0.25)
	q2 := quantile(v, 0.50)
	q3 := quantile(v, 0.75)

	// Bowley skewness. When q3 == q1 the distribution is perfectly concentrated
	// (all values equal within the IQR), which is maximal consistency.
	skewScore := 1.0
	if q3-q1 != 0 {
		skew := (q3 + q1 - 2*q2) / (q3 - q1)
		skewScore = clamp01(1 - math.Abs(skew))
	}

	// Median absolute deviation, normalized by the median. Low relative spread
	// (mad << median) means highly consistent values.
	median := q2
	madScore := 0.0
	if median >= 1 {
		devs := make([]float64, len(v))
		for i, x := range v {
			devs[i] = math.Abs(x - median)
		}
		sort.Float64s(devs)
		mad := quantile(devs, 0.50)
		madScore = clamp01((median - mad) / median)
	}

	return (skewScore + madScore) / 2
}

// scoreDuration rewards pairs whose activity spans the window (persistence) and
// whose connections recur across the hours of the day.
func scoreDuration(p PairAgg, windowSecs int64) float64 {
	if windowSecs <= 0 {
		return 0
	}
	span := p.LastTs - p.FirstTs
	if span < 0 {
		span = 0
	}
	coverage := clamp01(float64(span) / float64(windowSecs))

	// Consistency: distinct hours-of-day touched, over the ideal of a full day.
	active := 0
	hist := hourlyHistogram(p.TsList)
	for _, c := range hist {
		if c > 0 {
			active++
		}
	}
	consistency := clamp01(float64(active) / 24.0)

	return math.Max(coverage, consistency)
}

// scoreHistogram measures how evenly connections are spread across the 24 hours of
// the day via the coefficient of variation of the hourly bins. Even spread (low CV)
// scores high.
func scoreHistogram(ts []int64) float64 {
	hist := hourlyHistogram(ts)
	var sum, n float64
	for _, c := range hist {
		sum += float64(c)
		n++
	}
	if sum == 0 {
		return 0
	}
	mean := sum / n
	if mean == 0 {
		return 0
	}
	var variance float64
	for _, c := range hist {
		d := float64(c) - mean
		variance += d * d
	}
	variance /= n
	cv := math.Sqrt(variance) / mean
	if cv > 1 {
		return 0
	}
	return clamp01(1 - cv)
}

// hourlyHistogram bins timestamps by hour-of-day (UTC). A constant timezone offset
// does not affect evenness, so UTC is fine.
func hourlyHistogram(ts []int64) [24]uint64 {
	var h [24]uint64
	for _, t := range ts {
		hour := (t % 86400) / 3600
		if hour < 0 {
			hour += 24
		}
		if hour >= 0 && hour < 24 {
			h[hour]++
		}
	}
	return h
}

// ScoreLongConn maps a total connection duration (seconds) to a [0,1] score via
// interpolated tiers. Below base it is 0; it ramps through the low/med/high tier
// boundaries and saturates at 1.0.
func ScoreLongConn(totalDuration float64, lc LongConnParams) float64 {
	switch {
	case totalDuration < lc.BaseSeconds:
		return 0
	case totalDuration < lc.LowSeconds:
		return round3(0.25 * interp(totalDuration, lc.BaseSeconds, lc.LowSeconds))
	case totalDuration < lc.MedSeconds:
		return round3(0.25 + 0.25*interp(totalDuration, lc.LowSeconds, lc.MedSeconds))
	case totalDuration < lc.HighSeconds:
		return round3(0.5 + 0.25*interp(totalDuration, lc.MedSeconds, lc.HighSeconds))
	default:
		// Saturate: reaches 1.0 at twice the high threshold.
		return round3(clamp01(0.75 + 0.25*interp(totalDuration, lc.HighSeconds, 2*lc.HighSeconds)))
	}
}

// ApplyModifiers reranks a base regularity score using contextual signals and
// returns the final score plus the individual modifier contributions. Prevalence is
// the fraction of the network's hosts that contact the destination: rare
// destinations are boosted, ubiquitous ones penalized. Additive, never suppressive;
// the final score is clamped to [0, 1].
func ApplyModifiers(regularity, prevalence float64, mp ModifierParams) (float64, Modifiers) {
	var m Modifiers
	switch {
	case prevalence > 0 && prevalence <= mp.PrevalenceLowThreshold:
		m.PrevalenceScore = mp.PrevalenceIncrease
	case prevalence >= mp.PrevalenceHighThreshold:
		m.PrevalenceScore = -mp.PrevalenceDecrease
	}
	final := clamp01(regularity + m.PrevalenceScore + m.FirstSeenScore + m.ThreatIntelScore)
	return round3(final), m
}

// SeverityTier buckets a final score into a severity label for UI ranking and the
// linked alert severity.
func SeverityTier(final float64) string {
	switch {
	case final > 0.8:
		return "critical"
	case final > 0.6:
		return "high"
	case final > 0.4:
		return "medium"
	case final > 0.2:
		return "low"
	default:
		return "none"
	}
}

// quantile returns the linear-interpolated quantile q in [0,1] of a sorted slice.
func quantile(sorted []float64, q float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	pos := q * float64(n-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo < 0 {
		lo = 0
	}
	if hi >= n {
		hi = n - 1
	}
	frac := pos - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}

// interp returns the fraction of x between lo and hi, clamped to [0,1].
func interp(x, lo, hi float64) float64 {
	if hi <= lo {
		return 1
	}
	return clamp01((x - lo) / (hi - lo))
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

func round3(x float64) float64 {
	return math.Round(x*1000) / 1000
}
