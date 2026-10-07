package parser

import (
	"slices"
	"strings"
	"testing"
)

func mustContain(t *testing.T, sql string, wants ...string) {
	t.Helper()
	for _, w := range wants {
		if !strings.Contains(sql, w) {
			t.Errorf("expected %q in:\n%s", w, sql)
		}
	}
}

// The latest complete bucket is scored, with 0 when it was empty, against the
// history before it; the latest non-empty bucket (argMax) can be weeks old.
func TestVolumeScoredSQL_ScoresLatestCompleteBucket(t *testing.T) {
	sql := VolumeScoredSQL("`m` FINAL", "fractal_id = 'f'", "day", 7, "", false)
	mustContain(t, sql,
		"toDate(now('UTC')) - INTERVAL 1 DAY AS latest_bucket",
		"sumIf(cnt, bucket = latest_bucket) AS latest_count",
		"groupArrayIf(toFloat64(cnt), bucket < latest_bucket) AS hist",
		"bucket < toDate(now('UTC'))",
	)
	if strings.Contains(sql, "argMax") || strings.Contains(sql, "max(bucket)") {
		t.Errorf("the scored bucket must not be the latest non-empty one:\n%s", sql)
	}
}

// Missing buckets count as zeros from the entity's first bucket, entering the
// medians as a weight so memory follows active buckets only.
func TestVolumeScoredSQL_ZeroFillsHistory(t *testing.T) {
	sql := VolumeScoredSQL("`m` FINAL", "fractal_id = 'f'", "day", 7, "", false)
	mustContain(t, sql,
		"toUInt64(dateDiff('day', min(bucket), latest_bucket)) AS n_buckets",
		"toUInt64(n_buckets - length(hist)) AS zeros",
		"arrayReduce('quantileExactWeightedInterpolated(0.5)', arrayPushBack(hist, 0.), arrayPushBack(arrayMap(x -> toUInt64(1), hist), zeros)) AS baseline_median",
		"arrayReduce('quantileExactWeightedInterpolated(0.5)', arrayPushBack(dev, baseline_median), arrayPushBack(arrayMap(x -> toUInt64(1), dev), zeros)) AS mad",
		"(arraySum(dev) + zeros * baseline_median) / n_buckets AS mean_ad",
		"WHERE n_buckets >= 7",
	)
	if strings.Contains(sql, "medianExact") {
		t.Errorf("medianExact takes the upper middle value on even counts, not the median:\n%s", sql)
	}
}

// MAD = 0 must not force z to 0: it falls back to the mean absolute deviation,
// and a flat history scores any change at the finite sentinel.
func TestVolumeScoredSQL_MADZeroFallback(t *testing.T) {
	sql := VolumeScoredSQL("`m` FINAL", "fractal_id = 'f'", "day", 7, "", false)
	mustContain(t, sql,
		"toFloat64(latest_count) = baseline_median, 0.",
		"mad > 0, round(0.6745 * (latest_count - baseline_median) / mad, 4)",
		"mean_ad > 0, round((latest_count - baseline_median) / (1.253314 * mean_ad), 4)",
		"sign(latest_count - baseline_median) * 1000000.) AS z_score",
	)
	if strings.Contains(sql, "if(mad = 0, 0") {
		t.Errorf("the mad=0 -> z=0 guard hides spikes on steady entities:\n%s", sql)
	}
}

func TestVolumeScoredSQL_HourlyWindow(t *testing.T) {
	sql := VolumeScoredSQL("`m` FINAL", "fractal_id = 'f'", "hour", 24, "", false)
	mustContain(t, sql,
		"toStartOfHour(now('UTC')) - INTERVAL 1 HOUR AS latest_bucket",
		"dateDiff('hour', min(bucket), latest_bucket)",
		"bucket >= toStartOfHour(now('UTC')) - INTERVAL 30 DAY AND bucket < toStartOfHour(now('UTC'))",
		"WHERE n_buckets >= 24",
	)
}

func TestVolumeScoredSQL_Options(t *testing.T) {
	def := VolumeScoredSQL("src", "fractal_id = 'f'", "day", 0, "", false)
	mustContain(t, def, "WHERE n_buckets >= 7", "bucket >= toDate(now('UTC')) - INTERVAL 90 DAY")
	if strings.Contains(def, "days") {
		t.Errorf("days is only for the data view:\n%s", def)
	}

	withDays := VolumeScoredSQL("src", "fractal_id = 'f'", "day", 3, "toDate('2026-01-01')", true)
	mustContain(t, withDays,
		"arraySort(groupUniqArray(365)(toDate(bucket))) AS days",
		"bucket >= toDate('2026-01-01') AND",
		"WHERE n_buckets >= 3",
	)
}

// modelLookup() reads the same scoring definition as the data view, and exposes
// latest_bucket so an alert can tell which bucket its z_score describes.
func TestModelLookup_VolumeUsesSharedScoring(t *testing.T) {
	opts := mlookupOpts()
	opts.Models["vol"] = AnalyticsModelInfo{ID: "5", TableName: "model_vol", ModelType: "volume_baseline", MinSample: 5, TimeBucket: "hour", FractalID: "f1"}
	pipeline, err := ParseQuery(`* | model_lookup(model="vol", key=[user]) | z_score > 3.5`)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	res, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}
	want := VolumeScoredSQL("`model_vol` FINAL", "fractal_id IN ('f1')", "hour", 5, "", false)
	if !strings.Contains(res.SQL, want) {
		t.Errorf("expected the shared scoring subquery, got:\n%s", res.SQL)
	}
	for _, col := range []string{"z_score", "latest_count", "baseline_median", "mad", "n_buckets", "latest_bucket"} {
		if !slices.Contains(res.FieldOrder, col) {
			t.Errorf("expected output column %s, got %v", col, res.FieldOrder)
		}
	}
}
