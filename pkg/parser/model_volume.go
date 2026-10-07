package parser

import "fmt"

// VolumeFlatZ is the z_score of an entity whose history is perfectly flat (every
// bucket the same count, so MAD and mean absolute deviation are both 0) when the
// scored bucket differs from it: any change from a flat history is maximal. A
// finite sentinel keeps the value JSON-safe and sortable; its sign is the change's.
const VolumeFlatZ = 1000000

// VolumeMinBuckets returns the minimum buckets of history (zero-filled, from the
// entity's first bucket) an entity needs before it is scored; 0 means the default.
func VolumeMinBuckets(minSample int) int {
	if minSample > 0 {
		return minSample
	}
	return 7
}

// volumeWindow holds the SQL bounds of a volume_baseline model's scoring window
// on the bucket column: history starts at lower, latest is the latest complete
// bucket (the one scored), and upper is the current incomplete bucket, excluded.
type volumeWindow struct {
	unit, lower, latest, upper string
}

// volumeWindowFor bounds hourly models to 30 days and daily ones to 90, in UTC
// to match the buckets (toStartOfHour/toDate of the UTC log timestamp).
func volumeWindowFor(timeBucket string) volumeWindow {
	if timeBucket == "hour" {
		cur := "toStartOfHour(now('UTC'))"
		return volumeWindow{unit: "hour", lower: cur + " - INTERVAL 30 DAY", latest: cur + " - INTERVAL 1 HOUR", upper: cur}
	}
	cur := "toDate(now('UTC'))"
	return volumeWindow{unit: "day", lower: cur + " - INTERVAL 90 DAY", latest: cur + " - INTERVAL 1 DAY", upper: cur}
}

// VolumeScoredSQL scores every entity of a volume_baseline model, the one
// definition the data view, preview, stats, histogram and modelLookup() read.
//
// Each entity's series runs from its first bucket in the window to the latest
// complete bucket, with missing buckets counted as zero. The latest complete
// bucket is scored (latest_count, 0 when it was empty) against the history
// before it: baseline_median and mad are the median and median absolute
// deviation of that history, n_buckets its length. z_score is the modified
// z-score 0.6745*(x - median)/MAD (Iglewicz and Hoaglin); when MAD is 0 it falls
// back to (x - median)/(1.253314*MeanAD), and when the history is flat as well
// any change scores +/-VolumeFlatZ. Zeros enter the medians
// as a weight rather than a materialized array, so memory follows active buckets.
//
// source yields the volume state shape (fractal_id, entity_val, bucket,
// event_count); scope is a WHERE predicate on it. lower overrides the history
// bound (the preview passes its window start); "" uses the model's default.
// withDays adds the sorted active-day list, which only the data view needs.
func VolumeScoredSQL(source, scope, timeBucket string, minSample int, lower string, withDays bool) string {
	w := volumeWindowFor(timeBucket)
	if lower == "" {
		lower = w.lower
	}
	daysOut, daysMid, daysAgg := "", "", ""
	if withDays {
		daysOut, daysMid = ", days", ", days"
		daysAgg = ",\n            arraySort(groupUniqArray(365)(toDate(bucket))) AS days"
	}
	return fmt.Sprintf(`SELECT entity_val, latest_count, baseline_median, mad, n_buckets, latest_bucket%[1]s,
    multiIf(toFloat64(latest_count) = baseline_median, 0.,
        mad > 0, round(0.6745 * (latest_count - baseline_median) / mad, 4),
        mean_ad > 0, round((latest_count - baseline_median) / (1.253314 * mean_ad), 4),
        sign(latest_count - baseline_median) * %[2]d.) AS z_score
FROM (
    SELECT entity_val, latest_count, baseline_median, n_buckets, latest_bucket%[3]s,
        arrayMap(x -> abs(x - baseline_median), hist) AS dev,
        arrayReduce('quantileExactWeightedInterpolated(0.5)', arrayPushBack(dev, baseline_median), arrayPushBack(arrayMap(x -> toUInt64(1), dev), zeros)) AS mad,
        (arraySum(dev) + zeros * baseline_median) / n_buckets AS mean_ad
    FROM (
        SELECT entity_val,
            %[4]s AS latest_bucket,
            sumIf(cnt, bucket = latest_bucket) AS latest_count,
            groupArrayIf(toFloat64(cnt), bucket < latest_bucket) AS hist,
            toUInt64(dateDiff('%[5]s', min(bucket), latest_bucket)) AS n_buckets,
            toUInt64(n_buckets - length(hist)) AS zeros,
            arrayReduce('quantileExactWeightedInterpolated(0.5)', arrayPushBack(hist, 0.), arrayPushBack(arrayMap(x -> toUInt64(1), hist), zeros)) AS baseline_median%[6]s
        FROM (
            SELECT entity_val, bucket, sum(event_count) AS cnt
            FROM %[7]s
            WHERE %[8]s AND bucket >= %[9]s AND bucket < %[10]s
            GROUP BY entity_val, bucket
        )
        GROUP BY entity_val
    )
)
WHERE n_buckets >= %[11]d`,
		daysOut, VolumeFlatZ, daysMid, w.latest, w.unit, daysAgg,
		source, scope, lower, w.upper, VolumeMinBuckets(minSample))
}

// volumeScoredKeysPredicate selects, on the model table under alias, exactly the
// entities VolumeScoredSQL scores with the default window: n_buckets >= min
// holds iff the entity has a bucket at least min buckets before the latest one.
func volumeScoredKeysPredicate(alias, timeBucket string, minSample int) string {
	w := volumeWindowFor(timeBucket)
	return fmt.Sprintf("%[1]s.bucket >= %[2]s AND %[1]s.bucket <= %[3]s - INTERVAL %[4]d %[5]s",
		alias, w.lower, w.latest, VolumeMinBuckets(minSample), w.unit)
}
