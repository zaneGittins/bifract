package parser

import (
	"fmt"
	"strings"
)

// modelLookupHandler handles: model_lookup(model="name", key=[field1, field2])
//
// For rarity models it adds percent, confidence, model_count columns via a
// LEFT JOIN against a triple-nested scoring subquery over the model table.
// For first_seen models it adds first_seen, last_seen, is_new columns.
//
// The JOIN is applied at finalization via QueryPlan.wrapWithModelLookup().
type modelLookupHandler struct{}

func (h *modelLookupHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	modelName, _, err := parseModelLookupArgs(cmd.Arguments)
	if err != nil {
		return err
	}

	info, ok := ctx.Opts.Models[modelName]
	if !ok {
		// Model not found — register placeholder fields so downstream conditions
		// don't fail during the Declare phase. Execute will return a real error.
		ctx.Registry.Register("percent", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("confidence", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("model_count", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("first_seen", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("last_seen", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("is_new", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("z_score", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("baseline_median", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("latest_count", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("mad", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("n_buckets", FieldKindPerRow, "NULL", ctx.CmdIndex)
		return nil
	}

	switch info.ModelType {
	case "rarity":
		ctx.Registry.Register("percent", FieldKindPerRow, "_mlookup.percent", ctx.CmdIndex)
		ctx.Registry.Register("confidence", FieldKindPerRow, "_mlookup.confidence", ctx.CmdIndex)
		ctx.Registry.Register("model_count", FieldKindPerRow, "_mlookup.model_count", ctx.CmdIndex)
	case "first_seen":
		ctx.Registry.Register("first_seen", FieldKindPerRow, "_mlookup.first_seen", ctx.CmdIndex)
		ctx.Registry.Register("last_seen", FieldKindPerRow, "_mlookup.last_seen", ctx.CmdIndex)
		ctx.Registry.Register("is_new", FieldKindPerRow, "_mlookup.is_new", ctx.CmdIndex)
	case "volume_baseline":
		ctx.Registry.Register("z_score", FieldKindPerRow, "_mlookup.z_score", ctx.CmdIndex)
		ctx.Registry.Register("baseline_median", FieldKindPerRow, "_mlookup.baseline_median", ctx.CmdIndex)
		ctx.Registry.Register("latest_count", FieldKindPerRow, "_mlookup.latest_count", ctx.CmdIndex)
		ctx.Registry.Register("mad", FieldKindPerRow, "_mlookup.mad", ctx.CmdIndex)
		ctx.Registry.Register("n_buckets", FieldKindPerRow, "_mlookup.n_buckets", ctx.CmdIndex)
	}
	return nil
}

func (h *modelLookupHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if ctx.Plan.IsJoin {
		return fmt.Errorf("model_lookup() cannot be combined with join()")
	}
	if ctx.Plan.ModelLookupSQL != "" {
		return fmt.Errorf("model_lookup() cannot be used more than once")
	}

	modelName, keyFields, err := parseModelLookupArgs(cmd.Arguments)
	if err != nil {
		return err
	}
	if len(keyFields) == 0 {
		return fmt.Errorf("model_lookup() requires key=[field1, ...] parameter")
	}

	info, ok := ctx.Opts.Models[modelName]
	if !ok {
		return fmt.Errorf("model %q not found — create it in the Models UI first", modelName)
	}

	fractalID := ctx.Opts.FractalID
	if len(ctx.Opts.FractalIDs) > 0 {
		fractalID = ctx.Opts.FractalIDs[0]
	}

	switch info.ModelType {
	case "rarity":
		if len(keyFields) != 2 {
			return fmt.Errorf("model_lookup() for rarity models requires exactly 2 key fields: [partition_key, value_key]")
		}
		subSQL := buildRarityScoringSQL(info.TableName, fractalID, info.MinSample)
		outerPartRef := modelLookupFieldRef(keyFields[0])
		outerValRef := modelLookupFieldRef(keyFields[1])
		onClause := fmt.Sprintf("concat(%s, char(30), %s) = concat(_mlookup.partition_val, char(30), _mlookup.value_val)",
			outerPartRef, outerValRef)
		ctx.Plan.ModelLookupSQL = subSQL
		ctx.Plan.ModelLookupOn = onClause
		ctx.Plan.ModelLookupFields = []string{"partition_val", "value_val", "model_count", "model_total", "percent", "confidence"}

	case "first_seen":
		subSQL := buildFirstSeenScoringSQL(info.TableName, fractalID)
		var outerRefs []string
		for _, kf := range keyFields {
			outerRefs = append(outerRefs, modelLookupFieldRef(kf))
		}
		var onClause string
		if len(outerRefs) == 1 {
			onClause = fmt.Sprintf("%s = _mlookup.entity_key", outerRefs[0])
		} else {
			onClause = fmt.Sprintf("concat(%s) = _mlookup.entity_key", strings.Join(outerRefs, ", char(30), "))
		}
		ctx.Plan.ModelLookupSQL = subSQL
		ctx.Plan.ModelLookupOn = onClause
		ctx.Plan.ModelLookupFields = []string{"entity_key", "first_seen", "last_seen", "event_count", "is_new"}

	case "volume_baseline":
		subSQL := buildVolumeBaselineScoringSQL(info.TableName, fractalID, info.MinSample, info.TimeBucket)
		var outerRefs []string
		for _, kf := range keyFields {
			outerRefs = append(outerRefs, modelLookupFieldRef(kf))
		}
		var onClause string
		if len(outerRefs) == 1 {
			onClause = fmt.Sprintf("%s = _mlookup.entity_val", outerRefs[0])
		} else {
			onClause = fmt.Sprintf("concat(%s) = _mlookup.entity_val", strings.Join(outerRefs, ", char(30), "))
		}
		ctx.Plan.ModelLookupSQL = subSQL
		ctx.Plan.ModelLookupOn = onClause
		ctx.Plan.ModelLookupFields = []string{"entity_val", "latest_count", "baseline_median", "mad", "n_buckets", "z_score"}

	default:
		return fmt.Errorf("unknown model type %q for model %q", info.ModelType, modelName)
	}

	return nil
}

// buildRarityScoringSQL returns the triple-nested scoring subquery for a rarity model.
func buildRarityScoringSQL(tableName, fractalID string, minSample int) string {
	if minSample < 1 {
		minSample = 1
	}
	return fmt.Sprintf(`SELECT partition_val, value_val,
    event_count AS model_count,
    _total AS model_total,
    round(event_count / _total * 100.0, 4) AS percent,
    round(((_total - _unique) / _total) * 0.95, 4) AS confidence
FROM (
    SELECT partition_val, value_val, event_count,
        sum(event_count) OVER (PARTITION BY partition_val) AS _total,
        uniqExact(value_val) OVER (PARTITION BY partition_val) AS _unique
    FROM (
        SELECT partition_val, value_val, sum(event_count) AS event_count
        FROM %s FINAL
        WHERE fractal_id = '%s'
        GROUP BY partition_val, value_val
    )
)
WHERE event_count >= %d`,
		"`"+tableName+"`",
		escapeString(fractalID),
		minSample,
	)
}

// buildFirstSeenScoringSQL returns the scoring subquery for a first_seen model.
func buildFirstSeenScoringSQL(tableName, fractalID string) string {
	return fmt.Sprintf(`SELECT entity_key,
    min(first_seen) AS first_seen,
    max(last_seen) AS last_seen,
    sum(event_count) AS event_count,
    if(min(first_seen) >= now() - INTERVAL 1 HOUR, '1', '0') AS is_new
FROM %s FINAL
WHERE fractal_id = '%s'
GROUP BY entity_key`,
		"`"+tableName+"`",
		escapeString(fractalID),
	)
}

// volumeScoreBounds returns (lowerBound, upperBound) predicates on the bucket
// column. The upper bound excludes the current incomplete bucket; the lower bound
// caps history so reads stay bounded. Mirrors models.volumeScoreBounds.
func volumeScoreBounds(timeBucket string) (lower, upper string) {
	if timeBucket == "hour" {
		return "toStartOfHour(now()) - INTERVAL 30 DAY", "toStartOfHour(now())"
	}
	return "today() - 90", "today()"
}

// buildVolumeBaselineScoringSQL returns the per-entity modified z-score subquery
// for a volume_baseline model, joined against incoming logs on the entity field.
// It mirrors models.buildVolumeBaselineScoringSQL: baseline = median of complete
// daily counts, MAD = median absolute deviation, z = 0.6745*(latest-median)/MAD
// with the mad=0 -> z=0 guard.
func buildVolumeBaselineScoringSQL(tableName, fractalID string, minBuckets int, timeBucket string) string {
	if minBuckets < 1 {
		minBuckets = 7
	}
	lower, upper := volumeScoreBounds(timeBucket)
	return fmt.Sprintf(`SELECT entity_val, latest_count, baseline_median, mad, n_buckets, latest_bucket,
    if(mad = 0, 0, round(0.6745 * (toFloat64(latest_count) - baseline_median) / mad, 4)) AS z_score
FROM (
    SELECT entity_val, latest_count, baseline_median, n_buckets, latest_bucket,
        arrayReduce('medianExact', arrayMap(x -> abs(toFloat64(x) - baseline_median), cnts)) AS mad
    FROM (
        SELECT entity_val,
            groupArray(daily_count) AS cnts,
            arrayReduce('medianExact', groupArray(daily_count)) AS baseline_median,
            argMax(daily_count, bucket) AS latest_count,
            max(bucket) AS latest_bucket,
            count() AS n_buckets
        FROM (
            SELECT entity_val, bucket, sum(event_count) AS daily_count
            FROM %s FINAL
            WHERE fractal_id = '%s' AND bucket >= %s AND bucket < %s
            GROUP BY entity_val, bucket
        )
        GROUP BY entity_val
    )
)
WHERE n_buckets >= %d`,
		"`"+tableName+"`",
		escapeString(fractalID),
		lower, upper, minBuckets,
	)
}

// parseModelLookupArgs parses model= and key=[] arguments.
func parseModelLookupArgs(args []string) (modelName string, keyFields []string, err error) {
	for _, arg := range args {
		if strings.HasPrefix(arg, "model=") {
			modelName = strings.Trim(strings.TrimPrefix(arg, "model="), `"'`)
		} else if strings.HasPrefix(arg, "key=") {
			val := strings.TrimPrefix(arg, "key=")
			val = strings.Trim(val, "[]")
			for _, f := range strings.Split(val, ",") {
				f = strings.TrimSpace(f)
				if f != "" {
					keyFields = append(keyFields, f)
				}
			}
		}
	}
	if modelName == "" {
		return "", nil, fmt.Errorf("model_lookup() requires model= parameter")
	}
	return modelName, keyFields, nil
}

// resolveFieldRef converts a user field name to a ClickHouse expression reference.
// Extraction outputs (already produced by prior pipeline steps) are referenced directly.
// Standard log fields are referenced via the JSON sub-column.
func modelLookupFieldRef(field string) string {
	switch field {
	case "timestamp", "raw_log", "log_id", "fractal_id", "ingest_timestamp":
		return field
	default:
		return jsonFieldRef(field)
	}
}

func init() {
	registerCommand(&modelLookupHandler{}, "model_lookup")
}
