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
		ctx.Registry.Register("beacon_score", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("longconn_score", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("regularity_score", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("ts_score", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("ds_score", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("dur_score", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("hist_score", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("prevalence", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("prevalence_score", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("conn_count", FieldKindPerRow, "NULL", ctx.CmdIndex)
		ctx.Registry.Register("total_duration", FieldKindPerRow, "NULL", ctx.CmdIndex)
		return nil
	}

	// Register each output field as a model-lookup column: it exists only after the
	// JOIN wrap, so it resolves to its bare output name and conditions on it defer
	// to a post-join WHERE (see FieldKindJoined handling in conditions.go).
	reg := func(names ...string) {
		for _, n := range names {
			ctx.Registry.Register(n, FieldKindJoined, n, ctx.CmdIndex)
		}
	}
	switch info.ModelType {
	case "rarity":
		reg("percent", "confidence", "model_count")
	case "first_seen":
		reg("first_seen", "last_seen", "is_new")
	case "volume_baseline":
		reg("z_score", "baseline_median", "latest_count", "mad", "n_buckets")
	case "beacon":
		// beacon_score is the final verdict; the rest is the breakdown ("why").
		reg("beacon_score", "regularity_score", "ts_score", "ds_score", "dur_score", "hist_score", "prevalence", "prevalence_score", "conn_count")
	case "long_connection":
		reg("longconn_score", "total_duration", "conn_count", "prevalence", "prevalence_score")
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
		return fmt.Errorf("model %q not found: create it in the Models UI first", modelName)
	}

	// In prism context FractalIDs holds every member fractal: a model's table can
	// hold rows sourced from any member's logs (the model's MV scans all fractals
	// matching its filter), so scoring must scan every member, not just the one
	// the model happens to be registered under.
	fractalIDs := ctx.Opts.FractalIDs
	if len(fractalIDs) == 0 {
		fractalIDs = []string{ctx.Opts.FractalID}
	}

	switch info.ModelType {
	case "rarity":
		if len(keyFields) != 2 {
			return fmt.Errorf("model_lookup() for rarity models requires exactly 2 key fields: [partition_key, value_key]")
		}
		ctx.Plan.ModelLookupSQL = buildRarityScoringSQL(info.TableName, fractalIDs, info.MinSample)
		setModelLookupJoin(ctx, keyFields, []string{"partition_val", "value_val"})
		ctx.Plan.ModelLookupFields = []string{"model_count", "model_total", "percent", "confidence"}

	case "first_seen":
		ctx.Plan.ModelLookupSQL = buildFirstSeenScoringSQL(info.TableName, fractalIDs)
		setModelLookupJoin(ctx, keyFields, []string{"entity_key"})
		ctx.Plan.ModelLookupFields = []string{"first_seen", "last_seen", "event_count", "is_new"}

	case "volume_baseline":
		ctx.Plan.ModelLookupSQL = buildVolumeBaselineScoringSQL(info.TableName, fractalIDs, info.MinSample, info.TimeBucket)
		setModelLookupJoin(ctx, keyFields, []string{"entity_val"})
		ctx.Plan.ModelLookupFields = []string{"latest_count", "baseline_median", "mad", "n_buckets", "z_score"}

	case "beacon", "long_connection":
		if len(keyFields) != 3 {
			return fmt.Errorf("model_lookup() for %s models requires exactly 3 key fields: [src_ip, dst_ip, dst_port]", info.ModelType)
		}
		if info.ModelType == "beacon" {
			ctx.Plan.ModelLookupSQL = buildBeaconScoringSQL(info.TableName, fractalIDs)
			ctx.Plan.ModelLookupFields = []string{"beacon_score", "regularity_score", "ts_score", "ds_score", "dur_score", "hist_score", "prevalence", "prevalence_score", "conn_count"}
		} else {
			ctx.Plan.ModelLookupSQL = buildLongConnScoringSQL(info.TableName, fractalIDs)
			ctx.Plan.ModelLookupFields = []string{"longconn_score", "total_duration", "conn_count", "prevalence", "prevalence_score"}
		}
		// Positional key mapping: key[0]->src_ip, key[1]->dst_ip, key[2]->dst_port.
		setModelLookupJoin(ctx, keyFields, []string{"src_ip", "dst_ip", "dst_port"})

	default:
		return fmt.Errorf("unknown model type %q for model %q", info.ModelType, modelName)
	}

	return nil
}

// setModelLookupJoin projects the outer join keys as hidden `_mlk_k<i>` columns and
// builds the JOIN ON that matches them against the model-side key columns. The keys
// are projected in the source scan (where `fields.X` is available) by renderStandard
// and stripped from the result via EXCEPT in wrapWithModelLookup, so the enrichment
// works for any pipeline shape (bare, filtered, or with a trailing threshold).
func setModelLookupJoin(ctx *CommandContext, keyFields, rightCols []string) {
	keyExprs := make([]string, len(keyFields))
	leftParts := make([]string, len(keyFields))
	for i, kf := range keyFields {
		keyExprs[i] = modelLookupFieldRef(kf)
		leftParts[i] = fmt.Sprintf("_outer._mlk_k%d", i)
	}
	rightParts := make([]string, len(rightCols))
	for i, c := range rightCols {
		rightParts[i] = "_mlookup." + c
	}
	ctx.Plan.ModelLookupKeyExprs = keyExprs
	ctx.Plan.ModelLookupKeyFields = append([]string(nil), keyFields...)
	ctx.Plan.ModelLookupOn = concatModelKeys(leftParts) + " = " + concatModelKeys(rightParts)
}

// concatModelKeys joins key parts with the char(30) separator used throughout the
// model composite-key encoding. A single part needs no concat.
func concatModelKeys(parts []string) string {
	if len(parts) == 1 {
		return parts[0]
	}
	return "concat(" + strings.Join(parts, ", char(30), ") + ")"
}

// fractalIDInClause renders a `fractal_id IN (...)` predicate over one or more
// fractal IDs, each individually escaped.
func fractalIDInClause(fractalIDs []string) string {
	quoted := make([]string, len(fractalIDs))
	for i, id := range fractalIDs {
		quoted[i] = "'" + escapeString(id) + "'"
	}
	return "fractal_id IN (" + strings.Join(quoted, ", ") + ")"
}

// buildBeaconScoringSQL returns the latest scored row per pair from a beacon model's
// results table. FINAL collapses the ReplacingMergeTree to the newest scored_at.
// beacon_score is the final (modifier-adjusted) verdict; the subscores explain it.
func buildBeaconScoringSQL(tableName string, fractalIDs []string) string {
	return fmt.Sprintf(`SELECT src_ip, dst_ip, dst_port,
    final_score AS beacon_score,
    regularity_score, ts_score, ds_score, dur_score, hist_score,
    prevalence, prevalence_score, conn_count
FROM %s FINAL
WHERE %s`,
		"`"+tableName+"`",
		fractalIDInClause(fractalIDs),
	)
}

// buildLongConnScoringSQL returns the latest scored row per pair from a
// long_connection model's results table.
func buildLongConnScoringSQL(tableName string, fractalIDs []string) string {
	return fmt.Sprintf(`SELECT src_ip, dst_ip, dst_port,
    final_score AS longconn_score,
    total_duration, conn_count, prevalence, prevalence_score
FROM %s FINAL
WHERE %s`,
		"`"+tableName+"`",
		fractalIDInClause(fractalIDs),
	)
}

// buildRarityScoringSQL returns the triple-nested scoring subquery for a rarity model.
func buildRarityScoringSQL(tableName string, fractalIDs []string, minSample int) string {
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
        WHERE %s
        GROUP BY partition_val, value_val
    )
)
WHERE event_count >= %d`,
		"`"+tableName+"`",
		fractalIDInClause(fractalIDs),
		minSample,
	)
}

// buildFirstSeenScoringSQL returns the scoring subquery for a first_seen model.
// The aggregation and the derived is_new flag are split across two SELECT levels:
// computing is_new as if(min(first_seen) >= ...) in the SAME level as
// min(first_seen) AS first_seen makes the analyzer resolve the inner first_seen to
// the alias, yielding min(min(first_seen)) (nested aggregate, ClickHouse code 184).
func buildFirstSeenScoringSQL(tableName string, fractalIDs []string) string {
	// Two levels with NON-shadowing inner aliases (fs/ls/ec): shadowing the input
	// column names would make min(first_seen) AS first_seen + if(min(first_seen)...)
	// nest aggregates (code 184). The outer level derives is_new from the raw
	// DateTime column and stringifies the dates so the row scanner can read them
	// (DateTime64 -> *string is unsupported in the display scan path).
	return fmt.Sprintf(`SELECT entity_key,
    toString(fs) AS first_seen,
    toString(ls) AS last_seen,
    ec AS event_count,
    if(fs >= now() - INTERVAL 1 HOUR, '1', '0') AS is_new
FROM (
    SELECT entity_key,
        min(first_seen) AS fs,
        max(last_seen) AS ls,
        sum(event_count) AS ec
    FROM %s FINAL
    WHERE %s
    GROUP BY entity_key
)`,
		"`"+tableName+"`",
		fractalIDInClause(fractalIDs),
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
func buildVolumeBaselineScoringSQL(tableName string, fractalIDs []string, minBuckets int, timeBucket string) string {
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
            WHERE %s AND bucket >= %s AND bucket < %s
            GROUP BY entity_val, bucket
        )
        GROUP BY entity_val
    )
)
WHERE n_buckets >= %d`,
		"`"+tableName+"`",
		fractalIDInClause(fractalIDs),
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
	case "timestamp", normLogColumn, "log_id", "fractal_id", "ingest_timestamp", "normalizer":
		return field
	default:
		// The key is projected as _mlk_k<i> then concat/compared against the
		// model table's String key columns -- a non-skip-index context. Cast to
		// ::String so a Dynamic-stored path (pre-type-hint rows) still joins and
		// does not trigger ClickHouse error 44. No-op for concretely typed paths;
		// String content is identical, so match semantics are preserved.
		return groupableCast(jsonFieldRef(field))
	}
}

func init() {
	registerCommand(&modelLookupHandler{}, "model_lookup")
}
