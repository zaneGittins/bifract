package models

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var namedGroupRe = regexp.MustCompile(`\(\?(?:P?<[a-zA-Z_][a-zA-Z0-9_]*>|P'[a-zA-Z_][a-zA-Z0-9_]*')`)

// extractPattern strips named capture group syntax from a pattern so that
// ClickHouse's extract() function receives a plain positional capture group.
// (?<name>...) and (?P<name>...) become (...).
func extractPattern(pattern string) string {
	return namedGroupRe.ReplaceAllString(pattern, "(")
}

// aggOpts tunes how the model aggregation SELECT is built. It exists so the
// preview path can reproduce the day-chunked backfill exactly without changing
// the byte-stable MV/backfill output (which always uses the zero value).
type aggOpts struct {
	// dayBucket adds toDate(timestamp) to the rarity GROUP BY. The live backfill
	// runs one INSERT per UTC day, so a rarity pair's event_count equals the
	// number of distinct days it appeared. A single-pass preview scan would
	// instead collapse to event_count=1 (confidence 0 for everything); bucketing
	// by day reproduces the per-day chunking so preview == post-backfill output.
	// No effect on first_seen/volume_baseline, whose aggregates are day-invariant.
	dayBucket bool
}

// GenerateDDL returns (createTableSQL, createMVSQL) for the given model definition.
func GenerateDDL(def ModelDefinition, mt ModelType, tableName, mvName string) (string, string, error) {
	tableSQL, err := generateTableDDL(def, mt, tableName)
	if err != nil {
		return "", "", err
	}
	mvSQL, err := generateMVDDL(def, mt, tableName, mvName)
	if err != nil {
		return "", "", err
	}
	return tableSQL, mvSQL, nil
}

func generateTableDDL(def ModelDefinition, mt ModelType, tableName string) (string, error) {
	switch mt {
	case ModelTypeRarity:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    fractal_id    LowCardinality(String),
    partition_val String,
    value_val     String,
    event_count   SimpleAggregateFunction(sum, UInt64),
    days          AggregateFunction(groupUniqArray(365), Date)
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, partition_val, value_val)
SETTINGS index_granularity = 8192`, tableName), nil

	case ModelTypeFirstSeen:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    fractal_id  LowCardinality(String),
    entity_key  String,
    first_seen  SimpleAggregateFunction(min, DateTime64(3)),
    last_seen   SimpleAggregateFunction(max, DateTime64(3)),
    event_count SimpleAggregateFunction(sum, UInt64),
    days        AggregateFunction(groupUniqArray(365), Date)
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, entity_key)
SETTINGS index_granularity = 8192`, tableName), nil

	case ModelTypeVolumeBaseline:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    fractal_id  LowCardinality(String),
    entity_val  String,
    bucket      %s,
    event_count SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, entity_val, bucket)
SETTINGS index_granularity = 8192`, tableName, volumeBucketColType(def.TimeBucket)), nil

	default:
		return "", fmt.Errorf("unknown model type: %s", mt)
	}
}

// rarityDayGroupBy returns the extra GROUP BY term that splits a rarity
// aggregation by UTC day when opts.dayBucket is set. This makes a single-pass
// preview scan emit one row per (partition, value, day) with event_count=1,
// reproducing the day-chunked backfill (whose event_count counts distinct days).
func rarityDayGroupBy(opts aggOpts) string {
	if opts.dayBucket {
		return ", toDate(timestamp)"
	}
	return ""
}

// volumeBucketExpr returns the ClickHouse expression that buckets a log's parsed
// timestamp for a volume_baseline model.
func volumeBucketExpr(timeBucket string) string {
	if timeBucket == "hour" {
		return "toStartOfHour(timestamp)"
	}
	return "toDate(timestamp)"
}

// volumeBucketColType returns the CH column type for the bucket key.
func volumeBucketColType(timeBucket string) string {
	if timeBucket == "hour" {
		return "DateTime"
	}
	return "Date"
}

// volumeScoreBounds returns (lowerBound, upperBound) predicates on the bucket
// column for read-time scoring. The upper bound excludes the current, still
// incomplete bucket (whose count is artificially low); the lower bound caps how
// much history is read so scoring stays bounded at scale.
func volumeScoreBounds(timeBucket string) (lower, upper string) {
	if timeBucket == "hour" {
		return "toStartOfHour(now()) - INTERVAL 30 DAY", "toStartOfHour(now())"
	}
	return "today() - 90", "today()"
}

func generateMVDDL(def ModelDefinition, mt ModelType, tableName, mvName string) (string, error) {
	// Build the SELECT body using CTE chains. The MV reads from the local `logs`
	// table with no extra predicate; this output must remain byte-for-byte stable.
	selectSQL, err := buildModelSelect(def, mt, "logs", "", aggOpts{})
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS
%s`, mvName, tableName, selectSQL), nil
}

// BuildBackfillInsert returns a full `INSERT INTO <targetTable> <select>` that
// seeds a model from historical logs. It reuses the exact SELECT logic of the
// materialized view, but reads from sourceTable (the distributed logs table in
// cluster mode) and ANDs an extra predicate (the time window + ingest_timestamp
// dedup boundary) into the source filter.
//
// IMPORTANT: this performs NO DDL. It only inserts into an already-existing
// model table, so it can never orphan a table or materialized view.
func BuildBackfillInsert(def ModelDefinition, mt ModelType, targetTable, sourceTable, whereExtra string) (string, error) {
	selectSQL, err := buildModelSelect(def, mt, sourceTable, whereExtra, aggOpts{})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("INSERT INTO %s\n%s", targetTable, selectSQL), nil
}

// buildModelSelect builds the SELECT ... FROM <sourceTable> ... GROUP BY ...
// shared by the materialized view (sourceTable="logs", whereExtra="") and the
// backfill INSERT...SELECT (distributed source + time-window predicate).
// whereExtra, when non-empty, is ANDed into the source-scan WHERE clause.
func buildModelSelect(def ModelDefinition, mt ModelType, sourceTable, whereExtra string, opts aggOpts) (string, error) {
	var b strings.Builder

	// CTE chain for extractions
	if len(def.Extractions) > 0 {
		b.WriteString("WITH\n")
		// base CTE: filter + initial field selection
		b.WriteString("base AS (\n")
		b.WriteString("    SELECT fractal_id, timestamp")
		// Collect all fields referenced in extractions, aliased so downstream CTEs
		// can reference them by plain name (fields.X AS X) rather than via JSON traversal.
		seen := map[string]bool{}
		for _, ext := range def.Extractions {
			if !isExtractionOutput(ext.FromField, def.Extractions) && !seen[ext.FromField] {
				seen[ext.FromField] = true
				b.WriteString(fmt.Sprintf(", %s AS %s", chFieldRef(ext.FromField), ext.FromField))
			}
		}
		b.WriteString(fmt.Sprintf("\n    FROM %s\n", sourceTable))
		b.WriteString("    WHERE fractal_id != ''")
		for _, fc := range def.Filter {
			b.WriteString(fmt.Sprintf("\n    AND %s", filterConditionToSQL(fc)))
		}
		if whereExtra != "" {
			b.WriteString(fmt.Sprintf("\n    AND %s", whereExtra))
		}
		b.WriteString("\n),\n")

		// Extraction CTEs — reference columns by plain name since base CTE aliased them.
		prevCTE := "base"
		for i, ext := range def.Extractions {
			cteName := fmt.Sprintf("e%d", i)
			fromRef := ext.FromField
			sqlPat := chStringLiteral(extractPattern(ext.Pattern))
			b.WriteString(fmt.Sprintf("%s AS (\n", cteName))
			b.WriteString(fmt.Sprintf("    SELECT *, extract(%s, %s) AS %s\n",
				fromRef, sqlPat, ext.OutputField))
			b.WriteString(fmt.Sprintf("    FROM %s\n", prevCTE))
			b.WriteString(fmt.Sprintf("    WHERE extract(%s, %s) != ''",
				fromRef, sqlPat))
			if ext.MinLength > 0 {
				b.WriteString(fmt.Sprintf("\n    AND length(extract(%s, %s)) >= %d",
					fromRef, sqlPat, ext.MinLength))
			}
			if ext.Lowercase {
				// Rewrite the output field as lowercased in a sub-expression
				// by wrapping: we'll apply lower() in the final select projection
			}
			b.WriteString("\n),\n")
			prevCTE = cteName
		}
		// Remove trailing comma+newline and close WITH block
		sql := b.String()
		sql = strings.TrimSuffix(strings.TrimSuffix(sql, "\n"), ",")
		b.Reset()
		b.WriteString(sql)
		b.WriteString("\n")

		// Final SELECT from last CTE
		b.WriteString(buildFinalSelect(def, mt, prevCTE, opts))
	} else {
		// No extractions — SELECT directly from the source table
		b.WriteString(buildDirectSelect(def, mt, sourceTable, whereExtra, opts))
	}

	return b.String(), nil
}

// buildFinalSelect builds the final GROUP BY SELECT from the last CTE.
func buildFinalSelect(def ModelDefinition, mt ModelType, fromTable string, opts aggOpts) string {
	var b strings.Builder
	switch mt {
	case ModelTypeRarity:
		partRef := def.PartitionKey
		valRef := def.ValueKey
		b.WriteString(fmt.Sprintf("SELECT fractal_id,\n"))
		b.WriteString(fmt.Sprintf("    %s AS partition_val,\n", applyLowerIfNeeded(partRef, def.Extractions)))
		b.WriteString(fmt.Sprintf("    %s AS value_val,\n", applyLowerIfNeeded(valRef, def.Extractions)))
		b.WriteString("    toUInt64(1) AS event_count,\n")
		b.WriteString("    groupUniqArrayState(365)(toDate(timestamp)) AS days\n")
		b.WriteString(fmt.Sprintf("FROM %s\n", fromTable))
		b.WriteString(fmt.Sprintf("WHERE %s != '' AND %s != ''\n", partRef, valRef))
		b.WriteString(fmt.Sprintf("GROUP BY fractal_id, partition_val, value_val%s", rarityDayGroupBy(opts)))
	case ModelTypeFirstSeen:
		b.WriteString("SELECT fractal_id,\n")
		if len(def.KeyFields) == 1 {
			b.WriteString(fmt.Sprintf("    %s AS entity_key,\n", applyLowerIfNeeded(def.KeyFields[0], def.Extractions)))
		} else {
			parts := make([]string, len(def.KeyFields))
			for i, kf := range def.KeyFields {
				parts[i] = applyLowerIfNeeded(kf, def.Extractions)
			}
			b.WriteString(fmt.Sprintf("    concat(%s) AS entity_key,\n", strings.Join(parts, ", char(30), ")))
		}
		b.WriteString("    timestamp AS first_seen,\n")
		b.WriteString("    timestamp AS last_seen,\n")
		b.WriteString("    toUInt64(1) AS event_count,\n")
		b.WriteString("    groupUniqArrayState(365)(toDate(timestamp)) AS days\n")
		b.WriteString(fmt.Sprintf("FROM %s\n", fromTable))
		if len(def.KeyFields) > 0 {
			guards := make([]string, len(def.KeyFields))
			for i, kf := range def.KeyFields {
				guards[i] = fmt.Sprintf("%s != ''", kf)
			}
			b.WriteString(fmt.Sprintf("WHERE %s\n", strings.Join(guards, " AND ")))
		}
		b.WriteString("GROUP BY fractal_id, entity_key, first_seen, last_seen")
	case ModelTypeVolumeBaseline:
		b.WriteString("SELECT fractal_id,\n")
		if len(def.KeyFields) == 1 {
			b.WriteString(fmt.Sprintf("    %s AS entity_val,\n", applyLowerIfNeeded(def.KeyFields[0], def.Extractions)))
		} else {
			parts := make([]string, len(def.KeyFields))
			for i, kf := range def.KeyFields {
				parts[i] = applyLowerIfNeeded(kf, def.Extractions)
			}
			b.WriteString(fmt.Sprintf("    concat(%s) AS entity_val,\n", strings.Join(parts, ", char(30), ")))
		}
		b.WriteString(fmt.Sprintf("    %s AS bucket,\n", volumeBucketExpr(def.TimeBucket)))
		b.WriteString("    toUInt64(count()) AS event_count\n")
		b.WriteString(fmt.Sprintf("FROM %s\n", fromTable))
		if len(def.KeyFields) > 0 {
			guards := make([]string, len(def.KeyFields))
			for i, kf := range def.KeyFields {
				guards[i] = fmt.Sprintf("%s != ''", kf)
			}
			b.WriteString(fmt.Sprintf("WHERE %s\n", strings.Join(guards, " AND ")))
		}
		b.WriteString("GROUP BY fractal_id, entity_val, bucket")
	}
	return b.String()
}

// buildDirectSelect builds a SELECT directly from the source table (no extractions).
// whereExtra, when non-empty, is ANDed into the WHERE clause.
func buildDirectSelect(def ModelDefinition, mt ModelType, sourceTable, whereExtra string, opts aggOpts) string {
	var b strings.Builder
	b.WriteString("SELECT fractal_id")

	switch mt {
	case ModelTypeRarity:
		b.WriteString(fmt.Sprintf(",\n    %s AS partition_val,\n    %s AS value_val,\n    toUInt64(1) AS event_count,\n    groupUniqArrayState(365)(toDate(timestamp)) AS days\n",
			chFieldRef(def.PartitionKey), chFieldRef(def.ValueKey)))
		b.WriteString(fmt.Sprintf("FROM %s\n", sourceTable))
		b.WriteString("WHERE fractal_id != ''")
		for _, fc := range def.Filter {
			b.WriteString(fmt.Sprintf("\nAND %s", filterConditionToSQL(fc)))
		}
		if whereExtra != "" {
			b.WriteString(fmt.Sprintf("\nAND %s", whereExtra))
		}
		b.WriteString(fmt.Sprintf("\nAND %s != '' AND %s != ''\n", chFieldRef(def.PartitionKey), chFieldRef(def.ValueKey)))
		b.WriteString(fmt.Sprintf("GROUP BY fractal_id, partition_val, value_val%s", rarityDayGroupBy(opts)))
	case ModelTypeFirstSeen:
		if len(def.KeyFields) == 1 {
			b.WriteString(fmt.Sprintf(",\n    %s AS entity_key", chFieldRef(def.KeyFields[0])))
		} else {
			parts := make([]string, len(def.KeyFields))
			for i, kf := range def.KeyFields {
				parts[i] = chFieldRef(kf)
			}
			b.WriteString(fmt.Sprintf(",\n    concat(%s) AS entity_key", strings.Join(parts, ", char(30), ")))
		}
		b.WriteString(",\n    timestamp AS first_seen,\n    timestamp AS last_seen,\n    toUInt64(1) AS event_count,\n    groupUniqArrayState(365)(toDate(timestamp)) AS days\n")
		b.WriteString(fmt.Sprintf("FROM %s\n", sourceTable))
		b.WriteString("WHERE fractal_id != ''")
		for _, fc := range def.Filter {
			b.WriteString(fmt.Sprintf("\nAND %s", filterConditionToSQL(fc)))
		}
		if whereExtra != "" {
			b.WriteString(fmt.Sprintf("\nAND %s", whereExtra))
		}
		b.WriteString("\nGROUP BY fractal_id, entity_key, first_seen, last_seen")
	case ModelTypeVolumeBaseline:
		if len(def.KeyFields) == 1 {
			b.WriteString(fmt.Sprintf(",\n    %s AS entity_val", chFieldRef(def.KeyFields[0])))
		} else {
			parts := make([]string, len(def.KeyFields))
			for i, kf := range def.KeyFields {
				parts[i] = chFieldRef(kf)
			}
			b.WriteString(fmt.Sprintf(",\n    concat(%s) AS entity_val", strings.Join(parts, ", char(30), ")))
		}
		b.WriteString(fmt.Sprintf(",\n    %s AS bucket,\n    toUInt64(count()) AS event_count\n", volumeBucketExpr(def.TimeBucket)))
		b.WriteString(fmt.Sprintf("FROM %s\n", sourceTable))
		b.WriteString("WHERE fractal_id != ''")
		for _, fc := range def.Filter {
			b.WriteString(fmt.Sprintf("\nAND %s", filterConditionToSQL(fc)))
		}
		if whereExtra != "" {
			b.WriteString(fmt.Sprintf("\nAND %s", whereExtra))
		}
		for _, kf := range def.KeyFields {
			b.WriteString(fmt.Sprintf("\nAND %s != ''", chFieldRef(kf)))
		}
		b.WriteString("\nGROUP BY fractal_id, entity_val, bucket")
	}
	return b.String()
}

// chFieldRef converts a user-facing field name to a ClickHouse expression.
// Known log columns are referenced directly. JSON sub-columns get ::String so
// match() / extract() receive a concrete String type rather than Dynamic.
func chFieldRef(field string) string {
	switch field {
	case "timestamp", "raw_log", "log_id", "fractal_id", "ingest_timestamp":
		return field
	default:
		return "fields.`" + field + "`::String"
	}
}

// chNumericFieldRef references a field coerced to Float64 for aggregation. Network
// numeric fields (duration, bytes) are stored as String sub-columns, so an explicit
// toFloat64OrZero is required; non-numeric or absent values collapse to 0.
func chNumericFieldRef(field string) string {
	return fmt.Sprintf("toFloat64OrZero(%s)", chFieldRef(field))
}

// isExtractionOutput returns true if fieldName is produced by a prior extraction step.
func isExtractionOutput(fieldName string, steps []ExtractionStep) bool {
	for _, s := range steps {
		if s.OutputField == fieldName {
			return true
		}
	}
	return false
}

// applyLowerIfNeeded wraps fieldName with lower() if the extraction step that
// produces it has Lowercase=true.
func applyLowerIfNeeded(fieldName string, steps []ExtractionStep) string {
	for _, s := range steps {
		if s.OutputField == fieldName && s.Lowercase {
			return fmt.Sprintf("lower(%s)", fieldName)
		}
	}
	return fieldName
}

// filterConditionToSQL converts a FilterCondition to a ClickHouse WHERE expression.
func filterConditionToSQL(fc FilterCondition) string {
	ref := chFieldRef(fc.Field)
	switch fc.Op {
	case "=":
		return fmt.Sprintf("%s = %s", ref, chStringLiteral(fc.Value))
	case "!=":
		return fmt.Sprintf("%s != %s", ref, chStringLiteral(fc.Value))
	case "~":
		return fmt.Sprintf("match(%s, %s)", ref, chStringLiteral(fc.Value))
	case "!~":
		return fmt.Sprintf("NOT match(%s, %s)", ref, chStringLiteral(fc.Value))
	case "cidr":
		return cidrExpr(ref, fc.Value)
	case "!cidr":
		return "NOT " + cidrExpr(ref, fc.Value)
	default:
		return fmt.Sprintf("%s = %s", ref, chStringLiteral(fc.Value))
	}
}

func cidrExpr(fieldRef, subnet string) string {
	valid := fmt.Sprintf("(isIPv4String(%[1]s) OR isIPv6String(%[1]s))", fieldRef)
	safe := fmt.Sprintf("if(%s, %s, '0.0.0.0')", valid, fieldRef)
	return fmt.Sprintf("(isIPAddressInRange(%s, %s) AND %s)", safe, chStringLiteral(subnet), valid)
}

// chStringLiteral escapes a string for use in ClickHouse SQL as a single-quoted literal.
func chStringLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// ---------------------------------------------------------------------------
// Network analysis models (beacon / long_connection)
//
// A scheduled model owns three ClickHouse objects:
//   - state table   (AggregatingMergeTree, per (fractal,src,dst,port,day), TTL window)
//   - materialized view (logs -> state, maintained at ingest)
//   - results table (ReplacingMergeTree, written by the background scorer)
// The scorer reads only the compact state, never raw logs.
// ---------------------------------------------------------------------------

// netStateArrayCap bounds the per-day-per-pair timestamp/size arrays. Over a 14-day
// window this merges to ~28k points max, far more than enough to measure regularity
// while hard-bounding both ClickHouse and Go memory.
const netStateArrayCap = 2000

// BuildNetStateTableDDL returns the AggregatingMergeTree state table DDL. windowDays
// drives the TTL (retain enough for the largest window plus a small buffer) so a
// short-window model does not retain more state than it reads.
func BuildNetStateTableDDL(stateTable string, windowDays int) string {
	ttlDays := windowDays + 2
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    fractal_id LowCardinality(String),
    src        String,
    dst        String,
    port       String,
    day        Date,
    conn_count SimpleAggregateFunction(sum, UInt64),
    ts_state   AggregateFunction(groupArray(%d), UInt32),
    size_state AggregateFunction(groupArray(%d), Float64),
    dur_sum    SimpleAggregateFunction(sum, Float64),
    first_ts   SimpleAggregateFunction(min, DateTime64(3)),
    last_ts    SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, src, dst, port, day)
TTL day + INTERVAL %d DAY
SETTINGS index_granularity = 8192`, stateTable, netStateArrayCap, netStateArrayCap, ttlDays)
}

// BuildNetResultsTableDDL returns the ReplacingMergeTree results table DDL. The
// scorer inserts fresh rows each pass; reads take the latest via FINAL/argMax. The
// modifier columns (prevalence/first_seen/threat_intel) carry the full breakdown so
// the reviewer can see why a pair scored high; first_seen/threat_intel are reserved
// (0 until wired) and cost nothing to keep the schema stable.
func BuildNetResultsTableDDL(resultsTable string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    fractal_id         LowCardinality(String),
    src_ip             String,
    dst_ip             String,
    dst_port           String,
    regularity_score   Float64,
    ts_score           Float64,
    ds_score           Float64,
    dur_score          Float64,
    hist_score         Float64,
    prevalence         Float64,
    prevalence_total   UInt64,
    prevalence_score   Float64,
    first_seen_score   Float64,
    threat_intel_score Float64,
    final_score        Float64,
    conn_count         UInt64,
    total_duration     Float64,
    first_seen         DateTime64(3),
    last_seen          DateTime64(3),
    scored_at          DateTime64(3)
) ENGINE = ReplacingMergeTree(scored_at)
ORDER BY (fractal_id, src_ip, dst_ip, dst_port)
SETTINGS index_granularity = 8192`, resultsTable)
}

// netFieldMap resolves the model's network field map with defaults applied.
func netFieldMap(def ModelDefinition) NetworkFieldMap {
	return def.Network.WithDefaults()
}

// BuildNetStateMV returns the materialized view that maintains per-pair-per-day
// aggregation state at ingest. The model's filter is applied here so the state
// reflects the model's own scope.
func BuildNetStateMV(def ModelDefinition, mt ModelType, stateTable, mvName string) (string, error) {
	nf := netFieldMap(def)
	src := chFieldRef(nf.SrcField)
	dst := chFieldRef(nf.DstField)
	port := chFieldRef(nf.PortField)
	bytes := chNumericFieldRef(nf.BytesField)
	dur := chNumericFieldRef(nf.DurationField)

	var b strings.Builder
	b.WriteString(fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS\n", mvName, stateTable))
	b.WriteString("SELECT fractal_id,\n")
	b.WriteString(fmt.Sprintf("    %s AS src,\n", src))
	b.WriteString(fmt.Sprintf("    %s AS dst,\n", dst))
	b.WriteString(fmt.Sprintf("    %s AS port,\n", port))
	b.WriteString("    toDate(timestamp) AS day,\n")
	b.WriteString("    toUInt64(count()) AS conn_count,\n")
	b.WriteString(fmt.Sprintf("    groupArrayState(%d)(toUnixTimestamp(timestamp)) AS ts_state,\n", netStateArrayCap))
	b.WriteString(fmt.Sprintf("    groupArrayState(%d)(%s) AS size_state,\n", netStateArrayCap, bytes))
	b.WriteString(fmt.Sprintf("    sum(%s) AS dur_sum,\n", dur))
	b.WriteString("    min(timestamp) AS first_ts,\n")
	b.WriteString("    max(timestamp) AS last_ts\n")
	b.WriteString("FROM logs\n")
	b.WriteString(fmt.Sprintf("WHERE fractal_id != '' AND %s != '' AND %s != ''", src, dst))
	for _, fc := range def.Filter {
		b.WriteString(fmt.Sprintf("\n    AND %s", filterConditionToSQL(fc)))
	}
	b.WriteString("\nGROUP BY fractal_id, src, dst, port, day")
	return b.String(), nil
}

// fractalScopeClause returns the WHERE predicate scoping a state read to a fractal.
func fractalScopeClause(fractalID string) string {
	return fmt.Sprintf("fractal_id = %s", chStringLiteral(fractalID))
}

// BuildNetScoreReadQuery returns the query the scorer runs each pass over the state
// table (never raw logs). It reads only the recent day-buckets and pre-filters to
// qualifying pairs so the heavy scoring in Go runs over a bounded set.
func BuildNetScoreReadQuery(def ModelDefinition, mt ModelType, stateTable, fractalID string, windowDays int) string {
	var having string
	if mt == ModelTypeLongConnection {
		lc := def.LongConn.WithDefaults()
		having = fmt.Sprintf("HAVING total_duration >= %s", chFloatLiteral(lc.BaseSeconds))
	} else {
		windowSecs := int64(windowDays) * 86400
		bp := def.Beacon.WithDefaults(windowSecs)
		having = fmt.Sprintf("HAVING cnt >= %d AND cnt < %d", bp.MinConnections, bp.StrobeLimit)
	}
	// The Merge combinator must carry the same parameter as the stored parametric
	// groupArray(N) state, or ClickHouse rejects it (code 43). first_ts/last_ts are
	// intentionally omitted: the scorer derives the observed range from the merged,
	// window-trimmed timestamps, and scanning the SimpleAggregateFunction(min/max,
	// DateTime64) result columns is not supported by the row scanner.
	return fmt.Sprintf(`SELECT src, dst, port,
    sum(conn_count) AS cnt,
    groupArrayMerge(%d)(ts_state) AS ts_list,
    groupArrayMerge(%d)(size_state) AS size_list,
    sum(dur_sum) AS total_duration
FROM %s
WHERE %s AND day >= today() - %d
GROUP BY src, dst, port
%s`, netStateArrayCap, netStateArrayCap, stateTable, fractalScopeClause(fractalID), windowDays, having)
}

// BuildNetPrevalenceQuery returns the per-destination distinct-source counts over the
// window, used to rerank scores by prevalence (a ubiquitous destination is likely a
// shared service; a single-host destination is more interesting).
func BuildNetPrevalenceQuery(stateTable, fractalID string, windowDays int) string {
	return fmt.Sprintf(`SELECT dst, uniqExact(src) AS prev_total
FROM %s
WHERE %s AND day >= today() - %d
GROUP BY dst`, stateTable, fractalScopeClause(fractalID), windowDays)
}

// BuildNetNetworkSizeQuery returns the distinct-source count over the window, the
// denominator for the prevalence ratio.
func BuildNetNetworkSizeQuery(stateTable, fractalID string, windowDays int) string {
	return fmt.Sprintf(`SELECT uniqExact(src) AS network_size
FROM %s
WHERE %s AND day >= today() - %d`, stateTable, fractalScopeClause(fractalID), windowDays)
}

// BuildNetPreviewAgg returns a one-off aggregation over raw logs for the model
// builder preview, before any state table exists. It uses the same projection as the
// state read (per-pair cnt/ts_list/size_list/duration) but groups directly over the
// source table within the preview window, so the previewed scores match the warmed
// model. sourceTable is the (distributed or local) logs table.
func BuildNetPreviewAgg(def ModelDefinition, mt ModelType, sourceTable, fractalID string, windowDays int) (string, error) {
	nf := netFieldMap(def)
	src := chFieldRef(nf.SrcField)
	dst := chFieldRef(nf.DstField)
	port := chFieldRef(nf.PortField)
	bytes := chNumericFieldRef(nf.BytesField)
	dur := chNumericFieldRef(nf.DurationField)

	var having string
	if mt == ModelTypeLongConnection {
		lc := def.LongConn.WithDefaults()
		having = fmt.Sprintf("HAVING total_duration >= %s", chFloatLiteral(lc.BaseSeconds))
	} else {
		windowSecs := int64(windowDays) * 86400
		bp := def.Beacon.WithDefaults(windowSecs)
		having = fmt.Sprintf("HAVING cnt >= %d AND cnt < %d", bp.MinConnections, bp.StrobeLimit)
	}

	var b strings.Builder
	b.WriteString("SELECT\n")
	b.WriteString(fmt.Sprintf("    %s AS src, %s AS dst, %s AS port,\n", src, dst, port))
	b.WriteString("    toUInt64(count()) AS cnt,\n")
	b.WriteString(fmt.Sprintf("    groupArray(%d)(toUnixTimestamp(timestamp)) AS ts_list,\n", netStateArrayCap))
	b.WriteString(fmt.Sprintf("    groupArray(%d)(%s) AS size_list,\n", netStateArrayCap, bytes))
	b.WriteString(fmt.Sprintf("    sum(%s) AS total_duration\n", dur))
	b.WriteString(fmt.Sprintf("FROM %s\n", sourceTable))
	b.WriteString(fmt.Sprintf("WHERE %s AND %s != '' AND %s != '' AND timestamp >= now() - INTERVAL %d DAY", fractalScopeClause(fractalID), src, dst, windowDays))
	for _, fc := range def.Filter {
		b.WriteString(fmt.Sprintf("\n    AND %s", filterConditionToSQL(fc)))
	}
	b.WriteString("\nGROUP BY src, dst, port\n")
	b.WriteString(having)
	return b.String(), nil
}

// BuildNetPreviewPrevalence returns the per-destination distinct-source counts over
// raw logs for the preview window (no state table exists yet). It omits the pair
// HAVING so prevalence counts ALL sources, not only qualifying pairs.
func BuildNetPreviewPrevalence(def ModelDefinition, sourceTable, fractalID string, windowDays int) string {
	nf := netFieldMap(def)
	src := chFieldRef(nf.SrcField)
	dst := chFieldRef(nf.DstField)
	var b strings.Builder
	b.WriteString(fmt.Sprintf("SELECT %s AS dst, uniqExact(%s) AS prev_total\n", dst, src))
	b.WriteString(fmt.Sprintf("FROM %s\n", sourceTable))
	b.WriteString(fmt.Sprintf("WHERE %s AND %s != '' AND %s != '' AND timestamp >= now() - INTERVAL %d DAY", fractalScopeClause(fractalID), src, dst, windowDays))
	for _, fc := range def.Filter {
		b.WriteString(fmt.Sprintf("\n    AND %s", filterConditionToSQL(fc)))
	}
	b.WriteString("\nGROUP BY dst")
	return b.String()
}

// BuildNetPreviewNetworkSize returns the total distinct-source count over the preview
// window, the denominator for the prevalence ratio.
func BuildNetPreviewNetworkSize(def ModelDefinition, sourceTable, fractalID string, windowDays int) string {
	nf := netFieldMap(def)
	src := chFieldRef(nf.SrcField)
	dst := chFieldRef(nf.DstField)
	var b strings.Builder
	b.WriteString(fmt.Sprintf("SELECT uniqExact(%s) AS network_size\n", src))
	b.WriteString(fmt.Sprintf("FROM %s\n", sourceTable))
	b.WriteString(fmt.Sprintf("WHERE %s AND %s != '' AND %s != '' AND timestamp >= now() - INTERVAL %d DAY", fractalScopeClause(fractalID), src, dst, windowDays))
	for _, fc := range def.Filter {
		b.WriteString(fmt.Sprintf("\n    AND %s", filterConditionToSQL(fc)))
	}
	return b.String()
}

// chFloatLiteral renders a float as a ClickHouse numeric literal without exponent noise.
func chFloatLiteral(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
