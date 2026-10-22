package models

import (
	"bifract/pkg/parser"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var namedGroupRe = regexp.MustCompile(`\(\?P?(?:<[a-zA-Z_][a-zA-Z0-9_]*>|'[a-zA-Z_][a-zA-Z0-9_]*')`)

// extractPattern strips named capture group syntax from a pattern so that
// ClickHouse's extract() function receives a plain positional capture group.
// (?<name>...), (?P<name>...), (?P'name'...) and (?'name'...) become (...).
func extractPattern(pattern string) string {
	return namedGroupRe.ReplaceAllString(pattern, "(")
}

// GenerateDDL returns the CREATE TABLE statement for a model's state table. There
// is no materialized view: state is written by StateMaintainer and the backfill.
func GenerateDDL(def ModelDefinition, mt ModelType, tableName string) (string, error) {
	return generateTableDDL(def, mt, tableName)
}

func generateTableDDL(def ModelDefinition, mt ModelType, tableName string) (string, error) {
	h := historyLayoutFor(def, mt)
	switch mt {
	case ModelTypeRarity:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    fractal_id    LowCardinality(String),
    partition_val String,
    value_val     String,
    event_count   SimpleAggregateFunction(sum, UInt64),
    %s
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, partition_val, value_val%s)%s
SETTINGS index_granularity = 8192`, tableName, h.column(13), h.orderBy, h.ttl), nil

	case ModelTypeFirstSeen:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    fractal_id  LowCardinality(String),
    entity_key  String,
    first_seen  SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen   SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    event_count SimpleAggregateFunction(sum, UInt64),
    %s,
    %s %s
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, entity_key%s)%s
SETTINGS index_granularity = 8192`, tableName, h.column(11), FirstRecordedColumn, FirstRecordedDefinition, h.orderBy, h.ttl), nil

	case ModelTypeTLSH:
		// digest is the verbatim on-disk string, never a canonicalised form: it is
		// what tlsh() emits into the log filter, so it has to match what is stored.
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    fractal_id  LowCardinality(String),
    digest      String,
    first_seen  SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen   SimpleAggregateFunction(max, DateTime64(3, 'UTC')),
    event_count SimpleAggregateFunction(sum, UInt64),
    days        AggregateFunction(groupUniqArray(365), Date)
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, digest)
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
		return "DateTime('UTC')"
	}
	return "Date"
}

// historyLayout is how a model's state records days: a day set per key, or for a
// windowed model a day column in the sort key, one row per key and day, which a
// TTL can age out.
type historyLayout struct {
	dayName, dayType, daySelect string
	orderBy, ttl, groupBy       string
}

func historyLayoutFor(def ModelDefinition, mt ModelType) historyLayout {
	days := def.HistoryDaysFor(mt)
	if days == 0 {
		return historyLayout{dayName: "days", dayType: "AggregateFunction(groupUniqArray(365), Date)",
			daySelect: "groupUniqArrayState(365)(toDate(timestamp))"}
	}
	col := parser.HistoryDayColumn
	return historyLayout{
		dayName: col, dayType: "Date", daySelect: "toDate(timestamp)",
		orderBy: ", " + col,
		ttl:     fmt.Sprintf("\nTTL %s + INTERVAL %d DAY", col, parser.HistoryRetentionDays(days)),
		groupBy: ", " + col,
	}
}

// column is the day column's DDL, its name padded to the table's other names.
func (h historyLayout) column(width int) string {
	return fmt.Sprintf("%-*s %s", width, h.dayName, h.dayType)
}

func (h historyLayout) selectCol() string { return h.daySelect + " AS " + h.dayName }

// stateColumns names the columns a state insert writes, in its SELECT's order, so
// a table in another layout refuses the insert.
func stateColumns(def ModelDefinition, mt ModelType) []string {
	day := historyLayoutFor(def, mt).dayName
	switch mt {
	case ModelTypeRarity:
		return []string{"fractal_id", "partition_val", "value_val", "event_count", day}
	case ModelTypeFirstSeen:
		return []string{"fractal_id", "entity_key", "first_seen", "last_seen", "event_count", day, FirstRecordedColumn}
	case ModelTypeTLSH:
		return []string{"fractal_id", "digest", "first_seen", "last_seen", "event_count", day}
	case ModelTypeVolumeBaseline:
		return []string{"fractal_id", "entity_val", "bucket", "event_count"}
	}
	return nil
}

// BuildBackfillInsert returns a full `INSERT INTO <targetTable> <select>` that
// seeds a model from historical logs. It reuses the exact SELECT logic of the
// materialized view, but reads from sourceTable (the distributed logs table in
// cluster mode) and ANDs an extra predicate (the time window + ingest_timestamp
// dedup boundary) into the source filter.
//
// IMPORTANT: this performs NO DDL. It only inserts into an already-existing
// model table, so it can never orphan a table or materialized view.
func BuildBackfillInsert(def ModelDefinition, mt ModelType, targetTable, sourceTable, whereExtra, fractalID string) (string, error) {
	return buildInsert(def, mt, targetTable, sourceTable, whereExtra, fractalID, recordedHistory)
}

// BuildStateInsert is the live counterpart of BuildBackfillInsert, run by each
// state maintenance cycle: the same aggregation, but what it records is new to
// the model as of now.
func BuildStateInsert(def ModelDefinition, mt ModelType, targetTable, sourceTable, whereExtra, fractalID string) (string, error) {
	return buildInsert(def, mt, targetTable, sourceTable, whereExtra, fractalID, recordedLive)
}

func buildInsert(def ModelDefinition, mt ModelType, targetTable, sourceTable, whereExtra, fractalID, recorded string) (string, error) {
	selectSQL, err := buildModelSelect(def, mt, sourceTable, whereExtra, fractalID, recorded)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("INSERT INTO %s (%s)\n%s", targetTable, strings.Join(stateColumns(def, mt), ", "), selectSQL), nil
}

// first_recorded is when a first_seen model's state first held an entity, which
// is what "new" means to its alert; first_seen is event time and says nothing
// about when the model learned of it. Seeded history and rows that predate the
// column take the epoch, which min() makes absorbing and no recency check admits.
const (
	FirstRecordedColumn     = "first_recorded"
	FirstRecordedDefinition = "SimpleAggregateFunction(min, DateTime64(3, 'UTC')) DEFAULT " + recordedHistory

	recordedHistory = "toDateTime64(0, 3, 'UTC')"
	recordedLive    = "now64(3, 'UTC')"
)

// buildModelSelect builds the SELECT ... FROM <sourceTable> ... GROUP BY ...
// shared by the materialized view (sourceTable="logs", whereExtra="") and the
// backfill INSERT...SELECT (distributed source + time-window predicate).
// whereExtra, when non-empty, is ANDed into the source-scan WHERE clause.
// recorded is the first_recorded expression a first_seen model writes.
func buildModelSelect(def ModelDefinition, mt ModelType, sourceTable, whereExtra string, fractalID, recorded string) (string, error) {
	// An unscoped model MV aggregates every fractal's logs into the owning
	// fractal's table: an ingest-time cost on fractals that never read it, and
	// cross-fractal rows sitting behind nothing but the read-side predicate.
	// Refuse to generate one rather than emit a silently over-broad view.
	if fractalID == "" {
		return "", fmt.Errorf("model select: owning fractal_id is required to scope the source scan")
	}
	// tlsh() filters logs by the indexed digest, so it must be the verbatim value
	// stored in the field. Checked here as well as in validateDefinitionShape
	// because the startup reconcile paths rebuild from stored definitions without
	// re-validating them.
	if mt == ModelTypeTLSH {
		if len(def.Extractions) > 0 {
			return "", fmt.Errorf("model select: tlsh models cannot use extractions")
		}
		// The tlsh projection indexes KeyFields[0] directly. Update() does not
		// re-run validateDefinitionShape, so an edited definition can arrive here
		// with no key field at all: return an error rather than panicking.
		if len(def.KeyFields) != 1 || strings.TrimSpace(def.KeyFields[0]) == "" {
			return "", fmt.Errorf("model select: tlsh models take exactly one key field, got %d", len(def.KeyFields))
		}
	}

	var b strings.Builder

	// CTE chain: needed for an extraction, and for a source query that computes a
	// column, because the final SELECT names such columns rather than resolving
	// them as JSON paths.
	if len(def.Extractions) > 0 || len(def.projections) > 0 {
		b.WriteString("WITH\n")
		// base CTE: filter + every column the rest of the statement names.
		b.WriteString("base AS (\n")
		b.WriteString("    SELECT fractal_id, timestamp")
		seen := map[string]bool{}
		// The source query's own computed columns come first: one may carry the name
		// of a log field (lowercase(image) is still called image), and the computed
		// value is the one the author filtered and keyed on.
		for _, pr := range def.projections {
			if seen[pr.Alias] {
				continue
			}
			seen[pr.Alias] = true
			b.WriteString(fmt.Sprintf(",\n           %s AS %s", pr.Expr, pr.Alias))
		}
		// Then every field the final SELECT names by plain name: extraction inputs
		// and the model's own keys. Without the keys the statement referenced a
		// column the CTE never projected, which ClickHouse rejects (code 47).
		for _, f := range modelReferencedFields(def, mt) {
			if f == "" || seen[f] || isExtractionOutput(f, def.Extractions) {
				continue
			}
			seen[f] = true
			b.WriteString(fmt.Sprintf(",\n           %s AS %s", chFieldRef(f), f))
		}
		b.WriteString(fmt.Sprintf("\n    FROM %s\n", sourceTable))
		b.WriteString(fmt.Sprintf("    WHERE %s", fractalScopeClause(fractalID)))
		preds, err := sourceFilterSQL(def)
		if err != nil {
			return "", err
		}
		for _, p := range preds {
			b.WriteString(fmt.Sprintf("\n    AND %s", p))
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
			// Lowercase is applied in the final projection (applyLowerIfNeeded).
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
		final, err := buildFinalSelect(def, mt, prevCTE, recorded)
		if err != nil {
			return "", err
		}
		b.WriteString(final)
	} else {
		// No extractions: SELECT directly from the source table.
		direct, err := buildDirectSelect(def, mt, sourceTable, whereExtra, fractalID, recorded)
		if err != nil {
			return "", err
		}
		b.WriteString(direct)
	}

	return b.String(), nil
}

// buildFinalSelect builds the final GROUP BY SELECT from the last CTE.
func buildFinalSelect(def ModelDefinition, mt ModelType, fromTable, recorded string) (string, error) {
	h := historyLayoutFor(def, mt)
	var b strings.Builder
	switch mt {
	case ModelTypeRarity:
		partRef := def.PartitionKey
		valRef := def.ValueKey
		b.WriteString(fmt.Sprintf("SELECT fractal_id,\n"))
		b.WriteString(fmt.Sprintf("    %s AS partition_val,\n", applyLowerIfNeeded(partRef, def.Extractions)))
		b.WriteString(fmt.Sprintf("    %s AS value_val,\n", applyLowerIfNeeded(valRef, def.Extractions)))
		b.WriteString("    toUInt64(1) AS event_count,\n")
		b.WriteString("    " + h.selectCol() + "\n")
		b.WriteString(fmt.Sprintf("FROM %s\n", fromTable))
		b.WriteString(fmt.Sprintf("WHERE %s != '' AND %s != ''\n", partRef, valRef))
		b.WriteString("GROUP BY fractal_id, partition_val, value_val" + h.groupBy)
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
		b.WriteString(entityAggCols(h))
		b.WriteString(fmt.Sprintf(",\n    %s AS %s\n", recorded, FirstRecordedColumn))
		b.WriteString(fmt.Sprintf("FROM %s\n", fromTable))
		if len(def.KeyFields) > 0 {
			guards := make([]string, len(def.KeyFields))
			for i, kf := range def.KeyFields {
				guards[i] = fmt.Sprintf("%s != ''", kf)
			}
			b.WriteString(fmt.Sprintf("WHERE %s\n", strings.Join(guards, " AND ")))
		}
		b.WriteString("GROUP BY fractal_id, entity_key" + h.groupBy)
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
	case ModelTypeTLSH:
		b.WriteString("SELECT fractal_id,\n")
		b.WriteString(fmt.Sprintf("    %s AS digest,\n", def.KeyFields[0]))
		b.WriteString(entityAggCols(h) + "\n")
		b.WriteString(fmt.Sprintf("FROM %s\n", fromTable))
		b.WriteString(fmt.Sprintf("WHERE %s\n", tlshDigestGuard(def.KeyFields[0])))
		b.WriteString("GROUP BY fractal_id, digest")
	default:
		// An unhandled type used to fall through to an empty string, which rendered
		// a WITH clause with no SELECT after it.
		return "", fmt.Errorf("model select: %s has no aggregation over a computed scan", mt)
	}
	return b.String(), nil
}

// entityAggCols aggregates a first_seen or tlsh key's rows into one state row;
// grouping by the timestamp as well would write a row per event time.
func entityAggCols(h historyLayout) string {
	return `    min(timestamp) AS first_seen,
    max(timestamp) AS last_seen,
    count() AS event_count,
    ` + h.selectCol()
}

// buildDirectSelect builds a SELECT directly from the source table (no extractions).
// whereExtra, when non-empty, is ANDed into the WHERE clause.
func buildDirectSelect(def ModelDefinition, mt ModelType, sourceTable, whereExtra string, fractalID, recorded string) (string, error) {
	h := historyLayoutFor(def, mt)
	var b strings.Builder
	b.WriteString("SELECT fractal_id")

	switch mt {
	case ModelTypeRarity:
		b.WriteString(fmt.Sprintf(",\n    %s AS partition_val,\n    %s AS value_val,\n    toUInt64(1) AS event_count,\n    %s\n",
			chFieldRef(def.PartitionKey), chFieldRef(def.ValueKey), h.selectCol()))
		b.WriteString(fmt.Sprintf("FROM %s\n", sourceTable))
		b.WriteString(fmt.Sprintf("WHERE %s", fractalScopeClause(fractalID)))
		preds, err := sourceFilterSQL(def)
		if err != nil {
			return "", err
		}
		for _, p := range preds {
			b.WriteString(fmt.Sprintf("\nAND %s", p))
		}
		if whereExtra != "" {
			b.WriteString(fmt.Sprintf("\nAND %s", whereExtra))
		}
		b.WriteString(fmt.Sprintf("\nAND %s != '' AND %s != ''\n", chFieldRef(def.PartitionKey), chFieldRef(def.ValueKey)))
		b.WriteString("GROUP BY fractal_id, partition_val, value_val" + h.groupBy)
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
		b.WriteString(",\n" + entityAggCols(h))
		b.WriteString(fmt.Sprintf(",\n    %s AS %s\n", recorded, FirstRecordedColumn))
		b.WriteString(fmt.Sprintf("FROM %s\n", sourceTable))
		b.WriteString(fmt.Sprintf("WHERE %s", fractalScopeClause(fractalID)))
		preds, err := sourceFilterSQL(def)
		if err != nil {
			return "", err
		}
		for _, p := range preds {
			b.WriteString(fmt.Sprintf("\nAND %s", p))
		}
		if whereExtra != "" {
			b.WriteString(fmt.Sprintf("\nAND %s", whereExtra))
		}
		// A log without the key is not an entity. The extraction path and every
		// other type already skip it; without the guard the model recorded one
		// entity_key of '' (or of bare separators) that every keyless log joined.
		for _, kf := range def.KeyFields {
			b.WriteString(fmt.Sprintf("\nAND %s != ''", chFieldRef(kf)))
		}
		b.WriteString("\nGROUP BY fractal_id, entity_key" + h.groupBy)
	case ModelTypeTLSH:
		b.WriteString(fmt.Sprintf(",\n    %s AS digest", chFieldRef(def.KeyFields[0])))
		b.WriteString(",\n" + entityAggCols(h) + "\n")
		b.WriteString(fmt.Sprintf("FROM %s\n", sourceTable))
		b.WriteString(fmt.Sprintf("WHERE %s", fractalScopeClause(fractalID)))
		preds, err := sourceFilterSQL(def)
		if err != nil {
			return "", err
		}
		for _, p := range preds {
			b.WriteString(fmt.Sprintf("\nAND %s", p))
		}
		if whereExtra != "" {
			b.WriteString(fmt.Sprintf("\nAND %s", whereExtra))
		}
		b.WriteString(fmt.Sprintf("\nAND %s", tlshDigestGuard(chFieldRef(def.KeyFields[0]))))
		b.WriteString("\nGROUP BY fractal_id, digest")
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
		b.WriteString(fmt.Sprintf("WHERE %s", fractalScopeClause(fractalID)))
		preds, err := sourceFilterSQL(def)
		if err != nil {
			return "", err
		}
		for _, p := range preds {
			b.WriteString(fmt.Sprintf("\nAND %s", p))
		}
		if whereExtra != "" {
			b.WriteString(fmt.Sprintf("\nAND %s", whereExtra))
		}
		for _, kf := range def.KeyFields {
			b.WriteString(fmt.Sprintf("\nAND %s != ''", chFieldRef(kf)))
		}
		b.WriteString("\nGROUP BY fractal_id, entity_val, bucket")
	}
	return b.String(), nil
}

// chFieldRef converts a user-facing field name to a ClickHouse expression.
// Known log columns are referenced directly. JSON sub-columns get ::String so
// match() / extract() receive a concrete String type rather than Dynamic.
func chFieldRef(field string) string {
	switch field {
	case "timestamp", "norm_log", "log_id", "fractal_id", "ingest_timestamp", "normalizer":
		return field
	default:
		// Quote the way the query translator's jsonFieldRef does. A field name
		// reaches here from a model definition and, via the tlsh() fallback probe,
		// straight from a user's query, so an unescaped backtick or backslash would
		// close the identifier and let the rest of the name run as SQL.
		return "fields.`" + parser.EscapeCHBacktickIdent(field) + "`::String"
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
// sourceFilterSQL is the set of predicates a model's source contributes to its
// scan. def.SourceBQL is compiled by the translator; a model saved before that
// existed falls back to the structured list ddl.go renders itself.
func sourceFilterSQL(def ModelDefinition) ([]string, error) {
	if strings.TrimSpace(def.SourceBQL) != "" {
		if !def.compiledSet {
			return nil, fmt.Errorf("model source query was not resolved before rendering; " +
				"call Manager.ResolveSource so match() and the rest compile against this scope")
		}
		return def.compiled, nil
	}
	out := make([]string, 0, len(def.Filter))
	for _, fc := range def.Filter {
		out = append(out, filterConditionToSQL(fc))
	}
	return out, nil
}

func filterConditionToSQL(fc FilterCondition) string {
	ref := chFieldRef(fc.Field)
	// Wildcard equality mirrors BQL translator semantics: `field = "*"` means the
	// field has any non-empty value, and `field != "*"` means it is empty/missing.
	// Without this, `src_ip = "*"` would compile to a literal `= '*'` match (which
	// no log satisfies), silently emptying the model's state.
	if fc.Value == "*" {
		switch fc.Op {
		case "=":
			return fmt.Sprintf("%s != ''", ref)
		case "!=":
			return fmt.Sprintf("%s = ''", ref)
		}
	}
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

// netStateArrayCap bounds the per-day-per-pair timestamp/size arrays, enough to
// measure regularity while bounding what one pair's day costs to keep.
const netStateArrayCap = 2000

// netPairPointBudget bounds one pair's points: the longest window's day buckets
// (it reads back from today, so one more than its days) at the daily cap.
const netPairPointBudget = (maxNetWindowDays + 1) * netStateArrayCap

// netReadBlockRows keeps a block of wide pair rows small, since the driver decodes
// a whole block at once.
const netReadBlockRows = 256

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
    first_ts   SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_ts    SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, src, dst, port, day)
TTL day + INTERVAL %d DAY
SETTINGS index_granularity = 8192`, stateTable, netStateArrayCap, netStateArrayCap, ttlDays)
}

// BuildNetResultsTableDDL returns the ReplacingMergeTree results table DDL. The
// scorer inserts fresh rows each pass; reads take the latest via FINAL/argMax. The
// modifier columns (prevalence/first_seen/threat_intel) carry the full breakdown so
// the reviewer can see why a pair scored high; first_seen/threat_intel are 0 unless
// the model links a first_seen model or a lookup.
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
    first_seen         DateTime64(3, 'UTC'),
    last_seen          DateTime64(3, 'UTC'),
    scored_at          DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(scored_at)
ORDER BY (fractal_id, src_ip, dst_ip, dst_port)
SETTINGS index_granularity = 8192`, resultsTable)
}

// netFieldMap resolves the model's network field map with defaults applied.
func netFieldMap(def ModelDefinition) NetworkFieldMap {
	return def.Network.WithDefaults()
}

// BuildNetStateInsert is the network equivalent of BuildBackfillInsert: the same
// rolling-state aggregation the view used to compute at insert, over an explicit
// window, so a scheduled reader can maintain the state instead.
func BuildNetStateInsert(def ModelDefinition, stateTable, sourceTable, whereExtra, fractalID string) (string, error) {
	sel, err := buildNetStateSelect(def, sourceTable, whereExtra, fractalID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("INSERT INTO %s\n%s", stateTable, sel), nil
}

// buildNetStateSelect aggregates connection rows into rolling per-day state.
// whereExtra, when non-empty, is ANDed into the scan so a caller can bound it to
// an ingest window.
func buildNetStateSelect(def ModelDefinition, sourceTable, whereExtra, fractalID string) (string, error) {
	if fractalID == "" {
		return "", fmt.Errorf("net state: owning fractal_id is required to scope the source scan")
	}
	nf := netFieldMap(def)
	src := chFieldRef(nf.SrcField)
	dst := chFieldRef(nf.DstField)
	port := chFieldRef(nf.PortField)
	bytes := chNumericFieldRef(nf.BytesField)
	dur := chNumericFieldRef(nf.DurationField)

	var b strings.Builder
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
	b.WriteString(fmt.Sprintf("FROM %s\n", sourceTable))
	b.WriteString(fmt.Sprintf("WHERE %s AND %s != '' AND %s != ''", fractalScopeClause(fractalID), src, dst))
	if whereExtra != "" {
		b.WriteString(fmt.Sprintf("\n    AND %s", whereExtra))
	}
	preds, err := sourceFilterSQL(def)
	if err != nil {
		return "", err
	}
	// One flat aggregate over the log table: there is no layer here that could
	// compute a column, so a predicate naming one would reference nothing.
	if err := checkNoProjectedRefs(preds, def.projections); err != nil {
		return "", err
	}
	for _, p := range preds {
		b.WriteString(fmt.Sprintf("\n    AND %s", p))
	}
	b.WriteString("\nGROUP BY fractal_id, src, dst, port, day")
	return b.String(), nil
}

// TLSHDigestGuard and CHFieldRef expose the expressions the index MV is built
// from, so a caller reading digests out of a table directly (the rule tester's
// scratch table) selects the same column and applies the same shape guard. Two
// spellings of "a usable digest" would drift.
func TLSHDigestGuard(ref string) string { return tlshDigestGuard(ref) }

// CHFieldRef renders a log field as the column expression the model indexes.
func CHFieldRef(field string) string { return chFieldRef(field) }

// tlshDigestGuard admits only well-formed TLSH digests: 70 hex characters, with an
// optional "T1" version prefix. Producers emit nothing for inputs under 50 bytes,
// so absent and truncated values are routine in log data, and two of them compare
// at distance 0, which makes them match every needle. Enforcing shape at the MV
// keeps them out of the index entirely rather than relying on a query-time guard.
// Kept in lockstep with tlsh.Parse; see TestTLSHGuardMatchesParser.
func tlshDigestGuard(ref string) string {
	return fmt.Sprintf("match(%s, '^([Tt]1)?[0-9A-Fa-f]{70}$')", ref)
}

// fractalScopeClause returns the WHERE predicate scoping a state read to a fractal.
func fractalScopeClause(fractalID string) string {
	return fmt.Sprintf("fractal_id = %s", chStringLiteral(fractalID))
}

// netPairGate is the condition a pair must meet to be scored, over expressions
// for its connection count and total duration.
func netPairGate(def ModelDefinition, mt ModelType, windowDays int, cnt, dur string) string {
	if mt == ModelTypeLongConnection {
		lc := def.LongConn.WithDefaults()
		return fmt.Sprintf("%s >= %s", dur, chFloatLiteral(lc.BaseSeconds))
	}
	bp := def.Beacon.WithDefaults(int64(windowDays) * 86400)
	return fmt.Sprintf("%s >= %d AND %s < %d", cnt, bp.MinConnections, cnt, bp.StrobeLimit)
}

// netPairArraysSelect concatenates per-day rows (day_cnt, day_ts, day_sizes,
// day_dur) into one row per pair, the shape pairFromRow reads. threatIntel, an
// expression over dst, adds the ti_hit column.
func netPairArraysSelect(perDay, having, threatIntel string) string {
	ti := ""
	if threatIntel != "" {
		ti = fmt.Sprintf(",\n    toUInt64(%s) AS ti_hit", threatIntel)
	}
	q := fmt.Sprintf(`SELECT src, dst, port,
    sum(day_cnt) AS cnt,
    arrayFlatten(groupArray(day_ts)) AS ts_list,
    arrayFlatten(groupArray(day_sizes)) AS size_list,
    sum(day_dur) AS total_duration%s
FROM (
%s
)
GROUP BY src, dst, port`, ti, perDay)
	if having != "" {
		q += "\nHAVING " + having
	}
	return q
}

// netGatedPairsQuery returns the window's pairs that qualify for scoring, from
// the state's counters alone.
func netGatedPairsQuery(def ModelDefinition, mt ModelType, stateTable, fractalID string, windowDays int) string {
	return fmt.Sprintf(`SELECT src, dst, port
FROM %s
WHERE %s AND day >= today() - %d
GROUP BY src, dst, port
HAVING %s`, stateTable, fractalScopeClause(fractalID), windowDays,
		netPairGate(def, mt, windowDays, "sum(conn_count)", "sum(dur_sum)"))
}

// BuildNetScoreReadQuery returns the scorer's read of the qualifying pairs over
// the window's day buckets. Each day is merged on its own because
// groupArrayMerge(N) caps everything it merges at N, and pairs qualify first so
// unqualified ones never build arrays. The Merge parameter must match the stored
// state's (code 43). threatIntel is the threat intel column's expression, or "".
func BuildNetScoreReadQuery(def ModelDefinition, mt ModelType, stateTable, fractalID string, windowDays int, threatIntel string) string {
	scope := fmt.Sprintf("%s AND day >= today() - %d", fractalScopeClause(fractalID), windowDays)
	perDay := fmt.Sprintf(`    SELECT src, dst, port, day,
        sum(conn_count) AS day_cnt,
        groupArrayMerge(%d)(ts_state) AS day_ts,
        groupArrayMerge(%d)(size_state) AS day_sizes,
        sum(dur_sum) AS day_dur
    FROM %s
    WHERE %s
      AND (src, dst, port) GLOBAL IN (%s)
    GROUP BY src, dst, port, day`, netStateArrayCap, netStateArrayCap, stateTable, scope,
		netGatedPairsQuery(def, mt, stateTable, fractalID, windowDays))
	return netPairArraysSelect(perDay, "", threatIntel)
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

// BuildNetPairsQuery returns the window's qualifying (src, dst) pairs, the keys
// the first-seen modifier is read for.
func BuildNetPairsQuery(def ModelDefinition, mt ModelType, stateTable, fractalID string, windowDays int) string {
	return fmt.Sprintf("SELECT DISTINCT src, dst FROM (%s)", netGatedPairsQuery(def, mt, stateTable, fractalID, windowDays))
}

// BuildNetNetworkSizeQuery returns the distinct-source count over the window, the
// denominator for the prevalence ratio.
func BuildNetNetworkSizeQuery(stateTable, fractalID string, windowDays int) string {
	return fmt.Sprintf(`SELECT uniqExact(src) AS network_size
FROM %s
WHERE %s AND day >= today() - %d`, stateTable, fractalScopeClause(fractalID), windowDays)
}

// NetPreviewBounds is the window a network preview aggregates over: [Start, End)
// on the event timestamp, Days its length rounded up (it sets the strobe limit).
type NetPreviewBounds struct {
	Start, End time.Time
	Days       int
}

func (b NetPreviewBounds) sql() string {
	return fmt.Sprintf("timestamp >= toDateTime64('%s', 3, 'UTC') AND timestamp < toDateTime64('%s', 3, 'UTC')",
		b.Start.UTC().Format(chTimeLayout), b.End.UTC().Format(chTimeLayout))
}

// previewScope is the FROM and WHERE every preview read over raw logs shares:
// the fractal, the window, a source and destination, and the source filter.
func previewScope(def ModelDefinition, sourceTable, fractalID string, bounds NetPreviewBounds) (string, error) {
	nf := netFieldMap(def)
	src, dst := chFieldRef(nf.SrcField), chFieldRef(nf.DstField)
	preds, err := sourceFilterSQL(def)
	if err != nil {
		return "", err
	}
	where := fmt.Sprintf("FROM %s\nWHERE %s AND %s != '' AND %s != '' AND %s", sourceTable, fractalScopeClause(fractalID), src, dst, bounds.sql())
	for _, p := range preds {
		where += "\n    AND " + p
	}
	return where, nil
}

// BuildNetPreviewAgg returns the preview's aggregation over raw logs in the state
// read's per-pair shape; threatIntel is the threat intel column's expression, or "".
func BuildNetPreviewAgg(def ModelDefinition, mt ModelType, sourceTable, fractalID string, bounds NetPreviewBounds, threatIntel string) (string, error) {
	scope, err := previewScope(def, sourceTable, fractalID, bounds)
	if err != nil {
		return "", err
	}
	nf := netFieldMap(def)
	dayCap := netPreviewDayCap(bounds.Days)
	perDay := fmt.Sprintf(`    SELECT %s AS src, %s AS dst, %s AS port, toDate(timestamp) AS day,
        toUInt64(count()) AS day_cnt,
        groupArray(%d)(toUnixTimestamp(timestamp)) AS day_ts,
        groupArray(%d)(%s) AS day_sizes,
        sum(%s) AS day_dur
    %s
    GROUP BY src, dst, port, day`, chFieldRef(nf.SrcField), chFieldRef(nf.DstField), chFieldRef(nf.PortField),
		dayCap, dayCap, chNumericFieldRef(nf.BytesField), chNumericFieldRef(nf.DurationField), scope)
	return netPairArraysSelect(perDay, netPairGate(def, mt, bounds.Days, "cnt", "total_duration"), threatIntel), nil
}

// netPreviewDayCap is the points a preview keeps per pair and day, lowered past
// the longest window to stay within netPairPointBudget. A range not starting at
// midnight touches one calendar day more than it lasts.
func netPreviewDayCap(days int) int {
	if days < 1 {
		days = 1
	}
	if c := netPairPointBudget / (days + 1); c < netStateArrayCap {
		return c
	}
	return netStateArrayCap
}

// BuildNetPreviewPrevalence returns the per-destination distinct-source counts over
// raw logs for the preview window, over all sources rather than qualifying pairs.
func BuildNetPreviewPrevalence(def ModelDefinition, sourceTable, fractalID string, bounds NetPreviewBounds) (string, error) {
	scope, err := previewScope(def, sourceTable, fractalID, bounds)
	if err != nil {
		return "", err
	}
	nf := netFieldMap(def)
	return fmt.Sprintf("SELECT %s AS dst, uniqExact(%s) AS prev_total\n%s\nGROUP BY dst",
		chFieldRef(nf.DstField), chFieldRef(nf.SrcField), scope), nil
}

// BuildNetPreviewNetworkSize returns the total distinct-source count over the preview
// window, the denominator for the prevalence ratio.
func BuildNetPreviewNetworkSize(def ModelDefinition, sourceTable, fractalID string, bounds NetPreviewBounds) (string, error) {
	scope, err := previewScope(def, sourceTable, fractalID, bounds)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SELECT uniqExact(%s) AS network_size\n%s", chFieldRef(netFieldMap(def).SrcField), scope), nil
}

// BuildNetPreviewPairs returns the preview window's qualifying (src, dst) pairs
// over raw logs, the preview's counterpart of BuildNetPairsQuery.
func BuildNetPreviewPairs(def ModelDefinition, mt ModelType, sourceTable, fractalID string, bounds NetPreviewBounds) (string, error) {
	scope, err := previewScope(def, sourceTable, fractalID, bounds)
	if err != nil {
		return "", err
	}
	nf := netFieldMap(def)
	gate := netPairGate(def, mt, bounds.Days, "count()", "sum("+chNumericFieldRef(nf.DurationField)+")")
	return fmt.Sprintf("SELECT DISTINCT src, dst FROM (\nSELECT %s AS src, %s AS dst, %s AS port\n%s\nGROUP BY src, dst, port\nHAVING %s\n)",
		chFieldRef(nf.SrcField), chFieldRef(nf.DstField), chFieldRef(nf.PortField), scope, gate), nil
}

// chFloatLiteral renders a float as a ClickHouse numeric literal without exponent noise.
func chFloatLiteral(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// modelReferencedFields names every log field the final SELECT refers to by plain
// name: the model's keys and the inputs its extractions read. The CTE that feeds
// that SELECT has to project each one.
func modelReferencedFields(def ModelDefinition, mt ModelType) []string {
	var out []string
	switch mt {
	case ModelTypeRarity:
		out = append(out, def.PartitionKey, def.ValueKey)
	default:
		out = append(out, def.KeyFields...)
	}
	for _, ext := range def.Extractions {
		out = append(out, ext.FromField)
	}
	return out
}
