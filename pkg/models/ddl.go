package models

import (
	"fmt"
	"regexp"
	"strings"
)

var namedGroupRe = regexp.MustCompile(`\(\?(?:P?<[a-zA-Z_][a-zA-Z0-9_]*>|P'[a-zA-Z_][a-zA-Z0-9_]*')`)

// extractPattern strips named capture group syntax from a pattern so that
// ClickHouse's extract() function receives a plain positional capture group.
// (?<name>...) and (?P<name>...) become (...).
func extractPattern(pattern string) string {
	return namedGroupRe.ReplaceAllString(pattern, "(")
}

// GenerateDDL returns (createTableSQL, createMVSQL) for the given model definition.
func GenerateDDL(def ModelDefinition, mt ModelType, tableName, mvName string) (string, string, error) {
	tableSQL, err := generateTableDDL(mt, tableName)
	if err != nil {
		return "", "", err
	}
	mvSQL, err := generateMVDDL(def, mt, tableName, mvName)
	if err != nil {
		return "", "", err
	}
	return tableSQL, mvSQL, nil
}

func generateTableDDL(mt ModelType, tableName string) (string, error) {
	switch mt {
	case ModelTypeRarity:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    fractal_id    LowCardinality(String),
    partition_val String,
    value_val     String,
    event_count   SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, partition_val, value_val)
SETTINGS index_granularity = 8192`, tableName), nil

	case ModelTypeFirstSeen:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    fractal_id  LowCardinality(String),
    entity_key  String,
    first_seen  SimpleAggregateFunction(min, DateTime64(3)),
    last_seen   SimpleAggregateFunction(max, DateTime64(3)),
    event_count SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY (fractal_id, entity_key)
SETTINGS index_granularity = 8192`, tableName), nil

	default:
		return "", fmt.Errorf("unknown model type: %s", mt)
	}
}

func generateMVDDL(def ModelDefinition, mt ModelType, tableName, mvName string) (string, error) {
	// Build the SELECT body using CTE chains.
	selectSQL, err := buildMVSelect(def, mt)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`CREATE MATERIALIZED VIEW IF NOT EXISTS %s TO %s AS
%s`, mvName, tableName, selectSQL), nil
}

// buildMVSelect builds the SELECT ... FROM logs ... GROUP BY ... for the MV.
func buildMVSelect(def ModelDefinition, mt ModelType) (string, error) {
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
		b.WriteString("\n    FROM logs\n")
		b.WriteString("    WHERE fractal_id != ''")
		for _, fc := range def.Filter {
			b.WriteString(fmt.Sprintf("\n    AND %s", filterConditionToSQL(fc)))
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
		b.WriteString(buildFinalSelect(def, mt, prevCTE))
	} else {
		// No extractions — SELECT directly from logs
		b.WriteString(buildDirectSelect(def, mt))
	}

	return b.String(), nil
}

// buildFinalSelect builds the final GROUP BY SELECT from the last CTE.
func buildFinalSelect(def ModelDefinition, mt ModelType, fromTable string) string {
	var b strings.Builder
	switch mt {
	case ModelTypeRarity:
		partRef := def.PartitionKey
		valRef := def.ValueKey
		b.WriteString(fmt.Sprintf("SELECT fractal_id,\n"))
		b.WriteString(fmt.Sprintf("    %s AS partition_val,\n", applyLowerIfNeeded(partRef, def.Extractions)))
		b.WriteString(fmt.Sprintf("    %s AS value_val,\n", applyLowerIfNeeded(valRef, def.Extractions)))
		b.WriteString("    toUInt64(1) AS event_count\n")
		b.WriteString(fmt.Sprintf("FROM %s\n", fromTable))
		b.WriteString(fmt.Sprintf("WHERE %s != '' AND %s != ''\n", partRef, valRef))
		b.WriteString(fmt.Sprintf("GROUP BY fractal_id, partition_val, value_val"))
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
		b.WriteString("    toUInt64(1) AS event_count\n")
		b.WriteString(fmt.Sprintf("FROM %s\n", fromTable))
		if len(def.KeyFields) > 0 {
			guards := make([]string, len(def.KeyFields))
			for i, kf := range def.KeyFields {
				guards[i] = fmt.Sprintf("%s != ''", kf)
			}
			b.WriteString(fmt.Sprintf("WHERE %s\n", strings.Join(guards, " AND ")))
		}
		b.WriteString("GROUP BY fractal_id, entity_key, first_seen, last_seen")
	}
	return b.String()
}

// buildDirectSelect builds a SELECT directly from logs (no extractions).
func buildDirectSelect(def ModelDefinition, mt ModelType) string {
	var b strings.Builder
	b.WriteString("SELECT fractal_id")

	switch mt {
	case ModelTypeRarity:
		b.WriteString(fmt.Sprintf(",\n    %s AS partition_val,\n    %s AS value_val,\n    toUInt64(1) AS event_count\n",
			chFieldRef(def.PartitionKey), chFieldRef(def.ValueKey)))
		b.WriteString("FROM logs\n")
		b.WriteString("WHERE fractal_id != ''")
		for _, fc := range def.Filter {
			b.WriteString(fmt.Sprintf("\nAND %s", filterConditionToSQL(fc)))
		}
		b.WriteString(fmt.Sprintf("\nAND %s != '' AND %s != ''\n", chFieldRef(def.PartitionKey), chFieldRef(def.ValueKey)))
		b.WriteString(fmt.Sprintf("GROUP BY fractal_id, partition_val, value_val"))
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
		b.WriteString(",\n    timestamp AS first_seen,\n    timestamp AS last_seen,\n    toUInt64(1) AS event_count\n")
		b.WriteString("FROM logs\n")
		b.WriteString("WHERE fractal_id != ''")
		for _, fc := range def.Filter {
			b.WriteString(fmt.Sprintf("\nAND %s", filterConditionToSQL(fc)))
		}
		b.WriteString("\nGROUP BY fractal_id, entity_key, first_seen, last_seen")
	}
	return b.String()
}

// chFieldRef converts a user-facing field name to a ClickHouse expression.
// Known log columns are referenced directly. JSON sub-columns get ::String so
// match() / extract() receive a concrete String type rather than Dynamic.
func chFieldRef(field string) string {
	switch field {
	case "timestamp", "raw_log", "log_id", "fractal_id", "ingest_timestamp", "field_tokens":
		return field
	default:
		return "fields.`" + field + "`::String"
	}
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
