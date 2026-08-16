package parser

import (
	"fmt"
	"sort"
	"strings"
)

// SourceMode selects the physical backend a BQL query is translated against.
//
// The zero value SourceHot preserves the exact ClickHouse `logs` translation
// (JSON typed `fields`, norm_log n-gram index, skip-index-aware field refs).
// SourceIceberg retargets field access to JSONExtractString over the archive's
// flat-JSON `norm_log` String column and its `_ice_` promoted top-level columns,
// and drops index-specific optimizations that do not exist on Parquet. Every
// iceberg branch is gated on this value, so hot-mode output is unchanged.
type SourceMode int

const (
	// SourceHot is the default: ClickHouse `logs`/`logs_distributed`, JSON `fields`.
	SourceHot SourceMode = iota
	// SourceIceberg targets an Iceberg archive read through ClickHouse iceberg*()
	// table functions: fields live in a flat-JSON `norm_log` String column (no
	// Map, no ngram index) plus `_ice_` promoted columns for pruning.
	SourceIceberg
)

// icePromotedPrefix namespaces the typed top-level columns the archiver writes
// alongside the `fields` MAP so ClickHouse can prune Iceberg scans on them.
const icePromotedPrefix = "_ice_"

// IcePromotedFields returns the field names promoted to typed top-level Iceberg
// columns. v1 promotes the default type-hinted set only (a static schema
// addition); custom hints stay MAP-only until dynamic schema evolution lands.
//
// The archiver builds one column per entry and the translator emits the
// promoted-column pruning predicate for them, so BOTH sides MUST derive the set
// from here to stay in agreement.
func IcePromotedFields() []string {
	out := make([]string, 0, len(jsonDefaultTypeHintedFields))
	for f := range jsonDefaultTypeHintedFields {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

// IcePromotedColumn returns the sanitized `_ice_` column name for a field and
// whether the field is promoted. Only promoted fields get min/max + bloom
// pruning; unpromoted fields fall back to a full MAP scan.
func IcePromotedColumn(field string) (string, bool) {
	if !jsonDefaultTypeHintedFields[field] {
		return "", false
	}
	return icePromotedPrefix + sanitizeIceName(field), true
}

// sanitizeIceName maps a field name to a safe lowercase column identifier. The
// promoted set is already clean, but this guards against dots/special chars.
func sanitizeIceName(field string) string {
	var b strings.Builder
	b.Grow(len(field))
	for _, r := range field {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// mapFieldRef returns the ClickHouse field access for a field in Iceberg mode.
// The archiver serializes fields into the flat JSON `norm_log` String column, so
// a dotted BQL path is the literal top-level JSON key. JSONExtractString returns
// an empty string (not NULL) for a missing key, which the existence/`!=` codegen
// already tolerates.
func mapFieldRef(field string) string {
	return "JSONExtractString(norm_log, '" + escapeString(field) + "')"
}

// fieldRefMode returns the field reference for the given source mode: the JSON
// sub-column form in hot mode, the norm_log JSON extraction in iceberg mode.
func fieldRefMode(field string, mode SourceMode) string {
	if mode == SourceIceberg {
		return mapFieldRef(field)
	}
	return jsonFieldRef(field)
}

// contentColMode returns the free-text/content column for the given mode. Both
// modes use the norm_log column: hot mode has a materialized, n-gram-indexed
// norm_log; the iceberg archive stores norm_log as a plain JSON String (no
// index, but the same content), so free-text search matches identically.
func contentColMode(mode SourceMode) string {
	return normLogColumn
}

// icebergAllowedCommands is the v1 allowlist of pipeline commands whose codegen
// has been made source-mode aware. Commands outside this set (transforms,
// model_lookup, geo/ip, joins, traversal, comments, case, eval) reference
// columns, indexes, dictionaries, second-FROM joins, or Postgres state that do
// not exist for an Iceberg archive read, so they are rejected up front with a
// clear message rather than emitting invalid SQL. The set grows as more command
// paths are threaded.
var icebergAllowedCommands = map[string]bool{
	// aggregation family (resolve fields via the mode-aware registry)
	"count": true, "sum": true, "avg": true, "min": true, "max": true,
	"median": true, "percentile": true, "stddev": true, "mad": true, "iqr": true,
	"skewness": true, "kurtosis": true, "frequency": true, "top": true,
	"singleval": true, "selectfirst": true, "selectlast": true, "multi": true,
	"groupby": true,
	// post-processing
	"sort": true, "head": true, "tail": true, "headtail": true,
	"limit": true, "dedup": true,
	// column projection / rendering (fields resolve via the mode-aware registry)
	"table": true,
	// time bucketing over aggregates
	"timechart": true, "histogram": true, "bucket": true,
	// ATT&CK matrix: a GROUP BY over tags extracted from a mode-aware field ref
	"mitre": true, "attack": true,
}

// icebergSupportedFeatures reports whether a pipeline can be translated against
// an Iceberg archive in v1. Filters and free-text search (the Filter node) are
// always supported; only pipeline commands and per-row assignments are gated.
func icebergSupportedFeatures(pipeline *PipelineNode) error {
	if pipeline == nil {
		return nil
	}
	if len(pipeline.Assignments) > 0 {
		return fmt.Errorf("field assignments (:=) are not supported in archive search yet")
	}
	var unsupported string
	ForEachCommand(pipeline, func(cmd CommandNode) {
		if unsupported == "" && !icebergAllowedCommands[strings.ToLower(cmd.Name)] {
			unsupported = cmd.Name
		}
	})
	if unsupported != "" {
		return fmt.Errorf("command %q is not supported in archive search yet (supported: field filters, free-text/regex search, stats aggregations, groupby, sort, dedup, head/tail/limit, timechart, table)", unsupported)
	}
	return nil
}

// icebergPromotionQueryEnabled gates use of the `_ice_` promoted columns in
// generated queries. ENABLED: the dual predicate prunes via Parquet min/max and
// bloom filters on ClickHouse >= 26.6 (the pinned version). Earlier CH (26.2)
// mis-read Map/`_ice_`/bloom on iceberg; those bugs are fixed as of 26.6, so no
// query work-arounds are needed.
const icebergPromotionQueryEnabled = true

// icebergEqualityPredicate builds the field-equality predicate for iceberg mode.
// With promotion enabled it emits the correctness-safe dual predicate
// `JSONExtractString(norm_log,'x')='v' AND (_ice_x='v' OR _ice_x IS NULL)` (the
// norm_log clause is always correct; `_ice_x` prunes post-promotion files; IS
// NULL keeps pre-promotion files). With promotion disabled it emits the plain
// norm_log-extraction equality.
//
// promoted is the set of field names whose `_ice_` column actually exists on the
// table being queried, and it is per-table rather than global on purpose. The
// promoted set grows whenever a field is added to jsonDefaultTypeHintedFields,
// but an existing archive table only gains the new columns when an archiver next
// commits to it (ensureIceColumns). A dormant fractal that stopped ingesting
// therefore keeps an older, narrower schema forever. Referencing a column absent
// from that table's Iceberg metadata is not a lost optimization: ClickHouse fails
// the whole query with UNKNOWN_IDENTIFIER.
//
// A nil/empty set is the safe default: emit no `_ice_` reference at all. The
// norm_log clause alone is always correct, so a caller that does not supply the
// set loses pruning but never returns a wrong answer or an error.
func icebergEqualityPredicate(field, value string, promoted map[string]bool) string {
	mapRef := mapFieldRef(field)
	esc := escapeString(value)
	if !icebergPromotionQueryEnabled || !promoted[field] {
		return fmt.Sprintf("%s = '%s'", mapRef, esc)
	}
	col, isPromoted := IcePromotedColumn(field)
	if !isPromoted {
		return fmt.Sprintf("%s = '%s'", mapRef, esc)
	}
	return fmt.Sprintf("%s = '%s' AND (%s = '%s' OR %s IS NULL)", mapRef, esc, col, esc, col)
}
