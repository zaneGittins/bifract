package parser

import (
	"strings"
	"testing"
	"time"
)

// mlookupOpts returns QueryOptions with a set of analytics models registered for
// model_lookup() translation tests.
func mlookupOpts() QueryOptions {
	return QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
		FractalID: "f1",
		Models: map[string]AnalyticsModelInfo{
			"rare":   {ID: "1", TableName: "model_rare", ModelType: "rarity", MinSample: 1, FractalID: "f1"},
			"fs":     {ID: "2", TableName: "model_fs", ModelType: "first_seen", FractalID: "f1"},
			"beacon": {ID: "3", TableName: "model_beacon", ModelType: "beacon", FractalID: "f1"},
		},
	}
}

func translateML(t *testing.T, query string) string {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, mlookupOpts())
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}
	return result.SQL
}

// The key regression this fixes: the join keys are projected as hidden _mlk_k<i>
// columns in the source scan and stripped via EXCEPT after the join, so the ON
// clause resolves and no `fields.X` leaks into the join scope.
func TestModelLookup_BareEnrichmentProjectsKeys(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port])`)

	if !strings.Contains(sql, "AS _mlk_k0") || !strings.Contains(sql, "AS _mlk_k2") {
		t.Errorf("expected hidden key projections _mlk_k0.._mlk_k2, got:\n%s", sql)
	}
	if !strings.Contains(sql, "EXCEPT (_mlk_k0, _mlk_k1, _mlk_k2)") {
		t.Errorf("expected EXCEPT to strip hidden keys, got:\n%s", sql)
	}
	if !strings.Contains(sql, "_outer._mlk_k0") {
		t.Errorf("expected ON to reference _outer._mlk_k0, got:\n%s", sql)
	}
	// The ON must NOT reference a bare fields.X (that scope error is the bug).
	if strings.Contains(sql, "ON concat(fields.") {
		t.Errorf("ON must not reference raw fields.X, got:\n%s", sql)
	}
	if !strings.Contains(sql, "_mlookup.beacon_score") {
		t.Errorf("expected beacon_score output column, got:\n%s", sql)
	}
}

// A trailing threshold on a model output must be a post-join (deferred) filter that
// references the bare output column, not the source WHERE.
func TestModelLookup_TrailingThresholdDefersPostJoin(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.8`)

	// The filter must appear as an outer WHERE over the join wrap (SELECT * FROM (...) WHERE ...).
	joinIdx := strings.Index(sql, "LEFT JOIN")
	whereIdx := strings.LastIndex(sql, "beacon_score > 0.8")
	if joinIdx < 0 || whereIdx < 0 || whereIdx < joinIdx {
		t.Errorf("expected beacon_score filter AFTER the join, got:\n%s", sql)
	}
	// It must reference the bare output column, never _mlookup.beacon_score in a
	// scope where _mlookup is undefined.
	if strings.Contains(sql, "WHERE _mlookup.beacon_score") {
		t.Errorf("deferred filter must reference bare beacon_score, got:\n%s", sql)
	}
}

// Filters BEFORE model_lookup stay in the source WHERE (inside _outer).
func TestModelLookup_SourceFilterStaysInner(t *testing.T) {
	sql := translateML(t, `dst_ip="8.8.4.4" | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port])`)
	// The source scan (inside _outer) must carry the dst_ip filter.
	outerEnd := strings.Index(sql, ") AS _outer")
	if outerEnd < 0 {
		t.Fatalf("no _outer subquery, got:\n%s", sql)
	}
	inner := sql[:outerEnd]
	if !strings.Contains(inner, "`dst_ip`") || !strings.Contains(inner, "8.8.4.4") {
		t.Errorf("expected dst_ip filter inside _outer, got:\n%s", sql)
	}
}

// rarity still works and the composite key is matched against partition_val/value_val.
func TestModelLookup_RarityJoinShape(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="rare", key=[image, hash]) | confidence > 0.9`)
	if !strings.Contains(sql, "concat(_outer._mlk_k0, char(30), _outer._mlk_k1) = concat(_mlookup.partition_val, char(30), _mlookup.value_val)") {
		t.Errorf("unexpected rarity ON clause, got:\n%s", sql)
	}
	if !strings.Contains(sql, "EXCEPT (_mlk_k0, _mlk_k1)") {
		t.Errorf("expected EXCEPT for rarity keys, got:\n%s", sql)
	}
}

// table() listing model-output columns must NOT project them as raw JSON fields in
// the source scan (they come from the join); the deferred threshold must reference
// the real Float64 join column, not a String fields.X shadow.
func TestModelLookup_TableWithModelColumns(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.8 | table(src_ip, dst_ip, beacon_score, ts_score)`)
	if strings.Contains(sql, "`beacon_score`") || strings.Contains(sql, "`ts_score`") {
		t.Errorf("model output columns must not be projected as JSON fields, got:\n%s", sql)
	}
	if !strings.Contains(sql, "_mlookup.beacon_score") {
		t.Errorf("beacon_score must come from the join, got:\n%s", sql)
	}
	// The non-model table columns are still projected from the source.
	if !strings.Contains(sql, "fields.`src_ip`::String AS src_ip") {
		t.Errorf("expected src_ip projected in source, got:\n%s", sql)
	}
}

// first_seen with a single key matches the outer key against the entity_key column.
func TestModelLookup_FirstSeenSingleKey(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="fs", key=[src_ip]) | is_new = "1"`)
	if !strings.Contains(sql, "_outer._mlk_k0 = _mlookup.entity_key") {
		t.Errorf("unexpected first_seen ON clause, got:\n%s", sql)
	}
	if !strings.Contains(sql, "EXCEPT (_mlk_k0)") {
		t.Errorf("expected EXCEPT for single key, got:\n%s", sql)
	}
}
