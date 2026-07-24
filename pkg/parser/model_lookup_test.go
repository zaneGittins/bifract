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

// A clause mixing a model output with a source field must be split across the two
// scopes: the model threshold stays in the post-join WHERE, the log-field predicate
// pushes down to the source scan (where `fields.X` is the only place it resolves).
func TestModelLookup_MixedCompoundSplitsAcrossScopes(t *testing.T) {
	sql := translateML(t, `event_id="3" | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.5 AND NOT image=~zerotier`)

	outerEnd := strings.Index(sql, ") AS _outer")
	if outerEnd < 0 {
		t.Fatalf("no _outer subquery, got:\n%s", sql)
	}
	inner, outer := sql[:outerEnd], sql[outerEnd:]
	if !strings.Contains(inner, "`image`") {
		t.Errorf("image predicate must push down into the source scan, got:\n%s", sql)
	}
	// The post-join scope has no `fields` column; any leak there is the bug (CH code 47).
	if strings.Contains(outer, "fields.") {
		t.Errorf("post-join filter must not reference fields.X, got:\n%s", sql)
	}
	if !strings.Contains(outer, "beacon_score > 0.5") {
		t.Errorf("beacon_score threshold must stay post-join, got:\n%s", sql)
	}
}

// An OR cannot be distributed, so a mixed OR stays whole. It is still correct
// because both sides resolve in the post-join scope only when both are model
// outputs; this asserts the AND-only split does not corrupt OR grouping.
func TestModelLookup_ModelOnlyCompoundStaysWhole(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.5 OR conn_count > 100`)
	if !strings.Contains(sql, "(beacon_score > 0.5 OR conn_count > 100)") {
		t.Errorf("model-only OR compound should stay intact post-join, got:\n%s", sql)
	}
}

// The row limit must be applied AFTER the model filter. Leaving it on the source
// scan filters only the newest MaxRows logs, so results are a near-arbitrary subset
// that changes between runs whenever rows tie on timestamp.
func TestModelLookup_LimitAppliedAfterModelFilter(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.8`)

	whereIdx := strings.LastIndex(sql, "WHERE beacon_score > 0.8")
	limitIdx := strings.LastIndex(sql, "LIMIT")
	if whereIdx < 0 || limitIdx < whereIdx {
		t.Fatalf("LIMIT must follow the model filter, got:\n%s", sql)
	}
	if strings.Contains(sql[:strings.Index(sql, ") AS _outer")], "LIMIT") {
		t.Errorf("source scan must not carry the row limit ahead of the filter, got:\n%s", sql)
	}
}

// sort() on a model output belongs to the post-join layer; in the source scan the
// column does not exist yet (CH code 47).
func TestModelLookup_SortOnModelFieldDefers(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | sort(beacon_score, desc)`)
	outerEnd := strings.Index(sql, ") AS _outer")
	if outerEnd < 0 {
		t.Fatalf("no _outer subquery, got:\n%s", sql)
	}
	if strings.Contains(sql[:outerEnd], "ORDER BY beacon_score") {
		t.Errorf("sort on a model output must not bind to the source scan, got:\n%s", sql)
	}
	if !strings.Contains(sql[outerEnd:], "ORDER BY beacon_score DESC") {
		t.Errorf("expected post-join ORDER BY beacon_score DESC, got:\n%s", sql)
	}
}

// Aggregating after model_lookup() drops the join keys unless they are group
// columns. That must be a clear BQL error, not a bare ClickHouse code 215.
func TestModelLookup_AggregationWithoutKeysErrors(t *testing.T) {
	cases := map[string][]string{
		`* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | groupby(dst_ip)`: {"src_ip", "dst_port"},
		`* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | count()`:         {"src_ip", "dst_ip", "dst_port"},
	}
	for query, wantNamed := range cases {
		pipeline, err := ParseQuery(query)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, mlookupOpts())
		if err == nil {
			t.Fatalf("expected an error when aggregating away the model_lookup keys: %s", query)
		}
		for _, name := range wantNamed {
			if !strings.Contains(err.Error(), name) {
				t.Errorf("error should name the missing key %q, got: %v", name, err)
			}
		}
	}
}

// Aggregating on all three key fields keeps the join well-defined and must work.
func TestModelLookup_AggregationOnKeysWorks(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | table(src_ip, dst_ip, dst_port, count)`)
	if !strings.Contains(sql, "GROUP BY src_ip, dst_ip, dst_port") {
		t.Errorf("expected grouping on the key fields, got:\n%s", sql)
	}
}

// table() picks the columns: only the model outputs it names are displayed, even
// though the join still projects them all for deferred filters to reference.
func TestModelLookup_TableRestrictsDisplayedModelFields(t *testing.T) {
	pipeline, err := ParseQuery(`* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.5 | table(src_ip, dst_ip, ts_score)`)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, mlookupOpts())
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}
	for _, f := range result.FieldOrder {
		if f == "regularity_score" || f == "hist_score" || f == "beacon_score" {
			t.Errorf("unrequested model column %q in field order: %v", f, result.FieldOrder)
		}
	}
	var hasTs bool
	for _, f := range result.FieldOrder {
		if f == "ts_score" {
			hasTs = true
		}
	}
	if !hasTs {
		t.Errorf("requested ts_score missing from field order: %v", result.FieldOrder)
	}
	// The filtered-on column is still projected by the join.
	if !strings.Contains(result.SQL, "_mlookup.beacon_score") {
		t.Errorf("beacon_score must stay projected for the deferred filter, got:\n%s", result.SQL)
	}
}

// join() resolves its outer key the same way model_lookup does: `fields.X` exists
// only in the source scan, so gluing `_outer.` onto that expression cannot resolve
// (CH code 47). The key must be projected as the hidden _join_k column.
func TestJoin_OuterKeyProjectedNotInlined(t *testing.T) {
	sql := translateML(t, `proto="tcp" | join(dst_ip) { proto="udp" | groupby(dst_ip) }`)

	if strings.Contains(sql, "_outer.fields.") || strings.Contains(sql, "_outer.CAST(") {
		t.Errorf("ON must not glue _outer. onto a JSON expression, got:\n%s", sql)
	}
	if !strings.Contains(sql, "AS _join_k") {
		t.Errorf("expected the source scan to project the hidden join key, got:\n%s", sql)
	}
	if !strings.Contains(sql, "ON _outer._join_k = _join_sub.dst_ip") {
		t.Errorf("expected ON to match _join_k against the subquery key, got:\n%s", sql)
	}
	if !strings.Contains(sql, "EXCEPT (_join_k)") {
		t.Errorf("hidden key must be stripped from the output, got:\n%s", sql)
	}
}

// A key the outer query already names (a groupby output) is referenced directly,
// with no hidden projection.
func TestJoin_GroupedKeyNeedsNoProjection(t *testing.T) {
	sql := translateML(t, `* | groupby(dst_ip) | join(dst_ip) { * | groupby(dst_ip) }`)
	if strings.Contains(sql, "_join_k") {
		t.Errorf("an outer column key needs no hidden projection, got:\n%s", sql)
	}
	if !strings.Contains(sql, "ON _outer.dst_ip = _join_sub.dst_ip") {
		t.Errorf("expected a direct column match, got:\n%s", sql)
	}
}

// Every joined-in column comes through prefixed. `_join_sub.*` re-emitted the join
// key and collided with same-named outer columns (both sides produce `_count`),
// leaving duplicate names that no filter or sort could reference.
func TestJoin_OutputsArePrefixedAndKeyNotDuplicated(t *testing.T) {
	pipeline, err := ParseQuery(`* | groupby(dst_ip) | join(dst_ip) { * | groupby(dst_ip) }`)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, mlookupOpts())
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}
	if strings.Contains(result.SQL, "_join_sub.*") {
		t.Errorf("subquery columns must be projected explicitly, got:\n%s", result.SQL)
	}
	if !strings.Contains(result.SQL, "_join_sub._count AS _join__count") {
		t.Errorf("expected the subquery count prefixed, got:\n%s", result.SQL)
	}
	if strings.Contains(result.SQL, "AS _join_dst_ip") {
		t.Errorf("the join key is matched on, not carried through, got:\n%s", result.SQL)
	}
	// The joined column must be listed for display; previously it never was.
	if !contains(result.FieldOrder, "_join__count") {
		t.Errorf("joined column missing from field order: %v", result.FieldOrder)
	}
}

// sourceScanRegion returns the `FROM logs ...` text of the outer query's source
// scan, i.e. everything the innermost SELECT owns. Slicing at `) AS _outer` alone
// is not enough: the join wrapper's own SELECT list precedes it, and the joined
// subquery (with its own fields.X refs) follows it.
func sourceScanRegion(t *testing.T, sql string) string {
	t.Helper()
	start := strings.Index(sql, "FROM logs")
	end := strings.Index(sql, ") AS _outer")
	if start < 0 || end < start {
		t.Fatalf("could not locate the source scan in:\n%s", sql)
	}
	return sql[start:end]
}

// deferredTail returns the post-join layer: everything after the outermost
// `SELECT * FROM (...)` wrapper closes, which is where deferred filters/order land.
func deferredTail(t *testing.T, sql string) string {
	t.Helper()
	i := strings.LastIndex(sql, " AS _join_sub ON ")
	if i < 0 {
		t.Fatalf("no join wrap in:\n%s", sql)
	}
	return sql[i:]
}

// A filter or sort on a joined-in column must land after the join wrap.
func TestJoin_FilterAndSortOnJoinedColumnDefer(t *testing.T) {
	for _, q := range []string{
		`proto="tcp" | join(dst_ip) { * | groupby(dst_ip) } | _join__count > 100`,
		`proto="tcp" | join(dst_ip) { * | groupby(dst_ip) } | sort(_join__count, desc)`,
	} {
		sql := translateML(t, q)
		if strings.Contains(sourceScanRegion(t, sql), "_join__count") {
			t.Errorf("joined column must not bind to the source scan for %q, got:\n%s", q, sql)
		}
		if !strings.Contains(deferredTail(t, sql), "_join__count") {
			t.Errorf("expected a post-join reference for %q, got:\n%s", q, sql)
		}
	}
}

// Mixing a joined column with a source field in one AND clause must split across
// the two scopes, exactly as for model_lookup.
func TestJoin_MixedCompoundSplitsAcrossScopes(t *testing.T) {
	sql := translateML(t, `proto="tcp" | join(dst_ip) { * | groupby(dst_ip) } | _join__count > 100 AND service="ssl"`)
	if !strings.Contains(sourceScanRegion(t, sql), "`service`") {
		t.Errorf("service predicate must push down to the source scan, got:\n%s", sql)
	}
	if tail := deferredTail(t, sql); strings.Contains(tail, "fields.") {
		t.Errorf("post-join filter must not reference fields.X, got:\n%s", sql)
	}
}

// An OR cannot be distributed across scopes, so the log-field side must be exported
// from the source scan as a hidden column the post-join layer can reference. This is
// the generic mechanism: it is keyed on the expression being relocated, not on which
// command caused the relocation.
func TestDeferredScope_OrMixingExportsSourceColumn(t *testing.T) {
	cases := map[string]string{
		"model_lookup": `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.5 OR image=~zerotier`,
		"join":         `* | join(dst_ip) { * | groupby(dst_ip) } | _join__count > 10 OR image=~zerotier`,
		"window":       `* | mzscore(duration) | _modified_z > 3 OR image=~zerotier`,
	}
	for name, q := range cases {
		sql := translateML(t, q)
		if !strings.Contains(sql, "AS _dfr_0") {
			t.Errorf("%s: expected the log field exported as a hidden column, got:\n%s", name, sql)
		}
		if !strings.Contains(sql, "EXCEPT (_dfr_0)") {
			t.Errorf("%s: hidden column must be stripped from the result, got:\n%s", name, sql)
		}
		// The relocated predicate must read the exported column, never fields.X.
		tail := sql[strings.LastIndex(sql, ") WHERE "):]
		if strings.Contains(tail, "fields.") {
			t.Errorf("%s: deferred filter still references fields.X, got:\n%s", name, sql)
		}
		if !strings.Contains(tail, "_dfr_0") {
			t.Errorf("%s: deferred filter should read the exported column, got:\n%s", name, sql)
		}
	}
}

// The same export covers a relocated ORDER BY: once a deferred filter lifts the sort
// past the join, `sort(image)` can no longer read fields.image either.
func TestDeferredScope_RelocatedSortExportsSourceColumn(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.5 | sort(image, desc)`)
	if !strings.Contains(sql, "AS _dfr_0") {
		t.Errorf("expected the sort field exported, got:\n%s", sql)
	}
	if !strings.Contains(sql, "ORDER BY _dfr_0 DESC") {
		t.Errorf("expected the deferred sort to read the exported column, got:\n%s", sql)
	}
}

// Each distinct source expression is exported once, however many times it is used.
func TestDeferredScope_ExportsEachExpressionOnce(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.5 OR image=~a OR image=~b`)
	if strings.Count(sql, "AS _dfr_0") != 1 {
		t.Errorf("expected exactly one export of image, got:\n%s", sql)
	}
	if strings.Contains(sql, "_dfr_1") {
		t.Errorf("the same expression must reuse one hidden column, got:\n%s", sql)
	}
}

// Bare column names already resolve at every layer, so they are never exported.
func TestDeferredScope_BareColumnsNeedNoExport(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.5 OR conn_count > 100`)
	if strings.Contains(sql, "_dfr_") {
		t.Errorf("joined columns need no export, got:\n%s", sql)
	}
}

// Hidden columns are stripped from the result, so they must not be offered for display.
func TestDeferredScope_HiddenColumnsNotDisplayed(t *testing.T) {
	pipeline, err := ParseQuery(`* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.5 OR image=~zerotier`)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, mlookupOpts())
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}
	for _, f := range result.FieldOrder {
		if strings.HasPrefix(f, "_dfr_") {
			t.Errorf("hidden export leaked into field order: %v", result.FieldOrder)
		}
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
