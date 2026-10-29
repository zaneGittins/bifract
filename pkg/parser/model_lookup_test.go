package parser

import (
	"regexp"
	"slices"
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

// The join compares the log's key expressions to the model's key columns. It reads
// a scan that projects every logs column, so fields.X resolves in the ON.
func TestModelLookup_BareEnrichmentJoinsOnKeyExpressions(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port])`)

	const on = "AS _mlookup ON fields.`src_ip`::String = _mlookup.src_ip AND fields.`dst_ip`::String = _mlookup.dst_ip AND fields.`dst_port`::String = _mlookup.dst_port"
	if !strings.Contains(sql, on) {
		t.Errorf("expected ON %s, got:\n%s", on, sql)
	}
	if !strings.Contains(sql, " FROM logs INNER JOIN (") {
		t.Errorf("expected the join to read the logs table itself, got:\n%s", sql)
	}
	if strings.Contains(sql, "_mlk_k") || strings.Contains(sql, "EXCEPT") {
		t.Errorf("a scan-level join needs no hidden key columns, got:\n%s", sql)
	}
	if !strings.Contains(sql, "beacon_score") {
		t.Errorf("expected beacon_score output column, got:\n%s", sql)
	}
}

// A threshold on a model output reads the bare output column: on the model side of
// a strict join, and on the joined rows of a LEFT one, whose unscored rows carry
// the column's default.
func TestModelLookup_ThresholdReadsTheOutputColumn(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.8`)
	if !strings.Contains(sql, "WHERE beacon_score > 0.8) AS _mlookup") {
		t.Errorf("strict: expected the threshold on the model side, got:\n%s", sql)
	}
	sql = translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port], require=false) | beacon_score > 0.8`)
	if strings.Contains(sql, "beacon_score > 0.8) AS _mlookup") || mustLastIndex(t, sql, "beacon_score > 0.8") < mustIndex(t, sql, "_mlookup ON") {
		t.Errorf("require=false: expected the threshold on the joined rows, got:\n%s", sql)
	}
	if strings.Contains(sql, "fields.`beacon_score`") {
		t.Errorf("the threshold must read the output column, got:\n%s", sql)
	}
}

// Filters BEFORE model_lookup stay in the source scan below the join.
func TestModelLookup_SourceFilterStaysInner(t *testing.T) {
	sql := translateML(t, `dst_ip="8.8.4.4" | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port])`)
	inner := belowJoin(t, sql)
	if !strings.Contains(inner, "`dst_ip`") || !strings.Contains(inner, "8.8.4.4") {
		t.Errorf("expected dst_ip filter in the scan below the join, got:\n%s", sql)
	}
}

// rarity still works: one key field per model key column, so the ON is plain
// equalities rather than a char(30)-encoded pair.
func TestModelLookup_RarityJoinShape(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="rare", key=[image, hash]) | coverage > 0.9`)
	if !strings.Contains(sql, "ON fields.`image`::String = _mlookup.partition_val AND fields.`hash`::String = _mlookup.value_val") {
		t.Errorf("unexpected rarity ON clause, got:\n%s", sql)
	}
}

// table() listing model-output columns must NOT project them as raw JSON fields in
// the source scan: they are the join's columns, in the order table() names them.
func TestModelLookup_TableWithModelColumns(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.8 | table(src_ip, dst_ip, beacon_score, ts_score)`)
	if strings.Contains(sql, "`beacon_score`") || strings.Contains(sql, "`ts_score`") {
		t.Errorf("model output columns must not be projected as JSON fields, got:\n%s", sql)
	}
	if !strings.Contains(sql, "fields.`src_ip`::String AS src_ip, fields.`dst_ip`::String AS dst_ip, beacon_score, ts_score") {
		t.Errorf("expected the model columns projected where table() names them, got:\n%s", sql)
	}
}

// A clause mixing a model output with a source field must be split: the log-field
// predicate pushes down to the source scan, where its skip index can prune, and the
// model threshold filters the model side.
func TestModelLookup_MixedCompoundSplitsAcrossScopes(t *testing.T) {
	sql := translateML(t, `event_id="3" | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.5 AND NOT image=~zerotier`)
	if c := scanCap(sql); !strings.Contains(c, "`image`") {
		t.Errorf("image predicate must push down into the source scan, got:\n%s", sql)
	}
	if !strings.Contains(sql, "WHERE beacon_score > 0.5) AS _mlookup") {
		t.Errorf("beacon_score threshold must filter the model side, got:\n%s", sql)
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

// The row limit must never be applied before the model filter, or the filter only
// sees the newest MaxRows logs. A strict join applies the filter in the scan's own
// prefilter, ahead of the LIMIT there.
func TestModelLookup_LimitAppliedAfterModelFilter(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.8`)
	c := scanCap(sql)
	if !strings.Contains(c, "WHERE beacon_score > 0.8)") || !strings.Contains(c, "LIMIT 1000") {
		t.Errorf("the scan must apply the model filter before its LIMIT, got:\n%s", sql)
	}
}

// sort() on a model output orders the joined rows; the scan cannot be capped first,
// since its order does not decide which rows come first.
func TestModelLookup_SortOnModelFieldDefers(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | sort(beacon_score, desc)`)
	if scanCap(sql) != "" {
		t.Errorf("a sort on a model output must not cap the scan, got:\n%s", sql)
	}
	if mustLastIndex(t, sql, "ORDER BY beacon_score DESC") < mustIndex(t, sql, "_mlookup ON") {
		t.Errorf("expected ORDER BY beacon_score DESC on the joined rows, got:\n%s", sql)
	}
}

// Aggregating after model_lookup() moves the join to the scan level: the
// aggregation reads (scan JOIN model) so the keys need not survive it. The join
// must sit in the aggregating stage's FROM, before GROUP BY, with no outer wrap.
func TestModelLookup_AggregationAfterUsesScanLevelJoin(t *testing.T) {
	for _, query := range []string{
		`* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | groupby(dst_ip)`,
		`* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | count()`,
	} {
		sql := translateML(t, query)
		joinIdx := strings.Index(sql, "INNER JOIN")
		if joinIdx < 0 {
			t.Fatalf("expected scan-level INNER JOIN, got:\n%s", sql)
		}
		if strings.Contains(sql, "_outer.*") || strings.Contains(sql, "_mlk_k0") {
			t.Errorf("scan-level join must not use the outer wrap or hidden key projections, got:\n%s", sql)
		}
		if gbIdx := strings.Index(sql, "GROUP BY"); gbIdx >= 0 && gbIdx < joinIdx {
			t.Errorf("join must precede GROUP BY (rows enriched before aggregation), got:\n%s", sql)
		}
		// The ON compares the raw key expressions against the model key columns.
		if !strings.Contains(sql, "= _mlookup.src_ip") {
			t.Errorf("expected direct key expressions in ON, got:\n%s", sql)
		}
		// Strict mode's semi-join prefilter still narrows the scan.
		if !strings.Contains(sql, "GLOBAL IN (SELECT") && !strings.Contains(sql, "IN (SELECT _mlk_src") && !strings.Contains(sql, "IN (SELECT "+modelPrefilterAlias) {
			t.Errorf("expected strict key prefilter in the scan WHERE, got:\n%s", sql)
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
	// The filtered-on column still filters, on the model side.
	if !strings.Contains(result.SQL, "WHERE beacon_score > 0.5) AS _mlookup") {
		t.Errorf("beacon_score must still filter the model side, got:\n%s", result.SQL)
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
		"join":   `* | join(dst_ip) { * | groupby(dst_ip) } | _join__count > 10 OR image=~zerotier`,
		"window": `* | mzscore(duration) | _modified_z > 3 OR image=~zerotier`,
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
	sql := translateML(t, `* | join(dst_ip) { * | groupby(dst_ip) } | _join__count > 10 | sort(image, desc)`)
	if !strings.Contains(sql, "AS _dfr_0") {
		t.Errorf("expected the sort field exported, got:\n%s", sql)
	}
	if !strings.Contains(sql, "ORDER BY _dfr_0 DESC") {
		t.Errorf("expected the deferred sort to read the exported column, got:\n%s", sql)
	}
}

// Each distinct source expression is exported once, however many times it is used.
func TestDeferredScope_ExportsEachExpressionOnce(t *testing.T) {
	sql := translateML(t, `* | join(dst_ip) { * | groupby(dst_ip) } | _join__count > 10 OR image=~a OR image=~b`)
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
	pipeline, err := ParseQuery(`* | join(dst_ip) { * | groupby(dst_ip) } | _join__count > 10 OR image=~zerotier`)
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
	if !strings.Contains(sql, "fields.`src_ip`::String = _mlookup.entity_key") {
		t.Errorf("unexpected first_seen ON clause, got:\n%s", sql)
	}
}

// is_new means new to the model: when its state first recorded the entity, not
// the event time, which is old on a late or replayed log.
func TestModelLookup_FirstSeenIsNewReadsFirstRecorded(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="fs", key=[src_ip]) | is_new = "1"`)
	for _, want := range []string{
		"min(first_recorded) AS recorded_at",
		"if(recorded_at >= now() - INTERVAL 1 HOUR, '1', '0') AS is_new",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("want %q in:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "if(first_seen >=") {
		t.Errorf("is_new must not read the event time:\n%s", sql)
	}
}

// An alert reaching back past the hour, catching up on a backlog, still treats
// what the model recorded since its window start as new.
func TestModelLookup_FirstSeenIsNewCoversAlertWindow(t *testing.T) {
	opts := mlookupOpts()
	opts.ModelNewSince = time.Date(2026, 1, 1, 6, 30, 0, 0, time.UTC)
	sql := translateMLWith(t, `* | model_lookup(model="fs", key=[src_ip]) | is_new = "1"`, opts)
	want := "if(recorded_at >= least(now() - INTERVAL 1 HOUR, toDateTime64('2026-01-01 06:30:00.000', 3, 'UTC')), '1', '0') AS is_new"
	if !strings.Contains(sql, want) {
		t.Errorf("want %q in:\n%s", want, sql)
	}
}

// ---- strict mode (model_lookup default) ----

// translateMLWith is translateML against caller-supplied options (prism, cluster).
func translateMLWith(t *testing.T, query string, opts QueryOptions) string {
	t.Helper()
	return mlResult(t, query, opts).SQL
}

// Strict is the default: the join keeps only scored rows, and the same key set is
// pushed into the scan so the join is not fed every log in the range.
func TestModelLookup_StrictIsDefault(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port])`)

	if !strings.Contains(sql, "INNER JOIN") || strings.Contains(sql, "LEFT JOIN") {
		t.Errorf("expected an INNER JOIN by default, got:\n%s", sql)
	}
	want := "(fields.`src_ip`::String, fields.`dst_ip`::String, fields.`dst_port`::String) IN " +
		"(SELECT _mlk_src.src_ip, _mlk_src.dst_ip, _mlk_src.dst_port FROM `model_beacon` AS _mlk_src WHERE _mlk_src.fractal_id IN ('f1'))"
	if !strings.Contains(sql, want) {
		t.Errorf("expected the semi-join prefilter %s, got:\n%s", want, sql)
	}
	// It must filter the scan that picks the rows, as well as the rows joined.
	if !strings.Contains(scanCap(sql), want) || mustLastIndex(t, sql, want) < mustIndex(t, sql, "_mlookup ON") {
		t.Errorf("prefilter must be in the source scan WHERE, got:\n%s", sql)
	}
}

// strict=false restores the enrich-everything behaviour: LEFT JOIN, no prefilter.
func TestModelLookup_StrictFalseKeepsUnscoredRows(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port], strict=false)`)

	if !strings.Contains(sql, "LEFT JOIN") || strings.Contains(sql, "INNER JOIN") {
		t.Errorf("expected a LEFT JOIN for strict=false, got:\n%s", sql)
	}
	if strings.Contains(sql, "IN (SELECT src_ip, dst_ip, dst_port FROM") {
		t.Errorf("strict=false must not prefilter the scan (it would drop unscored rows), got:\n%s", sql)
	}
}

func TestModelLookup_StrictRejectsBadValue(t *testing.T) {
	pipeline, err := ParseQuery(`* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port], strict=maybe)`)
	if err == nil {
		_, err = TranslateToSQLWithOrder(pipeline, mlookupOpts())
	}
	if err == nil || !strings.Contains(err.Error(), "strict=") {
		t.Errorf("expected a strict= validation error, got: %v", err)
	}
}

// The prefilter reads the model table's raw key columns: no FINAL, no aggregation and
// no window functions, so it stays a two-column read of the sorting-key prefix.
func TestModelLookup_PrefilterReadsRawKeysOnly(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="rare", key=[image, hash])`)

	const want = "(fields.`image`::String, fields.`hash`::String) IN " +
		"(SELECT _mlk_src.partition_val, _mlk_src.value_val FROM `model_rare` AS _mlk_src WHERE _mlk_src.fractal_id IN ('f1'))"
	if !strings.Contains(sql, want) {
		t.Errorf("expected raw-key prefilter %s, got:\n%s", want, sql)
	}
}

// The prefilter's left side is ModelLookupKeyExprs verbatim, so it can never disagree
// with the key expressions the JOIN ON compares.
func TestModelLookup_PrefilterKeysMatchProjectedKeys(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port])`)

	inIdx := strings.Index(sql, " IN (SELECT _mlk_src.src_ip")
	if inIdx < 0 {
		t.Fatalf("no prefilter in:\n%s", sql)
	}
	lhs := sql[strings.LastIndex(sql[:inIdx], "AND "):inIdx]

	keyExprs := regexp.MustCompile(`([^\s(]+) = _mlookup\.`).FindAllStringSubmatch(sql, -1)
	if len(keyExprs) != 3 {
		t.Fatalf("expected 3 joined key expressions, got %d in:\n%s", len(keyExprs), sql)
	}
	for _, m := range keyExprs {
		if !strings.Contains(lhs, m[1]) {
			t.Errorf("join key %q is not the prefilter left side %q", m[1], lhs)
		}
	}
}

// A model type that scores keys away (rarity's min_sample) leaves its raw key set a
// superset of the scored one, so the prefilter reads the scored set instead: the
// join then drops nothing the scan kept, and the scan keeps its LIMIT.
func TestModelLookup_ScoredSetPrefilterKeepsScanLimit(t *testing.T) {
	opts := mlookupOpts()
	opts.Models["rare5"] = AnalyticsModelInfo{ID: "4", TableName: "model_rare5", ModelType: "rarity", MinSample: 5, FractalID: "f1"}
	sql := translateMLWith(t, `* | model_lookup(model="rare5", key=[image, hash])`, opts)

	c := scanCap(sql)
	if !strings.Contains(c, "(fields.`image`::String, fields.`hash`::String) IN (SELECT partition_val, value_val FROM (") || !strings.Contains(c, "days_observed >= 5") {
		t.Errorf("expected the scan prefiltered by the scored key set, got:\n%s", sql)
	}
	if !strings.Contains(c, "LIMIT 1000") {
		t.Errorf("expected the scan to keep its LIMIT, got:\n%s", sql)
	}
}

// A threshold on a model output that a LEFT JOIN can match on unscored rows filters
// the joined rows, so the scan must not be capped below it. Under a strict join the
// same threshold narrows the scan's prefilter and the scan keeps its LIMIT.
func TestModelLookup_ThresholdAndTheScanLimit(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port], strict=false) | beacon_score > 0.8`)
	if mustIndex(t, sql, "LIMIT 1000") < mustIndex(t, sql, "_mlookup ON") {
		t.Errorf("expected no LIMIT below the LEFT JOIN, got:\n%s", sql)
	}
	sql = translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.8`)
	if c := scanCap(sql); !strings.Contains(c, "LIMIT 1000") {
		t.Errorf("expected the strict join's scan to keep its LIMIT, got:\n%s", sql)
	}
}

// In a prism the prefilter must span every member fractal, exactly like the scoring
// subquery it mirrors; scoping it to one member would drop the others' rows.
func TestModelLookup_PrefilterCoversPrismMembers(t *testing.T) {
	opts := mlookupOpts()
	opts.FractalIDs = []string{"f1", "f2"}
	sql := translateMLWith(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port])`, opts)

	// The prefilter and the scoring subquery span the members, as the log scan does.
	if !strings.Contains(sql, "_mlk_src.fractal_id IN ('f1', 'f2')") || !regexp.MustCompile(`WHERE fractal_id IN \('f1', 'f2'\)\s+GROUP BY`).MatchString(sql) {
		t.Errorf("expected the prefilter and the scoring subquery to span the members, got:\n%s", sql)
	}
}

// Against a Distributed model table both the join and its prefilter must be GLOBAL:
// ClickHouse rejects a non-GLOBAL subquery over a Distributed table inside a query
// that already reads logs_distributed (distributed_product_mode='deny').
func TestModelLookup_ClusterUsesGlobal(t *testing.T) {
	opts := mlookupOpts()
	opts.TableName = "logs_distributed"
	opts.Models["beacon"] = AnalyticsModelInfo{ID: "3", TableName: "model_beacon_dist", ModelType: "beacon", FractalID: "f1", Distributed: true}
	sql := translateMLWith(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port])`, opts)

	if !strings.Contains(sql, "GLOBAL INNER JOIN") {
		t.Errorf("expected GLOBAL INNER JOIN in cluster mode, got:\n%s", sql)
	}
	if !strings.Contains(sql, "GLOBAL IN (SELECT _mlk_src.src_ip, _mlk_src.dst_ip, _mlk_src.dst_port FROM `model_beacon_dist`") {
		t.Errorf("expected the prefilter to use GLOBAL IN, got:\n%s", sql)
	}
}

// Single-node queries must not pay for GLOBAL.
func TestModelLookup_SingleNodeHasNoGlobal(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port])`)
	if strings.Contains(sql, "GLOBAL INNER JOIN") || strings.Contains(sql, "GLOBAL IN (SELECT _mlk_src") {
		t.Errorf("expected no GLOBAL model join or prefilter outside cluster mode, got:\n%s", sql)
	}
}

// The network scoring subquery must return exactly one row per pair, or an INNER JOIN
// multiplies log rows. FINAL cannot promise that: it collapses per shard, and a prism
// reads several fractals whose rows share the pair but not the sorting key.
func TestModelLookup_NetworkScoringIsOneRowPerPair(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="beacon", key=[src_ip, dst_ip, dst_port])`)

	if strings.Contains(sql, "FINAL") {
		t.Errorf("network scoring must not rely on FINAL, got:\n%s", sql)
	}
	if !strings.Contains(sql, "GROUP BY src_ip, dst_ip, dst_port") {
		t.Errorf("expected the scoring subquery to group by the pair, got:\n%s", sql)
	}
	if !strings.Contains(sql, "argMax(final_score, scored_at) AS beacon_score") {
		t.Errorf("expected the newest score per pair via argMax, got:\n%s", sql)
	}
}

// volume_baseline scores an entity only once it has min_sample buckets of history
// before the latest complete bucket, so the prefilter admits exactly the entities
// with a bucket that far back and the scan keeps its LIMIT.
func TestModelLookup_VolumePrefilterCarriesBucketBounds(t *testing.T) {
	opts := mlookupOpts()
	opts.Models["vol"] = AnalyticsModelInfo{ID: "5", TableName: "model_vol", ModelType: "volume_baseline", MinSample: 7, TimeBucket: "day", FractalID: "f1"}
	sql := translateMLWith(t, `* | model_lookup(model="vol", key=[user])`, opts)

	const want = "fields.`user`::String IN (SELECT _mlk_src.entity_val FROM `model_vol` AS _mlk_src " +
		"WHERE _mlk_src.fractal_id IN ('f1') AND _mlk_src.bucket >= toDate(now('UTC')) - INTERVAL 90 DAY " +
		"AND _mlk_src.bucket <= toDate(now('UTC')) - INTERVAL 1 DAY - INTERVAL 7 day)"
	if !strings.Contains(sql, want) {
		t.Errorf("expected history-bounded prefilter %s, got:\n%s", want, sql)
	}
	if !strings.Contains(scanCap(sql), "LIMIT 1000") {
		t.Errorf("an exact prefilter keeps the LIMIT on the source scan, got:\n%s", sql)
	}
}

// Several key fields against a single model key column (how first_seen and
// volume_baseline encode a composite entity) must still prefilter, using the model's
// own char(30) encoding. Skipping it there would leave a strict join with no scan
// filter and a LIMIT lifted above it: a full-range scan on the most common
// multi-field first_seen shape.
func TestModelLookup_CompositeKeyPrefilters(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="fs", key=[user, computer_name])`)

	const want = "concat(fields.`user`::String, char(30), fields.`computer_name`::String) IN " +
		"(SELECT _mlk_src.entity_key FROM `model_fs` AS _mlk_src WHERE _mlk_src.fractal_id IN ('f1'))"
	if !strings.Contains(sql, want) {
		t.Errorf("expected composite prefilter %s, got:\n%s", want, sql)
	}
	// The prefilter is exactly the ON predicate, so the scan keeps its LIMIT.
	if !strings.Contains(scanCap(sql), "LIMIT 1000") {
		t.Errorf("expected the LIMIT to stay on the source scan, got:\n%s", sql)
	}
}

// The prefilter and the JOIN ON must express the same predicate in every key shape:
// one drops rows at the scan on the strength of the other matching them.
func TestModelLookup_PrefilterMirrorsJoinOn(t *testing.T) {
	opts := mlookupOpts()
	opts.Models["vol"] = AnalyticsModelInfo{ID: "5", TableName: "model_vol", ModelType: "volume_baseline", MinSample: 1, TimeBucket: "day", FractalID: "f1"}
	cases := map[string]struct{ on, prefilter string }{
		`* | model_lookup(model="fs", key=[user])`: {
			on:        "ON fields.`user`::String = _mlookup.entity_key",
			prefilter: "fields.`user`::String IN (SELECT _mlk_src.entity_key",
		},
		`* | model_lookup(model="rare", key=[image, hash])`: {
			on:        "ON fields.`image`::String = _mlookup.partition_val AND fields.`hash`::String = _mlookup.value_val",
			prefilter: "(fields.`image`::String, fields.`hash`::String) IN (SELECT _mlk_src.partition_val, _mlk_src.value_val",
		},
		`* | model_lookup(model="vol", key=[user, computer_name])`: {
			on:        "ON concat(fields.`user`::String, char(30), fields.`computer_name`::String) = _mlookup.entity_val",
			prefilter: "concat(fields.`user`::String, char(30), fields.`computer_name`::String) IN (SELECT _mlk_src.entity_val",
		},
	}
	for query, want := range cases {
		sql := translateMLWith(t, query, opts)
		if !strings.Contains(sql, want.on) {
			t.Errorf("%s: expected ON %q, got:\n%s", query, want.on, sql)
		}
		if !strings.Contains(sql, want.prefilter) {
			t.Errorf("%s: expected prefilter %q, got:\n%s", query, want.prefilter, sql)
		}
	}
}

// --- Scan-level join (aggregation after model_lookup) ---

// Grouping BY a model output column: the column is per-row after the scan-level
// join, so it can be a group key.
func TestModelLookup_GroupByModelColumn(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="fs", key=[user]) | groupby(is_new)`)
	if !strings.Contains(sql, "GROUP BY is_new") {
		t.Errorf("expected GROUP BY is_new, got:\n%s", sql)
	}
	if strings.Contains(sql, "fields.`is_new`") {
		t.Errorf("is_new must resolve to the join column, not a JSON field, got:\n%s", sql)
	}
	if !strings.Contains(sql, "AS _mlookup ON") {
		t.Errorf("expected scan-level join, got:\n%s", sql)
	}
}

// Aggregating a model output column: avg(percent) must read the typed join
// column via toFloat64 (toFloat64OrNull rejects Float64 input).
func TestModelLookup_AggregateModelColumn(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="rare", key=[user, image]) | groupby(user, function=avg(percent))`)
	if !strings.Contains(sql, "avg(toFloat64(percent))") {
		t.Errorf("expected avg(toFloat64(percent)), got:\n%s", sql)
	}
	if !strings.Contains(sql, "GROUP BY") {
		t.Errorf("expected aggregation, got:\n%s", sql)
	}
}

// A filter on a model column between model_lookup and the aggregation filters rows
// (the model side of the join, here) before GROUP BY, never the final groups.
func TestModelLookup_ModelFilterAppliesBeforeAggregation(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="rare", key=[user, image]) | percent < 1 | groupby(user)`)
	if !strings.Contains(sql, "WHERE percent < 1) AS _mlookup ON") || mustIndex(t, sql, "percent < 1") > mustLastIndex(t, sql, "GROUP BY") {
		t.Errorf("percent filter must apply before GROUP BY, got:\n%s", sql)
	}
	if strings.Contains(sql, "HAVING percent") {
		t.Errorf("model filter must not become HAVING, got:\n%s", sql)
	}
	// require=false cannot narrow the model side: the filter reads the joined rows.
	sql = translateML(t, `* | model_lookup(model="rare", key=[user, image], require=false) | percent < 1 | groupby(user)`)
	if i := mustIndex(t, sql, "percent < 1"); i < mustIndex(t, sql, "_mlookup ON") || i > mustLastIndex(t, sql, "GROUP BY") {
		t.Errorf("percent filter must sit between the join and GROUP BY, got:\n%s", sql)
	}
}

// After an aggregation the lookup enriches the groups: the stage after GROUP BY
// joins them on the group column, and the scan keeps the strict prefilter, since a
// group whose key the model does not score is dropped by the join anyway.
func TestModelLookup_AggregationBeforeJoinsTheGroups(t *testing.T) {
	sql := translateML(t, `* | groupby(user) | model_lookup(model="fs", key=[user])`)
	gb, on := strings.Index(sql, "GROUP BY user"), strings.Index(sql, "ON ifNull(toString(_outer.user), '') = _mlookup.entity_key")
	if gb < 0 || on < 0 || gb > on {
		t.Errorf("expected the groups joined on the group column after GROUP BY, got:\n%s", sql)
	}
	if !strings.Contains(sql[:gb], "fields.`user`::String IN (SELECT _mlk_src.entity_key") {
		t.Errorf("expected the strict prefilter on the scan, got:\n%s", sql)
	}
	if strings.Contains(sql, "_mlk_k") {
		t.Errorf("the join reads the group column, so no hidden key is projected, got:\n%s", sql)
	}
}

// chain() steps may reference model columns when model_lookup precedes chain:
// the step condition compiles to the bare join column inside the sequence
// aggregate, and the step-union prefilter moves to the post-join WHERE (the scan
// keeps strict mode's key prefilter).
func TestModelLookup_ChainStepsOnModelColumns(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="fs", key=[user]) | chain(user, within="5m") { is_new="1"; event_type="process_creation" }`)
	if !strings.Contains(sql, "sequenceMatch") {
		t.Fatalf("expected sequenceMatch, got:\n%s", sql)
	}
	// The step compiles against the bare join column (numeric-equality codegen
	// stringifies it), never a JSON field shadow.
	if !strings.Contains(sql, "toString(is_new)") && !strings.Contains(sql, "is_new = '1'") {
		t.Errorf("expected bare is_new step condition, got:\n%s", sql)
	}
	if strings.Contains(sql, "fields.`is_new`") {
		t.Errorf("is_new must not resolve to a JSON field, got:\n%s", sql)
	}
	joinIdx := strings.Index(sql, "AS _mlookup ON")
	if joinIdx < 0 {
		t.Fatalf("expected scan-level join, got:\n%s", sql)
	}
	// The step union references a model column, so its WHERE copy must sit after
	// the join (the sequence aggregates in the SELECT also carry the conditions).
	if !strings.Contains(sql[joinIdx:], "OR (fields.`event_type`::String = 'process_creation')") {
		t.Errorf("step union referencing model columns must run post-join, got:\n%s", sql)
	}
	// Strict prefilter still narrows the scan.
	if !strings.Contains(sql, "IN (SELECT") {
		t.Errorf("expected the strict key prefilter on the scan, got:\n%s", sql)
	}
}

// A chain() step on a model column without model_lookup before chain is a clear
// BQL error, not ClickHouse code 47.
func TestModelLookup_ChainStepModelColumnRequiresOrder(t *testing.T) {
	pipeline, err := ParseQuery(`* | chain(user, within="5m") { is_new="1"; event_type="x" } | model_lookup(model="fs", key=[user])`)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	_, err = TranslateToSQLWithOrder(pipeline, mlookupOpts())
	if err == nil || !strings.Contains(err.Error(), "model_lookup() before chain()") {
		t.Errorf("expected ordering error, got: %v", err)
	}
}

// chain() after model_lookup with log-field-only steps keeps the step union on
// the scan (it prunes) while the join still enriches rows.
func TestModelLookup_ChainAfterModelLookupKeepsScanUnion(t *testing.T) {
	sql := translateML(t, `* | model_lookup(model="fs", key=[user]) | chain(user, within="5m") { event_type="login"; event_type="exec" }`)
	joinIdx := strings.Index(sql, "AS _mlookup ON")
	if joinIdx < 0 {
		t.Fatalf("expected scan-level join, got:\n%s", sql)
	}
	if !strings.Contains(sql, "((fields.`event_type`::String = 'login') OR (fields.`event_type`::String = 'exec'))") {
		t.Fatalf("expected step-union scan prefilter, got:\n%s", sql)
	}
}

// Model columns are ordinary columns of the enriched groups, so a later aggregation
// can group by them.
func TestModelLookup_GroupByModelColumnOfEnrichedGroups(t *testing.T) {
	sql := translateML(t, `* | groupby(user) | model_lookup(model="fs", key=[user]) | groupby(is_new)`)
	if on, gb := strings.Index(sql, "_mlookup ON"), strings.LastIndex(sql, "GROUP BY is_new"); on < 0 || gb < on {
		t.Errorf("expected GROUP BY is_new over the joined groups, got:\n%s", sql)
	}
}

// Cluster mode: the scan-level join must be GLOBAL so each shard joins its local
// scan against the broadcast model set and pre-aggregates.
func TestModelLookup_ScanLevelJoinIsGlobalOnCluster(t *testing.T) {
	opts := mlookupOpts()
	opts.TableName = "logs_distributed"
	m := opts.Models["fs"]
	m.Distributed = true
	opts.Models["fs"] = m
	pipeline, err := ParseQuery(`* | model_lookup(model="fs", key=[user]) | groupby(is_new)`)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}
	if !strings.Contains(result.SQL, "GLOBAL INNER JOIN") {
		t.Errorf("expected GLOBAL scan-level join on cluster, got:\n%s", result.SQL)
	}
	if !strings.Contains(result.SQL, "GLOBAL IN (SELECT") {
		t.Errorf("expected GLOBAL strict prefilter on cluster, got:\n%s", result.SQL)
	}
}

// A projection after the enriched groups reads them like any stage: the join has
// already run below it.
func TestModelLookup_ProjectionAfterEnrichedGroups(t *testing.T) {
	sql := translateML(t, `* | groupby(user) | model_lookup(model="fs", key=[user]) | table(user, _count)`)
	if !strings.HasPrefix(sql, "SELECT user, _count FROM") || !strings.Contains(sql, "ifNull(toString(_outer.user), '') = _mlookup.entity_key") {
		t.Errorf("expected the projection over the joined groups, got:\n%s", sql)
	}
}

// table() naming a model column after the enriched groups reads the joined column,
// and shows only the columns it names.
func TestModelLookup_ProjectionReadsModelColumnOfGroups(t *testing.T) {
	pipeline, err := ParseQuery(`* | groupby(user) | model_lookup(model="fs", key=[user]) | table(user, is_new)`)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, mlookupOpts())
	if err != nil {
		t.Fatalf("translate error: %v", err)
	}
	if !strings.Contains(result.SQL, "_mlookup.is_new AS is_new") {
		t.Errorf("model column must come from the join, got:\n%s", result.SQL)
	}
	want := []string{"user", "is_new"}
	if len(result.FieldOrder) != 2 || result.FieldOrder[0] != want[0] || result.FieldOrder[1] != want[1] {
		t.Errorf("expected field order %v, got %v", want, result.FieldOrder)
	}
}

// A model column that did not survive the aggregation is a clear BQL error from
// table() and from a later groupby stage, not CH code 47.
func TestModelLookup_UnavailableModelColumnErrorsCleanly(t *testing.T) {
	for query, wantErr := range map[string]string{
		`* | model_lookup(model="fs", key=[user]) | groupby(user) | table(user, is_new)`: "not available after the aggregation",
		`* | model_lookup(model="fs", key=[user]) | groupby(user) | groupby(is_new)`:     "not carried out of the previous aggregation",
	} {
		pipeline, err := ParseQuery(query)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		_, err = TranslateToSQLWithOrder(pipeline, mlookupOpts())
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Errorf("expected error containing %q for %s, got: %v", wantErr, query, err)
		}
	}
}

// The rarity column was renamed from confidence to coverage; an old filter must
// fail loudly rather than match nothing.
func TestRarityConfidenceFilterNamesTheRename(t *testing.T) {
	pipeline, err := ParseQuery(`* | model_lookup(model="rare", key=[image, hash]) | confidence > 0.9`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = TranslateToSQLWithOrder(pipeline, mlookupOpts())
	if err == nil || !strings.Contains(err.Error(), "coverage") {
		t.Fatalf("want an error naming coverage, got %v", err)
	}
}

// A windowed model reads its lookback, scores its window, and its strict
// prefilter admits only keys seen inside the window.
func TestModelLookup_HistoryWindow(t *testing.T) {
	opts := mlookupOpts()
	opts.Models["rare"] = AnalyticsModelInfo{ID: "1", TableName: "model_rare", ModelType: "rarity", MinSample: 1, FractalID: "f1", HistoryDays: 30}
	opts.Models["fs"] = AnalyticsModelInfo{ID: "2", TableName: "model_fs", ModelType: "first_seen", FractalID: "f1", HistoryDays: 90}

	rare := translateMLWith(t, `* | model_lookup(model="rare", key=[host, port])`, opts)
	mustHaveML(t, rare, "WHERE fractal_id IN ('f1') AND _day > toDate(now(), 'UTC') - 60")
	mustHaveML(t, rare, "arrayFilter(d -> d > toDate(now(), 'UTC') - 30, lookback_days) AS seen_days")
	mustHaveML(t, rare, modelPrefilterAlias+"._day > toDate(now(), 'UTC') - 30")

	fs := translateMLWith(t, `* | model_lookup(model="fs", key=[host])`, opts)
	mustHaveML(t, fs, "WHERE fractal_id IN ('f1') AND _day > toDate(now(), 'UTC') - 180")
	mustHaveML(t, fs, "minIf(first_seen, _day > toDate(now(), 'UTC') - 90) AS _fs")
	mustHaveML(t, fs, "if(recorded_at >= now() - INTERVAL 1 HOUR, '1', '0') AS is_new")
	mustHaveML(t, fs, modelPrefilterAlias+"._day > toDate(now(), 'UTC') - 90")

	// Without a window nothing names a day column the table does not have.
	plain := translateML(t, `* | model_lookup(model="fs", key=[host])`)
	if strings.Contains(plain, "_day") {
		t.Errorf("unwindowed model read bounds a day column:\n%s", plain)
	}
}

func mustHaveML(t *testing.T, sql, want string) {
	t.Helper()
	if !strings.Contains(sql, want) {
		t.Errorf("want %q in:\n%s", want, sql)
	}
}

// A model that keeps all its history reads its table as it always has; a
// windowed one counts its window and judges returns over twice it.
func TestFirstSeenAggSQLShapes(t *testing.T) {
	plain := FirstSeenAggSQL(FirstSeenAgg{Source: "`t` FINAL", Scope: "fractal_id = 'f'", KeyCol: "entity_key", Recorded: true, Days: true})
	for _, want := range []string{"min(first_recorded) AS recorded_at", "arraySort(groupUniqArrayMerge(365)(days)) AS days", "toDate(first_seen) AS since", "GROUP BY entity_key"} {
		mustHaveML(t, plain, want)
	}
	if strings.Contains(plain, HistoryDayColumn) {
		t.Errorf("unwindowed read names the day column:\n%s", plain)
	}
	if digest := FirstSeenAggSQL(FirstSeenAgg{Source: "`t`", Scope: "1", KeyCol: "digest"}); strings.Contains(digest, "first_recorded") || strings.Contains(digest, "AS days") {
		t.Errorf("a tlsh read asked for columns it did not want:\n%s", digest)
	}

	w := FirstSeenAggSQL(FirstSeenAgg{Source: "`t` FINAL", Scope: "fractal_id = 'f'", KeyCol: "entity_key", HistoryDays: 30, Recorded: true})
	for _, want := range []string{
		"WHERE fractal_id = 'f' AND _day > toDate(now(), 'UTC') - 60", // the lookback
		"minIf(first_seen, _day > toDate(now(), 'UTC') - 30) AS _fs",  // the window
		"HAVING length(_ds) > 0",                    // a key seen only before the window is gone
		"x.1 - _dr[i - 1].1 > 30",                   // a return follows an absence of the window
		"if(i = 1, x.1 > toDate(now(), 'UTC') - 30", // the oldest day only counts inside the window
	} {
		mustHaveML(t, w, want)
	}
	if HistoryRetentionDays(30) != 61 {
		t.Errorf("retention %d, want the lookback and a day", HistoryRetentionDays(30))
	}
	if got := WithHistory("", 7); got != "_day > toDate(now(), 'UTC') - 7" {
		t.Errorf("empty scope: %q", got)
	}
}

// LookupMembership probes as match() does, per list kind.
func TestLookupMembership(t *testing.T) {
	opts := QueryOptions{
		DictionaryDatabase:   "logs",
		Dictionaries:         map[string]map[string]string{"iocs": {"ip": "d_ioc"}, "pats": {"re": "d_pat"}, "names": {"n": "d_names"}},
		NetworkDicts:         map[string]bool{"d_ioc": true},
		PatternDicts:         map[string]bool{"d_pat": true},
		CaseInsensitiveDicts: map[string]bool{"names": true},
	}
	for _, c := range []struct{ dict, col, want string }{
		{"iocs", "ip", "dictHas('logs.d_ioc', toIPv6OrDefault(dst))"},
		{"pats", "re", "dictGetOrDefault('logs.d_pat', '_match', dst, '') = '1'"},
		{"names", "n", "dictHas('logs.d_names', lower(dst))"},
	} {
		got, err := LookupMembership("dst", c.dict, c.col, opts)
		if err != nil || got != c.want {
			t.Errorf("%s: got %q, %v; want %q", c.dict, got, err, c.want)
		}
	}
	opts.PatternPrefilters = map[string]string{"d_pat": "(SELECT pats)"}
	want := "(multiMatchAny(dst, (SELECT pats)) AND dictGetOrDefault('logs.d_pat', '_match', dst, '') = '1')"
	if got, err := LookupMembership("dst", "pats", "re", opts); err != nil || got != want {
		t.Errorf("prefiltered: got %q, %v; want %q", got, err, want)
	}
	if _, err := LookupMembership("dst", "iocs", "domain", opts); err == nil || !strings.Contains(err.Error(), "no key column") {
		t.Errorf("unknown column: %v", err)
	}
}

// --- Commands after modelLookup() read the model's columns ---

// mustIndex is strings.Index, failing the test when sub is absent, so a position
// comparison cannot pass because both sides are -1.
func mustIndex(t *testing.T, s, sub string) int {
	t.Helper()
	i := strings.Index(s, sub)
	if i < 0 {
		t.Fatalf("%q missing from:\n%s", sub, s)
	}
	return i
}

// mustLastIndex is strings.LastIndex, failing the test when sub is absent.
func mustLastIndex(t *testing.T, s, sub string) int {
	t.Helper()
	i := strings.LastIndex(s, sub)
	if i < 0 {
		t.Fatalf("%q missing from:\n%s", sub, s)
	}
	return i
}

// mlResult translates a model_lookup query, failing the test on any error.
func mlResult(t *testing.T, query string, opts QueryOptions) *TranslationResult {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %s: %v", query, err)
	}
	result, err := TranslateToSQLWithOrder(pipeline, opts)
	if err != nil {
		t.Fatalf("translate %s: %v", query, err)
	}
	return result
}

// scanCap returns the scan that picks the rows a scan-level join reads, when the
// rows are picked before the join: the subquery the joined scan matches by sorting
// key. "" when the join reads every row the scan's filters keep.
func scanCap(sql string) string {
	start := strings.Index(sql, "(timestamp, log_id) GLOBAL IN (SELECT timestamp, log_id FROM logs")
	if start < 0 {
		return ""
	}
	return balanced(sql, start+len("(timestamp, log_id) GLOBAL IN "))
}

// balanced returns the parenthesised run opening at sql[open].
func balanced(sql string, open int) string {
	depth := 0
	for i := open; i < len(sql); i++ {
		switch sql[i] {
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return sql[open : i+1]
			}
		}
	}
	return ""
}

// belowJoin returns the SQL that runs before the model's columns exist: the row
// selection and any subquery the join reads on its left.
func belowJoin(t *testing.T, sql string) string {
	t.Helper()
	mustIndex(t, sql, "_mlookup ON")
	out := scanCap(sql)
	if end := strings.Index(sql, ") AS _outer"); end >= 0 {
		depth := 0
		for i := end; i >= 0; i-- {
			switch sql[i] {
			case ')':
				depth++
			case '(':
				if depth--; depth == 0 {
					return out + sql[i:end+1]
				}
			}
		}
	}
	return out
}

// An assignment after the lookup is computed where the model columns exist, and is
// not dropped when no table() names it.
func TestModelLookup_AssignmentReadsModelColumn(t *testing.T) {
	for _, q := range []string{
		`event_id=22 | modelLookup(model="fs", key=[query]) | now(_now) | age := dateDiff("second", first_seen, _now)`,
		`event_id=22 | modelLookup(model="fs", key=[query]) | now(_now) | age := dateDiff("second", first_seen, _now) | table(query, first_seen, age)`,
	} {
		r := mlResult(t, q, mlookupOpts())
		if !strings.Contains(r.SQL, "AS age") || !slices.Contains(r.FieldOrder, "age") {
			t.Fatalf("%s: age was dropped, got:\n%s\nfields %v", q, r.SQL, r.FieldOrder)
		}
		if b := belowJoin(t, r.SQL); strings.Contains(b, "first_seen") || strings.Contains(b, "AS age") {
			t.Errorf("%s: age computed below the join:\n%s", q, r.SQL)
		}
	}
}

// A transform after the lookup can take a model column as input.
func TestModelLookup_TransformReadsModelColumn(t *testing.T) {
	r := mlResult(t, `* | modelLookup(model="fs", key=[user]) | concat([user, is_new], as=tag) | table(user, tag)`, mlookupOpts())
	if b := belowJoin(t, r.SQL); strings.Contains(b, "is_new") {
		t.Errorf("is_new read below the join:\n%s", r.SQL)
	}
	if strings.Contains(r.SQL, "fields.`is_new`") {
		t.Errorf("is_new resolved as a log field:\n%s", r.SQL)
	}
}

// A filter on a value derived from a model column runs after the join, never in the
// log scan where the column does not exist. Derived from model columns alone, it is
// still a property of the key and filters the model side.
func TestModelLookup_FilterOnDerivedColumnIsPostJoin(t *testing.T) {
	r := mlResult(t, `* | modelLookup(model="fs", key=[user]) | tag := concat(user, is_new) | tag = "bob1"`, mlookupOpts())
	if b := belowJoin(t, r.SQL); strings.Contains(b, "is_new") {
		t.Errorf("derived filter read below the join:\n%s", r.SQL)
	}
	if mustLastIndex(t, r.SQL, "= 'bob1'") < mustIndex(t, r.SQL, "_mlookup ON") {
		t.Errorf("derived filter must follow the join:\n%s", r.SQL)
	}

	r = mlResult(t, `* | modelLookup(model="fs", key=[user]) | n := event_count * 2 | n > 10`, mlookupOpts())
	if !strings.Contains(r.SQL, "* 2 > 10) AS _mlookup") {
		t.Errorf("a filter on model columns alone should narrow the model side:\n%s", r.SQL)
	}
}

// Without anything after it that drops rows, the lookup joins a scan that keeps its
// ORDER BY and LIMIT, so ClickHouse reads in sorting-key order and stops early.
func TestModelLookup_PlainLookupJoinsCappedScan(t *testing.T) {
	for _, q := range []string{
		`* | modelLookup(model="fs", key=[user])`,
		`* | modelLookup(model="beacon", key=[src_ip, dst_ip, dst_port])`,
		`* | modelLookup(model="fs", key=[user], require=false)`,
		`* | modelLookup(model="fs", key=[user]) | now(_now) | age := dateDiff("second", first_seen, _now)`,
		`* | modelLookup(model="fs", key=[user]) | sort(timestamp, order=desc)`,
	} {
		r := mlResult(t, q, mlookupOpts())
		c := scanCap(r.SQL)
		if !strings.Contains(c, "ORDER BY timestamp DESC, log_id DESC LIMIT 1000") && !strings.Contains(c, "ORDER BY timestamp DESC LIMIT 1000") {
			t.Errorf("%s: expected a capped scan below the join, got:\n%s", q, r.SQL)
		}
		if mustLastIndex(t, r.SQL, "LIMIT 1000") < mustIndex(t, r.SQL, "_mlookup ON") {
			t.Errorf("%s: the joined rows must keep their own LIMIT, got:\n%s", q, r.SQL)
		}
	}
}

// A filter that reads only model columns is a property of the key, so under an INNER
// join it filters the model side. The scan then reads only the matching keys and keeps
// its LIMIT, instead of reading the whole window before filtering.
func TestModelLookup_ModelOnlyFilterFiltersModelSide(t *testing.T) {
	opts := mlookupOpts()
	opts.FractalIDs = []string{"f1", "f2"}
	r := mlResult(t, `event_id=22 | modelLookup(model="fs", key=[query]) | is_new="1"`, opts)
	c := scanCap(r.SQL)
	if c == "" || !strings.Contains(c, "LIMIT 1000") {
		t.Fatalf("expected the scan to keep its LIMIT, got:\n%s", r.SQL)
	}
	if !strings.Contains(c, "fields.`query`::String IN (SELECT entity_key FROM (") || !strings.Contains(c, "WHERE is_new = '1')") {
		t.Errorf("expected the scan key set narrowed to new keys, got:\n%s", r.SQL)
	}
	if !strings.Contains(r.SQL, "WHERE is_new = '1') AS _mlookup") {
		t.Errorf("expected the model side of the join filtered, got:\n%s", r.SQL)
	}
}

// Unscored rows survive a LEFT JOIN with the model's columns at their defaults, which
// a model filter can match, so it cannot narrow the model side.
func TestModelLookup_RequireFalseFilterStaysAfterJoin(t *testing.T) {
	r := mlResult(t, `* | modelLookup(model="rare", key=[user, image], require=false) | percent < 0.1`, mlookupOpts())
	if c := scanCap(r.SQL); c != "" {
		t.Errorf("a LEFT JOIN filter must not cap the scan, got:\n%s", r.SQL)
	}
	if strings.Contains(r.SQL, "WHERE percent < 0.1) AS _mlookup") {
		t.Errorf("filter moved into the model side of a LEFT JOIN:\n%s", r.SQL)
	}
	if mustLastIndex(t, r.SQL, "percent < 0.1") < mustIndex(t, r.SQL, "_mlookup ON") {
		t.Errorf("filter must follow the join:\n%s", r.SQL)
	}
}

// A row cap after a post-join filter reads the filtered rows, so the join and the
// filter both run before it.
func TestModelLookup_CapAfterModelFilterReadsFilteredRows(t *testing.T) {
	r := mlResult(t, `* | modelLookup(model="rare", key=[user, image], require=false) | percent < 1 | head(10)`, mlookupOpts())
	c := limitedSubquery(t, r.SQL, "LIMIT 10")
	if !strings.Contains(c, "percent < 1") || !strings.Contains(c, "_mlookup ON") {
		t.Errorf("join and filter must precede head(10), got:\n%s", r.SQL)
	}
}

// The command is spelled modelLookup() in the docs; the scan-level join must not
// depend on the older model_lookup() spelling.
func TestModelLookup_EitherSpellingGroupsByModelColumn(t *testing.T) {
	for _, q := range []string{
		`* | modelLookup(model="fs", key=[user]) | groupby(is_new)`,
		`* | model_lookup(model="fs", key=[user]) | groupby(is_new)`,
	} {
		r := mlResult(t, q, mlookupOpts())
		if !strings.Contains(r.SQL, "GROUP BY is_new") {
			t.Errorf("%s: expected GROUP BY is_new, got:\n%s", q, r.SQL)
		}
	}
}

// --- NOT modelLookup() keeps the rows the model did not score ---

func TestModelLookup_NotKeepsUnscoredRows(t *testing.T) {
	r := mlResult(t, `event_id=22 | NOT modelLookup(model="fs", key=[query])`, mlookupOpts())
	want := "NOT (fields.`query`::String IN (SELECT _mlk_src.entity_key FROM `model_fs` AS _mlk_src WHERE _mlk_src.fractal_id IN ('f1')))"
	if !strings.Contains(r.SQL, want) {
		t.Errorf("expected anti-join %s, got:\n%s", want, r.SQL)
	}
	if strings.Contains(r.SQL, "JOIN") || slices.Contains(r.FieldOrder, "first_seen") {
		t.Errorf("a negated lookup filters; it adds no join and no columns, got:\n%s\nfields %v", r.SQL, r.FieldOrder)
	}
	// A filter keeps read-in-order and the scan LIMIT.
	if !strings.Contains(r.SQL, "ORDER BY timestamp DESC, log_id DESC LIMIT 1000") {
		t.Errorf("expected the plain scan order and limit, got:\n%s", r.SQL)
	}
}

// Rarity scores away partitions still learning (min_sample), so the raw keys are a
// superset of the scored ones; NOT must subtract the scored set or it would drop rows
// the model does not score.
func TestModelLookup_NotSubtractsScoredKeysWhenRawKeysDiffer(t *testing.T) {
	opts := mlookupOpts()
	opts.Models["rare5"] = AnalyticsModelInfo{ID: "4", TableName: "model_rare5", ModelType: "rarity", MinSample: 5, FractalID: "f1"}
	r := mlResult(t, `* | NOT modelLookup(model="rare5", key=[user, image])`, opts)
	if !strings.Contains(r.SQL, "NOT ((fields.`user`::String, fields.`image`::String) IN (SELECT partition_val, value_val FROM (") {
		t.Errorf("expected NOT IN over the scored keys, got:\n%s", r.SQL)
	}
	if !strings.Contains(r.SQL, "days_observed >= 5") {
		t.Errorf("expected the scored set to apply min_sample, got:\n%s", r.SQL)
	}
}

func TestModelLookup_NotCompoundsWithOtherConditions(t *testing.T) {
	r := mlResult(t, `* | NOT modelLookup(model="fs", key=[user]) OR user="admin"`, mlookupOpts())
	if !strings.Contains(r.SQL, "NOT (fields.`user`::String IN (SELECT _mlk_src.entity_key") || !strings.Contains(r.SQL, " OR ") {
		t.Errorf("expected the anti-join inside the OR, got:\n%s", r.SQL)
	}
}

func TestModelLookup_NotRejectsWhatItCannotHonour(t *testing.T) {
	for q, want := range map[string]string{
		`* | NOT modelLookup(model="fs", key=[user], require=false)`:     "require=",
		`* | modelLookup(model="fs", key=[user], require=true) OR x="1"`: "require=",
		`* | NOT modelLookup(model="nope", key=[user])`:                  "not found",
	} {
		pipeline, err := ParseQuery(q)
		if err == nil {
			_, err = TranslateToSQLWithOrder(pipeline, mlookupOpts())
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: expected an error mentioning %q, got %v", q, want, err)
		}
	}
}

// A negated lookup and a positive one can share a query: only the positive one joins.
func TestModelLookup_NotBesideAPositiveLookup(t *testing.T) {
	r := mlResult(t, `* | NOT modelLookup(model="fs", key=[user]) | modelLookup(model="rare", key=[user, image])`, mlookupOpts())
	if strings.Count(r.SQL, "_mlookup ON") != 1 || !strings.Contains(r.SQL, "NOT (fields.`user`::String IN (SELECT _mlk_src.entity_key FROM `model_fs`") {
		t.Errorf("expected one join and one anti-join, got:\n%s", r.SQL)
	}
}

// modelLookup() inside a case branch would need a join per branch.
func TestModelLookup_NotAllowedInCaseBranch(t *testing.T) {
	for _, name := range []string{"modelLookup", "model_lookup"} {
		q := `* | case { user="a" | ` + name + `(model="fs", key=[user]); * }`
		pipeline, err := ParseQuery(q)
		if err == nil {
			_, err = TranslateToSQLWithOrder(pipeline, mlookupOpts())
		}
		if err == nil || !strings.Contains(err.Error(), "case branch") {
			t.Errorf("%s: expected a case-branch error, got %v", q, err)
		}
	}
}

// A dedup on log fields keeps the same rows before or after a join that keeps every
// row, so it runs in the capped scan with the LIMIT.
func TestModelLookup_DedupOnLogFieldRunsBelowTheJoin(t *testing.T) {
	for _, q := range []string{
		`* | dedup(user) | modelLookup(model="fs", key=[user])`,
		`* | modelLookup(model="fs", key=[user]) | dedup(user)`,
	} {
		r := mlResult(t, q, mlookupOpts())
		if c := scanCap(r.SQL); !strings.Contains(c, "LIMIT 1 BY fields.`user`::String LIMIT 1000") {
			t.Errorf("%s: expected the dedup in the capped scan, got:\n%s", q, r.SQL)
		}
	}
	// A dedup on a model column needs the joined rows.
	r := mlResult(t, `* | modelLookup(model="fs", key=[user]) | dedup(is_new)`, mlookupOpts())
	if scanCap(r.SQL) != "" {
		t.Errorf("a dedup on a model column must not cap the scan, got:\n%s", r.SQL)
	}
}

// A groupby after a projection of aggregated rows (an assignment, a transform, or
// the model join onto groups) aggregates those rows. It used to aggregate inside
// the projection, which left its carried columns neither grouped nor aggregated
// (ClickHouse code 215).
func TestGroupByAfterAPostAggregationProjection(t *testing.T) {
	for _, q := range []string{
		`* | groupby(user) | x := _count * 2 | groupby(x)`,
		`* | groupby(user) | modelLookup(model="fs", key=[user]) | groupby(is_new)`,
	} {
		sql := translateML(t, q)
		if !regexp.MustCompile(`(?s)SELECT (x|is_new) AS (x|is_new), COUNT\(\*\) AS _count FROM \(SELECT user, _count, .*\) GROUP BY (x|is_new) LIMIT 1000\)$`).MatchString(sql) {
			t.Errorf("%s: expected the second groupby to count the projected rows, got:\n%s", q, sql)
		}
	}
}

// --- Review regressions: row selection around caps, keys and conditions ---

// limitedSubquery returns the parenthesised subquery that ends in the given LIMIT.
func limitedSubquery(t *testing.T, sql, limit string) string {
	t.Helper()
	end := regexp.MustCompile(regexp.QuoteMeta(limit) + `\)`).FindStringIndex(sql)
	if end == nil {
		t.Fatalf("no subquery ending in %s:\n%s", limit, sql)
	}
	depth := 0
	for i := end[1] - 1; i >= 0; i-- {
		switch sql[i] {
		case ')':
			depth++
		case '(':
			if depth--; depth == 0 {
				return sql[i:end[1]]
			}
		}
	}
	t.Fatalf("unbalanced SQL:\n%s", sql)
	return ""
}

// A cap written before a strict lookup caps the rows the lookup is handed, so the
// lookup's key filter runs after it: "take 10, keep the scored" is not "the first
// 10 scored".
func TestModelLookup_CapBeforeStrictLookupCapsFirst(t *testing.T) {
	for _, q := range []string{
		`* | head(10) | modelLookup(model="rare", key=[image, hash]) | percent < 5`,
		`* | dedup(user) | modelLookup(model="rare", key=[image, hash])`,
	} {
		sql := translateML(t, q)
		limit := "LIMIT 10"
		if strings.Contains(q, "dedup") {
			limit = "LIMIT 1 BY fields.`user`::String"
		}
		if c := limitedSubquery(t, sql, limit); strings.Contains(c, "_mlk_src") || strings.Contains(c, "partition_val") {
			t.Errorf("%s: the model key filter runs before the cap:\n%s", q, sql)
		}
	}
	// After a capped aggregation, the scan cannot be narrowed to scored keys.
	sql := translateML(t, `* | groupby(image, hash) | sort(_count, order=desc) | head(10) | modelLookup(model="rare", key=[image, hash])`)
	if strings.Contains(sql, "IN (SELECT _mlk_src") {
		t.Errorf("the top groups must be chosen from every group:\n%s", sql)
	}
}

// A dedup whose keys include the model key keeps or drops whole groups at the join,
// so the scan may still pick its rows narrowly.
func TestModelLookup_DedupByModelKeyFoldsIntoTheRowPick(t *testing.T) {
	sql := translateML(t, `* | dedup(user) | modelLookup(model="fs", key=[user])`)
	if !strings.Contains(scanCap(sql), "LIMIT 1 BY fields.`user`::String LIMIT 1000") {
		t.Errorf("expected the dedup in the row pick:\n%s", sql)
	}
}

// A filter written after a cap filters the rows the cap kept.
func TestModelLookup_FilterAfterCapRunsAfterIt(t *testing.T) {
	for q, limit := range map[string]string{
		`* | modelLookup(model="rare", key=[image, hash]) | head(10) | percent < 5`:             "LIMIT 10",
		`* | modelLookup(model="rare", key=[image, hash]) | dedup(user) | percent < 5`:          "LIMIT 1 BY fields.`user`::String",
		`* | modelLookup(model="rare", key=[image, hash]) | x := percent * 2 | head(5) | x > 3`: "LIMIT 5",
	} {
		sql := translateML(t, q)
		if c := limitedSubquery(t, sql, limit); strings.Contains(c, "percent < 5") || strings.Contains(c, "> 3") {
			t.Errorf("%s: filter applied before the cap:\n%s", q, sql)
		}
	}
}

// An alias named like a keyword is a computed column, absent from the scan that
// picks the rows.
func TestModelLookup_KeywordNamedAliasIsNotAScanColumn(t *testing.T) {
	for _, q := range []string{
		`* | modelLookup(model="rare", key=[image, hash]) | hour := percent | sort(hour)`,
		`* | modelLookup(model="rare", key=[image, hash]) | day := len(user) | dedup(day)`,
	} {
		if c := scanCap(translateML(t, q)); c != "" {
			t.Errorf("%s: the row pick reads a computed column:\n%s", q, c)
		}
	}
}

// Keys are matched as Strings, like the model's key columns: a JOIN will not compare
// a number with a String (ClickHouse code 386), and a dotted group key is quoted.
func TestModelLookup_KeysMatchAsQuotedStrings(t *testing.T) {
	for q, want := range map[string]string{
		`* | groupby(user, function=count(as=c)) | modelLookup(model="fs", key=[c])`:                        "ON ifNull(toString(_outer.c), '') = _mlookup.entity_key",
		`* | groupby("a.b", dst_ip, dst_port) | modelLookup(model="beacon", key=["a.b", dst_ip, dst_port])`: "ON ifNull(toString(_outer.`a.b`), '') = _mlookup.src_ip",
		`* | len(user) | modelLookup(model="fs", key=[_len])`:                                               "ON ifNull(toString(length(fields.`user`::String)), '') = _mlookup.entity_key",
		`* | modelLookup(model="fs", key=[user])`:                                                           "ON fields.`user`::String = _mlookup.entity_key",
	} {
		if sql := translateML(t, q); !strings.Contains(sql, want) {
			t.Errorf("%s: expected %q in:\n%s", q, want, sql)
		}
	}
	if sql := translateML(t, `* | groupby("a.b", dst_ip, dst_port) | modelLookup(model="beacon", key=["a.b", dst_ip, dst_port])`); !strings.Contains(sql, "SELECT `a.b`, dst_ip") {
		t.Errorf("a dotted carried column must stay quoted:\n%s", sql)
	}
}

// A condition command binds where the fields it reads exist: after an aggregation
// to HAVING, after a scan-level join to the joined rows, and a log field the
// aggregation does not carry is an error, as for any filter.
func TestModelLookup_ConditionBindsWhereItsFieldsExist(t *testing.T) {
	sql := translateML(t, `* | groupby(user, function=count(as=c)) | NOT modelLookup(model="fs", key=[c])`)
	if !strings.Contains(sql, "HAVING NOT (ifNull(toString(c), '') IN (SELECT _mlk_src.entity_key") {
		t.Errorf("expected the aggregate key tested in HAVING:\n%s", sql)
	}
	sql = translateML(t, `* | modelLookup(model="beacon", key=[a, b, c]) | NOT in(beacon_score, values=["1"])`)
	if b := belowJoin(t, sql); strings.Contains(b, "beacon_score") {
		t.Errorf("a condition on a model column must run on the joined rows:\n%s", sql)
	}
	pipeline, err := ParseQuery(`* | groupby(host) | NOT modelLookup(model="fs", key=[user])`)
	if err == nil {
		_, err = TranslateToSQLWithOrder(pipeline, mlookupOpts())
	}
	if err == nil || !strings.Contains(err.Error(), "does not exist after groupby()") {
		t.Errorf("expected a missing-field error, got %v", err)
	}
}

// log_id is not unique, so the joined read keeps the filters the picked rows passed;
// only the strict key filter is left to the INNER JOIN, which applies it exactly.
func TestModelLookup_RowPickKeepsTheFilters(t *testing.T) {
	sql := translateML(t, `user="bob" | modelLookup(model="fs", key=[user])`)
	outer := strings.Replace(sql, scanCap(sql), "", 1)
	if !strings.Contains(outer, "fields.`user`::String = 'bob'") {
		t.Errorf("the joined read must keep the filter:\n%s", sql)
	}
	if strings.Contains(outer, "IN (SELECT _mlk_src") {
		t.Errorf("the key filter is the join's to apply:\n%s", sql)
	}
}

// On a cluster the row pick reads logs_distributed, so it is a GLOBAL IN, and the
// joined read projects _shard_num from the table itself.
func TestModelLookup_RowPickOnACluster(t *testing.T) {
	opts := mlookupOpts()
	opts.TableName = "logs_distributed"
	opts.IncludeShardNum = true
	opts.Models["beacon"] = AnalyticsModelInfo{ID: "3", TableName: "model_beacon_dist", ModelType: "beacon", FractalID: "f1", Distributed: true}
	sql := translateMLWith(t, `* | modelLookup(model="beacon", key=[src_ip, dst_ip, dst_port])`, opts)
	for _, want := range []string{
		"(timestamp, log_id) GLOBAL IN (SELECT timestamp, log_id FROM logs_distributed",
		"FROM logs_distributed GLOBAL INNER JOIN",
		"toString(_shard_num) AS _shard_num",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("expected %q in:\n%s", want, sql)
		}
	}
}

// A subquery source (pgr) has no sorting key to pick rows by.
func TestModelLookup_NoRowPickOverASubquerySource(t *testing.T) {
	opts := mlookupOpts()
	opts.SourceSubquery = "SELECT 'a' AS user, '2026-01-01 00:00:00' AS timestamp, 'x' AS log_id"
	opts.SourceColumns = []string{"user", "timestamp", "log_id"}
	sql := translateMLWith(t, `* | modelLookup(model="fs", key=[user])`, opts)
	if strings.Contains(sql, "(timestamp, log_id) GLOBAL IN") {
		t.Errorf("expected no row pick over a subquery source:\n%s", sql)
	}
}

// After an aggregation on the beacon pair, the model's own key columns share the
// group columns' names; the join layer adds only the model's outputs, so nothing
// above it meets an ambiguous name.
func TestModelLookup_EnrichedGroupsAddOnlyModelOutputs(t *testing.T) {
	sql := translateML(t, `* | groupby(src_ip, dst_ip, dst_port) | modelLookup(model="beacon", key=[src_ip, dst_ip, dst_port])`)
	if !strings.Contains(sql, "SELECT _outer.*, _mlookup.beacon_score AS beacon_score") || strings.Contains(sql, "_mlookup.src_ip AS") {
		t.Errorf("expected only model outputs added to the groups:\n%s", sql)
	}
	pipeline, err := ParseQuery(`* | groupby(src_ip, dst_ip, dst_port, conn_count) | modelLookup(model="beacon", key=[src_ip, dst_ip, dst_port])`)
	if err == nil {
		_, err = TranslateToSQLWithOrder(pipeline, mlookupOpts())
	}
	if err == nil || !strings.Contains(err.Error(), `already have a column "conn_count"`) {
		t.Errorf("expected a clash error, got %v", err)
	}
}

// A filter after a cap that follows the lookup reads the capped rows, so it does not
// narrow the model side, which would select before the cap.
func TestModelLookup_FilterAfterCapStaysOffTheModelSide(t *testing.T) {
	sql := translateML(t, `* | modelLookup(model="rare", key=[image, hash]) | head(10) | percent < 5`)
	if strings.Contains(sql, "percent < 5) AS _mlookup") {
		t.Errorf("filter moved ahead of the cap:\n%s", sql)
	}
}

// An OR across a model column and a log field cannot split, so it filters the
// joined rows, where both resolve; nothing needs exporting.
func TestModelLookup_MixedOrFiltersJoinedRows(t *testing.T) {
	sql := translateML(t, `* | modelLookup(model="beacon", key=[src_ip, dst_ip, dst_port]) | beacon_score > 0.5 OR image=~zerotier`)
	if strings.Contains(sql, "_dfr_") || mustLastIndex(t, sql, "beacon_score > 0.5 OR") < mustIndex(t, sql, "_mlookup ON") {
		t.Errorf("expected the OR on the joined rows:\n%s", sql)
	}
	if b := belowJoin(t, sql); strings.Contains(b, "beacon_score") {
		t.Errorf("the OR read the model column below the join:\n%s", sql)
	}
}

// A field computed from a field computed from a model column still depends on the
// join: on model columns alone it narrows the model side, with a log field it
// filters the joined rows.
func TestModelLookup_ChainedAssignmentsDependOnTheJoin(t *testing.T) {
	sql := translateML(t, `* | modelLookup(model="rare", key=[image, hash]) | a := percent * 2 | b := a + 1 | b > 3`)
	if !strings.Contains(sql, "> 3) AS _mlookup") {
		t.Errorf("expected the chained filter on the model side:\n%s", sql)
	}
	sql = translateML(t, `* | modelLookup(model="fs", key=[user]) | a := concat(user, is_new) | b := concat(a, user) | b = "x"`)
	if b := belowJoin(t, sql); strings.Contains(b, "is_new") {
		t.Errorf("the chained filter read the model column below the join:\n%s", sql)
	}
}

// A NULL computed key is one the model does not score, so NOT keeps its row.
func TestModelLookup_NullKeyIsUnscored(t *testing.T) {
	sql := translateML(t, `* | x := tonumber(user) | NOT modelLookup(model="fs", key=[x])`)
	if !strings.Contains(sql, "NOT (ifNull(toString(") {
		t.Errorf("expected a NULL key read as '':\n%s", sql)
	}
}
