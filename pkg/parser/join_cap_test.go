package parser

import (
	"strings"
	"testing"
	"time"
)

func joinCapOpts() QueryOptions {
	return QueryOptions{
		StartTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		MaxRows:   1000,
		FractalID: "f",
	}
}

// splitJoin returns the SQL of the outer side of a join() and what follows the
// join's ON clause.
func splitJoin(t *testing.T, sql string) (outer, after string) {
	t.Helper()
	o := strings.Index(sql, ") AS _outer ")
	a := strings.Index(sql, ") AS _join_sub ON ")
	if o < 0 || a < 0 {
		t.Fatalf("not a join wrap:\n%s", sql)
	}
	return sql[:o], sql[a:]
}

// An inner join's row cap must count only rows that can match: otherwise it joins
// the newest MaxRows logs and silently drops every older match.
func TestInnerJoinCapCountsOnlyMatchingRows(t *testing.T) {
	sql := translateWith(t, `* | join(user) { a="1" | groupby(user) }`, joinCapOpts())
	outer, after := splitJoin(t, sql)

	prefilter := "fields.`user`::String GLOBAL IN (SELECT user FROM ("
	pf := strings.Index(outer, prefilter)
	limit := strings.LastIndex(outer, "LIMIT 1000")
	if pf < 0 || limit < 0 || pf > limit {
		t.Fatalf("outer scan must filter by the subquery's keys before its cap:\n%s", outer)
	}
	if !strings.HasSuffix(after, "ORDER BY timestamp DESC, log_id DESC LIMIT 1000") {
		t.Errorf("joined rows must be ordered and capped after the join:\n%s", after)
	}
}

// A cap written after the join caps joined rows, so it also runs after the prefilter.
func TestJoinThenHeadCapsJoinedRows(t *testing.T) {
	sql := translateWith(t, `* | join(user) { a="1" | groupby(user) } | head(5)`, joinCapOpts())
	outer, after := splitJoin(t, sql)
	if pf, lim := strings.Index(outer, "GLOBAL IN"), strings.LastIndex(outer, "LIMIT 5"); pf < 0 || lim < pf {
		t.Fatalf("head() after join must cap prefiltered rows:\n%s", outer)
	}
	if !strings.HasSuffix(after, "ORDER BY timestamp ASC LIMIT 5") {
		t.Errorf("head() after join must cap the joined rows:\n%s", after)
	}
}

// A cap written before the join picks rows first; only those are joined.
func TestHeadThenJoinCapsBeforeJoin(t *testing.T) {
	sql := translateWith(t, `* | head(20) | join(user) { a="1" | groupby(user) }`, joinCapOpts())
	outer, _ := splitJoin(t, sql)
	head := strings.Index(outer, "LIMIT 20")
	pf := strings.Index(outer, "GLOBAL IN")
	if head < 0 || pf < head {
		t.Fatalf("head() before join must pick rows before the key filter:\n%s", outer)
	}
}

// Grouped outer rows share their key, so filtering the scan by key keeps every
// matching group whole and the group cap counts only matching groups.
func TestGroupedInnerJoinPrefiltersScan(t *testing.T) {
	sql := translateWith(t, `* | groupby(user) | join(user) { a="1" | groupby(user) }`, joinCapOpts())
	outer, after := splitJoin(t, sql)
	where := strings.Index(outer, "fields.`user`::String GLOBAL IN (SELECT user FROM (")
	group := strings.Index(outer, "GROUP BY user")
	if where < 0 || group < where {
		t.Fatalf("grouped scan must filter by the subquery's keys before grouping:\n%s", outer)
	}
	if !strings.HasSuffix(after, "LIMIT 1000") {
		t.Errorf("joined groups must be capped after the join:\n%s", after)
	}
}

// A left join keeps every outer row, so its cap stays before the join without a
// key filter, and is reapplied to the joined rows.
func TestLeftJoinKeepsCapWithoutPrefilter(t *testing.T) {
	sql := translateWith(t, `* | join(user, type=left) { a="1" | groupby(user) }`, joinCapOpts())
	outer, after := splitJoin(t, sql)
	if strings.Contains(outer, "GLOBAL IN") {
		t.Errorf("left join must not filter the outer rows:\n%s", outer)
	}
	if !strings.Contains(outer, "LIMIT 1000") {
		t.Errorf("left join keeps the cap on the scan:\n%s", outer)
	}
	if !strings.HasSuffix(after, "ORDER BY timestamp DESC, log_id DESC LIMIT 1000") {
		t.Errorf("joined rows must be ordered and capped after the join:\n%s", after)
	}
}

// When the scan cannot be filtered by key (the join reads chained aggregation
// output), the cap moves past the inner join instead.
func TestInnerJoinWithoutPrefilterCapsAfterJoin(t *testing.T) {
	sql := translateWith(t, `* | groupby(user, host) | groupby(user, function=count()) | join(user) { a="1" | groupby(user) }`, joinCapOpts())
	outer, after := splitJoin(t, sql)
	if strings.Contains(outer, "LIMIT 1000") {
		t.Errorf("cap must not run before the join:\n%s", outer)
	}
	if !strings.HasSuffix(after, "LIMIT 1000") {
		t.Errorf("cap must run after the join:\n%s", after)
	}
}
