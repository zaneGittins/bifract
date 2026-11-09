package parser

import (
	"strings"
	"testing"
)

// An assignment after join() is computed above the join's wrap, where the joined
// columns exist. It was computed in the scan, which cannot read them, or dropped
// with the formatter join() did not render.
func TestAssignmentAfterJoinIsComputedAboveTheWrap(t *testing.T) {
	for q, want := range map[string]string{
		`* | join(dst_ip) { * | groupby(dst_ip) } | x := _join__count`:     "_join__count AS x FROM (SELECT _outer.*",
		`* | join(dst_ip) { * | groupby(dst_ip) } | x := _join__count * 2`: "_join__count)) * 2 AS x FROM (SELECT _outer.*",
		`* | join(dst_ip) { * | groupby(dst_ip) } | x := length(user)`:     "length(fields.`user`::String) AS x FROM (SELECT _outer.*",
	} {
		sql := mustTranslate(t, q, goldenBase())
		if !strings.Contains(sql, want) {
			t.Errorf("%s:\n%s", q, sql)
		}
		if strings.Contains(joinScan(sql), "AS x") {
			t.Errorf("%s: computed in the scan:\n%s", q, sql)
		}
	}
}

// A command reading a join()'s output computes above the wrap too, and a filter
// on a field computed from one reads it there by name: the scan, which runs before
// the join, can neither compute nor export it.
func TestCommandAfterJoinReadsItAboveTheWrap(t *testing.T) {
	for q, want := range map[string]string{
		`* | join(dst_ip) { * | groupby(dst_ip) } | lowercase(_join__count, output=x)`:    "AS x FROM (SELECT _outer.*",
		`* | join(dst_ip) { * | groupby(dst_ip) } | x := _join__count | y := x | y = "1"`: "WHERE y = '1'",
	} {
		sql := mustTranslate(t, q, goldenBase())
		if !strings.Contains(sql, want) {
			t.Errorf("%s: want %q in\n%s", q, want, sql)
		}
		if scan := joinScan(sql); strings.Contains(scan, "_join__count") || strings.Contains(scan, "AS x") {
			t.Errorf("%s: the scan reads the join's output:\n%s", q, sql)
		}
		if strings.Contains(sql, "_dfr_") {
			t.Errorf("%s: exported from the scan:\n%s", q, sql)
		}
	}
}

// dedup() keyed on a join()'s output would deduplicate before the join; it is
// refused rather than run on rows the join has not built.
func TestDedupOnJoinOutputIsRefused(t *testing.T) {
	pl, err := ParseQuery(`* | join(dst_ip) { * | groupby(dst_ip) } | x := _join__count | dedup(x)`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TranslateToSQLWithOrder(pl, goldenBase()); err == nil || !strings.Contains(err.Error(), "join() adds") {
		t.Errorf("got %v", err)
	}
}

// joinScan is the scan a join() wraps: what it reads before the join.
func joinScan(sql string) string {
	start := strings.Index(sql, "AS _join__count FROM (")
	end := strings.Index(sql, ") AS _outer")
	if start < 0 || end < start {
		return ""
	}
	return sql[start+len("AS _join__count FROM (") : end]
}

// The last definition of a name wins above a join() as below it, and the outer
// SELECT projects it once.
func TestRedefinitionAroundJoinProjectsTheLast(t *testing.T) {
	for q, want := range map[string]string{
		`* | join(dst_ip) { * | groupby(dst_ip) } | lowercase(_join__count, output=x) | lowercase(user, output=x) | x = "bob"`: "lower(fields.`user`::String) AS x",
		`* | join(dst_ip) { * | groupby(dst_ip) } | x := "z" | len(_join__count, as=x)`:                                        "length(toString(_join__count)) AS x",
		`* | join(dst_ip) { * | groupby(dst_ip) } | lowercase(_join__count, output=x) | len(user, as=x)`:                       "length(fields.`user`::String) AS x",
		`* | join(dst_ip) { * | groupby(dst_ip) } | x := _join__count | table(x)`:                                              "_join__count AS x",
		`* | join(dst_ip) { * | groupby(dst_ip) } | x := _join__count | modifiedzscore(x)`:                                     "toFloat64(_join__count) AS _mz_val",
	} {
		sql := mustTranslate(t, q, goldenBase())
		if !strings.Contains(sql, want) || strings.Contains(want, "AS x") && strings.Count(sql, "AS x") != 1 {
			t.Errorf("%s: want %q once:\n%s", q, want, sql)
		}
		if strings.Contains(joinScan(sql), "_join__count") {
			t.Errorf("%s: the scan reads the join's output:\n%s", q, sql)
		}
	}
}
