package parser

import (
	"strings"
	"testing"
	"time"
)

func alertLagOpts(lag int) QueryOptions {
	return QueryOptions{
		StartTime:          time.Now().Add(-time.Hour),
		EndTime:            time.Now(),
		MaxRows:            10,
		FractalID:          "f1",
		UseIngestTimestamp: true,
		MaxEventLagSeconds: lag,
	}
}

const lagPredicate = "dateDiff('second', timestamp, ingest_timestamp) <= 300"

// The lag filter belongs in the scan, not after it: alerts cap at 10k rows per
// evaluation, so a backlog flush filtered in Go would fill that budget and starve
// the genuinely new rows from the same window.
func TestMaxEventLagFiltersInQuery(t *testing.T) {
	p, err := ParseQuery(`level="error"`)
	if err != nil {
		t.Fatal(err)
	}

	sql, err := TranslateToSQL(p, alertLagOpts(300))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(sql, lagPredicate) {
		t.Errorf("lag predicate missing from:\n%s", sql)
	}

	off, err := TranslateToSQL(p, alertLagOpts(0))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if strings.Contains(off, "dateDiff('second', timestamp, ingest_timestamp)") {
		t.Errorf("lag predicate emitted while the setting is off:\n%s", off)
	}
}

// An aggregating alert must filter before it counts, or a backlog flush can trip a
// threshold rule on events the operator asked it to ignore.
func TestMaxEventLagAppliesBeforeAggregation(t *testing.T) {
	p, err := ParseQuery(`level="error" | groupby(user, function=count()) | _count > 5`)
	if err != nil {
		t.Fatal(err)
	}
	sql, err := TranslateToSQL(p, alertLagOpts(300))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	where := sql
	if i := strings.Index(sql, "GROUP BY"); i >= 0 {
		where = sql[:i]
	}
	if !strings.Contains(where, lagPredicate) {
		t.Errorf("lag predicate is not in the pre-aggregation WHERE:\n%s", sql)
	}
}

// A join correlates against the same table, so an event the alert was told to
// ignore must not come back as the other side of the match.
func TestMaxEventLagReachesJoinSubquery(t *testing.T) {
	p, err := ParseQuery("* | groupby(dst_ip) | join(dst_ip) { * | groupby(dst_ip) }")
	if err != nil {
		t.Fatal(err)
	}
	sql, err := TranslateToSQL(p, alertLagOpts(300))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if strings.Count(sql, lagPredicate) < 2 {
		t.Errorf("lag predicate did not reach the join subquery:\n%s", sql)
	}
}
