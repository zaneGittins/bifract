package storage

import (
	"slices"
	"strings"
	"testing"
)

// The converter compares system.tables.partition_key against these keys, and new installs
// take theirs from init-clickhouse.sql. If the two disagree, every start would re-convert.
func TestRepartitionTargetsMatchInitSQL(t *testing.T) {
	for _, target := range repartitionTargets {
		ddl, err := initTableDDL(target.table)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(ddl, "\nPARTITION BY "+target.key+"\n") {
			t.Errorf("%s: init-clickhouse.sql must declare PARTITION BY %s", target.table, target.key)
		}
		if !strings.HasPrefix(ddl, "CREATE TABLE IF NOT EXISTS "+target.table+" (") {
			t.Errorf("%s: unexpected DDL start %q", target.table, ddl[:min(60, len(ddl))])
		}
	}
}

func TestChPartitionString(t *testing.T) {
	cases := map[string]string{
		"6267fa7a-1e6d-4975-900a-15d070726fc7": `'6267fa7a-1e6d-4975-900a-15d070726fc7'`,
		"":                                     `''`,
		`f'q`:                                  `'f\'q'`,
		`a\b`:                                  `'a\\b'`,
	}
	for in, want := range cases {
		if got := chPartitionString(in); got != want {
			t.Errorf("chPartitionString(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestEngineTTLExtraction(t *testing.T) {
	cases := map[string]string{
		"AggregatingMergeTree PARTITION BY fractal_id ORDER BY (a) TTL toDateTime(ingest_timestamp) + toIntervalDay(99) SETTINGS index_granularity = 8192": "toDateTime(ingest_timestamp) + toIntervalDay(99)",
		"SummingMergeTree(cnt) ORDER BY (fractal_id, ingest_day, minute) SETTINGS index_granularity = 256":                                                 "",
		"MergeTree ORDER BY x TTL day + toIntervalDay(730)":                                                                                                "day + toIntervalDay(730)",
	}
	for engine, want := range cases {
		got := ""
		if m := engineTTLRe.FindStringSubmatch(engine); m != nil {
			got = m[1]
		}
		if got != want {
			t.Errorf("TTL of %q = %q, want %q", engine, got, want)
		}
	}
}

// A reset or clear that left a conversion's copy behind would have it copied back in.
func TestConversionTablesAreClearedAndReset(t *testing.T) {
	cleared := withConversionTables(resetShardedTables)
	stmts := strings.Join(ResetLogDataStatements(nil, nil), "\n")
	for _, target := range repartitionTargets {
		for _, suffix := range []string{repartOld, repartStage} {
			if !slices.Contains(cleared, target.table+suffix) {
				t.Errorf("ClearAllLogData does not truncate %s%s", target.table, suffix)
			}
		}
		for _, suffix := range []string{repartOld, repartStage, repartDone, repartSkip} {
			if !strings.Contains(stmts, "`"+target.table+suffix+"`") {
				t.Errorf("reset does not drop %s%s", target.table, suffix)
			}
		}
	}
	baselines := withConversionTables(endpointAnalysisTables)
	if slices.Contains(baselines, "logs_histogram"+repartOld) {
		t.Error("clearing baselines must not touch the histogram's conversion tables")
	}
}
