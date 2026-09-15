package parser

import (
	"strings"
	"testing"
	"time"
)

// ingest_timestamp reaches the detail panel as a hidden column rather than a
// result column, so these pin both halves: that the paths feeding that panel
// project it, and that the paths which cannot carry it never do. A subquery
// source does not project the column at all (ClickHouse code 47) and an
// aggregation leaves it neither grouped nor aggregated (code 215), so a leak
// into either shape is a failing query rather than a cosmetic slip.

func ingestOpts() QueryOptions {
	return QueryOptions{
		StartTime:         time.Now().Add(-time.Hour),
		EndTime:           time.Now(),
		MaxRows:           250,
		FractalID:         "f1",
		IncludeIngestTime: true,
	}
}

func translateIngest(t *testing.T, query string, opts QueryOptions) *TranslationResult {
	t.Helper()
	p, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	res, err := TranslateToSQLWithOrder(p, opts)
	if err != nil {
		t.Fatalf("translate %q: %v", query, err)
	}
	return res
}

const ingestSel = "ingest_timestamp AS " + ingestTimeColumn

func TestIngestTimeProjectedForRowShapes(t *testing.T) {
	queries := []string{
		`*`,
		`user="bob"`,
		`* | eval(x = 1)`,
		`* | sort(user)`,
		`* | head(10)`,
		`* | dedup(user)`,
		`* | table()`,
		`* | table(user, host)`,
		`* | eval(y = len(user)) | table(user, y)`,
	}
	for _, q := range queries {
		res := translateIngest(t, q, ingestOpts())
		if !strings.Contains(res.SQL, ingestSel) {
			t.Errorf("%q: hidden ingest column missing:\n%s", q, res.SQL)
		}
		for _, f := range res.FieldOrder {
			if f == ingestTimeColumn {
				t.Errorf("%q: hidden column leaked into field order %v", q, res.FieldOrder)
			}
		}
	}
}

func TestIngestTimeAbsentFromAggregations(t *testing.T) {
	queries := []string{
		`* | count()`,
		`* | groupby(user) | count()`,
		`* | top(user)`,
		`* | timechart(1h)`,
		`* | chain(user) { a="x"; b="y" }`,
	}
	for _, q := range queries {
		res := translateIngest(t, q, ingestOpts())
		if strings.Contains(res.SQL, ingestTimeColumn) {
			t.Errorf("%q: aggregated query must not project the ingest column:\n%s", q, res.SQL)
		}
	}
}

// A source command (pgr()) resolves to a subquery whose flat columns are the
// only ones that exist; referencing the logs table's ingest_timestamp there is
// an unknown identifier.
func TestIngestTimeAbsentOverSubquerySource(t *testing.T) {
	opts := ingestOpts()
	opts.SourceSubquery = "SELECT src_node, dst_node, label, log_id, toString(timestamp) AS timestamp, fractal_id FROM proc_lineage"
	opts.SourceColumns = []string{"src_node", "dst_node", "label", "log_id", "timestamp", "fractal_id"}

	for _, q := range []string{`*`, `label="x"`, `* | table(src_node, label)`, `* | sort(label)`, `* | head(5)`} {
		res := translateIngest(t, q, opts)
		if strings.Contains(res.SQL, "ingest_timestamp") {
			t.Errorf("%q: subquery source must not reference ingest_timestamp:\n%s", q, res.SQL)
		}
	}
}

// The alert engine and the rule tester leave the option off; their rows are
// matched and templated, not displayed.
func TestIngestTimeOffByDefault(t *testing.T) {
	opts := ingestOpts()
	opts.IncludeIngestTime = false
	for _, q := range []string{`*`, `* | sort(user)`, `* | table(user)`} {
		res := translateIngest(t, q, opts)
		if strings.Contains(res.SQL, ingestTimeColumn) {
			t.Errorf("%q: projected the hidden column with the option off:\n%s", q, res.SQL)
		}
	}
}

// A second reference to a name the query already binds resolves to that alias,
// not to the column, so the hidden column is dropped rather than carrying a
// value that is not the ingest time.
func TestIngestTimeSkippedWhenNameIsBound(t *testing.T) {
	for _, q := range []string{
		`* | table(timestamp, ingest_timestamp)`,
		`* | eval(ingest_timestamp = 1) | table(ingest_timestamp)`,
	} {
		res := translateIngest(t, q, ingestOpts())
		if strings.Contains(res.SQL, ingestTimeColumn) {
			t.Errorf("%q: must not alias over a bound name:\n%s", q, res.SQL)
		}
	}
}

// Recall reads the Iceberg archive, whose schema carries the same column.
func TestIngestTimeProjectedForIceberg(t *testing.T) {
	opts := ingestOpts()
	opts.SourceMode = SourceIceberg
	opts.UseIngestTimestamp = true
	opts.TableName = "icebergS3('loc')"

	res := translateIngest(t, `user="bob"`, opts)
	if !strings.Contains(res.SQL, ingestSel) {
		t.Errorf("iceberg row shape missing the hidden ingest column:\n%s", res.SQL)
	}
}

// table() resolved every name as a fields.`x` sub-path, so a base column came
// back empty on every row, and aliasing that empty over the column's own name
// shadowed the real column for anything selecting it later in the same SELECT.
func TestTableProjectsBaseColumns(t *testing.T) {
	cases := map[string]string{
		"log_id":           "log_id",
		"fractal_id":       "fractal_id",
		"norm_log":         "norm_log",
		"normalizer":       "normalizer",
		"ingest_timestamp": "ingest_timestamp",
	}
	for field, want := range cases {
		res := translateIngest(t, `* | table(`+field+`)`, ingestOpts())
		if strings.Contains(res.SQL, "fields.`"+field+"`") {
			t.Errorf("table(%s): resolved a base column as a JSON sub-path:\n%s", field, res.SQL)
		}
		if !strings.Contains(res.SQL, want) {
			t.Errorf("table(%s): missing the base column:\n%s", field, res.SQL)
		}
	}
}

// An eval() that rebinds a base column's name re-registers it as per-row, so
// table() must project the computed value rather than the column.
func TestTableBaseColumnOverriddenByEval(t *testing.T) {
	res := translateIngest(t, `* | eval(log_id = lower(user)) | table(log_id)`, ingestOpts())
	if !strings.Contains(res.SQL, "AS log_id") {
		t.Errorf("eval-bound name must project the computed value:\n%s", res.SQL)
	}
}
