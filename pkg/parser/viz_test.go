package parser

import (
	"slices"
	"strings"
	"testing"
)

func translateGraph(t *testing.T, q string, opts QueryOptions) *TranslationResult {
	t.Helper()
	res, err := TranslateToSQLWithOrder(mustParse(t, q), opts)
	if err != nil {
		t.Fatalf("translate %q: %v", q, err)
	}
	return res
}

// Raw rows keep their fields inside norm_log, where the graph renderer cannot
// find them, so graph() projects its own columns exactly as table() would.
func TestGraphProjectsItsFieldsOverRawRows(t *testing.T) {
	cases := []struct{ bare, withTable string }{
		{
			`gtest=1 | graph(child=process_guid, parent=parent_process_guid, labels=[image])`,
			`gtest=1 | table(process_guid, parent_process_guid, image) | graph(child=process_guid, parent=parent_process_guid, labels=[image])`,
		},
		{
			`* | graph(child=a, parent=b)`,
			`* | table(a, b) | graph(child=a, parent=b)`,
		},
		{
			`* | graph(child=a, parent=b, labels=[a], limit=50)`,
			`* | table(a, b) | graph(child=a, parent=b, labels=[a], limit=50)`,
		},
		{
			`* | c := lower(a) | graph(child=c, parent=b, labels=[x, y])`,
			`* | c := lower(a) | table(c, b, x, y) | graph(child=c, parent=b, labels=[x, y])`,
		},
	}
	for _, tc := range cases {
		got := translateGraph(t, tc.bare, serverOpts())
		want := translateGraph(t, tc.withTable, serverOpts())
		if got.SQL != want.SQL {
			t.Errorf("%s\n got: %s\nwant: %s", tc.bare, got.SQL, want.SQL)
		}
		if !slices.Equal(got.FieldOrder, want.FieldOrder) {
			t.Errorf("%s: field order %v, want %v", tc.bare, got.FieldOrder, want.FieldOrder)
		}
		if got.ChartType != "graph" {
			t.Errorf("%s: chart type %q", tc.bare, got.ChartType)
		}
	}
}

// graph() names fields, never aggregates: a log field called count is a column.
func TestGraphFieldNamedCountIsNotAnAggregate(t *testing.T) {
	for _, q := range []string{
		`* | graph(child=a, parent=b, labels=[count])`,
		`* | graph(child=count, parent=b)`,
	} {
		res := translateGraph(t, q, serverOpts())
		if res.IsAggregated || strings.Contains(res.SQL, "COUNT(*)") {
			t.Errorf("%s aggregated the rows: %s", q, res.SQL)
		}
		if !slices.Contains(res.FieldOrder, "count") {
			t.Errorf("%s: count is not a result column: %v", q, res.FieldOrder)
		}
	}
}

func TestGraphRejectsArgumentsItCannotProject(t *testing.T) {
	for q, want := range map[string]string{
		`* | graph(child=a, parent=b, labels=[lower(x)])`: "graph(): labels= takes field names",
		`* | graph(child=a, parent=b, labels=["x,y"])`:    "graph(): invalid identifier",
		`* | graph(child="a b", parent=b)`:                "graph(): invalid identifier",
	} {
		_, err := TranslateToSQLWithOrder(mustParse(t, q), serverOpts())
		if err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("%s: error %v, want prefix %q", q, err, want)
		}
	}
}

// Rows that are already shaped (an explicit table(), an aggregation, ptg(), a
// source command) carry the columns by name, so graph() leaves them alone.
func TestGraphKeepsShapedRows(t *testing.T) {
	pgr := serverOpts()
	pgr.SourceSubquery = "SELECT 'x' AS a, 'y' AS b, 'z' AS c"
	pgr.SourceColumns = []string{"a", "b", "c"}
	ptgOpts := serverOpts()
	ptgOpts.ProcessTree = &ProcessTree{}
	for _, tc := range []struct {
		q, without string
		opts       QueryOptions
	}{
		{`* | table(a, b, z) | graph(child=a, parent=b)`, `* | table(a, b, z)`, serverOpts()},
		{`* | graph(child=a, parent=b) | table(z)`, `* | table(z)`, serverOpts()},
		{`* | groupby(a, b) | graph(child=a, parent=b)`, `* | groupby(a, b)`, serverOpts()},
		{`* | count() | graph(child=a, parent=b)`, `* | count()`, serverOpts()},
		{`ptg(start="ABC") | graph(child=process_guid, parent=parent_guid, labels=image)`, `ptg(start="ABC")`, ptgOpts},
		{`* | graph(child=a, parent=b)`, `*`, pgr},
	} {
		got := translateGraph(t, tc.q, tc.opts)
		want := translateGraph(t, tc.without, tc.opts)
		if got.SQL != want.SQL || !slices.Equal(got.FieldOrder, want.FieldOrder) {
			t.Errorf("%s changed the projection\n got: %v %s\nwant: %v %s", tc.q, got.FieldOrder, got.SQL, want.FieldOrder, want.SQL)
		}
	}
}

// modifiedzscore()/madoutlier() wrap the projection in window layers that read
// their value column and add their own outputs, so a later projection keeps the
// one and never re-reads the others as log fields.
func TestProjectionAfterModifiedZScoreKeepsWindowColumns(t *testing.T) {
	for _, q := range []string{
		`* | modifiedzscore(a) | table(a, b)`,
		`* | madoutlier(a) | table(a, b)`,
		`* | modifiedzscore(a) | table(a, _modified_z)`,
		`* | modifiedzscore(a) | graph(child=a, parent=b)`,
		`* | madoutlier(a) | graph(child=a, parent=b, labels=[_is_outlier])`,
	} {
		res := translateGraph(t, q, serverOpts())
		if !strings.Contains(res.SQL, "AS _mz_val") {
			t.Errorf("%s: window layers read _mz_val, which is not projected: %s", q, res.SQL)
		}
		if strings.Contains(res.SQL, "fields.`_") {
			t.Errorf("%s: a window output is read as a log field: %s", q, res.SQL)
		}
		if slices.Contains(res.FieldOrder, "_mz_val") {
			t.Errorf("%s: the internal _mz_val column is displayed: %v", q, res.FieldOrder)
		}
	}
}
