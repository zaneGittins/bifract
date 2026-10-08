package parser

import (
	"strings"
	"testing"
)

func translateNumericViz(t *testing.T, query string) *TranslationResult {
	t.Helper()
	pipeline, err := ParseQuery(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	res, err := TranslateToSQLWithOrder(pipeline, serverOpts())
	if err != nil {
		t.Fatalf("translate %q: %v", query, err)
	}
	return res
}

// A chart that summarizes the result must see every row: the display LIMIT
// once made histogram() bin only the newest MaxRows events.
func TestDistributionChartsReadEveryRow(t *testing.T) {
	for _, q := range []string{
		`* | histogram(bytes)`,
		`* | boxplot(bytes)`,
		`* | groupby(host) | boxplot(_count)`,
		`* | groupby(host) | histogram(_count)`,
		`* | groupby(host) | multi(count(as=a), sum(bytes, as=b)) | scatter(x=a, y=b)`,
	} {
		sql := translateNumericViz(t, q).SQL
		if strings.Contains(sql, "LIMIT 1000") {
			t.Errorf("%s: the chart input is truncated by the display limit:\n%s", q, sql)
		}
	}
}

func TestBoxplot(t *testing.T) {
	res := translateNumericViz(t, `* | boxplot(bytes, by=image, fence=3, limit=5, outliers=4)`)
	if res.ChartType != "boxplot" {
		t.Fatalf("chart type = %q", res.ChartType)
	}
	for _, want := range []string{
		"quantilesGK(1000, 0.25, 0.5, 0.75)(_bp_val)",
		"groupArraySorted(4)(_bp_val)",
		"_bp_q[3] + 3 * (_bp_q[3] - _bp_q[1]) AS _bp_uf",
		"GROUP BY _group",
		"LIMIT 5",
		"fields.`image`::String AS _bp_group_src",
	} {
		if !strings.Contains(res.SQL, want) {
			t.Errorf("missing %q in:\n%s", want, res.SQL)
		}
	}
	if res.FieldOrder[0] != "_group" || res.ChartConfig["by"] != "image" {
		t.Errorf("grouped boxplot should lead with _group: %v %v", res.FieldOrder, res.ChartConfig)
	}

	if res := translateNumericViz(t, `* | boxplot(bytes)`); res.FieldOrder[0] != "_count" {
		t.Errorf("ungrouped boxplot should not list _group: %v", res.FieldOrder)
	}
	// After an aggregation the group may be a grouping key.
	if sql := translateNumericViz(t, `* | groupby([host, image]) | boxplot(_count, by=image)`).SQL; !strings.Contains(sql, "toString(image) AS _group") {
		t.Errorf("group key not read as a column:\n%s", sql)
	}
}

func TestNumericChartsRejectCollapsedFields(t *testing.T) {
	for _, q := range []string{
		`* | groupby(host) | boxplot(bytes)`,
		`* | groupby(host) | boxplot(_count, by=image)`,
		`* | groupby(host) | scatter(x=_count, y=bytes)`,
		`* | scatter(x=a, y=a)`,
		`* | scatter(x=a)`,
		`* | boxplot(bytes, fence=0)`,
	} {
		pipeline, err := ParseQuery(q)
		if err != nil {
			continue
		}
		if _, err := TranslateToSQLWithOrder(pipeline, serverOpts()); err == nil {
			t.Errorf("%s: expected an error", q)
		}
	}
}

func TestScatter(t *testing.T) {
	// Per event: the newest events that carry both values, bounded at the scan.
	res := translateNumericViz(t, `* | scatter(x=bytes_in, y=bytes_out, label=host, limit=300)`)
	for _, want := range []string{
		"isFinite(toFloat64OrNull(fields.`bytes_in`::String))",
		"ORDER BY timestamp DESC, log_id DESC LIMIT 300",
		"toString(_sc_label_src) AS host",
	} {
		if !strings.Contains(res.SQL, want) {
			t.Errorf("missing %q in:\n%s", want, res.SQL)
		}
	}
	if strings.Join(res.FieldOrder, ",") != "host,bytes_in,bytes_out" {
		t.Errorf("field order = %v", res.FieldOrder)
	}

	// Per group: the aggregate outputs are read as columns, no scan filter.
	res = translateNumericViz(t, `* | groupby(host) | multi(sum(orig_bytes, as=sent), sum(resp_bytes, as=received)) | scatter(x=received, y=sent, label=host)`)
	if !strings.Contains(res.SQL, "toFloat64OrNull(toString(received)) AS received") || strings.Contains(res.SQL, "_sc_x_src") {
		t.Errorf("aggregate outputs should be read as columns:\n%s", res.SQL)
	}
	if res.ChartConfig["xField"] != "received" || res.ChartConfig["labelField"] != "host" {
		t.Errorf("chart config = %v", res.ChartConfig)
	}
}
