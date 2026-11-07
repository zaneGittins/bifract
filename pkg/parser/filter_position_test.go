package parser

import (
	"strings"
	"testing"
)

// A filter reads the rows its position in the pipe produces.
func TestFilterReadsTheRowsAtItsPosition(t *testing.T) {
	cases := []struct{ name, query, want string }{
		{"in on a key lowered after the aggregation", `* | groupby(user) | lowercase(user) | in(user, values=["alice"])`, "WHERE user IN ('alice')"},
		{"cidr on a key rewritten after the aggregation", `* | groupby(src_ip) | lowercase(src_ip) | cidr(src_ip, "10.0.0.0/8")`, "WHERE (isIPAddressInRange(if((isIPv4String(src_ip)"},
		{"in before the rewrite reads the key", `* | groupby(user) | in(user, values=["alice"]) | lowercase(user)`, "fields.`user`::String IN ('alice') GROUP BY"},
		{"strict match on a key after the aggregation", `* | groupby(user) | match(dict="d", field=user, column=k, include=[v], strict=true)`, "GROUP BY"},
		{"a key conjunct still narrows the scan", `* | groupby(user) | _count > 1 in(user, values=["a"])`, "IN ('a') GROUP BY user HAVING _count > 1"},
		{"a key comparison conjunct still narrows the scan", `* | groupby(user) | _count > 1 user="a"`, "GROUP BY user HAVING _count > 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := translateAfterAggregation(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(res.SQL, tc.want) {
				t.Errorf("missing %q:\n%s", tc.want, res.SQL)
			}
		})
	}
}

func TestCommandsAfterAggregationReadOnlyItsOutputs(t *testing.T) {
	cases := []struct{ query, want string }{
		{`* | groupby(src_ip) | table(user)`, "user does not exist after groupby()"},
		{`* | groupby(src_ip) | table(src_ip, user)`, "user does not exist after groupby()"},
		{`* | count() | table(user)`, "user does not exist after count()"},
		{`* | groupby(src_ip) | sort(user)`, "user does not exist after groupby()"},
		{`* | groupby(src_ip) | sprintf("%s!", user, as=hx)`, "user does not exist after groupby()"},
		{`* | groupby(src_ip) | match(dict="d", field=user, column=k, include=[v])`, "user does not exist after groupby()"},
		{`* | count() | graph(child=a, parent=b)`, "graph(): b does not exist after count()"},
		{`* | groupby(user) | comment("x")`, "comment() reads log rows, so move it before groupby()"},
		{`* | groupby(user) | _count > 1 OR comment("x")`, "comment() reads log rows, so move it before groupby()"},
		{`* | chain(user) { a=1; b=2 } | !comment("x")`, "comment() reads log rows, so move it before chain()"},
	}
	for _, tc := range cases {
		if _, err := translateAfterAggregation(tc.query); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.query, err, tc.want)
		}
	}
	for _, q := range []string{
		`* | groupby(src_ip) | table(src_ip, _count)`,
		`* | groupby(user) | sum(bytes)`,
		`* | groupby(user) | chain(host) { a=1; b=2 }`,
		`* | groupby(user) | sort(_count) | table(user)`,
		`* | groupby(user) | sprintf("%s!", user, as=hx) | table(hx)`,
		`* | groupby(user) | len(user) | sort(_len)`,
		`* | comment("x") | groupby(user)`,
	} {
		if _, err := translateAfterAggregation(q); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// Charts filter on what they return, like any aggregation.
func TestFilterAfterChartReadsItsOutput(t *testing.T) {
	cases := []struct{ name, query, want string }{
		{"timechart count", `* | timechart(span=1h) | _count > 1`, "HAVING _count > 1"},
		{"timechart avg", `* | timechart(span=1h, function=avg(bytes)) | _avg > 1`, "HAVING _avg > 1"},
		{"timechart bucket", `* | timechart(span=1h) | time_bucket > "2026-01-01 05:00:00"`, "time_bucket > '2026-01-01 05:00:00' GROUP BY"},
		{"timechart series key", `* | timechart(span=1h, function=groupby(user)) | user="a"`, "GROUP BY"},
		{"heatmap value", `* | heatmap(x=user, y=host, count()) | _heatmap_value > 1`, "HAVING _heatmap_value > 1"},
		{"heatmap axis", `* | heatmap(x=user, y=host, count()) | _heatmap_x = "a"`, "fields.`user`::String = 'a' GROUP BY"},
		{"frequency share", `* | frequency(user) | _percentage > 10`, "_percentage > 10"},
		{"piechart key", `* | groupby(user) | piechart() | user="a"`, "GROUP BY"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := translateAfterAggregation(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(res.SQL, tc.want) {
				t.Errorf("missing %q:\n%s", tc.want, res.SQL)
			}
			if strings.Contains(res.SQL, "fields.`_") || strings.Contains(res.SQL, "fields.`time_bucket`") {
				t.Errorf("an output was read as a log field:\n%s", res.SQL)
			}
		})
	}
	rejected := []struct{ query, want string }{
		{`* | timechart(span=1h) | user="a"`, "user does not exist after timechart()"},
		{`* | heatmap(x=user, y=host, count()) | user="a"`, "user does not exist after heatmap()"},
		{`* | frequency(user) | user="a"`, "user does not exist after frequency()"},
		{`* | mitre() | user="a"`, "user does not exist after mitre()"},
		{`* | boxplot(bytes) | _median > 1`, "a filter cannot follow boxplot()"},
		{`* | histogram(bytes) | _bin_count > 1`, "a filter cannot follow histogram()"},
		{`* | analyzefields(user, bytes) | _events > 1`, "a filter cannot follow analyzefields()"},
	}
	for _, tc := range rejected {
		if _, err := translateAfterAggregation(tc.query); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.query, err, tc.want)
		}
	}
}

// A row cap on aggregated rows is a boundary: what follows reads the rows it kept.
func TestFilterAfterCapOnAggregatedRows(t *testing.T) {
	cases := []struct{ name, query, want string }{
		{"key filter after head", `* | groupby(user) | sort(_count, order=desc) | head(1) | user="bob"`, "LIMIT 1) WHERE user = 'bob'"},
		{"in after head", `* | groupby(user) | sort(_count, order=desc) | head(1) | in(user, values=["bob"])`, "LIMIT 1) WHERE user IN ('bob')"},
		{"aggregate filter after head", `* | groupby(user) | sort(_count, order=desc) | head(1) | _count > 5`, "LIMIT 1) WHERE _count > 5"},
		{"sort after head", `* | groupby(user) | sort(_count, order=desc) | head(2) | sort(user)`, "LIMIT 2) ORDER BY user"},
		{"filter after tail", `* | groupby(user) | sort(_count) | tail(1) | user="bob"`, "LIMIT 1) WHERE user = 'bob'"},
		{"filter after dedup", `* | groupby(user, host) | dedup(user) | host="h1"`, ") WHERE host = 'h1'"},
		{"filter before head stays inside", `* | groupby(user) | user="bob" | head(1)`, "fields.`user`::String = 'bob' GROUP BY"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := translateAfterAggregation(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(res.SQL, tc.want) {
				t.Errorf("missing %q:\n%s", tc.want, res.SQL)
			}
		})
	}
}

// A filter after a window score keeps scored rows; the baseline is every row.
func TestFilterAfterWindowScoreKeepsTheBaseline(t *testing.T) {
	for _, q := range []string{
		`* | outlier(bytes) | user="alice"`,
		`* | modifiedzscore(bytes) | in(user, values=["alice"])`,
		`* | outlier(bytes) | contains(user, "ali")`,
		`* | outlier(bytes) | "alice"`,
		`* | outlier(bytes) | _is_outlier = "1" AND user="alice"`,
	} {
		res, err := translateAfterAggregation(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if scan := scanWhere(res.SQL); strings.Contains(scan, "alice") || strings.Contains(scan, "'ali'") {
			t.Errorf("%s: the filter narrowed the baseline:\n%s", q, res.SQL)
		}
		if !strings.Contains(res.SQL, "alice") && !strings.Contains(res.SQL, "ali") {
			t.Errorf("%s: the filter was dropped:\n%s", q, res.SQL)
		}
	}
	res, err := translateAfterAggregation(`* | user="alice" | outlier(bytes)`)
	if err != nil {
		t.Fatal(err)
	}
	if scan := scanWhere(res.SQL); !strings.Contains(scan, "alice") {
		t.Errorf("a filter before the score should narrow the baseline:\n%s", res.SQL)
	}
}

// scanWhere is the WHERE of the logs scan in rendered SQL.
func scanWhere(sql string) string {
	_, after, _ := strings.Cut(sql, "FROM logs WHERE")
	where, _, _ := strings.Cut(after, " ORDER BY")
	return where
}

// mesh() draws the rows before it and scatter() plots events, so filters after
// them keep working as they did.
func TestFilterAfterMeshAndScatter(t *testing.T) {
	for _, q := range []string{
		`* | groupby(user, host) | mesh(src=user, dst=host) | _count > 1`,
		`* | groupby(user, host) | mesh(src=user, dst=host) | user="alice"`,
		`* | scatter(x=bytes, y=event_id) | bytes > 5`,
	} {
		if _, err := translateAfterAggregation(q); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}
