package parser

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Every command's declared parameters must actually reach its handler. A
// parameter that parses and is then dropped is the defect the schemas exist to
// prevent: the query runs, reports nothing, and answers a different question.
//
// commandProbes gives a minimal working query per command plus a value for each
// parameter. The test asserts two things:
//
//   - coverage: every registered command appears here, so a new command cannot
//     be added without declaring how to exercise it;
//   - effect: setting a parameter either changes the generated SQL or the plan,
//     or is rejected. Producing the same output as without it means the handler
//     ignored it.
type commandProbe struct {
	// base is a query that runs with no optional parameters set.
	base string
	// with maps a parameter name to the same query with that parameter given.
	with map[string]string
	// inert names parameters that legitimately change nothing in the generated
	// SQL, with the reason. Chart parameters are read by the browser, not the
	// query, so they show up as client plan state instead.
	inert map[string]string
}

var commandProbes = map[string]commandProbe{
	"avg":    aggProbe("avg"),
	"sum":    aggProbe("sum"),
	"max":    aggProbe("max"),
	"min":    aggProbe("min"),
	"median": aggProbe("median"),

	"percentile": aggProbe("percentile"),
	"stddev":     aggProbe("stddev"),
	"skewness":   aggProbe("skew"),
	"kurtosis":   aggProbe("kurt"),
	"mad":        aggProbe("mad"),
	"iqr":        aggProbe("iqr"),

	"count": {
		base: `* | count(user)`,
		with: map[string]string{
			"field":    `* | count(host)`,
			"unique":   `* | count(user, unique=true)`,
			"distinct": `* | count(user, distinct=true)`,
			"as":       `* | count(user, as=zzmarker)`,
		},
	},
	"selectfirst": {
		base: `* | selectFirst(user)`,
		with: map[string]string{
			"field": `* | selectFirst(host)`,
			"as":    `* | selectFirst(user, as=zzmarker)`,
		},
	},
	"selectlast": {
		base: `* | selectLast(user)`,
		with: map[string]string{
			"field": `* | selectLast(host)`,
			"as":    `* | selectLast(user, as=zzmarker)`,
		},
	},
	"top": {
		base: `* | top(user)`,
		with: map[string]string{
			"field":   `* | top(host)`,
			"percent": `* | top(user, percent=true)`,
			"limit":   `* | top(user, limit=25)`,
			"as":      `* | top(user, as=zzmarker)`,
		},
	},
	"frequency": {base: `* | frequency(user)`, with: map[string]string{"field": `* | frequency(host)`}},
	"headtail": {
		base: `* | headTail(user)`,
		with: map[string]string{"field": `* | headTail(host)`, "threshold": `* | headTail(user, 55)`},
	},
	"groupby": {
		base: `* | groupby(user)`,
		with: map[string]string{
			"fields":   `* | groupby(host)`,
			"function": `* | groupby(user, function=sum(bytes))`,
			"limit":    `* | groupby(user, limit=7)`,
			"distinct": `* | groupby(user, distinct=true)`,
			"unique":   `* | groupby(user, unique=true)`,
		},
	},
	"multi":          {base: `* | multi(count())`, with: map[string]string{"functions": `* | multi(avg(bytes))`}},
	"dedup":          {base: `* | dedup(user)`, with: map[string]string{"fields": `* | dedup(user, host)`}},
	"sort":           {base: `* | sort(bytes)`, with: map[string]string{"field": `* | sort(latency)`, "order": `* | sort(bytes, desc)`}},
	"limit":          {base: `* | limit(5)`, with: map[string]string{"n": `* | limit(7)`}},
	"head":           {base: `* | head(5)`, with: map[string]string{"n": `* | head(7)`}},
	"tail":           {base: `* | tail(5)`, with: map[string]string{"n": `* | tail(7)`}},
	"table":          {base: `* | table(user)`, with: map[string]string{"fields": `* | table(user, host)`, "limit": `* | table(user, limit=7)`}},
	"analyzefields":  {base: `* | analyzeFields()`, with: map[string]string{"fields": `* | analyzeFields(user)`, "limit": `* | analyzeFields(limit=7)`}},
	"histogram":      {base: `* | histogram(bytes)`, with: map[string]string{"field": `* | histogram(latency)`, "buckets": `* | histogram(bytes, buckets=7)`}},
	"modifiedzscore": {base: `* | mzscore(bytes)`, with: map[string]string{"field": `* | mzscore(latency)`}},
	"madoutlier": {
		base: `* | outlier(bytes)`,
		with: map[string]string{"field": `* | outlier(latency)`, "threshold": `* | outlier(bytes, 4.5)`},
	},

	"in":   {base: `* | in(user, ["a"])`, with: map[string]string{"field": `* | in(host, ["a"])`, "values": `* | in(user, ["a","b"])`}},
	"cidr": {base: `* | cidr(src_ip, "10.0.0.0/8")`, with: map[string]string{"field": `* | cidr(dst_ip, "10.0.0.0/8")`, "range": `* | cidr(src_ip, "192.168.0.0/16")`}},
	"comment": {
		base: `* | comment()`,
		with: map[string]string{"tags": `* | comment(tags=[triage])`, "keyword": `* | comment(keyword="x")`},
		// Both are resolved server-side into CommentLogIDs before translation, so
		// the SQL is the same pre-fetched id list either way.
		inert: map[string]string{"tags": "resolved to log ids before translation", "keyword": "resolved to log ids before translation"},
	},

	"lowercase":    {base: `* | lowercase(user)`, with: map[string]string{"field": `* | lowercase(host)`, "output": `* | lowercase(user, zzmarker)`, "as": `* | lowercase(user, as=zzmarker)`}},
	"uppercase":    {base: `* | uppercase(user)`, with: map[string]string{"field": `* | uppercase(host)`, "output": `* | uppercase(user, zzmarker)`, "as": `* | uppercase(user, as=zzmarker)`}},
	"len":          {base: `* | len(cmdline)`, with: map[string]string{"field": `* | len(other)`, "as": `* | len(cmdline, as=zzmarker)`}},
	"logsize":      {base: `* | logSize()`, with: map[string]string{"field": `* | logSize(cmdline)`, "as": `* | logSize(as=zzmarker)`}},
	"base64decode": {base: `* | base64decode(cmdline)`, with: map[string]string{"field": `* | base64decode(other)`, "as": `* | base64decode(cmdline, as=zzmarker)`}},
	"urldecode":    {base: `* | urldecode(url)`, with: map[string]string{"field": `* | urldecode(other)`, "as": `* | urldecode(url, as=zzmarker)`}},
	"coalesce":     {base: `* | coalesce(a, b)`, with: map[string]string{"fields": `* | coalesce(a, c)`, "as": `* | coalesce(a, b, as=zzmarker)`}},
	"concat":       {base: `* | concat([a,b])`, with: map[string]string{"fields": `* | concat([a,c])`, "as": `* | concat([a,b], as=zzmarker)`}},
	"hash":         {base: `* | hash(a)`, with: map[string]string{"fields": `* | hash(a, b)`, "as": `* | hash(a, as=zzmarker)`}},
	"now":          {base: `* | now()`, with: map[string]string{"outputField": `* | now(zzmarker)`, "as": `* | now(as=zzmarker)`}},
	"split":        {base: `* | split(path, "/", 1)`, with: map[string]string{"field": `* | split(other, "/", 1)`, "delimiter": `* | split(path, ":", 1)`, "index": `* | split(path, "/", 2)`, "as": `* | split(path, "/", 1, as=zzmarker)`}},
	"substr":       {base: `* | substr(cmdline, 1)`, with: map[string]string{"field": `* | substr(other, 1)`, "start": `* | substr(cmdline, 3)`, "length": `* | substr(cmdline, 1, 9)`, "as": `* | substr(cmdline, 1, as=zzmarker)`}},
	"levenshtein":  {base: `* | levenshtein(a, b)`, with: map[string]string{"s1": `* | levenshtein(c, b)`, "s2": `* | levenshtein(a, c)`, "as": `* | levenshtein(a, b, as=zzmarker)`}},
	"sprintf":      {base: `* | sprintf("%s", a)`, with: map[string]string{"format": `* | sprintf("%s!", a)`, "fields": `* | sprintf("%s", b)`, "as": `* | sprintf("%s", a, as=zzmarker)`}},
	"strftime":     {base: `* | strftime("%H")`, with: map[string]string{"format": `* | strftime("%M")`, "field": `* | strftime("%H", field=ts)`, "timezone": `* | strftime("%H", timezone="America/Denver")`, "as": `* | strftime("%H", as=zzmarker)`}},
	"regex":        {base: `* | regex("a(b)")`, with: map[string]string{"pattern": `* | regex("c(d)")`, "regex": `* | regex(regex="c(d)")`, "field": `* | regex("a(b)", field=cmdline)`, "as": `* | regex("a(b)", as=zzmarker)`}},
	"replace":      {base: `* | replace(msg, "a", "b")`, with: map[string]string{"field": `* | replace(other, "a", "b")`, "pattern": `* | replace(msg, "c", "b")`, "replacement": `* | replace(msg, "a", "d")`, "as": `* | replace(msg, "a", "b", as=zzmarker)`}},

	"match":       {base: `* | match(dict="d", field=user, column=k, include=[t])`, with: map[string]string{"dict": `* | match(dict="e", field=user, column=k, include=[t])`, "field": `* | match(dict="d", field=host, column=k, include=[t])`, "column": `* | match(dict="d", field=user, column=j, include=[t])`, "include": `* | match(dict="d", field=user, column=k, include=[t,u])`, "require": `* | match(dict="d", field=user, column=k, include=[t], require=true)`, "strict": `* | match(dict="d", field=user, column=k, include=[t], strict=true)`}},
	"lookupip":    {base: `* | lookupIP(field=src_ip, include=[country])`, with: map[string]string{"field": `* | lookupIP(field=dst_ip, include=[country])`, "include": `* | lookupIP(field=src_ip, include=[country,city])`}},
	"modelLookup": {base: `* | modelLookup(model="m", key=[user])`, with: map[string]string{"model": `* | modelLookup(model="n", key=[user])`, "key": `* | modelLookup(model="m", key=[user,host])`, "require": `* | modelLookup(model="m", key=[user], require=false)`, "strict": `* | modelLookup(model="m", key=[user], strict=false)`}},
	"tlsh":        {base: `* | tlsh(tlsh, dict="d")`, with: map[string]string{"field": `* | tlsh(other_tlsh, dict="d")`, "dict": `* | tlsh(tlsh, dict="e")`, "hash": `* | tlsh(tlsh, hash="` + strings.Repeat("a", 70) + `")`, "threshold": `* | tlsh(tlsh, dict="d", threshold=42)`}},

	"mitre":     {base: `* | mitre()`, with: map[string]string{"field": `* | mitre(other_tags)`, "by": `* | mitre(by=host)`, "limit": `* | mitre(limit=7)`}},
	"ptg":       {base: `* | ptg(start="g")`, with: map[string]string{"start": `* | ptg(start="h2")`, "depth": `* | ptg(start="g", depth=3)`, "direction": `* | ptg(start="g", direction=forward)`}},
	"case":      {base: `* | case { a="x" | r := "1" ; * | r := "2" }`, with: map[string]string{"branches": `* | case { a="y" | r := "1" ; * | r := "2" }`}},
	"chain":     {base: `* | chain(user) { a="x" ; b="y" }`, with: map[string]string{"fields": `* | chain(host) { a="x" ; b="y" }`, "within": `* | chain(user, within=5m) { a="x" ; b="y" }`, "sequence": `* | chain(user, sequence=any) { a="x" ; b="y" }`, "order": `* | chain(user, order=false) { a="x" ; b="y" }`}},
	"join":      {base: `* | join(user) { a="x" | groupby(user) }`, with: map[string]string{"key": `* | join(host) { a="x" | groupby(host) }`, "type": `* | join(user, type=left) { a="x" | groupby(user) }`, "max": `* | join(user, max=55) { a="x" | groupby(user) }`, "include": `* | join(user, include=[n]) { a="x" | groupby(user, n) }`}},
	"eval":      {base: `* | eval(z = bytes * 2)`, with: map[string]string{"expression": `* | eval(z = bytes * 3)`}},
	"timechart": {base: `* | timechart(span=1h, count())`, with: map[string]string{"function": `* | timechart(span=1h, sum(bytes))`, "span": `* | timechart(span=1d, count())`}},

	// Chart commands: their parameters steer the browser renderer, so they change
	// the plan's chart config rather than the SQL.
	"heatmap":    {base: `* | heatmap(x=user, y=host)`, with: map[string]string{"x": `* | heatmap(x=a, y=host)`, "y": `* | heatmap(x=user, y=b)`, "value": `* | heatmap(x=user, y=host, sum(bytes))`, "limit": `* | heatmap(x=user, y=host, limit=7)`}},
	"piechart":   chartProbe(`* | groupby(user) | piechart(`),
	"barchart":   chartProbe(`* | groupby(user) | barchart(`),
	"singleval":  {base: `* | count() | singleval()`, with: map[string]string{"field": `* | count() | singleval(user)`, "title": `* | count() | singleval(title="T")`}, inert: map[string]string{"field": "display only; the value comes from the preceding aggregate"}},
	"graph":      {base: `* | graph(child=a, parent=b)`, with: map[string]string{"child": `* | graph(child=c, parent=b)`, "parent": `* | graph(child=a, parent=c)`, "labels": `* | graph(child=a, parent=b, labels=[l])`, "render": `* | graph(child=a, parent=b, render=7)`, "limit": `* | graph(child=a, parent=b, limit=7)`}},
	"mesh":       {base: `* | groupby(a,b) | mesh(src=a, dst=b)`, with: map[string]string{"src": `* | groupby(a,b) | mesh(src=b, dst=b)`, "dst": `* | groupby(a,b) | mesh(src=a, dst=a)`, "labels": `* | groupby(a,b) | mesh(src=a, dst=b, labels=[l])`, "size": `* | groupby(a,b) | mesh(src=a, dst=b, size=s)`, "weight": `* | groupby(a,b) | mesh(src=a, dst=b, weight=w)`, "directed": `* | groupby(a,b) | mesh(src=a, dst=b, directed=true)`, "color": `* | groupby(a,b) | mesh(src=a, dst=b, color=c)`, "render": `* | groupby(a,b) | mesh(src=a, dst=b, render=7)`, "limit": `* | groupby(a,b) | mesh(src=a, dst=b, limit=7)`}},
	"graphworld": {base: `* | graphWorld()`, with: map[string]string{"lat": `* | graphWorld(lat=la)`, "lon": `* | graphWorld(lon=lo)`, "label": `* | graphWorld(label=city)`, "render": `* | graphWorld(render=7)`, "limit": `* | graphWorld(render=7)`}},
	"pgraph":     {base: `* | pgraph()`, with: map[string]string{"render": `* | pgraph(render=7)`, "limit": `* | pgraph(limit=7)`}},

	// pgr() generates the pipeline source and is resolved by the query layer, not
	// the translator, so its parameters are covered by provenance_test.go.
	"pgr": {},
}

// lookup finds a parameter's probe query, ignoring the casing the spec declared.
func (p commandProbe) lookup(name string) (string, bool) {
	for k, v := range p.with {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}
	return "", false
}

// aggProbe is the shape every single-operand aggregate shares.
func aggProbe(name string) commandProbe {
	return commandProbe{
		base: fmt.Sprintf(`* | %s(bytes)`, name),
		with: map[string]string{
			"field": fmt.Sprintf(`* | %s(latency)`, name),
			"as":    fmt.Sprintf(`* | %s(bytes, as=zzmarker)`, name),
		},
	}
}

// chartProbe is the shape the charts that read only the preceding aggregation
// share: a draw cap and a row cap, no fields of their own.
func chartProbe(prefix string) commandProbe {
	return commandProbe{
		base: prefix + `)`,
		with: map[string]string{
			"render": prefix + `render=7)`,
			"limit":  prefix + `limit=7)`,
		},
	}
}

func TestEveryCommandHasAProbe(t *testing.T) {
	seen := map[string]bool{}
	for _, n := range CommandSpecNames() {
		spec, _ := CommandSpecFor(n)
		seen[spec.Name] = true
	}
	var missing []string
	for name := range seen {
		if _, ok := commandProbes[name]; !ok {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("commands with no probe in commandProbes: %s\n"+
			"Add one so its parameters are checked for being read.", strings.Join(missing, ", "))
	}
	for name := range commandProbes {
		if !seen[name] {
			t.Errorf("commandProbes has %q, which is not a registered command", name)
		}
	}
}

func TestEveryDeclaredParameterIsRead(t *testing.T) {
	for _, n := range CommandSpecNames() {
		spec, _ := CommandSpecFor(n)
		probe, ok := commandProbes[spec.Name]
		if !ok || probe.base == "" {
			continue // coverage is asserted by TestEveryCommandHasAProbe
		}
		baseline, baseErr := probeOutput(probe.base)
		for _, p := range spec.Params {
			name := strings.ToLower(p.Name)
			query, given := probe.lookup(name)
			if !given {
				t.Errorf("%s(): parameter %s has no probe query", spec.Name, name)
				continue
			}
			if _, inert := probe.inert[name]; inert {
				continue
			}
			got, err := probeOutput(query)
			if err != nil {
				continue // a rejection is a read
			}
			if baseErr == nil && got == baseline {
				t.Errorf("%s(): %s= changes nothing.\n  without: %s\n  with:    %s\n"+
					"The handler is dropping it. Read it, reject it, or list it in the probe's inert map with a reason.",
					spec.Name, name, probe.base, query)
			}
		}
	}
}

// probeOutput is the observable result of a query: the SQL plus the chart
// configuration, since a chart parameter steers the browser rather than the
// query.
func probeOutput(query string) (string, error) {
	pipeline, err := ParseQuery(query)
	if err != nil {
		return "", err
	}
	res, err := TranslateToSQLWithOrder(pipeline, revOpts())
	if err != nil {
		return "", err
	}
	keys := make([]string, 0, len(res.ChartConfig))
	for k := range res.ChartConfig {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(res.SQL)
	for _, k := range keys {
		fmt.Fprintf(&b, "|%s=%v", k, res.ChartConfig[k])
	}
	return b.String(), nil
}

// One rule for every aggregate's output column, in both the command form and
// inside multi(). These drifted apart once and produced different names for the
// same aggregate depending on how it was written.
func TestAggregateNamingIsOneRule(t *testing.T) {
	aggs := []struct{ call, column string }{
		{`count(user)`, "_count"},
		{`sum(bytes)`, "_sum"},
		{`avg(bytes)`, "_avg"},
		{`max(bytes)`, "_max"},
		{`min(bytes)`, "_min"},
		{`median(bytes)`, "_median"},
		{`percentile(bytes)`, "_percentile"},
		{`stddev(bytes)`, "_stddev"},
		{`skew(bytes)`, "_skewness"},
		{`kurt(bytes)`, "_kurtosis"},
		{`selectFirst(user)`, "_first"},
		{`selectLast(user)`, "_last"},
		{`top(user)`, "_top"},
	}
	for _, a := range aggs {
		for _, q := range []string{`* | ` + a.call, `* | multi(` + a.call + `)`} {
			sql, err := probeOutput(q)
			if err != nil {
				t.Errorf("%s: %v", q, err)
				continue
			}
			if !strings.Contains(sql, " AS "+a.column) {
				t.Errorf("%s: want column %s, got: %s", q, a.column, sql)
			}
		}
	}
}

// Two aggregates of the same kind in one stage would otherwise collide on that
// one name: the second was dropped, or both were emitted and the server
// rejected the query.
func TestDuplicateAggregateNeedsAnAlias(t *testing.T) {
	for _, q := range []string{
		`* | multi(sum(a), sum(b))`,
		`* | groupby(u) | sum(a) | sum(b)`,
		`* | groupby(u) | selectFirst(a) | selectFirst(b)`,
	} {
		if _, err := probeOutput(q); err == nil {
			t.Errorf("%s: colliding aggregates accepted", q)
		}
	}
	// With as= they coexist.
	sql, err := probeOutput(`* | multi(sum(a, as=rx), sum(b, as=tx))`)
	if err != nil {
		t.Fatalf("named aggregates rejected: %v", err)
	}
	if !strings.Contains(sql, " AS rx") || !strings.Contains(sql, " AS tx") {
		t.Errorf("both aliases should survive: %s", sql)
	}
}

// Every column a command generates is underscore-prefixed, which is what tells
// it apart from a log field of the same name.
func TestGeneratedColumnsArePrefixed(t *testing.T) {
	for _, c := range []struct{ query, column string }{
		{`* | hash(a)`, "_hash"},
		{`* | regex("a(b)")`, "_regex"},
		{`* | frequency(user)`, "_value"},
		{`* | headTail(user)`, "_value"},
		{`* | mitre()`, "_attack_tag"},
		{`* | concat([a,b])`, "_concat"},
		{`* | len(a)`, "_len"},
		{`* | now()`, "_now"},
	} {
		sql, err := probeOutput(c.query)
		if err != nil {
			t.Errorf("%s: %v", c.query, err)
			continue
		}
		if !strings.Contains(sql, " AS "+c.column) {
			t.Errorf("%s: want generated column %s, got: %s", c.query, c.column, sql)
		}
	}
}

// render= caps the marks a chart draws; limit= caps the rows the query returns.
// They were one parameter, so limit= on a chart bounded neither.
func TestChartRenderAndLimitAreSeparate(t *testing.T) {
	sql, err := probeOutput(`* | groupby(user) | piechart(render=5, limit=50)`)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	if !strings.Contains(sql, "LIMIT 50") {
		t.Errorf("limit= should bound the rows: %s", sql)
	}
	if !strings.Contains(sql, "|render=5") {
		t.Errorf("render= should reach the chart config: %s", sql)
	}
}

// A command that collapses rows must be registered as aggregating: the registry
// decides where an assignment, a dedup and a model join attach, so one that
// aggregates silently puts all three at the wrong stage.
func TestAggregatingCommandsSaySo(t *testing.T) {
	for name, probe := range commandProbes {
		pipeline, err := ParseQuery(probe.base)
		if err != nil {
			continue // TestEveryCommandHasAProbe owns probe validity
		}
		res, err := TranslateToSQLWithOrder(pipeline, revOpts())
		if err != nil {
			continue
		}
		// Every base probe is one command over a bare `*`.
		if res.IsAggregated && !IsAggregatingCommand(name) {
			t.Errorf("%s() collapses rows but is not registered with registerAggregatingCommand; "+
				"an assignment, dedup or model join after it will bind to the wrong stage", name)
		}
	}
}

// A transform either adds a column of its own or writes its result back to the
// field it read. Which one it does is a fact callers depend on, so it is declared
// rather than inferred: a caller that keeps a query's predicates without its
// projection sees a rewritten column still carrying a log field's name while no
// longer holding that field's stored value.
func TestTransformOutputsAreDeclared(t *testing.T) {
	alias := regexp.MustCompile(`\bAS ([a-zA-Z_][a-zA-Z0-9_]*)`)
	for name, probe := range commandProbes {
		if !IsTransformCommand(name) {
			continue
		}
		sql, err := probeOutput(probe.base)
		if err != nil {
			continue // TestEveryCommandHasAProbe owns probe validity
		}
		// An output whose own name is also a field the expression reads is a
		// rewrite in place: lower(fields.`image`) AS image. An output the command
		// invented (_hash, or an as= name) reads some other field.
		rewrites := false
		for _, m := range alias.FindAllStringSubmatch(sql, -1) {
			if out := m[1]; strings.Contains(sql, "fields.`"+out+"`") {
				rewrites = true
			}
		}
		if rewrites != RewritesFieldInPlace(name) {
			t.Errorf("%s(): rewrites a field in place = %v, but RewritesFieldInPlace says %v; "+
				"update rewriteInPlaceCommandNames, and check every caller that keeps predicates "+
				"without the projection\n  %s", name, rewrites, RewritesFieldInPlace(name), sql)
		}
	}
}
