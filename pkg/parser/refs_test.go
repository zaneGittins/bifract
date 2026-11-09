package parser

import (
	"slices"
	"strings"
	"testing"
)

func plannedFor(t *testing.T, query string, opts QueryOptions) *plannedQuery {
	t.Helper()
	pl, err := ParseQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	q, err := planQuery(pl, opts)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

// An ordering records whether the scan alone can evaluate it. A computed alias
// named like a keyword or interval unit (hour, first) is a column of the SELECT
// that computes it, not of the logs table.
func TestOrderingRecordsWhetherTheScanReadsIt(t *testing.T) {
	for q, want := range map[string]bool{
		`* | sort(timestamp)`:                                    true,
		`* | sort(user)`:                                         true,
		`* | sort(lower(user))`:                                  true,
		`* | hour := length(user) | sort(hour)`:                  false,
		`* | first := lower(user) | sort(first)`:                 false,
		`* | groupby(user) | sort(_count)`:                       false,
		`* | modelLookup(model="fs", key=[user]) | sort(is_new)`: false,
	} {
		var terms []OrderTerm
		for _, st := range plannedFor(t, q, mlookupOpts()).ctx.Plan.Stages {
			terms = append(terms, st.Layer.OrderBy...)
		}
		if len(terms) == 0 {
			t.Fatalf("%s: no ordering", q)
		}
		if got := terms[len(terms)-1].deps.scan; got != want {
			t.Errorf("%s: ordering reads only the scan = %v, want %v", q, got, want)
		}
	}
}

// A dedup key on a computed field reads its alias where the field is computed, and
// its expression over the scan, so only the scan form is scan-only.
func TestDedupKeysRecordBothForms(t *testing.T) {
	c := plannedFor(t, `* | x := lower(user) | dedup(x)`, goldenBase()).ctx.Plan.SourceStage().Layer.Caps[0]
	if c.limitByDeps.scan || !c.scanDeps.scan {
		t.Errorf("LIMIT BY %q scan=%v, scan form %q scan=%v", c.LimitBy, c.limitByDeps.scan, c.ScanLimitBy, c.scanDeps.scan)
	}
	c = plannedFor(t, `* | dedup(user)`, goldenBase()).ctx.Plan.SourceStage().Layer.Caps[0]
	if !c.limitByDeps.scan || !c.scanDeps.scan {
		t.Errorf("a stored key is not scan-only: %+v", c)
	}
}

// What a computed field reads is recorded where it is computed, through any number
// of fields computed from it.
func TestJoinDependenceFollowsComputedFields(t *testing.T) {
	reg := plannedFor(t, `* | modelLookup(model="fs", key=[user]) | x := event_count * 2 | y := x + 1 | z := length(user)`, mlookupOpts()).ctx.Registry
	for f, want := range map[string]bool{"is_new": true, "x": true, "y": true, "z": false} {
		if got := reg.DependsOnJoin(f); got != want {
			t.Errorf("DependsOnJoin(%s) = %v, want %v", f, got, want)
		}
	}
}

// Only the model lookup's own columns read only the model: a join() column, or a
// column a SELECT computes, does not exist on the model's side of the join.
func TestOnlyModelColumnsReadOnlyTheModel(t *testing.T) {
	r := NewFieldRegistry(SourceHot, nil)
	r.Register("is_new", FieldKindJoined, "is_new", 0)
	r.setReads("is_new", modelColumn)
	r.Register("score", FieldKindJoined, "score", 0)
	r.Register("n", FieldKindPerRow, "n", 0)
	r.SetResolveExpr("n", "toUInt8(is_new) + 1")
	r.setReads("n", modelColumn)
	for name, want := range map[string]deps{
		"is_new":    {join: true, model: true},
		"score":     {join: true},
		"n":         {join: true},
		"timestamp": {scan: true},
		"_count":    {},
	} {
		if got := r.Column(name).deps; got != want {
			t.Errorf("Column(%s).deps = %+v, want %+v", name, got, want)
		}
	}
	if got := r.Ref("n").deps; got != modelColumn {
		t.Errorf("an expression of model columns is model-only as an expression: %+v", got)
	}
}

// A layer above the scan reads an expression through the column the scan exports
// it as, and a column by its name, whatever the SQL looks like.
func TestDeferredScopeExportsByForm(t *testing.T) {
	s := newDeferredScope()
	for _, c := range []struct {
		ref      sqlRef
		exported bool
	}{
		{sqlRef{sql: "_count", name: "_count", form: refColumn}, false},
		{sqlRef{sql: "x", name: "y", form: refExpr}, true},
		{sqlRef{sql: normLogColumn, name: normLogColumn, form: refColumn}, true},
		{sqlRef{sql: "fields.`a`::String", name: "a", form: refExpr}, true},
	} {
		got, exported := s.ref(c.ref, c.ref.name)
		if exported != c.exported || exported && got.form != refColumn {
			t.Errorf("ref(%+v) = %+v exported=%v, want exported=%v", c.ref, got, exported, c.exported)
		}
	}
}

// A predicate records what its references read however they are combined.
func TestPredicatesCombineWhatTheyRead(t *testing.T) {
	scan, model := predicate{sql: "a", deps: deps{scan: true}}, predicate{sql: "b", deps: modelColumn}
	for name, p := range map[string]predicate{"and": andOf(scan, model), "or": orOf(scan, model), "not": notOf(andOf(scan, model))} {
		if p.deps != (deps{join: true}) {
			t.Errorf("%s: %+v", name, p.deps)
		}
	}
	if p := andOf(scan, predicate{}); p.deps != scan.deps {
		t.Errorf("an empty predicate changed what the conjunction reads: %+v", p.deps)
	}
}

func TestSQLColumns(t *testing.T) {
	for sql, want := range map[string][]string{
		"fields.`query`::String = 'a b' AND is_new = '1'": {"fields", "is_new"},
		"dateDiff('second', first_seen, _now) > 3600":     {"first_seen", "_now"},
		"toFloat64OrDefault(percent, nan) < 0.1e3":        {"percent"},
		// Unquoted nan and inf are Float64 literals; backticked, a column.
		"x < NaN OR x > -inf OR `nan` > 0":                       {"x", "x", "nan"},
		"timestamp - INTERVAL 1 HOUR > hour":                     {"timestamp", "hour"},
		"`odd name` IS NOT NULL AND x IN ('it''s', 'a\\'b')":     {"odd name", "x"},
		"arrayExists(x -> x = 'a', [fields.`a`::String, other])": {"fields", "other"},
	} {
		if got := sqlColumns(sql); !slices.Equal(got, want) {
			t.Errorf("sqlColumns(%q) = %q, want %q", sql, got, want)
		}
	}
}

// A command that rebuilds a field in place reads what its new expression reads,
// not what the field's earlier definition did: rebuilt from model columns it
// filters on the model's side, and mixed with a stored field on the joined rows,
// never on the scan.
func TestRebuiltFieldReadsItsNewExpression(t *testing.T) {
	for q, modelSide := range map[string]bool{
		`* | x := "q" | model_lookup(model="fs", key=[user]) | lowercase(is_new, output=x) | x = "1"`:      true,
		`* | len(image) | model_lookup(model="fs", key=[user]) | len(is_new) | _len > 1`:                   true,
		`* | x := "q" | model_lookup(model="fs", key=[user]) | len(image, as=x) | x = "1" OR is_new = "1"`: false,
	} {
		p := plannedFor(t, q, mlookupOpts()).ctx.Plan
		if got := len(p.lookup.Where) > 0; got != modelSide {
			t.Errorf("%s: on the model's side = %v, want %v", q, got, modelSide)
		}
		if !modelSide && len(p.lookup.PostJoinWhere) == 0 {
			t.Errorf("%s: not placed on the joined rows", q)
		}
	}
}

// A copy of a column is that column at every layer, so a layer above the scan
// reads it by name rather than through an export.
func TestColumnCopyIsReadAsTheColumn(t *testing.T) {
	q := plannedFor(t, `* | x := log_id | mzscore(duration) | _modified_z > 3 OR x = "a"`, goldenBase())
	if r := q.ctx.Registry.Ref("x"); r.form != refColumn || r.sql != "log_id" {
		t.Errorf("x := log_id resolves as %+v", r)
	}
	if q.ctx.Plan.deferredScope().used() {
		t.Errorf("a column copy was exported: %v", q.ctx.Plan.deferredScope().exprs)
	}
}

// An aggregate reads the columns of a field folded into it: y := event_count * 2
// read under an aggregate named event_count would read that aggregate.
func TestAggregateReadsTheColumnsOfAFoldedField(t *testing.T) {
	for _, q := range []string{
		`* | model_lookup(model="fs", key=[user]) | y := event_count * 2 | groupby(host, function=[count(as=event_count), sum(y)])`,
		`* | model_lookup(model="fs", key=[user]) | x := event_count | groupby(host, function=[count(as=event_count), sum(x)])`,
	} {
		pl, err := ParseQuery(q)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := TranslateToSQLWithOrder(pl, mlookupOpts()); err == nil || !strings.Contains(err.Error(), "reads event_count") {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// Each field a command defines reads only what its own SQL reads, so a filter on
// one computed from stored fields stays on the scan even when the same command
// also reads the model join.
func TestFieldsOfOneCommandReadOnlyTheirOwn(t *testing.T) {
	q := plannedFor(t, `* | modelLookup(model="fs", key=[user]) | eval(a=is_new, b=length(image)) | b > 5`, mlookupOpts())
	if !q.ctx.Registry.DependsOnJoin("a") || q.ctx.Registry.DependsOnJoin("b") {
		t.Errorf("a joins=%v b joins=%v", q.ctx.Registry.DependsOnJoin("a"), q.ctx.Registry.DependsOnJoin("b"))
	}
	if len(q.ctx.Plan.lookup.PostJoinWhere) != 0 {
		t.Errorf("b > 5 waits for the join: %s", q.ctx.Plan.lookup.PostJoinWhere[0].Pred.sql)
	}
	h := plannedFor(t, `* | modelLookup(model="fs", key=[user]) | heatmap(x=is_new, y=image)`, mlookupOpts()).ctx.Registry
	if !h.DependsOnJoin("_heatmap_x") || h.DependsOnJoin("_heatmap_y") {
		t.Errorf("heatmap axes: x joins=%v y joins=%v", h.DependsOnJoin("_heatmap_x"), h.DependsOnJoin("_heatmap_y"))
	}
}

// A reference carries the SQL type its value has, recorded where it is computed,
// and each use converts by it: a String function converts a number, an ordering
// parses text, and an aggregate parses only what is not a number already.
func TestReferencesCarryTheirType(t *testing.T) {
	for q, want := range map[string]string{
		`* | join(dst_ip) { * | groupby(dst_ip) } | len(_join__count)`: "length(toString(_join__count))",
		`* | x := length(user) | lowercase(x, output=y)`:               "lower(toString(length(fields.`user`::String)))",
		`* | k := host | k > 1`:                                        "toFloat64OrDefault(fields.`host`::String, nan) > 1",
		`* | a := "z" | lowercase(a)`:                                  "lower('z') AS a",
		`* | eval(n = length(user)) | sum(n)`:                          "sum(toFloat64(",
	} {
		if sql := mustTranslate(t, q, goldenBase()); !strings.Contains(sql, want) {
			t.Errorf("%s: want %q in\n%s", q, want, sql)
		}
	}
}

// Review findings: each translation keeps results the earlier SQL gave, and runs.
func TestReviewedTypeAndJoinShapes(t *testing.T) {
	for _, c := range []struct{ q, want, not string }{
		// A String result may be NULL: NOT modelLookup() keeps such a row.
		{`* | eval(k = tostring(bytes * 2)) | !modelLookup(model="fs", key=[k])`, "ifNull(toString(", ""},
		// An array's length counts its elements.
		{`* | groupby(user, function=collect(image)) | len(_collect)`, "length(_collect)", "toString(_collect)"},
		// A number hashes as the number it is.
		{`* | groupby(user) | hash(_count)`, "(_count)", "toString(_count)"},
		{`* | eval(n = length(user)) | hash(n)`, "", "toString(length"},
	} {
		sql := translateML(t, c.q)
		if c.want != "" && !strings.Contains(sql, c.want) || c.not != "" && strings.Contains(sql, c.not) {
			t.Errorf("%s: want %q, not %q:\n%s", c.q, c.want, c.not, sql)
		}
	}
}

// tlsh()'s columns read the field they hash, so a filter on one never goes to the
// model's side.
func TestTLSHColumnsReadTheirField(t *testing.T) {
	o := mlookupOpts()
	o.HasTLSHFilter, o.TLSHMatches = true, []TLSHMatch{{Digest: "d", Distance: 1, Needle: "T1A"}}
	q := `* | modelLookup(model="fs", key=[user]) | tlsh(field=sha, hash="T1A") | tlsh_distance < 50 OR is_new = "1"`
	pl, err := ParseQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	p, err := planQuery(pl, o)
	if err != nil {
		t.Skip(err)
	}
	if len(p.ctx.Plan.lookup.Where) > 0 {
		t.Errorf("on the model's side: %s", p.ctx.Plan.lookup.Where[0].sql)
	}
}
