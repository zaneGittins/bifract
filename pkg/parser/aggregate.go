package parser

import "fmt"

// aggregate is an aggregate expression. scoped renders the same aggregate over
// only the rows cond holds for, as a case branch computes it.
type aggregate interface {
	sql() string
	scoped(cond string) string
}

// aggCall is a call to an aggregate function: fn(params)(args).
type aggCall struct {
	fn     string
	params string // a parametric aggregate's parameters, "" for none
	args   string
	// time marks min() or max() of a time, which is a time too.
	time bool
}

func (a aggCall) sql() string { return a.fn + a.paramList() + "(" + a.args + ")" }

// scoped is the -If combinator. COUNT(*) counts the rows cond holds for, and
// count(x) those where x is also not NULL.
func (a aggCall) scoped(cond string) string {
	if a.fn == "COUNT" || a.fn == "count" {
		if a.args == "*" {
			return "countIf(" + cond + ")"
		}
		return fmt.Sprintf("countIf((%s) IS NOT NULL AND (%s))", a.args, cond)
	}
	return a.fn + "If" + a.paramList() + "(" + a.args + ", " + cond + ")"
}

func (a aggCall) paramList() string {
	if a.params == "" {
		return ""
	}
	return "(" + a.params + ")"
}

// countAll is COUNT(*).
var countAll = aggCall{fn: "COUNT", args: "*"}

// madAgg is the median absolute deviation of v.
type madAgg struct{ v string }

func (m madAgg) sql() string { return m.over("groupArray(" + m.v + ")") }

func (m madAgg) scoped(cond string) string {
	return m.over(fmt.Sprintf("groupArrayIf(%s, %s)", m.v, cond))
}

func (m madAgg) over(values string) string {
	return fmt.Sprintf("arrayReduce('median', arrayMap(x -> abs(x - arrayReduce('median', %[1]s)), %[1]s))", values)
}

// aggDiff is one aggregate less another, as the interquartile range is.
type aggDiff struct{ a, b aggCall }

func (d aggDiff) sql() string { return d.a.sql() + " - " + d.b.sql() }

func (d aggDiff) scoped(cond string) string {
	return d.a.scoped(cond) + " - " + d.b.scoped(cond)
}

// topShare is top(percent=true): the n most frequent values of v with the share
// of rows each accounts for. approx_top_k returns (value, count, error) triples;
// topKWeightedWithCount, which this used to call, exists in no ClickHouse release.
type topShare struct {
	n int
	v string
}

func (t topShare) sql() string {
	return t.render(aggCall{fn: "approx_top_k", params: fmt.Sprint(t.n), args: t.v}.sql(), "count(*)")
}

func (t topShare) scoped(cond string) string {
	return t.render(aggCall{fn: "approx_top_k", params: fmt.Sprint(t.n), args: t.v}.scoped(cond), "countIf("+cond+")")
}

func (t topShare) render(top, rows string) string {
	return fmt.Sprintf("arrayMap(x -> (x.1, round(x.2 * 100 / %s, 2)), %s)", rows, top)
}

// rankCorrSample bounds the rows rankcorr() ranks per group. Spearman needs every
// value held to rank it, so an unbounded rankCorr grows with the scan; a sample of
// this size keeps memory fixed with a standard error near 0.003. Parallel merges
// pick the sample, so a larger group can differ by about that between runs.
const rankCorrSample = 100000

// corrAgg is corr() or rankcorr() of x and y, rounded to 4 places (+ 0 turns -0
// into 0) and NULL when undefined (fewer than two rows, or a constant operand).
// corr() is Pearson via corrStable: plain corr() loses precision on values far
// from zero, such as epoch times, and returned 1.17 on test data. rankcorr() is
// Spearman over a bounded sample of the pairs.
type corrAgg struct {
	rank bool
	x, y string
}

func (c corrAgg) sql() string { return c.render("") }

func (c corrAgg) scoped(cond string) string { return c.render(cond) }

func (c corrAgg) render(cond string) string {
	if !c.rank {
		call := aggCall{fn: "corrStable", args: c.x + ", " + c.y}
		sql := call.sql()
		if cond != "" {
			sql = call.scoped(cond)
		}
		return fmt.Sprintf("ifNotFinite(round(%s, 4) + 0, NULL)", sql)
	}
	keep := fmt.Sprintf("isNotNull(%s) AND isNotNull(%s)", c.x, c.y)
	if cond != "" {
		keep += " AND (" + cond + ")"
	}
	sample := fmt.Sprintf("groupArraySampleIf(%d, 1)((%s, %s), %s)", rankCorrSample, c.x, c.y, keep)
	return fmt.Sprintf("ifNotFinite(round(arrayReduce('rankCorr', arrayMap(t -> assumeNotNull(t.1), %[1]s), arrayMap(t -> assumeNotNull(t.2), %[1]s)), 4) + 0, NULL)", sample)
}

// operandAggregate is the aggregate a one-operand BQL aggregate computes over v,
// reading through reg what else it reads.
func operandAggregate(reg *FieldRegistry, name, v string) (aggregate, bool) {
	switch name {
	case "sum", "avg", "max", "min", "median":
		return aggCall{fn: name, args: v}, true
	case "percentile":
		return aggCall{fn: "quantiles", params: "0.5, 0.75, 0.99", args: v}, true
	case "stddev":
		return aggCall{fn: "stddevPop", args: v}, true
	case "skewness", "skew":
		return aggCall{fn: "skewPop", args: v}, true
	case "kurtosis", "kurt":
		return aggCall{fn: "kurtPop", args: v}, true
	case "mad":
		return madAgg{v: v}, true
	case "collect":
		return aggCall{fn: "groupArray", args: v}, true
	case "selectfirst":
		return aggCall{fn: "argMin", args: v + ", " + reg.Column("timestamp").sql}, true
	case "selectlast":
		return aggCall{fn: "argMax", args: v + ", " + reg.Column("timestamp").sql}, true
	}
	return nil, false
}

// aggType is the SQL type an aggregate computes: an array for the ones that
// collect values, the value itself for the ones that pick one, and otherwise a
// number.
func aggType(a aggregate) valueType {
	switch a := a.(type) {
	case topShare:
		return typeArray
	case aggCall:
		if a.time {
			return typeTime
		}
		switch a.fn {
		case "groupArray", "groupUniqArray", "topK", "quantiles", "approx_top_k":
			return typeArray
		case "argMin", "argMax", "any", "anyLast":
			return typeString
		}
	}
	return typeNumber
}

// aggSelect is the column an aggregate projects under alias. COUNT(*) reads no
// column; any other aggregate records what it reads where it is built.
func aggSelect(a aggregate, alias string) SelectExpr {
	return SelectExpr{Expr: a.sql(), Alias: alias, agg: a, colsKnown: a == aggregate(countAll)}
}

// aggSelectFrom is aggSelect reading the columns referenced since m.
func (r *FieldRegistry) aggSelectFrom(a aggregate, alias string, m readMark) SelectExpr {
	s := aggSelect(a, alias)
	s.cols, s.colsKnown = r.since(m).columns(), true
	return s
}
