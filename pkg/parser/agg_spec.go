package parser

import (
	"fmt"
	"slices"
	"strings"
)

// Aggregate specs are the functions written inside multi(), groupby(function=)
// and bucket(function=). They are dispatched here from the parsed spec, so a
// name, an operand and an option are read once and never re-derived from text.

// aggOptions are the named options every aggregate spec accepts.
type aggOptions struct {
	operand  Argument
	hasField bool
	alias    string
	distinct bool
	percent  bool
}

// readAggOptions splits a spec's arguments into its operand and its options. The
// operand is field= when named and the first bare argument otherwise.
func readAggOptions(spec *AggSpec) aggOptions {
	var o aggOptions
	for _, a := range spec.Args {
		switch strings.ToLower(a.Name) {
		case "":
			if !o.hasField {
				o.operand, o.hasField = a, true
			}
		case "field":
			o.operand, o.hasField = a, true
		case "as":
			o.alias = a.Value()
		case "distinct", "unique":
			o.distinct = strings.EqualFold(a.Value(), "true")
		case "percent":
			o.percent = strings.EqualFold(a.Value(), "true")
		}
	}
	return o
}

// aggOperandNumeric resolves an operand for a numeric aggregate, adding the cast
// its type needs. A field's cast follows what the registry knows about it; an
// expression's follows what it compiles to, because the OrNull/OrZero casts take
// a String and wrapping an already-numeric expression is illegal (sum(len(x))
// failed at the server with "Illegal type UInt64").
func aggOperandNumeric(a Argument, registry *FieldRegistry) (string, error) {
	if name := a.FieldName(); name != "" {
		return numericCast(name, resolveFieldRef(name, registry), registry), nil
	}
	if a.Kind != ArgExpr || a.Expr == nil {
		return "", fmt.Errorf("%s is not a value", a)
	}
	sql, typ, err := compileExpr(a.Expr, registry)
	if err != nil {
		return "", err
	}
	if typ == TypeNumber {
		return sql, nil
	}
	return fmt.Sprintf("toFloat64OrNull(%s)", sql), nil
}

// aggregateAlias is the column an aggregate projects: the author's as= when
// given, otherwise the aggregate's own name with an underscore. One rule, so the
// same aggregate written as a pipeline command and inside multi() names its
// column the same, and every aggregate honours as=.
func aggregateAlias(as, agg string) (string, error) {
	if as == "" {
		as = "_" + agg
	}
	return sanitizeIdentifier(as)
}

// aggSpecNames are the aggregate specifications processAggSpec renders. They are
// not scalar functions, so an expression position holding one is the owning
// handler's to interpret rather than a typo. One list, so an alias cannot be
// recognised in one place and unknown in another.
var aggSpecNames = map[string]bool{
	"count": true, "avg": true, "sum": true, "max": true, "min": true,
	"percentile": true, "stddev": true, "skewness": true, "skew": true,
	"kurtosis": true, "kurt": true, "iqr": true, "selectfirst": true,
	"selectlast": true, "collect": true, "top": true, "median": true,
	"mad": true, "multi": true, "corr": true, "rankcorr": true,
}

// processAggSpec renders one aggregate spec into selects. ok is false when the
// name is not an aggregate; err reports a recognised name used wrongly.
func processAggSpec(spec *AggSpec, selects *[]SelectExpr, computedFields map[string]bool, registry *FieldRegistry) (ok bool, err error) {
	if spec == nil {
		return false, nil
	}
	name := strings.ToLower(spec.Name)
	if !aggSpecNames[name] {
		return false, nil
	}
	r := &aggRenderer{name: name, opts: readAggOptions(spec), registry: registry, selects: selects, computed: computedFields}
	switch name {
	case "corr", "rankcorr":
		err = r.correlation(spec.Args)
	case "count":
		err = r.count()
	case "max", "min":
		err = r.of(name, true, aggCall{fn: name, args: "timestamp"}, r.single)
	case "selectfirst":
		err = r.of("first", false, aggCall{fn: "min", args: "timestamp"}, r.single)
	case "selectlast":
		err = r.of("last", false, aggCall{fn: "max", args: "timestamp"}, r.single)
	case "collect":
		err = r.of("collect", false, aggCall{fn: "groupArray", args: "toString(timestamp)"}, r.single)
	case "top":
		err = r.of("top", false, nil, r.top)
	case "iqr":
		err = r.iqr()
	default:
		err = r.of(canonicalAggregate(name), true, nil, r.single)
	}
	return err == nil, err
}

// aggRenderer renders one aggregate spec into a select list.
type aggRenderer struct {
	name     string
	opts     aggOptions
	registry *FieldRegistry
	selects  *[]SelectExpr
	computed map[string]bool
}

// emit adds the aggregate's column under alias, refusing a different column of
// the same name.
func (r *aggRenderer) emit(alias string, a aggregate) error {
	sql := a.sql()
	for _, sel := range *r.selects {
		// A column carried from an earlier stage has no alias of its own and does
		// not compete.
		if sel.Name() == alias && sel.Alias != "" && (sel.Expr != sql || sel.Alias != alias) {
			return fmt.Errorf("%s(): %s and %s both produce the column %s; name one with as=", r.name, sel.Expr, sql, alias)
		}
	}
	if !slices.ContainsFunc(*r.selects, func(sel SelectExpr) bool { return sel.Expr == sql && sel.Alias == alias }) {
		*r.selects = append(*r.selects, aggSelect(a, alias))
	}
	r.computed[alias] = true
	return nil
}

func (r *aggRenderer) fail(err error) error { return fmt.Errorf("%s(): %w", r.name, err) }

// operand renders the operand, as a number when numeric. ResolveArg casts a raw
// JSON ref to ::String, which these grouping contexts need for a Dynamic-stored
// path.
func (r *aggRenderer) operand(numeric bool) (string, error) {
	if numeric {
		return aggOperandNumeric(r.opts.operand, r.registry)
	}
	return ResolveArg(r.opts.operand, r.registry)
}

// of emits the aggregate build makes of the operand under agg's column name, or
// ofTime when the operand is the event time and ofTime is set.
func (r *aggRenderer) of(agg string, numeric bool, ofTime aggregate, build func(v string) aggregate) error {
	out, err := aggregateAlias(r.opts.alias, agg)
	if err != nil {
		return r.fail(err)
	}
	if ofTime != nil && r.opts.operand.FieldName() == "timestamp" {
		return r.emit(out, ofTime)
	}
	v, err := r.operand(numeric)
	if err != nil {
		return r.fail(err)
	}
	return r.emit(out, build(v))
}

func (r *aggRenderer) single(v string) aggregate {
	a, _ := operandAggregate(r.name, v)
	return a
}

func (r *aggRenderer) top(v string) aggregate {
	if r.opts.percent {
		return topShare{n: 10, v: v}
	}
	return aggCall{fn: "topK", params: "10", args: v}
}

// count is COUNT(*), or with a field the rows where it is set, or its distinct
// values.
func (r *aggRenderer) count() error {
	if !r.opts.hasField {
		out, err := aggregateAlias(r.opts.alias, "count")
		if err != nil {
			return r.fail(err)
		}
		return r.emit(out, countAll)
	}
	fn := "count"
	if r.opts.distinct {
		fn = "uniqExact"
	}
	return r.of("count", false, nil, func(v string) aggregate { return aggCall{fn: fn, args: v} })
}

// correlation is corr() or rankcorr() of two numeric operands.
func (r *aggRenderer) correlation(args []Argument) error {
	xa, ya, ok := correlationOperands(args)
	if !ok {
		return fmt.Errorf("%s() needs two numeric fields, e.g. %s(orig_bytes, resp_bytes)", r.name, r.name)
	}
	out, err := aggregateAlias(r.opts.alias, r.name)
	if err != nil {
		return r.fail(err)
	}
	x, err := aggOperandNumeric(xa, r.registry)
	if err != nil {
		return r.fail(err)
	}
	y, err := aggOperandNumeric(ya, r.registry)
	if err != nil {
		return r.fail(err)
	}
	return r.emit(out, corrAgg{rank: r.name == "rankcorr", x: x, y: y})
}

// iqr projects three columns, so as= names the range and the quartiles take
// their own fixed names.
func (r *aggRenderer) iqr() error {
	out, err := aggregateAlias(r.opts.alias, "iqr")
	if err != nil {
		return r.fail(err)
	}
	v, err := r.operand(true)
	if err != nil {
		return r.fail(err)
	}
	q1, q3 := aggCall{fn: "quantile", params: "0.25", args: v}, aggCall{fn: "quantile", params: "0.75", args: v}
	for _, col := range []SelectExpr{aggSelect(q1, "_q1"), aggSelect(q3, "_q3"), aggSelect(aggDiff{a: q3, b: q1}, out)} {
		if err := r.emit(col.Alias, col.agg); err != nil {
			return err
		}
	}
	return nil
}

// canonicalAggregate is the name an aggregate's output column is named after.
func canonicalAggregate(name string) string {
	switch name {
	case "skew":
		return "skewness"
	case "kurt":
		return "kurtosis"
	}
	return name
}

// asAggSpec reads an argument as an aggregate specification. A bare call in a
// field position parses as an expression, so it is accepted here too.
func asAggSpec(a Argument) (*AggSpec, bool) {
	switch a.Kind {
	case ArgAggSpec:
		return a.Agg, a.Agg != nil
	case ArgExpr:
		if a.Expr == nil || a.Expr.Kind != ExprCall {
			return nil, false
		}
		spec := &AggSpec{Name: a.Expr.Value, Pos: a.Pos}
		for _, arg := range a.Expr.Args {
			spec.Args = append(spec.Args, Argument{Kind: ArgExpr, Expr: arg})
		}
		return spec, true
	}
	return nil, false
}

// aggAliasClash rejects a second aggregate that would project a column an
// earlier one already owns. Every aggregate names its column after itself, so
// two of the same kind in one stage collide; without this the second is dropped
// or emitted twice, and the query answers a question nobody asked.
func aggAliasClash(stage *QueryStage, agg, alias, sql string) error {
	prior, clash := exprAliasClash(stage, alias, sql)
	if !clash {
		return nil
	}
	return fmt.Errorf("%s(): %s and %s both produce the column %s; name one with as=", agg, prior, sql, alias)
}
