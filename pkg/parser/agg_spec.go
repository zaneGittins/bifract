package parser

import (
	"fmt"
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
	sql, typ, err := compileExpr(a.Expr, registry, "")
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
	"mad": true, "multi": true,
}

// processAggSpec renders one aggregate spec into selectFields. ok is false when
// the name is not an aggregate; err reports a recognised name used wrongly.
func processAggSpec(spec *AggSpec, selectFields *[]string, computedFields map[string]bool, registry *FieldRegistry) (ok bool, err error) {
	if spec == nil {
		return false, nil
	}
	name := strings.ToLower(spec.Name)
	if !aggSpecNames[name] {
		return false, nil
	}
	o := readAggOptions(spec)

	emit := func(alias, sql string) error {
		want := sql + " AS " + alias
		for _, sel := range *selectFields {
			if strings.Trim(extractFieldAlias(sel), "`") != alias || sel == want {
				continue
			}
			if idx := strings.LastIndex(sel, " AS "); idx >= 0 {
				return fmt.Errorf("%s(): %s and %s both produce the column %s; name one with as=",
					name, sel[:idx], sql, alias)
			}
		}
		*selectFields = append(*selectFields, want)
		computedFields[alias] = true
		return nil
	}
	// ref and num are the operand's plain and numeric renderings, resolved lazily
	// so count() needs no operand at all. ResolveArg casts a raw JSON ref to
	// ::String, which these grouping contexts need for a Dynamic-stored path.
	ref := func() (string, error) { return ResolveArg(o.operand, registry) }
	num := func() (string, error) { return aggOperandNumeric(o.operand, registry) }

	render := func(agg, format string, resolve func() (string, error)) (bool, error) {
		alias, err := aggregateAlias(o.alias, agg)
		if err != nil {
			return false, fmt.Errorf("%s(): %w", name, err)
		}
		sql, err := resolve()
		if err != nil {
			return false, fmt.Errorf("%s(): %w", name, err)
		}
		return true, emit(alias, fmt.Sprintf(format, sql))
	}
	// timestamped covers the aggregates that read the event time directly when
	// their operand is the timestamp column.
	timestamped := func(agg, tsSQL, format string, resolve func() (string, error)) (bool, error) {
		if o.operand.FieldName() == "timestamp" {
			return render(agg, "%s", func() (string, error) { return tsSQL, nil })
		}
		return render(agg, format, resolve)
	}

	switch name {
	case "count":
		switch {
		case o.hasField && o.distinct:
			return render("count", "uniqExact(%s)", ref)
		case o.hasField:
			return render("count", "count(%s)", ref)
		default:
			return render("count", "%s", func() (string, error) { return "COUNT(*)", nil })
		}
	case "avg":
		return render("avg", "avg(%s)", num)
	case "sum":
		return render("sum", "sum(%s)", num)
	case "max":
		return timestamped("max", "max(timestamp)", "max(%s)", num)
	case "min":
		return timestamped("min", "min(timestamp)", "min(%s)", num)
	case "percentile":
		return render("percentile", "quantiles(0.5, 0.75, 0.99)(%s)", num)
	case "stddev":
		return render("stddev", "stddevPop(%s)", num)
	case "skewness", "skew":
		return render("skewness", "skewPop(%s)", num)
	case "kurtosis", "kurt":
		return render("kurtosis", "kurtPop(%s)", num)
	case "median":
		return render("median", "median(%s)", num)
	case "mad":
		return render("mad", "arrayReduce('median', arrayMap(x -> abs(x - arrayReduce('median', groupArray(%[1]s))), groupArray(%[1]s)))", num)
	case "iqr":
		cast, err := num()
		if err != nil {
			return false, fmt.Errorf("iqr(): %w", err)
		}
		// iqr() projects three columns, so as= names the range and the quartiles
		// take their own fixed names.
		alias, err := aggregateAlias(o.alias, "iqr")
		if err != nil {
			return false, fmt.Errorf("iqr(): %w", err)
		}
		for _, e := range []struct{ alias, sql string }{
			{"_q1", fmt.Sprintf("quantile(0.25)(%s)", cast)},
			{"_q3", fmt.Sprintf("quantile(0.75)(%s)", cast)},
			{alias, fmt.Sprintf("quantile(0.75)(%s) - quantile(0.25)(%s)", cast, cast)},
		} {
			if err := emit(e.alias, e.sql); err != nil {
				return false, err
			}
		}
		return true, nil
	case "selectfirst":
		return timestamped("first", "min(timestamp)", "argMin(%s, timestamp)", ref)
	case "selectlast":
		return timestamped("last", "max(timestamp)", "argMax(%s, timestamp)", ref)
	case "collect":
		if o.operand.FieldName() == "timestamp" {
			return render("collect", "groupArray(%s)", func() (string, error) { return "toString(timestamp)", nil })
		}
		return render("collect", "groupArray(%s)", ref)
	case "top":
		if o.percent {
			return render("top", "arrayMap(x -> (x.1, round(x.2 * 100 / count(*), 2)), topKWeightedWithCount(10)(%s, 1))", ref)
		}
		return render("top", "topK(10)(%s)", ref)
	}
	return false, nil
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
