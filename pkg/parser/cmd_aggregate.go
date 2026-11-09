package parser

import (
	"fmt"
	"strings"
)

// countHandler handles count(), count(field), count(field, unique=true)
type countHandler struct{}

func (h *countHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Registry.Register("_count", FieldKindAggregate, "COUNT(*)", ctx.CmdIndex)
	ctx.Plan.IsAggregated = true
	return nil
}

func (h *countHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	source := ctx.Plan.CurrentStage()

	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	arg, hasField := b.First("field")
	unique := b.Flag("unique", false) || b.Flag("distinct", false)
	alias, err := aggregateAlias(b.Str("as", ""), "count")
	if err != nil {
		return fmt.Errorf("count(): %w", err)
	}

	// Bare count() after groupby: push second stage to count the number of groups.
	// count(field) or count(field, unique=true) still adds to the groupby stage.
	if ctx.Plan.HasGroupBy && len(source.Layer.GroupBy) > 0 && !hasField && !unique {
		if err := pushAggregationStage(ctx); err != nil {
			return fmt.Errorf("count (stage finalize): %w", err)
		}
		ctx.Plan.HasGroupBy = false

		newSource := ctx.Plan.CurrentStage()
		newSource.Layer.Selects = append(newSource.Layer.Selects, aggSelect(countAll, "_count"))
		ctx.Plan.IsAggregated = true
		ctx.Plan.aggregationOutputs["_count"] = "COUNT(*)"
		// The outer COUNT(*) is a fresh aggregate of this stage; register its kind
		// and bare resolve so HAVING _count > N is typed numeric, not coerced.
		ctx.Registry.Register("_count", FieldKindAggregate, "_count", ctx.CmdIndex)
		ctx.Registry.SetResolveExpr("_count", "_count")
		return nil
	}

	// count(field) and count(field, unique=true). The operand may be an
	// expression, which resolves to the SQL it compiles to rather than a field.
	if hasField {
		ref, err := ResolveArg(arg, ctx.Registry)
		if err != nil {
			return fmt.Errorf("count(): %w", err)
		}
		agg := aggCall{fn: "count", args: ref}
		if unique {
			agg.fn = "uniqExact"
		}
		sqlExpr := agg.sql()
		if err := aggAliasClash(source, "count", alias, sqlExpr); err != nil {
			return err
		}
		source.Layer.addColumn(aggSelect(agg, alias))
		ctx.Plan.IsAggregated = true
		ctx.Plan.aggregationOutputs[alias] = sqlExpr
		ctx.Registry.Register(alias, FieldKindAggregate, alias, ctx.CmdIndex)
		ctx.Registry.SetResolveExpr(alias, alias)
		return nil
	}

	// Bare count() without groupby
	source.Layer.addColumn(aggSelect(countAll, alias))
	ctx.Plan.IsAggregated = true
	ctx.Plan.aggregationOutputs[alias] = "COUNT(*)"
	ctx.Registry.Register(alias, FieldKindAggregate, alias, ctx.CmdIndex)
	ctx.Registry.SetResolveExpr(alias, alias)

	// When count() is used with case statements, GROUP BY the case-produced fields
	for _, cmd2 := range ctx.Pipeline.Commands() {
		if cmd2.Name == "case" {
			compiled, err := compileCase(cmd2.Branches, ctx.Registry, ctx.Opts)
			if err != nil {
				return err
			}
			if len(compiled.assignments) > 0 {
				for _, assignment := range compiled.assignments {
					source.Layer.groupBy(assignment.Field, assignment.Field)
				}
			} else {
				outputField := caseOutputColumn
				source.Layer.groupBy(outputField, outputField)
			}
		}
	}
	return nil
}

// simpleAggHandler is every aggregate command whose shape is one numeric operand
// in, one column out (see operandAggregate). Keeping them in one handler is what
// makes them agree on naming, on as=, and on chaining over a prior aggregate's
// output.
type simpleAggHandler struct {
	name string // the BQL name, which is also the output column: sum -> _sum
}

func (h *simpleAggHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) == 0 {
		return nil
	}
	alias, err := commandAggAlias(cmd, h.name)
	if err != nil {
		return nil
	}
	ctx.Registry.Register(alias, FieldKindAggregate, alias, ctx.CmdIndex)
	ctx.Plan.IsAggregated = true
	return nil
}

func (h *simpleAggHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	alias, err := aggregateAlias(b.Str("as", ""), h.name)
	if err != nil {
		return fmt.Errorf("%s(): %w", h.name, err)
	}
	field := arg.FieldName()
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		// Chained over a prior aggregation's output column, which is already a
		// number: read it directly rather than through the string casts.
		agg, _ := operandAggregate(h.name, fmt.Sprintf("toFloat64(%s)", field))
		ctx.Plan.outer.selects = append(ctx.Plan.outer.selects, aggSelect(agg, alias))
		ctx.Plan.outer.reads = append(ctx.Plan.outer.reads, field)
		ctx.Plan.aggregationOutputs[alias] = agg.sql()
	} else {
		// max/min over the event time read the column itself: a DateTime cannot
		// take the numeric casts, and the extreme of a timestamp is a timestamp.
		operand := field
		if (h.name != "max" && h.name != "min") || field != "timestamp" {
			if operand, err = aggOperandNumeric(arg, ctx.Registry); err != nil {
				return fmt.Errorf("%s(): %w", h.name, err)
			}
		}
		agg, _ := operandAggregate(h.name, operand)
		if err := aggAliasClash(source, h.name, alias, agg.sql()); err != nil {
			return err
		}
		source.Layer.Selects = append(source.Layer.Selects, aggSelect(agg, alias))
		ctx.Plan.aggregationOutputs[alias] = agg.sql()
	}
	ctx.Registry.SetResolveExpr(alias, alias)
	ctx.Plan.IsAggregated = true
	return nil
}

// commandAggAlias is the output column an aggregate command projects, read
// during Declare where only the schema is available.
func commandAggAlias(cmd CommandNode, agg string) (string, error) {
	b, err := BindCommand(cmd)
	if err != nil {
		return "", err
	}
	return aggregateAlias(b.Str("as", ""), agg)
}

// frequencyHandler handles frequency(field)
type frequencyHandler struct{}

func (h *frequencyHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register("_count", FieldKindAggregate, "count(*)", ctx.CmdIndex)
		ctx.Registry.Register("_percentage", FieldKindWindow, "_percentage", ctx.CmdIndex)
		ctx.Registry.Register("_cumulative_pct", FieldKindWindow, "_cumulative_pct", ctx.CmdIndex)
		ctx.Registry.Register("_value", FieldKindAggregate, "_value", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *frequencyHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	fieldRef, err := ResolveArg(arg, ctx.Registry)
	if err != nil {
		return fmt.Errorf("frequency(): %w", err)
	}
	source := ctx.Plan.CurrentStage()

	source.Layer.groupBy(fieldRef, keyName(arg, fieldRef))
	source.Layer.Selects = append(source.Layer.Selects,
		SelectExpr{Expr: fieldRef, Alias: "_value"},
		SelectExpr{Expr: "count(*)", Alias: "_count"},
		SelectExpr{Expr: "round(_count * 100.0 / sum(_count) OVER (), 2)", Alias: "_percentage"},
		SelectExpr{Expr: "round(sum(_count) OVER (ORDER BY _count DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 / sum(_count) OVER (), 2)", Alias: "_cumulative_pct"},
	)
	source.Layer.OrderBy = append(source.Layer.OrderBy, desc("_count"))
	ctx.Plan.aggregationOutputs["_count"] = "count(*)"
	ctx.Plan.aggregationOutputs["_percentage"] = "_percentage"
	ctx.Plan.aggregationOutputs["_cumulative_pct"] = "_cumulative_pct"
	ctx.Registry.SetResolveExpr("_count", "_count")
	ctx.Registry.SetResolveExpr("_percentage", "_percentage")
	ctx.Registry.SetResolveExpr("_cumulative_pct", "_cumulative_pct")
	ctx.Registry.SetResolveExpr("_value", "_value")
	ctx.Plan.IsAggregated = true
	return nil
}

// iqrHandler handles iqr(field) - interquartile range
type iqrHandler struct{}

func (h *iqrHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register("_q1", FieldKindAggregate, "_q1", ctx.CmdIndex)
		ctx.Registry.Register("_q3", FieldKindAggregate, "_q3", ctx.CmdIndex)
		ctx.Registry.Register("_iqr", FieldKindAggregate, "_iqr", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *iqrHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	field := arg.FieldName()
	iqrAlias, err := aggregateAlias(b.Str("as", ""), "iqr")
	if err != nil {
		return fmt.Errorf("iqr(): %w", err)
	}
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		v := fmt.Sprintf("toFloat64(%s)", field)
		q1, q3 := aggCall{fn: "quantile", params: "0.25", args: v}, aggCall{fn: "quantile", params: "0.75", args: v}
		ctx.Plan.outer.selects = append(ctx.Plan.outer.selects,
			aggSelect(q1, "_q1"), aggSelect(q3, "_q3"), aggSelect(aggDiff{a: q3, b: q1}, iqrAlias))
		ctx.Plan.outer.reads = append(ctx.Plan.outer.reads, field)
		ctx.Plan.aggregationOutputs["_q1"] = q1.sql()
		ctx.Plan.aggregationOutputs["_q3"] = q3.sql()
		ctx.Plan.aggregationOutputs[iqrAlias] = iqrAlias
	} else {
		cast, err := aggOperandNumeric(arg, ctx.Registry)
		if err != nil {
			return err
		}
		q1, q3 := aggCall{fn: "quantile", params: "0.25", args: cast}, aggCall{fn: "quantile", params: "0.75", args: cast}
		source.Layer.Selects = append(source.Layer.Selects, aggSelect(q1, "_q1"), aggSelect(q3, "_q3"), aggSelect(aggDiff{a: q3, b: q1}, iqrAlias))
		ctx.Plan.aggregationOutputs["_q1"] = q1.sql()
		ctx.Plan.aggregationOutputs["_q3"] = q3.sql()
		ctx.Plan.aggregationOutputs[iqrAlias] = iqrAlias
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// headtailHandler handles headTail(field, threshold)
type headtailHandler struct{}

func (h *headtailHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register("_count", FieldKindAggregate, "count(*)", ctx.CmdIndex)
		ctx.Registry.Register("_percentage", FieldKindWindow, "_percentage", ctx.CmdIndex)
		ctx.Registry.Register("_cumulative_pct", FieldKindWindow, "_cumulative_pct", ctx.CmdIndex)
		ctx.Registry.Register("_segment", FieldKindWindow, "_segment", ctx.CmdIndex)
		ctx.Registry.Register("_value", FieldKindAggregate, "_value", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *headtailHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	threshold := b.Str("threshold", "80")
	if err := validateNumeric(threshold); err != nil {
		return fmt.Errorf("headTail(): invalid threshold: %w", err)
	}
	fieldRef, err := ResolveArg(arg, ctx.Registry)
	if err != nil {
		return fmt.Errorf("headTail(): %w", err)
	}
	source := ctx.Plan.CurrentStage()

	source.Layer.groupBy(fieldRef, keyName(arg, fieldRef))
	cumulExpr := "round(sum(_count) OVER (ORDER BY _count DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 / sum(_count) OVER (), 2)"
	source.Layer.Selects = append(source.Layer.Selects,
		SelectExpr{Expr: fieldRef, Alias: "_value"},
		SelectExpr{Expr: "count(*)", Alias: "_count"},
		SelectExpr{Expr: "round(_count * 100.0 / sum(_count) OVER (), 2)", Alias: "_percentage"},
		SelectExpr{Expr: cumulExpr, Alias: "_cumulative_pct"},
		SelectExpr{Expr: fmt.Sprintf("CASE WHEN %s <= %s THEN 'head' ELSE 'tail' END", cumulExpr, threshold), Alias: "_segment"},
	)
	source.Layer.OrderBy = append(source.Layer.OrderBy, desc("_count"))
	ctx.Plan.aggregationOutputs["_count"] = "count(*)"
	ctx.Plan.aggregationOutputs["_percentage"] = "_percentage"
	ctx.Plan.aggregationOutputs["_cumulative_pct"] = "_cumulative_pct"
	ctx.Plan.aggregationOutputs["_segment"] = "_segment"
	ctx.Registry.SetResolveExpr("_count", "_count")
	ctx.Registry.SetResolveExpr("_percentage", "_percentage")
	ctx.Registry.SetResolveExpr("_cumulative_pct", "_cumulative_pct")
	ctx.Registry.SetResolveExpr("_segment", "_segment")
	ctx.Registry.SetResolveExpr("_value", "_value")
	ctx.Plan.IsAggregated = true
	return nil
}

// selectEdgeHandler handles selectFirst(field) and selectLast(field): the value
// of the field on the earliest or latest event.
type selectEdgeHandler struct {
	name string // selectFirst or selectLast
	out  string // the output column's base name: first or last
}

func (h *selectEdgeHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) == 0 {
		return nil
	}
	ctx.Plan.IsAggregated = true
	if alias, err := commandAggAlias(cmd, h.out); err == nil {
		ctx.Registry.Register(alias, FieldKindAggregate, alias, ctx.CmdIndex)
	}
	return nil
}

func (h *selectEdgeHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	alias, err := aggregateAlias(b.Str("as", ""), h.out)
	if err != nil {
		return fmt.Errorf("%s(): %w", h.name, err)
	}
	// Over the event time itself it is the earliest or latest time.
	var agg aggregate = aggCall{fn: map[string]string{"first": "min", "last": "max"}[h.out], args: "timestamp"}
	if arg.FieldName() != "timestamp" {
		ref, err := ResolveArg(arg, ctx.Registry)
		if err != nil {
			return fmt.Errorf("%s(): %w", h.name, err)
		}
		agg, _ = operandAggregate(strings.ToLower(h.name), ref)
	}
	source := ctx.Plan.CurrentStage()
	if err := aggAliasClash(source, h.name, alias, agg.sql()); err != nil {
		return err
	}
	source.Layer.Selects = append(source.Layer.Selects, aggSelect(agg, alias))
	ctx.Plan.IsAggregated = true
	return nil
}

// topHandler handles top(field, percent=true, limit=N, as=alias)
type topHandler struct{}

func (h *topHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	if !b.Has("field") {
		return nil
	}
	alias, err := aggregateAlias(b.Str("as", ""), "top")
	if err != nil {
		return nil
	}
	ctx.Registry.Register(alias, FieldKindAggregate, alias, ctx.CmdIndex)
	ctx.Plan.IsAggregated = true
	return nil
}

func (h *topHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	showPercent := b.Flag("percent", false)
	topN := 10
	if n := b.Int("limit", 0); n > 0 {
		topN = n
	}
	alias, err := aggregateAlias(b.Str("as", ""), "top")
	if err != nil {
		return fmt.Errorf("top(): %w", err)
	}
	fieldRef, err := ResolveArg(arg, ctx.Registry)
	if err != nil {
		return fmt.Errorf("top(): %w", err)
	}
	source := ctx.Plan.CurrentStage()
	var agg aggregate = aggCall{fn: "topK", params: fmt.Sprint(topN), args: fieldRef}
	if showPercent {
		agg = topShare{n: topN, v: fieldRef}
	}
	source.Layer.Selects = append(source.Layer.Selects, aggSelect(agg, alias))
	ctx.Registry.SetResolveExpr(alias, alias)
	ctx.Plan.IsAggregated = true
	return nil
}

// multiHandler handles multi(count(), avg(response_time), sum(bytes))
type multiHandler struct{}

func (h *multiHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	for range cmd.Args {
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *multiHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	return projectAggregates(ctx, ctx.Plan.CurrentStage(), b.Flat("functions"), ctx.Registry.AllComputed())
}

// madHandler handles mad(field) - median absolute deviation
type madHandler struct{}

func (h *madHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) == 0 {
		return nil
	}
	// The alias, not the default name: registering _mad for mad(x, as=spread)
	// left a later _mad filter pointing at a column the stage never projects.
	alias, err := commandAggAlias(cmd, "mad")
	if err != nil {
		return nil
	}
	ctx.Registry.Register("_median", FieldKindWindow, "_median", ctx.CmdIndex)
	ctx.Registry.Register(alias, FieldKindWindow, alias, ctx.CmdIndex)
	ctx.Plan.IsAggregated = true
	return nil
}

func (h *madHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err := rejectMeasureAfterAggregation(arg, ctx, "mad"); err != nil {
		return err
	}
	field := arg.FieldName()
	madAlias, err := aggregateAlias(b.Str("as", ""), "mad")
	if err != nil {
		return fmt.Errorf("mad(): %w", err)
	}
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		numericExpr := fmt.Sprintf("toFloat64(%s)", field)
		ctx.Plan.outer.madExpr = numericExpr
		ctx.Plan.outer.selects = append(ctx.Plan.outer.selects,
			selectAs("any(_median_val)", "_median"), selectAs(fmt.Sprintf("median(abs(%s - _median_val))", numericExpr), madAlias))
		ctx.Plan.outer.reads = append(ctx.Plan.outer.reads, field, "_median_val")
		ctx.Registry.SetResolveExpr("_median", "any(_median_val)")
		ctx.Registry.SetResolveExpr(madAlias, fmt.Sprintf("median(abs(%s - _median_val))", numericExpr))
		ctx.Plan.aggregationOutputs["_median"] = "any(_median_val)"
		ctx.Plan.aggregationOutputs[madAlias] = fmt.Sprintf("median(abs(%s - _median_val))", numericExpr)
	} else {
		fieldRef, err := ResolveArg(arg, ctx.Registry)
		if err != nil {
			return fmt.Errorf("mad(): %w", err)
		}
		numericExpr := fmt.Sprintf("toFloat64OrNull(%s)", fieldRef)
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: numericExpr, Alias: "_mad_val"})
		ctx.Plan.outer.madExpr = "_mad_val"
		ctx.Plan.outer.selects = append(ctx.Plan.outer.selects,
			selectAs("any(_median_val)", "_median"), selectAs("median(abs(_mad_val - _median_val))", madAlias))
		ctx.Plan.outer.reads = append(ctx.Plan.outer.reads, "_mad_val", "_median_val")
		ctx.Registry.SetResolveExpr("_median", "any(_median_val)")
		ctx.Registry.SetResolveExpr(madAlias, "median(abs(_mad_val - _median_val))")
		ctx.Plan.aggregationOutputs["_median"] = "any(_median_val)"
		ctx.Plan.aggregationOutputs[madAlias] = "median(abs(_mad_val - _median_val))"
	}
	// Declare's registrations are gone if an earlier aggregation pushed a stage,
	// which left a later _mad filter classified as a log field and pushed into the
	// scan's WHERE, where the column does not exist.
	registerWindowOutputs(ctx, "_median", madAlias)
	ctx.Plan.IsAggregated = true
	return nil
}

// exprAliasClash reports whether the stage already projects this alias from a
// different expression, which UpsertSelect would silently replace.
func exprAliasClash(stage *QueryStage, alias, sql string) (string, bool) {
	for _, sel := range stage.Layer.Selects {
		// A column without an alias is carried forward from an earlier stage, not
		// a competing expression.
		if sel.Alias != "" && sel.Name() == alias && sel.Expr != sql {
			return sel.Expr, true
		}
	}
	return "", false
}

// groupbyHandler handles groupby(field1, field2, ...)
type groupbyHandler struct{}

func (h *groupbyHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.HasGroupBy = true
	// Without function= a groupby counts, producing _count. With it, the specs
	// name their own outputs and _count exists only if one of them is a count.
	if b, err := BindCommand(cmd); err != nil || !b.Has("function") {
		ctx.Registry.Register("_count", FieldKindAggregate, "COUNT(*)", ctx.CmdIndex)
	}
	return nil
}

// countedKeyRef resolves the key distinct= counts, as a group key would resolve but without
// projecting it: it is not an output of the aggregation.
func countedKeyRef(keyArg Argument, computed map[string]bool, isMultiStage bool, ctx *CommandContext) (string, error) {
	arg := keyArg.Value()
	switch {
	case keyArg.FieldName() == "":
		sql, err := ResolveArg(keyArg, ctx.Registry)
		if err != nil {
			return "", fmt.Errorf("groupby(): %w", err)
		}
		return sql, nil
	case computed[arg], isMultiStage, arg == "timestamp", arg == normLogColumn, arg == "log_id", arg == "normalizer":
		return arg, nil
	}
	return ctx.Registry.fieldRef(arg), nil
}

func (h *groupbyHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	// After an aggregation the groupby aggregates the rows the pipeline has at this
	// point: an aggregating stage's groups, or a projection of them (an assignment,
	// a transform, a model join), whose carried columns are no group keys here.
	cur := ctx.Plan.CurrentStage()
	multiStage := len(cur.Layer.GroupBy) > 0 || (!cur.IsSource && !cur.Aggregates && len(cur.Layer.Selects) > 0)
	if multiStage {
		if err := pushAggregationStage(ctx); err != nil {
			return fmt.Errorf("groupby (stage finalize): %w", err)
		}
	}
	source := ctx.Plan.CurrentStage()
	computedFields := ctx.Registry.AllComputed()

	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	if n := b.Int("limit", 0); n > 0 {
		source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	}
	specs, err := groupbyFunctions(b)
	if err != nil {
		return err
	}
	if err := projectAggregates(ctx, source, specs, computedFields); err != nil {
		return err
	}
	hasFunction := len(specs) > 0

	// distinct= counts the distinct values of the last key within the groups of the others,
	// as timechart's groupby(f, distinct=true) does per bucket, so that key is not grouped by.
	keys := b.Flat("fields")
	var counted *Argument
	if (b.Flag("distinct", false) || b.Flag("unique", false)) && len(keys) > 0 {
		counted, keys = &keys[len(keys)-1], keys[:len(keys)-1]
	}
	for _, key := range keys {
		if err := groupByKey(ctx, source, key, computedFields, multiStage); err != nil {
			return err
		}
	}
	if counted != nil {
		ref, err := countedKeyRef(*counted, computedFields, multiStage, ctx)
		if err != nil {
			return err
		}
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("COUNT(DISTINCT %s)", ref), Alias: "_count",
		})
		ctx.Plan.IsAggregated = true
		hasFunction = true // the distinct count is _count; no default COUNT(*) beside it
	}

	// Without function= the groupby counts: groupby(field) is groupby(field,
	// function=count()). _count is registered as a numeric aggregate of this stage
	// even when an earlier stage carried one, since this COUNT(*) redefines it.
	if !hasFunction {
		source.Layer.addColumn(aggSelect(countAll, "_count"))
		ctx.Plan.IsAggregated = true
		ctx.Registry.Register("_count", FieldKindAggregate, "_count", ctx.CmdIndex)
		ctx.Registry.SetResolveExpr("_count", "_count")
	}
	// aggregationOutputs drives the chained-aggregation wrapper (sum/avg on a prior
	// aggregation output). Only the first, non-multi-stage groupby seeds it so that
	// chaining still operates on the stage SELECT rather than the wrapper path.
	if !multiStage {
		ctx.Plan.aggregationOutputs["_count"] = "COUNT(*)"
	}
	return nil
}

// keyName is the column a grouping key read from arg is output as: the field it
// names, or the name an expression key is derived as.
func keyName(arg Argument, ref string) string {
	if name := arg.FieldName(); name != "" {
		return name
	}
	if alias, err := ArgAlias(arg); err == nil {
		return alias
	}
	return ref
}

// projectAggregates adds aggregate specs to the stage and registers each new
// output as an aggregate, so assembleGroupBySelects can identify them.
func projectAggregates(ctx *CommandContext, source *QueryStage, specs []Argument, computedFields map[string]bool) error {
	before := len(source.Layer.Selects)
	if err := applyAggSpecs(specs, &source.Layer.Selects, computedFields, ctx); err != nil {
		return err
	}
	for _, sel := range source.Layer.Selects[before:] {
		ctx.Registry.Register(sel.Name(), FieldKindAggregate, sel.Name(), ctx.CmdIndex)
		ctx.Plan.aggregationOutputs[sel.Name()] = sel.Name()
	}
	return nil
}

// groupByKey groups the stage by one key, a field or an expression, and records
// the alias it is addressable by after the aggregation.
func groupByKey(ctx *CommandContext, source *QueryStage, key Argument, computedFields map[string]bool, multiStage bool) error {
	arg := key.Value()
	// A model column the previous aggregation did not output is gone: the entry
	// was dropped by ScopeToOutputs, so ask the lookup's outputs directly.
	if e := ctx.Registry.Get(arg); e == nil && multiStage && contains(ctx.Plan.lookup.Outputs, arg) {
		return fmt.Errorf("groupby(): model column %q was not carried out of the previous aggregation; group by it there or aggregate it first", arg)
	}
	var fieldRef, keyAlias string
	if key.FieldName() == "" {
		// An expression key is projected under a derived name and grouped by that
		// alias, so the rest of the pipeline sees an ordinary column.
		sql, err := ResolveArg(key, ctx.Registry)
		if err != nil {
			return fmt.Errorf("groupby(): %w", err)
		}
		alias, err := ArgAlias(key)
		if err != nil {
			return fmt.Errorf("groupby(): %w", err)
		}
		if prior, clash := exprAliasClash(source, alias, sql); clash {
			return fmt.Errorf("groupby(): %s and %s both produce the column %s; name one with as=", arg, prior, alias)
		}
		source.Layer.UpsertSelect(SelectExpr{Expr: sql, Alias: alias})
		ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
		ctx.Registry.SetResolveExpr(alias, alias)
		fieldRef, keyAlias = alias, alias
	} else {
		safe, err := sanitizeIdentifier(arg)
		if err != nil {
			return fmt.Errorf("groupby(): %w", err)
		}
		keyAlias = safe
		switch {
		case arg == "timestamp" || arg == normLogColumn || arg == "log_id" || arg == "normalizer":
			fieldRef = arg
		case computedFields[arg] || multiStage:
			// Computed fields, and in multi-stage every field, are read by alias.
			fieldRef = arg
		default:
			fieldRef = ctx.Registry.fieldRef(arg)
		}
	}
	source.Layer.groupBy(fieldRef, unquoteIdent(keyAlias))
	// The key survives the aggregation, addressable by the alias it is projected
	// under. Recorded here, where the name the query used is still known: by
	// classify time the plan holds only the alias or the expression depending on
	// what else ran, which is not enough to recover the name.
	ctx.Registry.SetGroupKeyAlias(arg, keyAlias, ctx.CmdIndex)
	return nil
}

func init() {
	registerAggregatingCommand(&countHandler{}, "count")
	registerAggregatingCommand(&simpleAggHandler{name: "sum"}, "sum")
	registerAggregatingCommand(&simpleAggHandler{name: "avg"}, "avg")
	registerAggregatingCommand(&simpleAggHandler{name: "max"}, "max")
	registerAggregatingCommand(&simpleAggHandler{name: "min"}, "min")
	registerAggregatingCommand(&simpleAggHandler{name: "median"}, "median")
	registerAggregatingCommand(&simpleAggHandler{name: "percentile"}, "percentile")
	registerAggregatingCommand(&simpleAggHandler{name: "stddev"}, "stddev", "stdDev")
	registerAggregatingCommand(&simpleAggHandler{name: "skewness"}, "skewness", "skew")
	registerAggregatingCommand(&simpleAggHandler{name: "kurtosis"}, "kurtosis", "kurt")
	registerAggregatingCommand(&frequencyHandler{}, "frequency")
	registerAggregatingCommand(&iqrHandler{}, "iqr")
	registerAggregatingCommand(&headtailHandler{}, "headtail")
	registerAggregatingCommand(&selectEdgeHandler{name: "selectFirst", out: "first"}, "selectfirst")
	registerAggregatingCommand(&selectEdgeHandler{name: "selectLast", out: "last"}, "selectlast")
	registerAggregatingCommand(&topHandler{}, "top")
	registerAggregatingCommand(&multiHandler{}, "multi")
	registerAggregatingCommand(&madHandler{}, "mad")
	registerAggregatingCommand(&groupbyHandler{}, "groupby")
}

func init() {
	registerSpec(&CommandSpec{Name: "count", Params: []ParamSpec{
		field("field"), namedLit("unique"), namedLit("distinct"), as(),
	}})
	for _, n := range []string{"sum", "avg", "max", "min", "median", "percentile", "skewness", "kurtosis", "mad", "iqr", "stddev"} {
		registerSpec(&CommandSpec{Name: n, Params: []ParamSpec{reqField("field"), as()}}, n)
	}
	registerSpec(commandSpecs["skewness"], "skew")
	registerSpec(commandSpecs["kurtosis"], "kurt")
	registerSpec(commandSpecs["stddev"], "stdDev")

	registerSpec(&CommandSpec{Name: "frequency", Params: []ParamSpec{reqField("field")}})
	// headTail(src_ip, 90) and headTail(src_ip, threshold=90) are both written.
	registerSpec(&CommandSpec{Name: "headtail", Params: []ParamSpec{
		reqField("field"),
		ParamSpec{Name: "threshold", Kind: ParamLiteral, Positional: true},
	}})
	registerSpec(&CommandSpec{Name: "selectfirst", Params: []ParamSpec{reqField("field"), as()}})
	registerSpec(&CommandSpec{Name: "selectlast", Params: []ParamSpec{reqField("field"), as()}})
	registerSpec(&CommandSpec{Name: "top", Params: []ParamSpec{
		field("field"), namedLit("percent"), namedLit("limit"), as(),
	}})
	registerSpec(&CommandSpec{Name: "multi", FreeForm: true, Params: []ParamSpec{
		ParamSpec{Name: "functions", Kind: ParamAggSpec, Positional: true, Variadic: true},
	}})
	registerSpec(&CommandSpec{Name: "groupby", Params: []ParamSpec{
		fields("fields"), namedAgg("function"), namedLit("limit"), namedLit("distinct"), namedLit("unique"),
	}})
}

// applyAggSpecs renders aggregate specs into a stage's SELECT list, expanding a
// multi(...) wrapper into its members.
func applyAggSpecs(args []Argument, selects *[]SelectExpr, computedFields map[string]bool, ctx *CommandContext) error {
	for _, a := range args {
		agg, ok := asAggSpec(a)
		if !ok {
			return fmt.Errorf("unknown aggregation function: %s (did you mean multi([...])?)", a)
		}
		specs := []*AggSpec{agg}
		where := ""
		if strings.EqualFold(agg.Name, "multi") {
			specs, where = nil, " in multi()"
			for _, member := range aggFields(agg) {
				inner, ok := asAggSpec(member)
				if !ok {
					return fmt.Errorf("unknown aggregation function in multi(): %s", member)
				}
				specs = append(specs, inner)
			}
		}
		for _, spec := range specs {
			ok, err := processAggSpec(spec, selects, computedFields, ctx.Registry)
			if err != nil {
				return err
			}
			if !ok {
				// Reject unknown aggregation functions instead of silently degrading
				// to COUNT(*), which would discard the spec and return a
				// plausible-but-wrong result.
				return fmt.Errorf("unknown aggregation function%s: %s()", where, spec.Name)
			}
			ctx.Plan.IsAggregated = true
		}
	}
	return nil
}

// stageOutputAliases is the set of column names a stage projects, as a later
// stage addresses them. The backticks a dotted name carries in SQL are not part
// of the name: keying them in meant `a.b` was scoped out of the registry and the
// next stage re-resolved it as a JSON path the stage does not have.
func stageOutputAliases(stage *QueryStage) map[string]bool {
	out := make(map[string]bool)
	for _, name := range columnNames(stage.Layer.Selects) {
		out[name] = true
	}
	return out
}

// groupbyFunctions reads groupby(function=): one aggregate, multi(...), or a
// list of aggregates, which means the same as multi(). A list used to bind to
// nothing, so its aggregates were silently dropped.
func groupbyFunctions(b *Bound) ([]Argument, error) {
	a, ok := b.First("function")
	if !ok {
		return nil, nil
	}
	if a.Agg != nil {
		return []Argument{{Kind: ArgAggSpec, Agg: a.Agg}}, nil
	}
	items := []Argument{a}
	if a.Kind == ArgList {
		items = a.List
	}
	var specs []Argument
	for _, item := range items {
		spec, ok := asAggSpec(item)
		if !ok {
			return nil, fmt.Errorf("groupby(): function= takes aggregates such as count() or sum(bytes), got %s", item)
		}
		specs = append(specs, Argument{Kind: ArgAggSpec, Agg: spec})
	}
	return specs, nil
}
