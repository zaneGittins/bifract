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

	// Bare count() after groupby: push second stage to count the number of groups.
	// count(field) or count(field, unique=true) still adds to the groupby stage.
	if ctx.Plan.HasGroupBy && len(source.Layer.GroupBy) > 0 && !hasField && !unique {
		if err := assembleGroupBySelects(ctx, source, nil); err != nil {
			return fmt.Errorf("count (stage finalize): %w", err)
		}
		prevOutputs := make(map[string]bool)
		for _, sel := range source.Layer.Selects {
			alias := extractFieldAlias(sel.String())
			if alias != "" {
				prevOutputs[alias] = true
			}
		}
		ctx.Plan.PushStage()
		ctx.Plan.IsAggregated = false
		ctx.Plan.aggregationOutputs = make(map[string]string)
		ctx.Plan.outerAggregations = nil
		ctx.Plan.outerAggFieldOrder = nil
		ctx.Registry.ScopeToOutputs(prevOutputs)
		ctx.Plan.HasGroupBy = false

		newSource := ctx.Plan.CurrentStage()
		newSource.Layer.Selects = append(newSource.Layer.Selects, SelectExpr{Expr: "COUNT(*) AS _count"})
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
		sqlExpr := fmt.Sprintf("count(%s)", ref)
		if unique {
			sqlExpr = fmt.Sprintf("uniqExact(%s)", ref)
		}
		expr := fmt.Sprintf("%s AS _count", sqlExpr)
		if !contains(selectExprStrings(source.Layer.Selects), expr) {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: expr})
		}
		ctx.Plan.IsAggregated = true
		ctx.Plan.aggregationOutputs["_count"] = sqlExpr
		ctx.Registry.SetResolveExpr("_count", "_count")
		return nil
	}

	// Bare count() without groupby
	if !contains(selectExprStrings(source.Layer.Selects), "COUNT(*) AS _count") {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*) AS _count"})
	}
	ctx.Plan.IsAggregated = true
	ctx.Plan.aggregationOutputs["_count"] = "COUNT(*)"
	ctx.Registry.SetResolveExpr("_count", "_count")

	// When count() is used with case statements, GROUP BY the case-produced fields
	for _, cmd2 := range ctx.Pipeline.Commands {
		if cmd2.Name == "case" && cmd2.Block != "" {
			caseExpr := cmd2.Block
			compiled, err := compileCase(caseExpr, ctx.Registry, ctx.Opts)
			if err != nil {
				return err
			}
			if len(compiled.assignments) > 0 {
				for _, assignment := range compiled.assignments {
					if !contains(source.Layer.GroupBy, assignment.Field) {
						source.Layer.GroupBy = append(source.Layer.GroupBy, assignment.Field)
					}
				}
			} else {
				outputField := "case_result"
				if !contains(source.Layer.GroupBy, outputField) {
					source.Layer.GroupBy = append(source.Layer.GroupBy, outputField)
				}
			}
		}
	}
	return nil
}

// simpleAggHandler handles sum, avg, max, min, median aggregation commands.
type simpleAggHandler struct {
	name   string // "sum", "avg", "max", "min", "median"
	alias  string // "_sum", "_avg", "_max", "_min", "_median"
	chFunc string // "sum", "avg", "max", "min", "median"
}

func (h *simpleAggHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register(h.alias, FieldKindAggregate, h.alias, ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *simpleAggHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	field := arg.FieldName()
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		expr := fmt.Sprintf("%s(toFloat64(%s)) AS %s", h.chFunc, field, h.alias)
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations, expr)
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, h.alias)
		ctx.Plan.aggregationOutputs[h.alias] = fmt.Sprintf("%s(toFloat64(%s))", h.chFunc, field)
		ctx.Registry.SetResolveExpr(h.alias, h.alias)
	} else if (h.name == "max" || h.name == "min") && field == "timestamp" {
		alias := h.name + "_timestamp"
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s(timestamp) AS %s", h.name, alias)})
		ctx.Plan.aggregationOutputs[alias] = fmt.Sprintf("%s(timestamp)", h.name)
		ctx.Registry.SetResolveExpr(alias, alias)
	} else {
		cast, err := aggOperandNumeric(arg, ctx.Registry)
		if err != nil {
			return fmt.Errorf("%s(): %w", h.name, err)
		}
		sqlExpr := fmt.Sprintf("%s(%s)", h.chFunc, cast)
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", sqlExpr, h.alias)})
		ctx.Plan.aggregationOutputs[h.alias] = sqlExpr
		ctx.Registry.SetResolveExpr(h.alias, h.alias)
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// percentileHandler handles percentile(field)
type percentileHandler struct{}

func (h *percentileHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register("_percentile", FieldKindAggregate, "_percentile", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *percentileHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	field := arg.FieldName()
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		expr := fmt.Sprintf("quantiles(0.5, 0.75, 0.99)(toFloat64(%s)) AS _percentile", field)
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations, expr)
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, "_percentile")
		ctx.Plan.aggregationOutputs["_percentile"] = fmt.Sprintf("quantiles(0.5, 0.75, 0.99)(toFloat64(%s))", field)
		ctx.Registry.SetResolveExpr("_percentile", "_percentile")
	} else {
		cast, err := aggOperandNumeric(arg, ctx.Registry)
		if err != nil {
			return err
		}
		alias, err := aggOutputAlias("percentile_", arg)
		if err != nil {
			return fmt.Errorf("percentile(): %w", err)
		}
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("quantiles(0.5, 0.75, 0.99)(%s) AS %s", cast, alias),
		})
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// stddevHandler handles stddev/stdDev(field)
type stddevHandler struct{}

func (h *stddevHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register("_stddev", FieldKindAggregate, "_stddev", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *stddevHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	field := arg.FieldName()
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		expr := fmt.Sprintf("stddevPop(toFloat64(%s)) AS _stddev", field)
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations, expr)
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, "_stddev")
		ctx.Plan.aggregationOutputs["_stddev"] = fmt.Sprintf("stddevPop(toFloat64(%s))", field)
		ctx.Registry.SetResolveExpr("_stddev", "_stddev")
	} else {
		cast, err := aggOperandNumeric(arg, ctx.Registry)
		if err != nil {
			return err
		}
		alias, err := aggOutputAlias("stddev_", arg)
		if err != nil {
			return fmt.Errorf("stdDev(): %w", err)
		}
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("stddevPop(%s) AS %s", cast, alias),
		})
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// skewnessHandler handles skewness/skew(field)
type skewnessHandler struct{}

func (h *skewnessHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register("_skewness", FieldKindAggregate, "_skewness", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *skewnessHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	field := arg.FieldName()
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		expr := fmt.Sprintf("skewPop(toFloat64(%s)) AS _skewness", field)
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations, expr)
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, "_skewness")
		ctx.Plan.aggregationOutputs["_skewness"] = fmt.Sprintf("skewPop(toFloat64(%s))", field)
		ctx.Registry.SetResolveExpr("_skewness", "_skewness")
	} else {
		cast, err := aggOperandNumeric(arg, ctx.Registry)
		if err != nil {
			return err
		}
		sqlExpr := fmt.Sprintf("skewPop(%s)", cast)
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS _skewness", sqlExpr)})
		ctx.Plan.aggregationOutputs["_skewness"] = sqlExpr
		ctx.Registry.SetResolveExpr("_skewness", "_skewness")
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// kurtosisHandler handles kurtosis/kurt(field)
type kurtosisHandler struct{}

func (h *kurtosisHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register("_kurtosis", FieldKindAggregate, "_kurtosis", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *kurtosisHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	field := arg.FieldName()
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		expr := fmt.Sprintf("kurtPop(toFloat64(%s)) AS _kurtosis", field)
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations, expr)
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, "_kurtosis")
		ctx.Plan.aggregationOutputs["_kurtosis"] = fmt.Sprintf("kurtPop(toFloat64(%s))", field)
		ctx.Registry.SetResolveExpr("_kurtosis", "_kurtosis")
	} else {
		cast, err := aggOperandNumeric(arg, ctx.Registry)
		if err != nil {
			return err
		}
		sqlExpr := fmt.Sprintf("kurtPop(%s)", cast)
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS _kurtosis", sqlExpr)})
		ctx.Plan.aggregationOutputs["_kurtosis"] = sqlExpr
		ctx.Registry.SetResolveExpr("_kurtosis", "_kurtosis")
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// frequencyHandler handles frequency(field)
type frequencyHandler struct{}

func (h *frequencyHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register("_count", FieldKindAggregate, "count(*)", ctx.CmdIndex)
		ctx.Registry.Register("_percentage", FieldKindAggregate, "_percentage", ctx.CmdIndex)
		ctx.Registry.Register("_cumulative_pct", FieldKindAggregate, "_cumulative_pct", ctx.CmdIndex)
		ctx.Registry.Register("value", FieldKindAggregate, "value", ctx.CmdIndex)
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

	source.Layer.GroupBy = append(source.Layer.GroupBy, fieldRef)
	source.Layer.Selects = append(source.Layer.Selects,
		SelectExpr{Expr: fmt.Sprintf("%s AS value", fieldRef)},
		SelectExpr{Expr: "count(*) AS _count"},
		SelectExpr{Expr: "round(_count * 100.0 / sum(_count) OVER (), 2) AS _percentage"},
		SelectExpr{Expr: "round(sum(_count) OVER (ORDER BY _count DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 / sum(_count) OVER (), 2) AS _cumulative_pct"},
	)
	source.Layer.OrderBy = append(source.Layer.OrderBy, "_count DESC")
	ctx.Plan.aggregationOutputs["_count"] = "count(*)"
	ctx.Plan.aggregationOutputs["_percentage"] = "_percentage"
	ctx.Plan.aggregationOutputs["_cumulative_pct"] = "_cumulative_pct"
	ctx.Registry.SetResolveExpr("_count", "_count")
	ctx.Registry.SetResolveExpr("_percentage", "_percentage")
	ctx.Registry.SetResolveExpr("_cumulative_pct", "_cumulative_pct")
	ctx.Registry.SetResolveExpr("value", "value")
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
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	field := arg.FieldName()
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations,
			fmt.Sprintf("quantile(0.25)(toFloat64(%s)) AS _q1", field),
			fmt.Sprintf("quantile(0.75)(toFloat64(%s)) AS _q3", field),
			fmt.Sprintf("quantile(0.75)(toFloat64(%s)) - quantile(0.25)(toFloat64(%s)) AS _iqr", field, field))
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, "_q1", "_q3", "_iqr")
		ctx.Plan.aggregationOutputs["_q1"] = fmt.Sprintf("quantile(0.25)(toFloat64(%s))", field)
		ctx.Plan.aggregationOutputs["_q3"] = fmt.Sprintf("quantile(0.75)(toFloat64(%s))", field)
		ctx.Plan.aggregationOutputs["_iqr"] = "_iqr"
	} else {
		cast, err := aggOperandNumeric(arg, ctx.Registry)
		if err != nil {
			return err
		}
		source.Layer.Selects = append(source.Layer.Selects,
			SelectExpr{Expr: fmt.Sprintf("quantile(0.25)(%s) AS _q1", cast)},
			SelectExpr{Expr: fmt.Sprintf("quantile(0.75)(%s) AS _q3", cast)},
			SelectExpr{Expr: fmt.Sprintf("quantile(0.75)(%s) - quantile(0.25)(%s) AS _iqr", cast, cast)},
		)
		ctx.Plan.aggregationOutputs["_q1"] = fmt.Sprintf("quantile(0.25)(%s)", cast)
		ctx.Plan.aggregationOutputs["_q3"] = fmt.Sprintf("quantile(0.75)(%s)", cast)
		ctx.Plan.aggregationOutputs["_iqr"] = "_iqr"
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// headtailHandler handles headTail(field, threshold)
type headtailHandler struct{}

func (h *headtailHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register("_count", FieldKindAggregate, "count(*)", ctx.CmdIndex)
		ctx.Registry.Register("_percentage", FieldKindAggregate, "_percentage", ctx.CmdIndex)
		ctx.Registry.Register("_cumulative_pct", FieldKindAggregate, "_cumulative_pct", ctx.CmdIndex)
		ctx.Registry.Register("_segment", FieldKindAggregate, "_segment", ctx.CmdIndex)
		ctx.Registry.Register("value", FieldKindAggregate, "value", ctx.CmdIndex)
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

	source.Layer.GroupBy = append(source.Layer.GroupBy, fieldRef)
	cumulExpr := "round(sum(_count) OVER (ORDER BY _count DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 100.0 / sum(_count) OVER (), 2)"
	source.Layer.Selects = append(source.Layer.Selects,
		SelectExpr{Expr: fmt.Sprintf("%s AS value", fieldRef)},
		SelectExpr{Expr: "count(*) AS _count"},
		SelectExpr{Expr: "round(_count * 100.0 / sum(_count) OVER (), 2) AS _percentage"},
		SelectExpr{Expr: fmt.Sprintf("%s AS _cumulative_pct", cumulExpr)},
		SelectExpr{Expr: fmt.Sprintf("CASE WHEN %s <= %s THEN 'head' ELSE 'tail' END AS _segment", cumulExpr, threshold)},
	)
	source.Layer.OrderBy = append(source.Layer.OrderBy, "_count DESC")
	ctx.Plan.aggregationOutputs["_count"] = "count(*)"
	ctx.Plan.aggregationOutputs["_percentage"] = "_percentage"
	ctx.Plan.aggregationOutputs["_cumulative_pct"] = "_cumulative_pct"
	ctx.Plan.aggregationOutputs["_segment"] = "_segment"
	ctx.Registry.SetResolveExpr("_count", "_count")
	ctx.Registry.SetResolveExpr("_percentage", "_percentage")
	ctx.Registry.SetResolveExpr("_cumulative_pct", "_cumulative_pct")
	ctx.Registry.SetResolveExpr("_segment", "_segment")
	ctx.Registry.SetResolveExpr("value", "value")
	ctx.Plan.IsAggregated = true
	return nil
}

// selectfirstHandler handles selectFirst(field)
type selectfirstHandler struct{}

func (h *selectfirstHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *selectfirstHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	field := arg.FieldName()
	fieldRef, err := ResolveArg(arg, ctx.Registry)
	if err != nil {
		return fmt.Errorf("selectFirst(): %w", err)
	}
	alias, err := aggOutputAlias("first_", arg)
	if err != nil {
		return fmt.Errorf("selectFirst(): %w", err)
	}
	source := ctx.Plan.CurrentStage()
	if field == "timestamp" {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "min(timestamp) AS first_timestamp"})
	} else {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("argMin(%s, timestamp) AS %s", fieldRef, alias),
		})
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// selectlastHandler handles selectLast(field)
type selectlastHandler struct{}

func (h *selectlastHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *selectlastHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	field := arg.FieldName()
	fieldRef, err := ResolveArg(arg, ctx.Registry)
	if err != nil {
		return fmt.Errorf("selectLast(): %w", err)
	}
	alias, err := aggOutputAlias("last_", arg)
	if err != nil {
		return fmt.Errorf("selectLast(): %w", err)
	}
	source := ctx.Plan.CurrentStage()
	if field == "timestamp" {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "max(timestamp) AS last_timestamp"})
	} else {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("argMax(%s, timestamp) AS %s", fieldRef, alias),
		})
	}
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
	alias := b.Str("as", "")
	if a, ok := b.First("field"); ok {
		if alias == "" {
			if alias, err = aggOutputAlias("top_", a); err != nil {
				return nil
			}
		}
		ctx.Registry.Register(alias, FieldKindAggregate, alias, ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
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
	alias := b.Str("as", "")
	if alias == "" {
		if alias, err = aggOutputAlias("top_", arg); err != nil {
			return fmt.Errorf("top(): %w", err)
		}
	} else if _, err := sanitizeIdentifier(alias); err != nil {
		return fmt.Errorf("top(): %w", err)
	}
	fieldRef, err := ResolveArg(arg, ctx.Registry)
	if err != nil {
		return fmt.Errorf("top(): %w", err)
	}
	source := ctx.Plan.CurrentStage()
	if showPercent {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("arrayMap(x -> (x.1, round(x.2 * 100 / count(*), 2)), topKWeightedWithCount(%d)(%s, 1)) AS %s", topN, fieldRef, alias),
		})
	} else {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("topK(%d)(%s) AS %s", topN, fieldRef, alias),
		})
	}
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
	source := ctx.Plan.CurrentStage()
	prevAliases := make(map[string]bool)
	for _, sel := range source.Layer.Selects {
		prevAliases[extractFieldAlias(sel.String())] = true
	}

	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	selectFields := selectExprStrings(source.Layer.Selects)
	computedFields := ctx.Registry.AllComputed()
	if err := applyAggSpecs(b.Flat("functions"), &selectFields, computedFields, ctx); err != nil {
		return err
	}
	// Rebuild selects from the string slice
	source.Layer.Selects = nil
	for _, sf := range selectFields {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: sf})
	}
	// Register new aggregation outputs so assembleGroupBySelects can identify them
	for _, sel := range source.Layer.Selects {
		alias := extractFieldAlias(sel.String())
		if alias != "" && !prevAliases[alias] {
			ctx.Registry.Register(alias, FieldKindAggregate, alias, ctx.CmdIndex)
			ctx.Plan.aggregationOutputs[alias] = alias
		}
	}
	return nil
}

// madHandler handles mad(field) - median absolute deviation
type madHandler struct{}

func (h *madHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register("_median", FieldKindAggregate, "_median", ctx.CmdIndex)
		ctx.Registry.Register("_mad", FieldKindAggregate, "_mad", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *madHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	field := arg.FieldName()
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		numericExpr := fmt.Sprintf("toFloat64(%s)", field)
		ctx.Plan.MADWindowExpr = numericExpr
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations,
			"any(_median_val) AS _median",
			fmt.Sprintf("median(abs(%s - _median_val)) AS _mad", numericExpr))
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, "_median", "_mad")
		ctx.Registry.SetResolveExpr("_median", "any(_median_val)")
		ctx.Registry.SetResolveExpr("_mad", fmt.Sprintf("median(abs(%s - _median_val))", numericExpr))
		ctx.Plan.aggregationOutputs["_median"] = "any(_median_val)"
		ctx.Plan.aggregationOutputs["_mad"] = fmt.Sprintf("median(abs(%s - _median_val))", numericExpr)
	} else {
		fieldRef, err := ResolveArg(arg, ctx.Registry)
		if err != nil {
			return fmt.Errorf("mad(): %w", err)
		}
		numericExpr := fmt.Sprintf("toFloat64OrNull(%s)", fieldRef)
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS _mad_val", numericExpr)})
		ctx.Plan.MADWindowExpr = "_mad_val"
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations,
			"any(_median_val) AS _median",
			"median(abs(_mad_val - _median_val)) AS _mad")
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, "_median", "_mad")
		ctx.Registry.SetResolveExpr("_median", "any(_median_val)")
		ctx.Registry.SetResolveExpr("_mad", "median(abs(_mad_val - _median_val))")
		ctx.Plan.aggregationOutputs["_median"] = "any(_median_val)"
		ctx.Plan.aggregationOutputs["_mad"] = "median(abs(_mad_val - _median_val))"
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// bucketHandler handles bucket(span=1h, function=count())
type bucketHandler struct{}

func (h *bucketHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) >= 2 {
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *bucketHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	span := b.Str("span", "")
	spec := b.Agg("function")
	if span == "" || spec == nil {
		return nil
	}
	source := ctx.Plan.CurrentStage()

	n, unit := parseBucketSpan(span)
	bucketExpr := getBucketExpression(n, unit, bucketTimezone(ctx))
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS time_bucket", bucketExpr)})

	switch name := strings.ToLower(spec.Name); name {
	case "count":
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*) AS bucket_count"})
		ctx.Plan.IsAggregated = true
	case "sum":
		o := readAggOptions(spec)
		cast, err := aggOperandNumeric(o.operand, ctx.Registry)
		if err != nil {
			return fmt.Errorf("bucket(): %w", err)
		}
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("sum(%s) AS bucket_sum", cast),
		})
		ctx.Plan.IsAggregated = true
	default:
		return fmt.Errorf("bucket(): function= accepts count() or sum(), got %s()", spec.Name)
	}
	source.Layer.GroupBy = append(source.Layer.GroupBy, bucketExpr)
	return nil
}

// exprAliasClash reports whether the stage already projects this alias from a
// different expression, which UpsertSelect would silently replace.
func exprAliasClash(stage *QueryStage, alias, sql string) (string, bool) {
	want := sql + " AS " + alias
	for _, sel := range stage.Layer.Selects {
		s := sel.String()
		if strings.Trim(extractFieldAlias(s), "`") != alias || s == want {
			continue
		}
		// A select with no " AS " is the column carried forward from an earlier
		// stage, not a competing expression. Slicing on the missing separator
		// panicked the translator on query text a user controls.
		idx := strings.LastIndex(s, " AS ")
		if idx < 0 {
			continue
		}
		return s[:idx], true
	}
	return "", false
}

// groupbyHandler handles groupby(field1, field2, ...)
type groupbyHandler struct{}

func (h *groupbyHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.HasGroupBy = true
	ctx.Plan.GroupByCount++
	// groupby always produces _count (explicit or default COUNT(*))
	ctx.Registry.Register("_count", FieldKindAggregate, "COUNT(*)", ctx.CmdIndex)
	return nil
}

func (h *groupbyHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	isMultiStage := len(ctx.Plan.CurrentStage().Layer.GroupBy) > 0

	if isMultiStage {
		// Finalize the previous stage's SELECT before pushing a new stage.
		prevStage := ctx.Plan.CurrentStage()
		if err := assembleGroupBySelects(ctx, prevStage, nil); err != nil {
			return fmt.Errorf("groupby (stage finalize): %w", err)
		}
		// Record what the previous stage outputs so the new stage can reference them.
		prevOutputs := make(map[string]bool)
		for _, sel := range prevStage.Layer.Selects {
			alias := extractFieldAlias(sel.String())
			if alias != "" {
				prevOutputs[alias] = true
			}
		}

		ctx.Plan.PushStage()

		// Reset aggregation state for the new stage.
		ctx.Plan.IsAggregated = false
		ctx.Plan.aggregationOutputs = make(map[string]string)
		ctx.Plan.outerAggregations = nil
		ctx.Plan.outerAggFieldOrder = nil

		// Snapshot the registry: clear non-output fields so the new stage
		// only sees what the previous stage produces.
		ctx.Registry.ScopeToOutputs(prevOutputs)
	}

	source := ctx.Plan.CurrentStage()
	computedFields := ctx.Registry.AllComputed()
	hasFunction := false

	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	if n := b.Int("limit", 0); n > 0 {
		source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	}

	if spec := b.Agg("function"); spec != nil {
		hasFunction = true
		prevAliases := make(map[string]bool)
		for _, sel := range source.Layer.Selects {
			prevAliases[extractFieldAlias(sel.String())] = true
		}
		selectFields := selectExprStrings(source.Layer.Selects)
		if err := applyAggSpecs([]Argument{{Kind: ArgAggSpec, Agg: spec}}, &selectFields, computedFields, ctx); err != nil {
			return err
		}
		source.Layer.Selects = nil
		for _, sf := range selectFields {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: sf})
		}
		for _, sel := range source.Layer.Selects {
			alias := extractFieldAlias(sel.String())
			if alias != "" && !prevAliases[alias] {
				ctx.Registry.Register(alias, FieldKindAggregate, alias, ctx.CmdIndex)
				ctx.Plan.aggregationOutputs[alias] = alias
			}
		}
	}

	for _, keyArg := range b.Flat("fields") {
		arg := keyArg.Value()
		// A join-output column only exists per-row under the scan-level model
		// join, which feeds the FIRST aggregation stage only; grouping it
		// anywhere else would reference a column that scope does not have
		// (ClickHouse code 47). In a later stage the entry was dropped by
		// ScopeToOutputs (a carried group key re-registers as Base), so ask
		// ModelLookupFields directly.
		if e := ctx.Registry.Get(arg); e != nil && e.Kind == FieldKindJoined && !ctx.Plan.ModelLookupAtScan {
			return fmt.Errorf("groupby(): %q is produced by a join and is not available here; model_lookup() outputs can be grouped by placing model_lookup() before the aggregation", arg)
		} else if e == nil && isMultiStage && contains(ctx.Plan.ModelLookupOutputs, arg) {
			return fmt.Errorf("groupby(): model column %q was not carried out of the previous aggregation; group by it there or aggregate it first", arg)
		}
		var fieldRef string
		if keyArg.FieldName() == "" {
			// An expression key is projected under a derived name and grouped by
			// that alias, so the rest of the pipeline sees an ordinary column.
			sql, err := ResolveArg(keyArg, ctx.Registry)
			if err != nil {
				return fmt.Errorf("groupby(): %w", err)
			}
			alias, err := ArgAlias(keyArg)
			if err != nil {
				return fmt.Errorf("groupby(): %w", err)
			}
			if prior, clash := exprAliasClash(source, alias, sql); clash {
				return fmt.Errorf("groupby(): %s and %s both produce the column %s; name one with as=", arg, prior, alias)
			}
			source.Layer.UpsertSelect(SelectExpr{Expr: fmt.Sprintf("%s AS %s", sql, alias)})
			ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
			ctx.Registry.SetResolveExpr(alias, alias)
			fieldRef = alias
		} else if computedFields[arg] {
			fieldRef = arg
		} else {
			switch arg {
			case "timestamp", normLogColumn, "log_id", "normalizer":
				fieldRef = arg
			default:
				if isMultiStage {
					// In multi-stage, fields reference previous stage output by alias
					fieldRef = arg
				} else {
					fieldRef = ctx.Registry.fieldRef(arg)
				}
			}
		}
		if !contains(source.Layer.GroupBy, fieldRef) {
			source.Layer.GroupBy = append(source.Layer.GroupBy, fieldRef)
		}
		// The key survives the aggregation, addressable by the alias it is
		// projected under. Recorded here, where the name the query used is still
		// known: by classify time the plan holds only the alias or the expression
		// depending on what else ran, which is not enough to recover the name.
		ctx.Registry.SetGroupKeyAlias(arg, sanitizedAlias(arg), ctx.CmdIndex)
	}

	// distinct= counts distinct values of the last group key, so it is applied
	// after the keys are known.
	if b.Flag("distinct", false) || b.Flag("unique", false) {
		if len(source.Layer.GroupBy) > 0 {
			lastField := source.Layer.GroupBy[len(source.Layer.GroupBy)-1]
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
				Expr: fmt.Sprintf("COUNT(DISTINCT %s) AS _count", groupableCast(lastField)),
			})
			ctx.Plan.IsAggregated = true
		}
	}

	// Default aggregation: when no function= was specified, add COUNT(*)
	// as the default aggregation function. This makes groupby(field)
	// equivalent to groupby(field, function=count()).
	if !hasFunction {
		if !contains(selectExprStrings(source.Layer.Selects), "COUNT(*) AS _count") {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*) AS _count"})
		}
		ctx.Plan.IsAggregated = true
	}

	// Register _count's kind and bare resolve expression so post-aggregation
	// filters (HAVING _count > N) treat it as a numeric aggregate of THIS stage,
	// not a JSON string. This must happen even in multi-stage pipelines, where
	// ScopeToOutputs reset the carried _count: the new groupby produces a fresh
	// COUNT(*) that redefines it.
	if !hasFunction {
		ctx.Registry.Register("_count", FieldKindAggregate, "_count", ctx.CmdIndex)
		ctx.Registry.SetResolveExpr("_count", "_count")
	}
	// aggregationOutputs drives the chained-aggregation wrapper (sum/avg on a prior
	// aggregation output). Only the first, non-multi-stage groupby seeds it so that
	// chaining still operates on the stage SELECT rather than the wrapper path.
	if !isMultiStage {
		ctx.Plan.aggregationOutputs["_count"] = "COUNT(*)"
	}
	return nil
}

func init() {
	registerAggregatingCommand(&countHandler{}, "count")
	registerAggregatingCommand(&simpleAggHandler{name: "sum", alias: "_sum", chFunc: "sum"}, "sum")
	registerAggregatingCommand(&simpleAggHandler{name: "avg", alias: "_avg", chFunc: "avg"}, "avg")
	registerAggregatingCommand(&simpleAggHandler{name: "max", alias: "_max", chFunc: "max"}, "max")
	registerAggregatingCommand(&simpleAggHandler{name: "min", alias: "_min", chFunc: "min"}, "min")
	registerAggregatingCommand(&simpleAggHandler{name: "median", alias: "_median", chFunc: "median"}, "median")
	registerAggregatingCommand(&percentileHandler{}, "percentile")
	registerAggregatingCommand(&stddevHandler{}, "stddev", "stdDev")
	registerAggregatingCommand(&skewnessHandler{}, "skewness", "skew")
	registerAggregatingCommand(&kurtosisHandler{}, "kurtosis", "kurt")
	registerAggregatingCommand(&frequencyHandler{}, "frequency")
	registerAggregatingCommand(&iqrHandler{}, "iqr")
	registerCommand(&headtailHandler{}, "headtail")
	registerAggregatingCommand(&selectfirstHandler{}, "selectfirst")
	registerAggregatingCommand(&selectlastHandler{}, "selectlast")
	registerAggregatingCommand(&topHandler{}, "top")
	registerAggregatingCommand(&multiHandler{}, "multi")
	registerAggregatingCommand(&madHandler{}, "mad")
	registerAggregatingCommand(&bucketHandler{}, "bucket")
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
	// bucket(1h, count()) and bucket(span=1h, function=count()) are both written.
	registerSpec(&CommandSpec{Name: "bucket", Params: []ParamSpec{
		reqLit("span"),
		ParamSpec{Name: "function", Kind: ParamAggSpec, Positional: true},
	}})
	registerSpec(&CommandSpec{Name: "groupby", Params: []ParamSpec{
		fields("fields"), namedAgg("function"), namedLit("limit"), namedLit("distinct"), namedLit("unique"),
	}})
}

// applyAggSpecs renders aggregate specs into a stage's SELECT list, expanding a
// multi(...) wrapper into its members.
func applyAggSpecs(args []Argument, selectFields *[]string, computedFields map[string]bool, ctx *CommandContext) error {
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
			ok, err := processAggSpec(spec, selectFields, computedFields, ctx.Registry)
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
