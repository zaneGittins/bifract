package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// countHandler handles count()
type countHandler struct{}

func (h *countHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Registry.Register("_count", FieldKindAggregate, "COUNT(*)", ctx.CmdIndex)
	ctx.Plan.IsAggregated = true
	return nil
}

func (h *countHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	source := ctx.Plan.CurrentStage()
	if !contains(selectExprStrings(source.Layer.Selects), "COUNT(*) AS _count") {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*) AS _count"})
	}
	ctx.Plan.IsAggregated = true
	ctx.Plan.aggregationOutputs["_count"] = "COUNT(*)"
	ctx.Registry.SetResolveExpr("_count", "_count")

	// When count() is used with case statements, GROUP BY the case-produced fields
	for _, cmd2 := range ctx.Pipeline.Commands {
		if cmd2.Name == "case" && len(cmd2.Arguments) > 0 {
			caseExpr := cmd2.Arguments[0]
			_, _, caseAssignments := parseCaseConditions(caseExpr)
			if len(caseAssignments) > 0 {
				for _, assignment := range caseAssignments {
					if !contains(source.Layer.GroupBy, assignment.Field) {
						source.Layer.GroupBy = append(source.Layer.GroupBy, assignment.Field)
					}
				}
			} else {
				outputField := "case_result"
				if len(cmd2.Arguments) > 1 {
					outputField = cmd2.Arguments[1]
				}
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
	name     string // "sum", "avg", "max", "min", "median"
	alias    string // "_sum", "_avg", "_max", "_min", "_median"
	chFunc   string // "sum", "avg", "max", "min", "median"
}

func (h *simpleAggHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		ctx.Registry.Register(h.alias, FieldKindAggregate, h.alias, ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *simpleAggHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
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
		fieldRef := resolveFieldRef(field, ctx.Registry)
		sqlExpr := fmt.Sprintf("%s(toFloat64OrNull(%s))", h.chFunc, fieldRef)
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
	if len(cmd.Arguments) > 0 {
		ctx.Registry.Register("_percentile", FieldKindAggregate, "_percentile", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *percentileHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		expr := fmt.Sprintf("quantiles(0.5, 0.75, 0.99)(toFloat64(%s)) AS _percentile", field)
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations, expr)
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, "_percentile")
		ctx.Plan.aggregationOutputs["_percentile"] = fmt.Sprintf("quantiles(0.5, 0.75, 0.99)(toFloat64(%s))", field)
		ctx.Registry.SetResolveExpr("_percentile", "_percentile")
	} else {
		fieldRef := resolveFieldRef(field, ctx.Registry)
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("quantiles(0.5, 0.75, 0.99)(toFloat64OrNull(%s)) AS percentile_%s", fieldRef, escapeString(field)),
		})
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// stddevHandler handles stddev/stdDev(field)
type stddevHandler struct{}

func (h *stddevHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		ctx.Registry.Register("_stddev", FieldKindAggregate, "_stddev", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *stddevHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		expr := fmt.Sprintf("stddevPop(toFloat64(%s)) AS _stddev", field)
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations, expr)
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, "_stddev")
		ctx.Plan.aggregationOutputs["_stddev"] = fmt.Sprintf("stddevPop(toFloat64(%s))", field)
		ctx.Registry.SetResolveExpr("_stddev", "_stddev")
	} else {
		fieldRef := resolveFieldRef(field, ctx.Registry)
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("stddevPop(toFloat64OrNull(%s)) AS stddev_%s", fieldRef, escapeString(field)),
		})
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// skewnessHandler handles skewness/skew(field)
type skewnessHandler struct{}

func (h *skewnessHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		ctx.Registry.Register("_skewness", FieldKindAggregate, "_skewness", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *skewnessHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		expr := fmt.Sprintf("skewPop(toFloat64(%s)) AS _skewness", field)
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations, expr)
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, "_skewness")
		ctx.Plan.aggregationOutputs["_skewness"] = fmt.Sprintf("skewPop(toFloat64(%s))", field)
		ctx.Registry.SetResolveExpr("_skewness", "_skewness")
	} else {
		fieldRef := resolveFieldRef(field, ctx.Registry)
		sqlExpr := fmt.Sprintf("skewPop(toFloat64OrNull(%s))", fieldRef)
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
	if len(cmd.Arguments) > 0 {
		ctx.Registry.Register("_kurtosis", FieldKindAggregate, "_kurtosis", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *kurtosisHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		expr := fmt.Sprintf("kurtPop(toFloat64(%s)) AS _kurtosis", field)
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations, expr)
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, "_kurtosis")
		ctx.Plan.aggregationOutputs["_kurtosis"] = fmt.Sprintf("kurtPop(toFloat64(%s))", field)
		ctx.Registry.SetResolveExpr("_kurtosis", "_kurtosis")
	} else {
		fieldRef := resolveFieldRef(field, ctx.Registry)
		sqlExpr := fmt.Sprintf("kurtPop(toFloat64OrNull(%s))", fieldRef)
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
	if len(cmd.Arguments) > 0 {
		ctx.Registry.Register("_count", FieldKindAggregate, "count(*)", ctx.CmdIndex)
		ctx.Registry.Register("_percentage", FieldKindAggregate, "_percentage", ctx.CmdIndex)
		ctx.Registry.Register("_cumulative_pct", FieldKindAggregate, "_cumulative_pct", ctx.CmdIndex)
		ctx.Registry.Register("value", FieldKindAggregate, "value", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *frequencyHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
	fieldRef := resolveFieldRef(field, ctx.Registry)
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
	if len(cmd.Arguments) > 0 {
		ctx.Registry.Register("_q1", FieldKindAggregate, "_q1", ctx.CmdIndex)
		ctx.Registry.Register("_q3", FieldKindAggregate, "_q3", ctx.CmdIndex)
		ctx.Registry.Register("_iqr", FieldKindAggregate, "_iqr", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *iqrHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
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
		fieldRef := resolveFieldRef(field, ctx.Registry)
		source.Layer.Selects = append(source.Layer.Selects,
			SelectExpr{Expr: fmt.Sprintf("quantile(0.25)(toFloat64OrNull(%s)) AS _q1", fieldRef)},
			SelectExpr{Expr: fmt.Sprintf("quantile(0.75)(toFloat64OrNull(%s)) AS _q3", fieldRef)},
			SelectExpr{Expr: fmt.Sprintf("quantile(0.75)(toFloat64OrNull(%s)) - quantile(0.25)(toFloat64OrNull(%s)) AS _iqr", fieldRef, fieldRef)},
		)
		ctx.Plan.aggregationOutputs["_q1"] = fmt.Sprintf("quantile(0.25)(toFloat64OrNull(%s))", fieldRef)
		ctx.Plan.aggregationOutputs["_q3"] = fmt.Sprintf("quantile(0.75)(toFloat64OrNull(%s))", fieldRef)
		ctx.Plan.aggregationOutputs["_iqr"] = "_iqr"
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// headtailHandler handles headTail(field, threshold)
type headtailHandler struct{}

func (h *headtailHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
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
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
	threshold := "80"
	for _, arg := range cmd.Arguments[1:] {
		if strings.HasPrefix(arg, "threshold=") {
			threshold = strings.TrimPrefix(arg, "threshold=")
		} else if !strings.Contains(arg, "=") {
			threshold = arg
		}
	}
	fieldRef := resolveFieldRef(field, ctx.Registry)
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
	if len(cmd.Arguments) > 0 {
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *selectfirstHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
	source := ctx.Plan.CurrentStage()
	if field == "timestamp" {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "min(timestamp) AS first_timestamp"})
	} else {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("argMin(%s, timestamp) AS first_%s", resolveFieldRef(field, ctx.Registry), escapeString(field)),
		})
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// selectlastHandler handles selectLast(field)
type selectlastHandler struct{}

func (h *selectlastHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *selectlastHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
	source := ctx.Plan.CurrentStage()
	if field == "timestamp" {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "max(timestamp) AS last_timestamp"})
	} else {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("argMax(%s, timestamp) AS last_%s", resolveFieldRef(field, ctx.Registry), escapeString(field)),
		})
	}
	ctx.Plan.IsAggregated = true
	return nil
}

// topHandler handles top(field, percent=true, limit=N, as=alias)
type topHandler struct{}

func (h *topHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	alias := ""
	field := ""
	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "as=") {
			alias = strings.TrimPrefix(arg, "as=")
		} else if !strings.Contains(arg, "=") && field == "" {
			field = arg
		}
	}
	if field != "" {
		if alias == "" {
			alias = "top_" + field
		}
		ctx.Registry.Register(alias, FieldKindAggregate, alias, ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *topHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	field := ""
	showPercent := false
	topN := 10
	alias := ""
	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "percent=") {
			showPercent = strings.TrimPrefix(arg, "percent=") == "true"
		} else if strings.HasPrefix(arg, "limit=") {
			if n, err := strconv.Atoi(strings.TrimPrefix(arg, "limit=")); err == nil && n > 0 {
				topN = n
			}
		} else if strings.HasPrefix(arg, "as=") {
			alias = strings.TrimPrefix(arg, "as=")
		} else if !strings.Contains(arg, "=") && field == "" {
			field = arg
		}
	}
	if field == "" {
		return nil
	}
	if alias == "" {
		alias = "top_" + field
	}
	fieldRef := resolveFieldRef(field, ctx.Registry)
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
	for range cmd.Arguments {
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

	selectFields := selectExprStrings(source.Layer.Selects)
	computedFields := ctx.Registry.AllComputed()
	for _, fn := range cmd.Arguments {
		if processStatsFn(fn, &selectFields, computedFields) {
			ctx.Plan.IsAggregated = true
		}
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
	if len(cmd.Arguments) > 0 {
		ctx.Registry.Register("_median", FieldKindAggregate, "_median", ctx.CmdIndex)
		ctx.Registry.Register("_mad", FieldKindAggregate, "_mad", ctx.CmdIndex)
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *madHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
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
		fieldRef := resolveFieldRef(field, ctx.Registry)
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
	if len(cmd.Arguments) >= 2 {
		ctx.Plan.IsAggregated = true
	}
	return nil
}

func (h *bucketHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) < 2 {
		return nil
	}
	span := cmd.Arguments[0]
	function := cmd.Arguments[1]
	source := ctx.Plan.CurrentStage()

	n, unit := parseBucketSpan(span)
	bucketExpr := getBucketExpression(n, unit)
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS time_bucket", bucketExpr)})

	if strings.Contains(function, "count()") {
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "COUNT(*) AS bucket_count"})
		ctx.Plan.IsAggregated = true
	} else if strings.Contains(function, "sum(") {
		f := extractFunctionField(function, "sum")
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("sum(toFloat64OrNull(%s)) AS bucket_sum", jsonFieldRef(f)),
		})
		ctx.Plan.IsAggregated = true
	}
	source.Layer.GroupBy = append(source.Layer.GroupBy, bucketExpr)
	return nil
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

	for i := 0; i < len(cmd.Arguments); i++ {
		arg := cmd.Arguments[i]

		if arg == "function=" && i+1 < len(cmd.Arguments) {
			funcDef := cmd.Arguments[i+1]
			i++

			prevAliases := make(map[string]bool)
			for _, sel := range source.Layer.Selects {
				prevAliases[extractFieldAlias(sel.String())] = true
			}

			selectFields := selectExprStrings(source.Layer.Selects)
			if strings.HasPrefix(funcDef, "multi(") {
				inner := funcDef[len("multi("): len(funcDef)-1]
				for _, fn := range splitTopLevelArgs(inner) {
					if processStatsFn(fn, &selectFields, computedFields) {
						ctx.Plan.IsAggregated = true
					}
				}
			} else if processStatsFn(funcDef, &selectFields, computedFields) {
				ctx.Plan.IsAggregated = true
			} else {
				selectFields = append(selectFields, "COUNT(*) AS _count")
				ctx.Plan.IsAggregated = true
			}
			source.Layer.Selects = nil
			for _, sf := range selectFields {
				source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: sf})
			}
			// Register new aggregation outputs
			for _, sel := range source.Layer.Selects {
				alias := extractFieldAlias(sel.String())
				if alias != "" && !prevAliases[alias] {
					ctx.Registry.Register(alias, FieldKindAggregate, alias, ctx.CmdIndex)
					ctx.Plan.aggregationOutputs[alias] = alias
				}
			}
		} else if strings.HasPrefix(arg, "limit=") {
			if n, err := strconv.Atoi(strings.TrimPrefix(arg, "limit=")); err == nil && n > 0 {
				source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
			}
		} else if arg == "distinct=true" {
			if len(source.Layer.GroupBy) > 0 {
				lastField := source.Layer.GroupBy[len(source.Layer.GroupBy)-1]
				source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
					Expr: fmt.Sprintf("COUNT(DISTINCT %s) AS _count", lastField),
				})
				ctx.Plan.IsAggregated = true
			}
		} else if arg == "distinct=false" {
			// Explicit non-distinct, ignore
		} else {
			var fieldRef string
			if computedFields[arg] {
				fieldRef = arg
			} else {
				switch arg {
				case "timestamp", "raw_log", "log_id":
					fieldRef = arg
				default:
					if isMultiStage {
						// In multi-stage, fields reference previous stage output by alias
						fieldRef = arg
					} else {
						fieldRef = jsonFieldRef(arg)
					}
				}
			}
			if !contains(source.Layer.GroupBy, fieldRef) {
				source.Layer.GroupBy = append(source.Layer.GroupBy, fieldRef)
			}
		}
	}

	// Register default _count aggregation output. In multi-stage pipelines,
	// skip this: the new stage should not pre-populate aggregationOutputs
	// because subsequent aggregation commands (sum, avg, etc.) need to
	// operate directly on the stage's SELECT, not the chained wrapper path.
	if !isMultiStage {
		ctx.Plan.aggregationOutputs["_count"] = "COUNT(*)"
		ctx.Registry.SetResolveExpr("_count", "_count")
	}
	return nil
}

func init() {
	registerCommand(&countHandler{}, "count")
	registerCommand(&simpleAggHandler{name: "sum", alias: "_sum", chFunc: "sum"}, "sum")
	registerCommand(&simpleAggHandler{name: "avg", alias: "_avg", chFunc: "avg"}, "avg")
	registerCommand(&simpleAggHandler{name: "max", alias: "_max", chFunc: "max"}, "max")
	registerCommand(&simpleAggHandler{name: "min", alias: "_min", chFunc: "min"}, "min")
	registerCommand(&simpleAggHandler{name: "median", alias: "_median", chFunc: "median"}, "median")
	registerCommand(&percentileHandler{}, "percentile")
	registerCommand(&stddevHandler{}, "stddev", "stdDev")
	registerCommand(&skewnessHandler{}, "skewness", "skew")
	registerCommand(&kurtosisHandler{}, "kurtosis", "kurt")
	registerCommand(&frequencyHandler{}, "frequency")
	registerCommand(&iqrHandler{}, "iqr")
	registerCommand(&headtailHandler{}, "headtail")
	registerCommand(&selectfirstHandler{}, "selectfirst")
	registerCommand(&selectlastHandler{}, "selectlast")
	registerCommand(&topHandler{}, "top")
	registerCommand(&multiHandler{}, "multi")
	registerCommand(&madHandler{}, "mad")
	registerCommand(&bucketHandler{}, "bucket")
	registerCommand(&groupbyHandler{}, "groupby")
}
