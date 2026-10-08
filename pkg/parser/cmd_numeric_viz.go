package parser

import (
	"fmt"
	"strconv"
)

// chartColumn returns the column a whole-result chart reads for field. After an
// aggregation it must be one of the stage's outputs; before one, a log field is
// projected by the scan under scanAlias, and scanExpr is the expression behind it.
func chartColumn(arg Argument, ctx *CommandContext, cmdName, scanAlias string) (col, scanExpr string, err error) {
	field := arg.FieldName()
	if field == "" {
		return "", "", fmt.Errorf("%s(): expected a field name, got %s", cmdName, arg)
	}
	if _, ok := ctx.Plan.aggregationOutputs[field]; ok {
		return sanitizedAlias(field), "", nil
	}
	if alias, ok := ctx.Registry.GroupKeyAlias(field); ok {
		return sanitizedAlias(alias), "", nil
	}
	if ctx.Registry.IsComputed(field) {
		return sanitizedAlias(field), "", nil
	}
	source := ctx.Plan.CurrentStage()
	if len(source.Layer.GroupBy) > 0 || ctx.Plan.HasGroupBy || ctx.Plan.IsAggregated {
		return "", "", fmt.Errorf("%s(): the preceding aggregation does not produce %s; use one of its outputs, or put %s() before the aggregation",
			cmdName, field, cmdName)
	}
	ref, err := ResolveArg(arg, ctx.Registry)
	if err != nil {
		return "", "", fmt.Errorf("%s(): %w", cmdName, err)
	}
	scanExpr = groupableCast(ref)
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", scanExpr, scanAlias)})
	ctx.Registry.SetResolveExpr(scanAlias, ref)
	return scanAlias, scanExpr, nil
}

func chartNumeric(col string) string {
	return fmt.Sprintf("toFloat64OrNull(toString(%s))", col)
}

// boxplotHandler handles boxplot(field, by=group, fence=1.5, limit=20, outliers=10).
// One pass per group: quartiles from a Greenwald-Khanna sketch (mergeable, rank
// error 0.1%) and the k most extreme values at each end, from which the Tukey
// whiskers and outliers are exact whenever fewer than k values lie past a fence.
type boxplotHandler struct{}

func (h *boxplotHandler) Declare(cmd CommandNode, ctx *CommandContext) error { return nil }

func (h *boxplotHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	arg, ok := b.First("field")
	if !ok {
		return fmt.Errorf("boxplot() requires a numeric field, e.g. boxplot(bytes) or boxplot(bytes, by=host)")
	}
	fence := b.Float("fence", 1.5)
	if fence <= 0 || fence > 10 {
		return fmt.Errorf("boxplot(): fence must be between 0 and 10, got %v", fence)
	}
	groups := clampInt(b.Int("limit", 20), 1, 100)
	extremes := clampInt(b.Int("outliers", 10), 1, 100)

	valCol, _, err := chartColumn(arg, ctx, "boxplot", "_bp_src")
	if err != nil {
		return err
	}
	groupExpr := "''"
	groupName := ""
	if byArg, ok := b.First("by"); ok {
		groupName = byArg.FieldName()
		col, _, err := chartColumn(byArg, ctx, "boxplot", "_bp_group_src")
		if err != nil {
			return err
		}
		groupExpr = fmt.Sprintf("toString(%s)", col)
	}

	f := strconv.FormatFloat(fence, 'f', -1, 64)
	k := strconv.Itoa(extremes)
	ctx.Plan.ChartLayers = []QueryLayer{
		{Selects: []SelectExpr{
			{Expr: chartNumeric(valCol) + " AS _bp_val"},
			{Expr: groupExpr + " AS _group"},
		}},
		{
			Selects: []SelectExpr{
				{Expr: "_group"},
				{Expr: "count() AS _count"},
				{Expr: "quantilesGK(1000, 0.25, 0.5, 0.75)(_bp_val) AS _bp_q"},
				{Expr: "min(_bp_val) AS _bp_min"},
				{Expr: "max(_bp_val) AS _bp_max"},
				{Expr: "avg(_bp_val) AS _bp_mean"},
				{Expr: "stddevPop(_bp_val) AS _bp_sd"},
				{Expr: fmt.Sprintf("groupArraySorted(%s)(_bp_val) AS _bp_low", k)},
				{Expr: fmt.Sprintf("arrayMap(x -> -x, groupArraySorted(%s)(-_bp_val)) AS _bp_high", k)},
			},
			Where:   []string{"isFinite(_bp_val)"},
			GroupBy: []string{"_group"},
			OrderBy: []string{"_count DESC", "_group ASC"},
			Limit:   fmt.Sprintf("LIMIT %d", groups),
		},
		{Selects: []SelectExpr{
			{Expr: "*"},
			{Expr: fmt.Sprintf("_bp_q[1] - %s * (_bp_q[3] - _bp_q[1]) AS _bp_lf", f)},
			{Expr: fmt.Sprintf("_bp_q[3] + %s * (_bp_q[3] - _bp_q[1]) AS _bp_uf", f)},
		}},
		{Selects: []SelectExpr{
			{Expr: "_group"},
			{Expr: "_count"},
			{Expr: "round(_bp_min, 4) AS _min"},
			{Expr: "round(_bp_q[1], 4) AS _q1"},
			{Expr: "round(_bp_q[2], 4) AS _median"},
			{Expr: "round(_bp_q[3], 4) AS _q3"},
			{Expr: "round(_bp_max, 4) AS _max"},
			{Expr: "round(_bp_mean, 4) AS _mean"},
			{Expr: "round(_bp_sd, 4) AS _stddev"},
			{Expr: "round(_bp_lf, 4) AS _lower_fence"},
			{Expr: "round(_bp_uf, 4) AS _upper_fence"},
			// A whisker is the most extreme value inside its fence. When every
			// retained extreme lies past the fence the true one is unseen, so the
			// whisker is drawn at the fence.
			{Expr: "round(if(arrayExists(x -> x >= _bp_lf, _bp_low), arrayFirst(x -> x >= _bp_lf, _bp_low), _bp_lf), 4) AS _whisker_low"},
			{Expr: "round(if(arrayExists(x -> x <= _bp_uf, _bp_high), arrayFirst(x -> x <= _bp_uf, _bp_high), _bp_uf), 4) AS _whisker_high"},
			{Expr: "arrayMap(x -> round(x, 4), arrayFilter(x -> x < _bp_lf, _bp_low)) AS _outliers_low"},
			{Expr: "arrayMap(x -> round(x, 4), arrayFilter(x -> x > _bp_uf, _bp_high)) AS _outliers_high"},
		}},
	}
	ctx.Plan.ChartReadsAllRows = true
	ctx.Plan.ChartFieldOrder = []string{"_group", "_count", "_min", "_q1", "_median", "_q3", "_max",
		"_mean", "_stddev", "_lower_fence", "_upper_fence", "_whisker_low", "_whisker_high",
		"_outliers_low", "_outliers_high"}
	if groupName == "" {
		ctx.Plan.ChartFieldOrder = ctx.Plan.ChartFieldOrder[1:]
	}
	ctx.Plan.ChartType = "boxplot"
	ctx.Plan.ChartConfig["field"] = arg.FieldName()
	ctx.Plan.ChartConfig["by"] = groupName
	ctx.Plan.ChartConfig["fence"] = fence
	ctx.Plan.ChartConfig["outliers"] = extremes
	return nil
}

// scatterHandler handles scatter(x=field, y=field, label=field, line=, limit=5000). It
// plots rows as they are: after an aggregation one point per group, before one a
// point per event (the newest limit= events).
type scatterHandler struct{}

func (h *scatterHandler) Declare(cmd CommandNode, ctx *CommandContext) error { return nil }

func (h *scatterHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	xArg, hasX := b.First("x")
	yArg, hasY := b.First("y")
	if !hasX || !hasY {
		return fmt.Errorf("scatter() requires x= and y=, e.g. groupby(host, function=[sum(bytes_in), sum(bytes_out)]) | scatter(x=_sum_bytes_in, y=_sum_bytes_out, label=host)")
	}
	limit := clampInt(b.Int("limit", 5000), 1, 50000)
	// line= is drawn by the browser over the returned points; the query is the same.
	line := b.Str("line", "")
	if line != "" && line != "diagonal" && line != "trend" {
		return fmt.Errorf("scatter(): line= accepts diagonal (y = x) or trend (best fit), got %q", line)
	}

	xName, yName := xArg.FieldName(), yArg.FieldName()
	if xName != "" && xName == yName {
		return fmt.Errorf("scatter(): x= and y= must be different fields")
	}
	xCol, xScan, err := chartColumn(xArg, ctx, "scatter", "_sc_x_src")
	if err != nil {
		return err
	}
	yCol, yScan, err := chartColumn(yArg, ctx, "scatter", "_sc_y_src")
	if err != nil {
		return err
	}
	selects := []SelectExpr{
		{Expr: fmt.Sprintf("%s AS %s", chartNumeric(xCol), sanitizedAlias(xName))},
		{Expr: fmt.Sprintf("%s AS %s", chartNumeric(yCol), sanitizedAlias(yName))},
	}
	order := []string{xName, yName}
	labelName := ""
	if labelArg, ok := b.First("label"); ok {
		labelName = labelArg.FieldName()
		col, _, err := chartColumn(labelArg, ctx, "scatter", "_sc_label_src")
		if err != nil {
			return err
		}
		if labelName == xName || labelName == yName {
			return fmt.Errorf("scatter(): label= must differ from x= and y=")
		}
		selects = append([]SelectExpr{{Expr: fmt.Sprintf("toString(%s) AS %s", col, sanitizedAlias(labelName))}}, selects...)
		order = append([]string{labelName}, order...)
	}

	ctx.Plan.ChartLayers = []QueryLayer{
		{Selects: []SelectExpr{
			{Expr: chartNumeric(xCol) + " AS _sc_x"},
			{Expr: chartNumeric(yCol) + " AS _sc_y"},
			{Expr: "*"},
		}},
		{
			Selects: selects,
			Where:   []string{"isFinite(_sc_x)", "isFinite(_sc_y)"},
			Limit:   fmt.Sprintf("LIMIT %d", limit),
		},
	}
	// Per-event points: plot the newest events that carry both values, so the
	// limit is not spent on rows the chart would drop.
	source := ctx.Plan.CurrentStage()
	for _, e := range []string{xScan, yScan} {
		if e != "" {
			source.Layer.Where = append(source.Layer.Where, fmt.Sprintf("isFinite(toFloat64OrNull(%s))", e))
		}
	}
	if source.IsSource && len(source.Layer.GroupBy) == 0 && !ctx.Plan.IsAggregated && !ctx.Plan.HasGroupBy {
		if len(source.Layer.OrderBy) == 0 {
			source.Layer.OrderBy = []string{"timestamp DESC", "log_id DESC"}
		}
		if source.Layer.Limit == "" {
			source.Layer.Limit = fmt.Sprintf("LIMIT %d", limit)
		}
	}
	ctx.Plan.ChartReadsAllRows = true
	ctx.Plan.ChartFieldOrder = order
	ctx.Plan.ChartType = "scatter"
	ctx.Plan.ChartConfig["xField"] = xName
	ctx.Plan.ChartConfig["yField"] = yName
	ctx.Plan.ChartConfig["labelField"] = labelName
	ctx.Plan.ChartConfig["limit"] = limit
	ctx.Plan.ChartConfig["line"] = line
	return nil
}

func clampInt(v, lo, hi int) int {
	return max(lo, min(v, hi))
}

func init() {
	registerAggregatingCommand(&boxplotHandler{}, "boxplot")
	registerAggregatingCommand(&scatterHandler{}, "scatter")
	registerSpec(&CommandSpec{Name: "boxplot", Params: []ParamSpec{
		field("field"), namedField("by"), namedLit("fence"), namedLit("limit"), namedLit("outliers"),
	}})
	registerSpec(&CommandSpec{Name: "scatter", Params: []ParamSpec{
		namedField("x"), namedField("y"), namedField("label"), namedLit("line"), namedLit("limit"),
	}})
}
