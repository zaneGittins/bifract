package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// modifiedZScoreHandler handles modifiedzscore/modifiedz/mzscore(field)
type modifiedZScoreHandler struct{}

func (h *modifiedZScoreHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		ctx.Registry.Register("_median", FieldKindWindow, "_median", ctx.CmdIndex)
		ctx.Registry.Register("_mad", FieldKindWindow, "_mad", ctx.CmdIndex)
		ctx.Registry.Register("_modified_z", FieldKindWindow, "_modified_z", ctx.CmdIndex)
	}
	return nil
}

func (h *modifiedZScoreHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
	source := ctx.Plan.CurrentStage()

	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		ctx.Plan.ModifiedZScoreExpr = fmt.Sprintf("toFloat64(%s)", field)
	} else {
		fieldRef := resolveFieldRef(field, ctx.Registry)
		source.Layer.Selects = append(source.Layer.Selects,
			SelectExpr{Expr: fmt.Sprintf("toFloat64OrNull(%s) AS _mz_val", fieldRef)})
		ctx.Plan.ModifiedZScoreExpr = "_mz_val"
	}
	ctx.Registry.SetResolveExpr("_modified_z", "_modified_z")
	ctx.Registry.SetResolveExpr("_median", "_median")
	ctx.Registry.SetResolveExpr("_mad", "_mad")
	return nil
}

// madOutlierHandler handles madoutlier/outlier(field, threshold)
type madOutlierHandler struct{}

func (h *madOutlierHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		ctx.Registry.Register("_median", FieldKindWindow, "_median", ctx.CmdIndex)
		ctx.Registry.Register("_mad", FieldKindWindow, "_mad", ctx.CmdIndex)
		ctx.Registry.Register("_modified_z", FieldKindWindow, "_modified_z", ctx.CmdIndex)
		ctx.Registry.Register("_is_outlier", FieldKindWindow, "_is_outlier", ctx.CmdIndex)
	}
	return nil
}

func (h *madOutlierHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return nil
	}
	field := cmd.Arguments[0]
	outlierThreshold := "3.5"
	for _, arg := range cmd.Arguments[1:] {
		if strings.HasPrefix(arg, "threshold=") {
			outlierThreshold = strings.TrimPrefix(arg, "threshold=")
		} else if !strings.Contains(arg, "=") {
			outlierThreshold = arg
		}
	}
	if err := validateNumeric(outlierThreshold); err != nil {
		return fmt.Errorf("madOutlier(): invalid threshold: %w", err)
	}
	ctx.Plan.OutlierThreshold = outlierThreshold

	source := ctx.Plan.CurrentStage()
	if _, isAggOutput := ctx.Plan.aggregationOutputs[field]; isAggOutput {
		ctx.Plan.ModifiedZScoreExpr = fmt.Sprintf("toFloat64(%s)", field)
	} else {
		fieldRef := resolveFieldRef(field, ctx.Registry)
		source.Layer.Selects = append(source.Layer.Selects,
			SelectExpr{Expr: fmt.Sprintf("toFloat64OrNull(%s) AS _mz_val", fieldRef)})
		ctx.Plan.ModifiedZScoreExpr = "_mz_val"
	}
	ctx.Registry.SetResolveExpr("_modified_z", "_modified_z")
	ctx.Registry.SetResolveExpr("_median", "_median")
	ctx.Registry.SetResolveExpr("_mad", "_mad")
	ctx.Registry.SetResolveExpr("_is_outlier", "_is_outlier")
	return nil
}

// histogramHandler handles histogram(field, buckets=20)
type histogramHandler struct{}

func (h *histogramHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *histogramHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return fmt.Errorf("histogram() requires a field argument")
	}
	field := cmd.Arguments[0]
	if strings.Contains(field, "=") {
		return fmt.Errorf("histogram() first argument must be a field name, got %q", field)
	}
	buckets := 20
	for _, arg := range cmd.Arguments[1:] {
		if strings.HasPrefix(arg, "buckets=") {
			if n, err := strconv.Atoi(strings.TrimPrefix(arg, "buckets=")); err == nil && n > 0 {
				if n > 200 {
					n = 200
				}
				buckets = n
			}
		}
	}

	source := ctx.Plan.CurrentStage()
	computedFields := ctx.Registry.AllComputed()

	// For raw fields, add a computed column so the value is available by alias
	if _, ok := computedFields[field]; !ok {
		if _, ok2 := ctx.Plan.aggregationOutputs[field]; !ok2 {
			fieldRef := resolveFieldRef(field, ctx.Registry)
			source.Layer.Selects = append(source.Layer.Selects,
				SelectExpr{Expr: fmt.Sprintf("toFloat64OrNull(%s) AS _hist_val", fieldRef)})
			ctx.Registry.SetResolveExpr("_hist_val", fmt.Sprintf("toFloat64OrNull(%s)", fieldRef))
		}
	}

	ctx.Plan.HistogramField = field
	ctx.Plan.HistogramBuckets = buckets
	ctx.Plan.ChartType = "histogram"
	ctx.Plan.ChartConfig["field"] = field
	ctx.Plan.ChartConfig["buckets"] = buckets
	return nil
}

func init() {
	registerCommand(&modifiedZScoreHandler{}, "modifiedzscore", "modifiedz", "mzscore")
	registerCommand(&madOutlierHandler{}, "madoutlier", "outlier")
	registerCommand(&histogramHandler{}, "histogram")
}
