package parser

import (
	"fmt"
)

// modifiedZScoreHandler handles modifiedzscore/modifiedz/mzscore(field)
type modifiedZScoreHandler struct{}

func (h *modifiedZScoreHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register("_median", FieldKindWindow, "_median", ctx.CmdIndex)
		ctx.Registry.Register("_mad", FieldKindWindow, "_mad", ctx.CmdIndex)
		ctx.Registry.Register("_modified_z", FieldKindWindow, "_modified_z", ctx.CmdIndex)
	}
	return nil
}

func (h *modifiedZScoreHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	arg, ok := b.First("field")
	if !ok {
		return nil
	}
	if err := setWindowValue(arg, ctx); err != nil {
		return fmt.Errorf("modifiedZScore(): %w", err)
	}
	ctx.Registry.SetResolveExpr("_modified_z", "_modified_z")
	ctx.Registry.SetResolveExpr("_median", "_median")
	ctx.Registry.SetResolveExpr("_mad", "_mad")
	return nil
}

// setWindowValue points the plan's modified-z input at the argument's value: an
// aggregate output is read by its alias, anything else is projected as _mz_val.
func setWindowValue(arg Argument, ctx *CommandContext) error {
	if name := arg.FieldName(); name != "" {
		if _, isAggOutput := ctx.Plan.aggregationOutputs[name]; isAggOutput {
			ctx.Plan.ModifiedZScoreExpr = fmt.Sprintf("toFloat64(%s)", name)
			return nil
		}
	}
	fieldRef, err := ResolveArg(arg, ctx.Registry)
	if err != nil {
		return err
	}
	ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects,
		SelectExpr{Expr: fmt.Sprintf("toFloat64OrNull(%s) AS _mz_val", fieldRef)})
	ctx.Plan.ModifiedZScoreExpr = "_mz_val"
	return nil
}

// madOutlierHandler handles madoutlier/outlier(field, threshold)
type madOutlierHandler struct{}

func (h *madOutlierHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Args) > 0 {
		ctx.Registry.Register("_median", FieldKindWindow, "_median", ctx.CmdIndex)
		ctx.Registry.Register("_mad", FieldKindWindow, "_mad", ctx.CmdIndex)
		ctx.Registry.Register("_modified_z", FieldKindWindow, "_modified_z", ctx.CmdIndex)
		ctx.Registry.Register("_is_outlier", FieldKindWindow, "_is_outlier", ctx.CmdIndex)
	}
	return nil
}

func (h *madOutlierHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	arg, ok := b.First("field")
	if !ok {
		return nil
	}
	outlierThreshold := b.Str("threshold", "3.5")
	if err := validateNumeric(outlierThreshold); err != nil {
		return fmt.Errorf("madOutlier(): invalid threshold: %w", err)
	}
	ctx.Plan.OutlierThreshold = outlierThreshold

	if err := setWindowValue(arg, ctx); err != nil {
		return fmt.Errorf("madOutlier(): %w", err)
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
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	arg, ok := b.First("field")
	if !ok {
		return fmt.Errorf("histogram() requires a field argument")
	}
	field := arg.FieldName()
	if field == "" {
		return fmt.Errorf("histogram() first argument must be a field name, got %s", arg)
	}
	buckets := b.Int("buckets", 20)
	if buckets <= 0 {
		buckets = 20
	}
	buckets = min(buckets, 200)

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
	registerAggregatingCommand(&modifiedZScoreHandler{}, "modifiedzscore", "modifiedz", "mzscore")
	registerAggregatingCommand(&madOutlierHandler{}, "madoutlier", "outlier")
	registerAggregatingCommand(&histogramHandler{}, "histogram")
}

func init() {
	modZ := &CommandSpec{Name: "modifiedzscore", Params: []ParamSpec{reqField("field")}}
	registerSpec(modZ, "modifiedzscore", "modifiedz", "mzscore")
	outlier := &CommandSpec{Name: "madoutlier", Params: []ParamSpec{
		reqField("field"),
		ParamSpec{Name: "threshold", Kind: ParamLiteral, Positional: true},
	}}
	registerSpec(outlier, "madoutlier", "outlier")
	// The handler names the missing-field case precisely; the spec covers shape.
	registerSpec(&CommandSpec{Name: "histogram", Params: []ParamSpec{field("field"), namedLit("buckets")}})
}
