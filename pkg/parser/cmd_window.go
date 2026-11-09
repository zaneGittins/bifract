package parser

import (
	"fmt"
)

// windowValueColumn holds the value modifiedzscore()/madoutlier() measure.
const windowValueColumn = "_mz_val"

// modifiedZScoreHandler handles modifiedzscore/modifiedz/mzscore(field)
type modifiedZScoreHandler struct{}

func (h *modifiedZScoreHandler) keepsRows()            {}
func (h *modifiedZScoreHandler) scoresAgainstAllRows() {}

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
	if err := setWindowValue(arg, ctx, "modifiedZScore"); err != nil {
		return err
	}
	registerWindowOutputs(ctx, "_modified_z", "_median", "_mad")
	return nil
}

// registerWindowOutputs re-registers a window command's output columns after any
// stage push. A pushed stage scopes the registry to that stage's outputs, so
// the entries Declare made are gone by the time a later filter or sort is
// classified, and the column reads as an ordinary log field.
func registerWindowOutputs(ctx *CommandContext, names ...string) {
	readsFrom := ctx.Registry.mark()
	for _, n := range names {
		ctx.Registry.Register(n, FieldKindWindow, n, ctx.CmdIndex)
		ctx.Registry.define(n, n, readsFrom)
	}
}

// measuresAggregateOutput reports whether a window command's operand is a column
// the preceding aggregation produced.
func measuresAggregateOutput(arg Argument, ctx *CommandContext) bool {
	name := arg.FieldName()
	if name == "" {
		return false
	}
	_, ok := ctx.Plan.aggregationOutputs[name]
	return ok
}

// rejectMeasureAfterAggregation reports the case where a window command is asked
// to measure a log field the preceding aggregation collapsed. These commands
// need one value per row; after a GROUP BY the rows are groups and the field is
// gone, which reached the server as "not under aggregate function and not in
// GROUP BY".
func rejectMeasureAfterAggregation(arg Argument, ctx *CommandContext, name string) error {
	if measuresAggregateOutput(arg, ctx) {
		return nil
	}
	if len(ctx.Plan.CurrentStage().Layer.GroupBy) == 0 && !ctx.Plan.HasGroupBy {
		return nil
	}
	if f := arg.FieldName(); f != "" && ctx.Registry.IsComputed(f) {
		return nil
	}
	return fmt.Errorf("%s(%s): the preceding aggregation does not produce that column; measure one of its outputs, or put %s() before the aggregation",
		name, arg, name)
}

// setWindowValue points the plan's modified-z input at the argument's value: an
// aggregate output is read by its alias, anything else is projected as _mz_val.
func setWindowValue(arg Argument, ctx *CommandContext, name string) error {
	if err := rejectMeasureAfterAggregation(arg, ctx, name); err != nil {
		return err
	}
	if measuresAggregateOutput(arg, ctx) {
		ctx.Plan.window.zScoreExpr = fmt.Sprintf("toFloat64(%s)", ctx.Registry.Column(arg.FieldName()).sql)
		return nil
	}
	ref, err := resolveArgRef(arg, ctx.Registry)
	if err != nil {
		return err
	}
	value := selectAs(numericCast(ref), windowValueColumn)
	if plan := ctx.Plan; ref.deps.wrap && plan.join != nil && plan.join.SubSQL != "" {
		// A join()'s output exists only above its wrap, below the window layers.
		plan.format.aboveJoin = append(plan.format.aboveJoin, value)
	} else {
		plan.CurrentStage().Layer.Selects = append(plan.CurrentStage().Layer.Selects, value)
	}
	ctx.Plan.window.zScoreExpr = windowValueColumn
	return nil
}

// madOutlierHandler handles madoutlier/outlier(field, threshold)
type madOutlierHandler struct{}

func (h *madOutlierHandler) keepsRows()            {}
func (h *madOutlierHandler) scoresAgainstAllRows() {}

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
	ctx.Plan.window.outlierThreshold = outlierThreshold

	if err := setWindowValue(arg, ctx, "madOutlier"); err != nil {
		return err
	}
	registerWindowOutputs(ctx, "_modified_z", "_median", "_mad", "_is_outlier")
	return nil
}

// histogramHandler handles histogram(field, buckets=20)
type histogramHandler struct{}

func (h *histogramHandler) buildsOutputLater() {}

func (h *histogramHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *histogramHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	readsFrom := ctx.Registry.mark()
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

	if err := rejectMeasureAfterAggregation(arg, ctx, "histogram"); err != nil {
		return err
	}
	source := ctx.Plan.CurrentStage()
	computedFields := ctx.Registry.AllComputed()

	// The bucketing layers read one numeric column. Which expression produces it
	// depends on where the field comes from, and only this phase knows: the
	// window layers run after the registry has been scoped to stage outputs.
	switch {
	case ctx.Plan.aggregationOutputs[field]:
		// An aggregate's output column is already numeric.
		ctx.Plan.histogram.valueExpr = fmt.Sprintf("toFloat64(%s)", ctx.Registry.Column(field).sql)
	case computedFields[field]:
		// A computed column is projected by this stage under its own alias.
		ctx.Plan.histogram.valueExpr = fmt.Sprintf("toFloat64OrNull(toString(%s))", field)
	default:
		// A raw log field has to be projected before the window layers can see it.
		fieldRef := ctx.Registry.Resolve(field)
		source.Layer.Selects = append(source.Layer.Selects,
			SelectExpr{Expr: fmt.Sprintf("toFloat64OrNull(%s)", fieldRef), Alias: "_hist_val"})
		ctx.Registry.define("_hist_val", fmt.Sprintf("toFloat64OrNull(%s)", fieldRef), readsFrom)
		ctx.Plan.histogram.valueExpr = "_hist_val"
	}

	ctx.Plan.histogram.buckets = buckets
	ctx.Plan.chart.readsAllRows = true
	ctx.Plan.chart.kind = "histogram"
	ctx.Plan.chart.config["field"] = field
	ctx.Plan.chart.config["buckets"] = buckets
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
