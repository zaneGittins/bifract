package parser

import "fmt"

// rankCorrSample bounds the rows rankcorr() ranks per group. Spearman needs every
// value held to rank it, so an unbounded rankCorr grows with the scan; a sample of
// this size keeps memory fixed with a standard error near 0.003. Parallel merges
// pick the sample, so a larger group can differ by about that between runs.
const rankCorrSample = 100000

// correlationSQL renders corr() or rankcorr() over two numeric operands, rounded
// to 4 places (+ 0 turns -0 into 0) and NULL when undefined (fewer than two rows,
// or a constant operand). corr() is Pearson via corrStable: plain corr() loses precision on
// values far from zero, such as epoch times, and returned 1.17 on test data.
func correlationSQL(name, x, y string) string {
	if name == "corr" {
		return fmt.Sprintf("ifNotFinite(round(corrStable(%s, %s), 4) + 0, NULL)", x, y)
	}
	sample := fmt.Sprintf("groupArraySampleIf(%d, 1)((%[2]s, %[3]s), isNotNull(%[2]s) AND isNotNull(%[3]s))", rankCorrSample, x, y)
	return fmt.Sprintf("ifNotFinite(round(arrayReduce('rankCorr', arrayMap(t -> assumeNotNull(t.1), %[1]s), arrayMap(t -> assumeNotNull(t.2), %[1]s)), 4) + 0, NULL)", sample)
}

// correlationOperands finds the two operands of a correlation spec: x= and y=
// when named, otherwise the first two bare arguments.
func correlationOperands(args []Argument) (x, y Argument, ok bool) {
	var bare []Argument
	var hasX, hasY bool
	for _, a := range args {
		switch a.Name {
		case "x":
			x, hasX = a, true
		case "y":
			y, hasY = a, true
		case "":
			bare = append(bare, a)
		}
	}
	for _, a := range bare {
		if !hasX {
			x, hasX = a, true
		} else if !hasY {
			y, hasY = a, true
		}
	}
	return x, y, hasX && hasY
}

// correlationHandler handles corr(x, y) and rankcorr(x, y): Pearson and Spearman
// correlation of two numeric fields, per group after a groupby.
type correlationHandler struct{ name string }

func (h *correlationHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	alias, err := commandAggAlias(cmd, h.name)
	if err != nil {
		return nil
	}
	ctx.Registry.Register(alias, FieldKindAggregate, alias, ctx.CmdIndex)
	ctx.Plan.IsAggregated = true
	return nil
}

func (h *correlationHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	xArg, hasX := b.First("x")
	yArg, hasY := b.First("y")
	if !hasX || !hasY {
		return fmt.Errorf("%s() needs two numeric fields, e.g. %s(orig_bytes, resp_bytes)", h.name, h.name)
	}
	alias, err := aggregateAlias(b.Str("as", ""), h.name)
	if err != nil {
		return fmt.Errorf("%s(): %w", h.name, err)
	}

	// Over a prior aggregation's outputs (one row per group) the correlation is
	// across groups, computed in an outer stage like any chained aggregate.
	_, xOut := ctx.Plan.aggregationOutputs[xArg.FieldName()]
	_, yOut := ctx.Plan.aggregationOutputs[yArg.FieldName()]
	if xOut != yOut {
		return fmt.Errorf("%s(): both fields must be outputs of the preceding aggregation, or neither", h.name)
	}
	if xOut {
		sqlExpr := correlationSQL(h.name, fmt.Sprintf("toFloat64(%s)", xArg.FieldName()), fmt.Sprintf("toFloat64(%s)", yArg.FieldName()))
		ctx.Plan.outerAggregations = append(ctx.Plan.outerAggregations, sqlExpr+" AS "+alias)
		ctx.Plan.outerAggFieldOrder = append(ctx.Plan.outerAggFieldOrder, alias)
		ctx.Plan.aggregationOutputs[alias] = sqlExpr
	} else {
		x, err := aggOperandNumeric(xArg, ctx.Registry)
		if err != nil {
			return fmt.Errorf("%s(): %w", h.name, err)
		}
		y, err := aggOperandNumeric(yArg, ctx.Registry)
		if err != nil {
			return fmt.Errorf("%s(): %w", h.name, err)
		}
		source := ctx.Plan.CurrentStage()
		sqlExpr := correlationSQL(h.name, x, y)
		if err := aggAliasClash(source, h.name, alias, sqlExpr); err != nil {
			return err
		}
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", sqlExpr, alias)})
		ctx.Plan.aggregationOutputs[alias] = sqlExpr
	}
	ctx.Registry.SetResolveExpr(alias, alias)
	ctx.Plan.IsAggregated = true
	return nil
}

func init() {
	for _, n := range []string{"corr", "rankcorr"} {
		registerAggregatingCommand(&correlationHandler{name: n}, n)
		registerSpec(&CommandSpec{Name: n, Params: []ParamSpec{
			{Name: "x", Kind: ParamField, Positional: true, Required: true},
			{Name: "y", Kind: ParamField, Positional: true, Required: true},
			as(),
		}})
	}
}
