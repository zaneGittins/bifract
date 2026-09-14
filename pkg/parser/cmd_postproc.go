package parser

import (
	"fmt"
	"strings"
)

// sortHandler handles sort(field, direction)
type sortHandler struct{}

func (h *sortHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *sortHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	arg, ok := b.First("field")
	if !ok {
		return nil
	}

	direction := "ASC"
	if d := strings.ToUpper(b.Str("order", "")); d == "DESC" || d == "ASC" {
		direction = d
	}

	fieldRef, err := sortFieldRef(arg, ctx)
	if err != nil {
		return fmt.Errorf("sort(): %w", err)
	}
	source := ctx.Plan.CurrentStage()
	source.Layer.OrderBy = append(source.Layer.OrderBy, fieldRef+" "+direction)
	return nil
}

// sortFieldRef resolves the sort key. A computed column is ordered by its alias;
// a raw JSON subcolumn is cast to ::String so a Dynamic-stored path is orderable
// (a bare Dynamic ref errors 44).
func sortFieldRef(arg Argument, ctx *CommandContext) (string, error) {
	if arg.IsFieldName() {
		name := arg.Expr.Value
		if ctx.Registry.IsComputed(name) || ctx.Registry.Has(name) {
			return name, nil
		}
		switch name {
		case "timestamp", "log_id", "normalizer":
			return name, nil
		case normLogColumn:
			return contentColMode(ctx.Opts.SourceMode), nil
		}
	}
	return ResolveArg(arg, ctx.Registry)
}

// limitHandler handles limit(n)
type limitHandler struct{}

func (h *limitHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *limitHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	n, err := rowCount(cmd, 0)
	if err != nil {
		return fmt.Errorf("limit(): %w", err)
	}
	if n > 0 {
		ctx.Plan.CurrentStage().Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	}
	return nil
}

// headHandler handles head(n) - first N events ordered by timestamp ASC
type headHandler struct{}

func (h *headHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *headHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	source := ctx.Plan.CurrentStage()
	n, err := rowCount(cmd, 200)
	if err != nil {
		return fmt.Errorf("head(): %w", err)
	}
	// On aggregated stages timestamp is neither grouped nor aggregated, so
	// ordering by it is invalid (ClickHouse error 215). Keep the existing
	// ordering (e.g. a prior sort) and just take its first N rows.
	if !isAggregatedStage(ctx.Plan, source) {
		source.Layer.OrderBy = []string{"timestamp ASC"}
	}
	source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	return nil
}

// tailHandler handles tail(n) - last N events ordered by timestamp DESC
type tailHandler struct{}

func (h *tailHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *tailHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	source := ctx.Plan.CurrentStage()
	n, err := rowCount(cmd, 200)
	if err != nil {
		return fmt.Errorf("tail(): %w", err)
	}
	// On aggregated stages timestamp is neither grouped nor aggregated, so
	// ordering by it is invalid (ClickHouse error 215). The "last N" rows are
	// the first N of the reversed existing ordering.
	if isAggregatedStage(ctx.Plan, source) {
		source.Layer.OrderBy = reverseOrderBy(source.Layer.OrderBy)
	} else {
		source.Layer.OrderBy = []string{"timestamp DESC"}
	}
	source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	return nil
}

// rowCount reads a row-count argument, rejecting a negative or non-numeric one
// rather than letting it reach LIMIT verbatim.
func rowCount(cmd CommandNode, def int) (int, error) {
	b, err := BindCommand(cmd)
	if err != nil {
		return 0, err
	}
	raw := b.Str("n", "")
	if raw == "" {
		return def, nil
	}
	return validateInt(raw)
}

// isAggregatedStage reports whether the given stage produces aggregated rows,
// in which case ordering by raw columns like timestamp is invalid.
func isAggregatedStage(plan *QueryPlan, stage *QueryStage) bool {
	return plan.IsAggregated || len(stage.Layer.GroupBy) > 0
}

// reverseOrderBy flips the direction of each ORDER BY term so that the tail of
// a result set becomes its head. Terms without an explicit direction default to
// ASC in ClickHouse, so they become DESC.
func reverseOrderBy(order []string) []string {
	if len(order) == 0 {
		return order
	}
	flipped := make([]string, len(order))
	for i, term := range order {
		t := strings.TrimSpace(term)
		switch {
		case strings.HasSuffix(strings.ToUpper(t), " DESC"):
			flipped[i] = t[:len(t)-len(" DESC")] + " ASC"
		case strings.HasSuffix(strings.ToUpper(t), " ASC"):
			flipped[i] = t[:len(t)-len(" ASC")] + " DESC"
		default:
			flipped[i] = t + " DESC"
		}
	}
	return flipped
}

// stageOutputs returns the column names a stage projects: its group keys plus
// the aliases of its SELECT expressions. Group keys are still raw field
// references at Execute time (assembleGroupBySelects rewrites them to aliases
// later), so they are reduced to their field name.
func stageOutputs(stage *QueryStage) map[string]bool {
	outputs := make(map[string]bool)
	for _, gf := range stage.Layer.GroupBy {
		outputs[extractFieldName(gf)] = true
	}
	for _, sel := range stage.Layer.Selects {
		if alias := strings.Trim(extractFieldAlias(sel.String()), "`"); alias != "" {
			outputs[alias] = true
		}
	}
	return outputs
}

// scanExprFor returns an expression for a computed field that is valid over the
// raw scan, so a dedup on it can be pushed below an aggregation: the expression
// the registry recorded for it, or failing that the one the stage's SELECT
// computes it with. Empty when the field's value exists only as an output column
// of the stage, which a scan-level LIMIT BY cannot reference.
func scanExprFor(field string, ctx *CommandContext, stage *QueryStage) string {
	if entry := ctx.Registry.Get(field); entry != nil && entry.ResolveAs != "" {
		return ctx.Registry.Resolve(field)
	}
	for _, sel := range stage.Layer.Selects {
		s := sel.String()
		if strings.Trim(extractFieldAlias(s), "`") != field {
			continue
		}
		if idx := strings.LastIndex(s, " AS "); idx != -1 {
			return s[:idx]
		}
	}
	return ""
}

// dedupHandler handles dedup(field1, field2, ...) using LIMIT 1 BY.
//
// Where the LIMIT BY lands depends on whether rows have been aggregated yet,
// because ClickHouse evaluates LIMIT BY after GROUP BY:
//
//   - Before an aggregation, it must collapse the rows the aggregation will
//     read, so it goes to ScanLimitBy and rendering wraps the scan in it.
//     Written into the aggregating SELECT it would instead try to dedup the
//     groups, using a key that SELECT does not produce (ClickHouse code 215).
//   - After an aggregation, it dedups the result rows and belongs on that
//     SELECT, keyed by an output column rather than the source field.
type dedupHandler struct{}

func (h *dedupHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *dedupHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	args := b.Flat("fields")
	if len(args) == 0 {
		return nil
	}
	stage := ctx.Plan.CurrentStage()
	// Position in the pipeline, not Plan.IsAggregated: that flag is set during
	// Declare by an aggregation anywhere in the pipeline, including one this
	// dedup precedes.
	firstAgg := firstAggregatingCommandIndex(ctx.Pipeline.Commands)
	postAggregation := firstAgg < ctx.CmdIndex
	// An aggregation still to come renders this dedup around the scan, where the
	// stage's computed columns do not exist yet.
	beforeAggregation := firstAgg < len(ctx.Pipeline.Commands) && !postAggregation
	outputs := stageOutputs(stage)

	var dedupFields []string
	for _, arg := range args {
		field := arg.Value()
		if !arg.IsFieldName() {
			// A computed key exists only where its expression can be evaluated: on
			// the scan. After an aggregation the columns it reads are gone.
			if postAggregation {
				return fmt.Errorf("dedup(%s): %q is computed from columns the preceding aggregation does not produce; dedup on a grouped field or move dedup() before the aggregation", argList(args), field)
			}
			sql, err := ResolveArg(arg, ctx.Registry)
			if err != nil {
				return fmt.Errorf("dedup(): %w", err)
			}
			dedupFields = append(dedupFields, sql)
			continue
		}
		switch {
		case postAggregation && outputs[field]:
			dedupFields = append(dedupFields, field)
		case postAggregation:
			return fmt.Errorf("dedup(%s): %q is not produced by the preceding aggregation; dedup on a grouped field or move dedup() before the aggregation", argList(args), field)
		case !ctx.Registry.IsComputed(field):
			// LIMIT BY is a grouping context: cast raw JSON subcolumns to
			// ::String so Dynamic-stored paths don't trigger error 44.
			dedupFields = append(dedupFields, resolveFieldRef(field, ctx.Registry))
		case !beforeAggregation:
			// Rendered on the same SELECT that computes the column, so its alias
			// resolves.
			dedupFields = append(dedupFields, field)
		default:
			expr := scanExprFor(field, ctx, stage)
			if expr == "" {
				return fmt.Errorf("dedup(%s): %q is computed after the scan and cannot be deduplicated before an aggregation; dedup on a log field instead", argList(args), field)
			}
			dedupFields = append(dedupFields, expr)
		}
	}

	limitBy := fmt.Sprintf("LIMIT 1 BY %s", strings.Join(dedupFields, ", "))
	if postAggregation {
		stage.Layer.LimitBy = limitBy
	} else {
		stage.Layer.ScanLimitBy = limitBy
	}
	return nil
}

func init() {
	registerCommand(&sortHandler{}, "sort")
	registerCommand(&limitHandler{}, "limit")
	registerCommand(&headHandler{}, "head")
	registerCommand(&tailHandler{}, "tail")
	registerCommand(&dedupHandler{}, "dedup")
}

func init() {
	// The direction is accepted either way: sort(bytes, desc) and sort(bytes, order=desc).
	registerSpec(&CommandSpec{Name: "sort", Params: []ParamSpec{
		reqField("field"),
		ParamSpec{Name: "order", Kind: ParamLiteral, Positional: true},
	}})
	registerSpec(&CommandSpec{Name: "limit", Params: []ParamSpec{reqLit("n")}})
	registerSpec(&CommandSpec{Name: "head", Params: []ParamSpec{lit("n")}})
	registerSpec(&CommandSpec{Name: "tail", Params: []ParamSpec{lit("n")}})
	registerSpec(&CommandSpec{Name: "dedup", Params: []ParamSpec{fields("fields")}})
}
