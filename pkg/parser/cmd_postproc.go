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

	descending := strings.EqualFold(b.Str("order", ""), "desc")

	ref, err := sortFieldRef(arg, ctx)
	if err != nil {
		return fmt.Errorf("sort(): %w", err)
	}
	source := ctx.Plan.CurrentStage()
	source.Layer.OrderBy = append(source.Layer.OrderBy, orderBy(ref, descending))
	return nil
}

// sortFieldRef resolves the sort key. A computed column is ordered by its alias;
// a raw JSON subcolumn is cast to ::String so a Dynamic-stored path is orderable
// (a bare Dynamic ref errors 44).
func sortFieldRef(arg Argument, ctx *CommandContext) (sqlRef, error) {
	reg := ctx.Registry
	if arg.IsFieldName() {
		name := arg.Expr.Value
		if reg.IsComputed(name) || reg.Has(name) {
			return reg.Column(name), nil
		}
		// count, sum and avg name the aggregate's output where one is, as they do in
		// a filter.
		if reg.bareAggregate(name) && reg.Has("_"+name) {
			return reg.Column("_" + name), nil
		}
		switch name {
		case "timestamp", "log_id", "normalizer", normLogColumn:
			return reg.Column(name), nil
		}
	}
	return resolveArgRef(arg, reg)
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
		capRows(ctx)
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
	if onScan(ctx) || !isAggregatedStage(ctx.Plan, source) {
		source.Layer.OrderBy = []OrderTerm{asc("timestamp")}
	}
	source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	capRows(ctx)
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
	if !onScan(ctx) && isAggregatedStage(ctx.Plan, source) {
		source.Layer.OrderBy = reverseOrderBy(source.Layer.OrderBy)
	} else {
		source.Layer.OrderBy = []OrderTerm{desc("timestamp")}
	}
	source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	capRows(ctx)
	return nil
}

// capRows makes the row selection a command just set on the scan a cap that the
// rest of the pipeline reads through. After an aggregation or on a later stage
// the selection stays where it is: those rows are already a stage's output.
func capRows(ctx *CommandContext) {
	if !onScan(ctx) {
		return
	}
	l := &ctx.Plan.SourceStage().Layer
	l.Caps = append(l.Caps, RowCap{At: ctx.CmdIndex, OrderBy: l.OrderBy, LimitBy: l.LimitBy, ScanLimitBy: l.LimitBy, limitByDeps: l.limitByDeps, scanDeps: l.limitByDeps, Limit: l.Limit})
	l.OrderBy, l.LimitBy, l.limitByDeps, l.Limit = nil, "", deps{}, ""
}

// onScan reports whether the running command reads the raw scan: no stage or
// aggregation precedes it.
func onScan(ctx *CommandContext) bool {
	return len(ctx.Plan.Stages) == 1 && ctx.CmdIndex < firstAggregatingCommandIndex(ctx.Pipeline.Commands())
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
// a result set becomes its head.
func reverseOrderBy(order []OrderTerm) []OrderTerm {
	flipped := make([]OrderTerm, len(order))
	for i, t := range order {
		t.Desc = !t.Desc
		flipped[i] = t
	}
	return flipped
}

// stageOutputs returns the column names a stage projects: its group keys plus
// the names of its SELECT columns.
func stageOutputs(stage *QueryStage) map[string]bool {
	outputs := make(map[string]bool)
	for _, k := range stage.Layer.GroupBy {
		outputs[k.Name] = true
	}
	for _, name := range columnNames(stage.Layer.Selects) {
		outputs[name] = true
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
	expr := stageSelectExpr(stage, field)
	if expr != "" {
		ctx.Registry.computed(field, expr)
	}
	return expr
}

// dedupHandler handles dedup(field1, field2, ...) using LIMIT 1 BY. On the scan
// it is a row cap (see capRows); after an aggregation it dedups the result rows,
// keyed by an output column.
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
	firstAgg := firstAggregatingCommandIndex(ctx.Pipeline.Commands())
	postAggregation := firstAgg < ctx.CmdIndex
	// A dedup on the scan caps the rows the rest of the pipeline reads. Read
	// through, it renders around the scan, where the stage's computed columns do
	// not exist yet, so each key also has its scan form.
	aggregationAhead := firstAgg < len(ctx.Pipeline.Commands()) && !postAggregation
	outputs := stageOutputs(stage)

	var keys, scanKeys []string
	keyDeps, scanDeps := noDeps, noDeps
	both := func(d deps) { keyDeps, scanDeps = keyDeps.and(d), scanDeps.and(d) }
	scannable := true
	for _, arg := range args {
		field := arg.Value()
		if !arg.IsFieldName() {
			// A computed key exists only where its expression can be evaluated: on
			// the scan. After an aggregation the columns it reads are gone.
			if postAggregation {
				return fmt.Errorf("dedup(%s): %q is computed from columns the preceding aggregation does not produce; dedup on a grouped field or move dedup() before the aggregation", argList(args), field)
			}
			r, err := resolveArgRef(arg, ctx.Registry)
			if err != nil {
				return fmt.Errorf("dedup(): %w", err)
			}
			keys, scanKeys = append(keys, r.sql), append(scanKeys, r.sql)
			both(r.deps)
			continue
		}
		switch {
		case postAggregation && outputs[field]:
			col := ctx.Registry.Column(field)
			keys = append(keys, col.sql)
			keyDeps = keyDeps.and(col.deps)
		case postAggregation:
			return fmt.Errorf("dedup(%s): %q is not produced by the preceding aggregation; dedup on a grouped field or move dedup() before the aggregation", argList(args), field)
		case !ctx.Registry.IsComputed(field):
			// LIMIT BY is a grouping context: cast raw JSON subcolumns to
			// ::String so Dynamic-stored paths don't trigger error 44.
			r := ctx.Registry.Ref(field)
			keys, scanKeys = append(keys, r.sql), append(scanKeys, r.sql)
			both(r.deps)
		default:
			// On the SELECT that computes the column its alias resolves.
			col := ctx.Registry.Column(field)
			keys = append(keys, col.sql)
			keyDeps = keyDeps.and(col.deps)
			var expr string
			sd, _ := ctx.Registry.track(func() error { expr = scanExprFor(field, ctx, stage); return nil })
			scanDeps = scanDeps.and(sd.deps())
			if expr == "" {
				if aggregationAhead {
					return fmt.Errorf("dedup(%s): %q is computed after the scan and cannot be deduplicated before an aggregation; dedup on a log field instead", argList(args), field)
				}
				scannable = false
			}
			scanKeys = append(scanKeys, expr)
		}
	}

	if keyDeps.wrap {
		return fmt.Errorf("dedup(%s): a key reads a column join() adds, which exists only after the join; dedup before the join, or on the log's own fields", argList(args))
	}
	limitBy := fmt.Sprintf("LIMIT 1 BY %s", strings.Join(keys, ", "))
	scanLimitBy := fmt.Sprintf("LIMIT 1 BY %s", strings.Join(scanKeys, ", "))
	switch {
	case postAggregation:
		stage.Layer.LimitBy, stage.Layer.limitByDeps = limitBy, keyDeps
	case onScan(ctx):
		stage.Layer.LimitBy, stage.Layer.limitByDeps = limitBy, keyDeps
		capRows(ctx)
		c := &stage.Layer.Caps[len(stage.Layer.Caps)-1]
		c.ScanLimitBy = ""
		if scannable {
			c.ScanLimitBy, c.scanKeys, c.scanDeps = scanLimitBy, scanKeys, scanDeps
		}
	case aggregationAhead:
		stage.Layer.ScanLimitBy, stage.Layer.scanLimitByDeps = scanLimitBy, scanDeps
	default:
		stage.Layer.ScanLimitBy, stage.Layer.scanLimitByDeps = limitBy, keyDeps
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
