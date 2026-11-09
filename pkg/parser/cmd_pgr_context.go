package parser

import (
	"fmt"
	"slices"
	"strings"
)

// pgrContextHandler compiles pgrContext(), which adds the process-creation facts of each row's
// process as pgr_* columns. It is a lookup like modelLookup(): proc_lineage read by key for the
// processes in range, joined on process_guid, so the columns filter, group and sort like any other.
type pgrContextHandler struct{}

// pgrContextColumn is one column pgrContext() can add: the argument naming it, its name, and its
// value on a lineage row. The parent's command line has no expression, being the parent's row.
type pgrContextColumn struct{ arg, name, expr string }

var pgrContextColumns = []pgrContextColumn{
	{"commandline", "pgr_commandline", "commandline"},
	{"user", "pgr_user", `if(proc_user = '\0', '', proc_user)`},
	{"image", "pgr_image", "image"},
	{"parent_image", "pgr_parent_image", "parent_image"},
	{"parent_guid", "pgr_parent_guid", "parent_guid"},
	{"parent_commandline", "pgr_parent_commandline", ""},
	{"hash", "pgr_hash", "hash"},
	{"original_file_name", "pgr_original_file_name", "original_file_name"},
	{"start_time", "pgr_start_time", "toString(timestamp)"},
}

// pgrContextArgs returns the columns a pgrContext() asks for, all of them by default, and
// whether rows without lineage are dropped.
func pgrContextArgs(cmd CommandNode) ([]pgrContextColumn, bool, error) {
	b, err := BindCommand(cmd)
	if err != nil {
		return nil, false, err
	}
	strict := false
	if b.Has("require") {
		switch strings.ToLower(b.Str("require", "")) {
		case "true":
			strict = true
		case "false":
		default:
			return nil, false, fmt.Errorf("pgrContext() require= must be true or false")
		}
	}
	var cols []pgrContextColumn
	for _, a := range b.All("columns") {
		name := strings.ToLower(a.Value())
		i := slices.IndexFunc(pgrContextColumns, func(c pgrContextColumn) bool { return c.arg == name })
		if a.Kind != ArgLiteral || i < 0 {
			return nil, false, fmt.Errorf("pgrContext() has no column %s: choose from %s", a.String(), pgrContextArgNames())
		}
		if !slices.Contains(cols, pgrContextColumns[i]) {
			cols = append(cols, pgrContextColumns[i])
		}
	}
	if len(cols) == 0 {
		cols = pgrContextColumns
	}
	return cols, strict, nil
}

func pgrContextArgNames() string {
	names := make([]string, len(pgrContextColumns))
	for i, c := range pgrContextColumns {
		names[i] = c.arg
	}
	return strings.Join(names, ", ")
}

func (h *pgrContextHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	cols, _, err := pgrContextArgs(cmd)
	if err != nil {
		return err
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.name
		ctx.Registry.Register(c.name, FieldKindJoined, c.name, ctx.CmdIndex)
	}
	ctx.Plan.lookup.Outputs = names
	return nil
}

func (h *pgrContextHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if err := checkPgrSource("pgrContext", ctx); err != nil {
		return err
	}
	if err := claimLookup(ctx.Plan, pgrContextName); err != nil {
		return err
	}
	cols, strict, err := pgrContextArgs(cmd)
	if err != nil {
		return err
	}
	ks := &modelKeySet{keyFields: []string{pgrFilterField}, rightCols: []string{pgrFilterField},
		global: ctx.Opts.readsDistributed(), rescore: func() string { return pgrContextSQL(ctx, cols) }}
	ks.scoringSQL = ks.rescore()
	for _, c := range cols {
		ks.fields = append(ks.fields, c.name)
	}
	ctx.Plan.lookup.Fields = ks.fields
	ctx.Plan.lookup.keys = ks
	ctx.Plan.lookup.Cmd = ctx.CmdIndex
	ctx.Plan.lookup.Strict = strict
	keyExprs := []string{modelLookupFieldRef(pgrFilterField, ctx.Registry)}
	if ctx.Plan.lookup.AtScan {
		ctx.Plan.lookup.KeyExprs = keyExprs
		return nil
	}
	return joinModelAfterAggregation(ctx, ks.keyFields, keyExprs)
}

// pgrContextSQL is the lookup: one row per process in range from its newest lineage row, as
// ReplacingMergeTree keeps it, preferring a value on a tie. The parent's command line is the
// parent's own row, by key.
func pgrContextSQL(ctx *CommandContext, cols []pgrContextColumn) string {
	in := ctx.Opts.subqueryIn(false)
	table := procLineageTable(ctx.Opts)
	inRange := pgrScope(ctx, fmt.Sprintf("proc_key %s (%s)", in, pgrCandidates(ctx)))
	parent := slices.ContainsFunc(cols, func(c pgrContextColumn) bool { return c.expr == "" })
	sel := []string{"process_guid"}
	if parent {
		sel = append(sel, "argMax(parent_key, timestamp) AS parent_key")
	}
	for _, c := range cols {
		if c.expr != "" {
			sel = append(sel, fmt.Sprintf("argMax(%[1]s, (timestamp, %[1]s != '')) AS %[2]s", c.expr, c.name))
		}
	}
	rows := fmt.Sprintf("SELECT %s FROM %s WHERE %s GROUP BY process_guid", strings.Join(sel, ", "), table, inRange)
	if !parent {
		return rows
	}
	outer := []string{"c.process_guid AS process_guid"}
	for _, c := range cols {
		if c.expr == "" {
			outer = append(outer, "p.commandline AS "+c.name)
		} else {
			outer = append(outer, "c."+c.name+" AS "+c.name)
		}
	}
	parents := fmt.Sprintf("SELECT proc_key, argMax(commandline, (timestamp, commandline != '')) AS commandline FROM %s WHERE %s GROUP BY proc_key", table,
		pgrScope(ctx, fmt.Sprintf("proc_key %s (SELECT parent_key FROM %s WHERE %s)", in, table, inRange)))
	return fmt.Sprintf("SELECT %s FROM (%s) AS c LEFT JOIN (%s) AS p ON c.parent_key = p.proc_key", strings.Join(outer, ", "), rows, parents)
}

// pgrContextName is the command's name as the plan records the lookup it holds.
const pgrContextName = "pgrContext"

// claimLookup takes the plan's one lookup join for the named command.
func claimLookup(plan *QueryPlan, name string) error {
	if plan.join != nil {
		return fmt.Errorf("%s() cannot be combined with join()", name)
	}
	if plan.hasModelLookup() {
		if plan.lookup.name == name {
			return fmt.Errorf("%s() cannot be used more than once", name)
		}
		return fmt.Errorf("%s() cannot be combined with %s() in one query", name, plan.lookup.name)
	}
	plan.lookup.name = name
	return nil
}

func init() {
	registerCommand(&pgrContextHandler{}, "pgrContext")
	registerSpec(&CommandSpec{Name: "pgrContext", Params: []ParamSpec{
		{Name: "columns", Kind: ParamLiteral, Positional: true, Variadic: true},
		namedLit("require"),
	}})
}
