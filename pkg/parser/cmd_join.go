package parser

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	joinDefaultMaxRows = 10000
	joinHardMaxRows    = 100000

	// joinHiddenKeyCol is the alias the source scan projects the outer join key
	// under, so the ON clause has a column to reference inside _outer.
	joinHiddenKeyCol = "_join_k"
)

// joinHandler handles join(key, type=inner|left, max=N, include=[f1,f2]) { subquery }
type joinHandler struct{}

func (h *joinHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.IsJoin = true
	return nil
}

func (h *joinHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	blockBody := strings.TrimSpace(cmd.Block)
	if blockBody == "" {
		return fmt.Errorf("join() subquery cannot be empty")
	}

	joinKey := b.Str("key", "")
	joinType := "inner"
	if a, given := b.First("type"); given {
		val := a.Value()
		switch val {
		case "inner", "left":
			joinType = val
		default:
			return fmt.Errorf("join() type must be 'inner' or 'left', got '%s'", val)
		}
	}
	maxRows := joinDefaultMaxRows
	if a, given := b.First("max"); given {
		val := a.Value()
		n, err := strconv.Atoi(val)
		if err != nil || n <= 0 {
			return fmt.Errorf("join() max must be a positive integer, got '%s'", val)
		}
		maxRows = min(n, joinHardMaxRows)
	}
	includeFields := b.Strings("include")

	if joinKey == "" {
		return fmt.Errorf("join() requires a join key field, e.g. join(user) { ... }")
	}

	// Validate join key identifier
	if _, err := sanitizeIdentifier(joinKey); err != nil {
		return fmt.Errorf("join() invalid join key: %w", err)
	}

	// Parse and translate the subquery
	subPipeline, err := ParseQuery(blockBody)
	if err != nil {
		return fmt.Errorf("join() subquery parse error: %w", err)
	}

	// Check for nested joins
	for _, subCmd := range subPipeline.Commands {
		if strings.ToLower(subCmd.Name) == "join" {
			return fmt.Errorf("join() does not support nested joins")
		}
	}

	subResult, err := TranslateToSQLWithOrder(subPipeline, subqueryOptions(ctx, maxRows))
	if err != nil {
		return fmt.Errorf("join() subquery translation error: %w", err)
	}

	// The ON clause reads the key off the subquery by name, so the subquery must
	// actually output it. A bare count() after groupby is the common trap: it
	// re-aggregates into a single row and drops the group key entirely.
	if !contains(subResult.FieldOrder, joinKey) {
		return fmt.Errorf("join() subquery must output the join key %q, but returns [%s]%s",
			joinKey, strings.Join(subResult.FieldOrder, ", "), joinSubqueryHint(subPipeline, joinKey))
	}
	// include= names columns read off the subquery, so they must exist there too.
	for _, f := range includeFields {
		if !contains(subResult.FieldOrder, f) {
			return fmt.Errorf("join() include=%s is not produced by the subquery, which returns [%s]",
				f, strings.Join(subResult.FieldOrder, ", "))
		}
	}

	// Resolve the join key reference for the outer query
	// The outer query uses the field as it appears in its SELECT (alias or JSON ref).
	// The subquery must also produce a column with this name.
	safeKey, _ := sanitizeIdentifier(joinKey)

	ctx.Plan.IsJoin = true
	ctx.Plan.JoinType = joinType
	ctx.Plan.JoinKey = safeKey
	ctx.Plan.JoinSubSQL = subResult.SQL
	ctx.Plan.JoinInclude = includeFields
	ctx.Plan.JoinOutputs = subResult.FieldOrder
	ctx.Plan.JoinMaxRows = maxRows

	// Register the joined-in columns so a filter or sort on one is deferred to a
	// post-join layer instead of the source scan, where it does not exist yet.
	// Same mechanism as model_lookup()'s outputs.
	for _, col := range joinCarriedColumns(subResult.FieldOrder, includeFields, safeKey) {
		name := joinPrefixed(col)
		ctx.Registry.Register(name, FieldKindJoined, name, ctx.CmdIndex)
	}

	return nil
}

// joinCarriedColumns returns the subquery columns the join wrapper brings back,
// honouring include= when present. The join key itself is excluded: it is matched on,
// not carried through. Single source of truth for the SQL projection (wrapWithJoin),
// the registry entries, and the display order -- these must not drift apart.
func joinCarriedColumns(subFields, includeFields []string, joinKey string) []string {
	src := subFields
	if len(includeFields) > 0 {
		src = includeFields
	}
	out := make([]string, 0, len(src))
	for _, f := range src {
		if f == joinKey {
			continue
		}
		out = append(out, f)
	}
	return out
}

// joinPrefixed is the name a carried subquery column is exposed under.
func joinPrefixed(col string) string { return "_join_" + col }

// joinDisplayNames returns the prefixed names of the columns the join contributes.
func joinDisplayNames(plan *QueryPlan) []string {
	cols := joinCarriedColumns(plan.JoinOutputs, plan.JoinInclude, plan.JoinKey)
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = joinPrefixed(c)
	}
	return out
}

// joinSubqueryHint points at the usual cause when a subquery drops its group key.
func joinSubqueryHint(sub *PipelineNode, joinKey string) string {
	for _, c := range sub.Commands {
		if strings.EqualFold(c.Name, "count") && len(c.Args) == 0 {
			return fmt.Sprintf(" -- a bare count() after groupby(%s) counts the groups and drops %s; groupby() already returns a count, so drop the count()", joinKey, joinKey)
		}
	}
	return ""
}

func init() {
	registerCommand(&joinHandler{}, "join")
}

func init() {
	// Arguments[0] is the raw subquery body, so positional counting and unknown
	// names cannot apply: the block carries the sub-pipeline's own syntax.
	registerSpec(&CommandSpec{Name: "join", FreeForm: true, Params: []ParamSpec{
		field("key"), namedLit("type"), namedLit("max"), namedList("include"),
	}})
}

// subqueryOptions is the context a nested pipeline is translated in. Every field
// is listed rather than copying ctx.Opts wholesale: the scope guards live here,
// and a field added later must be considered rather than inherited by accident.
func subqueryOptions(ctx *CommandContext, maxRows int) QueryOptions {
	return QueryOptions{
		bindingWork:           ctx.Opts.bindingWork,
		StartTime:             ctx.Opts.StartTime,
		EndTime:               ctx.Opts.EndTime,
		MaxRows:               maxRows,
		FractalID:             ctx.Opts.FractalID,
		FractalIDs:            ctx.Opts.FractalIDs,
		IncludeEmptyFractalID: ctx.Opts.IncludeEmptyFractalID,
		Dictionaries:          ctx.Opts.Dictionaries,
		CaseInsensitiveDicts:  ctx.Opts.CaseInsensitiveDicts,
		GeoIPEnabled:          ctx.Opts.GeoIPEnabled,
		DictionaryDatabase:    ctx.Opts.DictionaryDatabase,
		TableName:             ctx.Opts.TableName,
		UseIngestTimestamp:    ctx.Opts.UseIngestTimestamp,
		MaxEventLagSeconds:    ctx.Opts.MaxEventLagSeconds,
		DisplayTimezone:       ctx.Opts.DisplayTimezone,
	}
}
