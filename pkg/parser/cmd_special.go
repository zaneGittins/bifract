package parser

import (
	"fmt"
	"strings"
)

// chainAnchorColumn carries chain()'s matched-sequence timestamps. Present in the
// result data for drilldown and alert evidence, suppressed from the display columns.
const chainAnchorColumn = "_chain_ts"

// chainDoneColumn marks when a matched sequence became visible, in whatever time
// basis the query is scoped by. Distinct from chainAnchorColumn, which is always
// event time because it keys the log lookup off the sorting key.
const chainDoneColumn = "_chain_done"

// tableHandler handles table(field1, field2, ...)
type tableHandler struct{}

func (h *tableHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.HasTableCmd = true
	return nil
}

func (h *tableHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.HasTableCmd = true
	source := ctx.Plan.CurrentStage()

	// When a prior command already aggregated (groupby, multi, stats, etc.),
	// table() is a pure projection over the existing outputs: keep their SELECT
	// expressions and just restrict/reorder to the requested columns. Re-deriving
	// them as raw JSON fields (fields.`x`) or re-adding them to GROUP BY would be
	// wrong, since they are computed columns of this stage, not log columns.
	if ctx.Plan.IsAggregated || len(source.Layer.GroupBy) > 0 {
		return h.executeProjection(cmd, ctx, source)
	}

	// Clear existing selects (table replaces default)
	source.Layer.Selects = nil

	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	if n, err := validateInt(b.Str("limit", "")); err == nil {
		source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	}

	var nonAggregateFields []string

	for _, arg := range b.Flat("fields") {
		ctx.Plan.TableHasExplicitColumns = true
		field := arg.FieldName()

		if selects, ok, err := tableAggregateSelects(arg, ctx); err != nil {
			return fmt.Errorf("table(): %w", err)
		} else if ok {
			source.Layer.Selects = append(source.Layer.Selects, selects...)
			ctx.Plan.IsAggregated = true
			continue
		}

		if field == "timestamp" {
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: "timestamp"})
			nonAggregateFields = append(nonAggregateFields, "timestamp")
		} else if entry := ctx.Registry.Get(field); entry != nil && entry.Kind == FieldKindJoined {
			// Produced by a JOIN wrapper (model_lookup/join), not the source scan.
			// Skip it here (projecting fields.`x` would be wrong and would shadow the
			// real join column); the wrapper adds it and it is carried in FieldOrder.
			// Under the scan-level join there is no wrapper to add it: the column
			// survives an aggregation only as a group key or aggregate, so anything
			// else must error rather than silently drop the requested column.
			if ctx.Plan.ModelLookupAtScan {
				st := ctx.Plan.CurrentStage()
				exposed := contains(st.Layer.GroupBy, field)
				for _, sel := range st.Layer.Selects {
					if exposed {
						break
					}
					exposed = extractFieldAlias(sel.String()) == field
				}
				if !exposed {
					return fmt.Errorf("table(): model column %q is not available after the aggregation; group by it or aggregate it", field)
				}
				continue
			}
			ctx.Plan.TableJoinedFields = append(ctx.Plan.TableJoinedFields, field)
			continue
		} else if entry := ctx.Registry.Get(field); entry != nil && (entry.Kind == FieldKindPerRow || entry.Kind == FieldKindAssignment) {
			safeAlias, err := ArgAlias(arg)
			if err != nil {
				return fmt.Errorf("table(): %w", err)
			}
			computedExpr := ctx.Registry.Resolve(field)
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", computedExpr, safeAlias)})
			nonAggregateFields = append(nonAggregateFields, field)
		} else if ctx.Opts.SourceSubquery != "" {
			// Over a subquery source (a source command like pgr()) every field is a flat
			// column; resolve bare via the registry rather than as a fields.`x` JSON path.
			safeAlias, err := ArgAlias(arg)
			if err != nil {
				return fmt.Errorf("table(): %w", err)
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", resolveFieldRef(field, ctx.Registry), safeAlias)})
			nonAggregateFields = append(nonAggregateFields, field)
		} else {
			safeAlias, err := ArgAlias(arg)
			if err != nil {
				return fmt.Errorf("table(): %w", err)
			}
			ref := groupableCast(ctx.Registry.fieldRef(field))
			if field == "" {
				if ref, err = ResolveArg(arg, ctx.Registry); err != nil {
					return fmt.Errorf("table(): %w", err)
				}
				// Register the derived alias, as groupby() does. Without it a later
				// sort/dedup/filter on the name resolved it as a JSON sub-column
				// that exists on no row.
				alias := strings.Trim(safeAlias, "`")
				ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
				ctx.Registry.SetResolveExpr(alias, alias)
				nonAggregateFields = append(nonAggregateFields, alias)
				source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", ref, safeAlias)})
				continue
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", ref, safeAlias)})
			nonAggregateFields = append(nonAggregateFields, field)
		}
	}

	// Auto GROUP BY for non-aggregate fields when table() includes aggregations
	if ctx.Plan.IsAggregated && len(nonAggregateFields) > 0 {
		for _, field := range nonAggregateFields {
			if field == "timestamp" {
				source.Layer.GroupBy = append(source.Layer.GroupBy, "timestamp")
			} else {
				source.Layer.GroupBy = append(source.Layer.GroupBy, resolveFieldRef(field, ctx.Registry))
			}
		}
	}
	return nil
}

// executeProjection handles table() applied to an already-aggregated stage.
// table() then acts as a pure projection over the prior aggregation's outputs,
// so it finalizes that stage and selects the requested columns from it as a
// subquery (the same staging the chained-groupby path uses). This avoids
// re-resolving aggregate/group outputs as raw JSON fields or re-grouping them.
func (h *tableHandler) executeProjection(cmd CommandNode, ctx *CommandContext, prevStage *QueryStage) error {
	// Finalize the aggregated stage so its group keys and aggregate columns are
	// materialized into its SELECT before we project from it. A prior stage that
	// has no GROUP BY (e.g. a post-aggregation assignment stage) is already
	// assembled, so finalizing it would wrongly clear its SELECT.
	if len(prevStage.Layer.GroupBy) > 0 {
		if err := assembleGroupBySelects(ctx, prevStage, nil); err != nil {
			return fmt.Errorf("table (stage finalize): %w", err)
		}
	}
	prevOutputs := stageOutputAliases(prevStage)

	ctx.Plan.PushStage()
	ctx.Plan.IsAggregated = false
	ctx.Plan.aggregationOutputs = make(map[string]string)
	ctx.Plan.outerAggregations = nil
	ctx.Plan.outerAggFieldOrder = nil
	ctx.Registry.ScopeToOutputs(prevOutputs)

	newStage := ctx.Plan.CurrentStage()
	newStage.Layer.Selects = nil
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	if n, err := validateInt(b.Str("limit", "")); err == nil {
		newStage.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	}
	for _, arg := range b.Flat("fields") {
		ctx.Plan.TableHasExplicitColumns = true

		// The bare count keyword maps to the prior stage's default _count output.
		name := strings.Trim(arg.Value(), "`")
		if aggName, _, ok := aggregateCall(arg); ok && aggName == "count" {
			name = "_count"
		}
		if prevOutputs[name] {
			newStage.Layer.Selects = append(newStage.Layer.Selects, SelectExpr{Expr: name})
			continue
		}
		// A model column the aggregation did not output: under the outer join
		// wrap it is added after this stage (restrict display to it); under the
		// scan-level join nothing re-adds it, so error rather than emit a bare
		// reference this stage cannot resolve.
		if contains(ctx.Plan.ModelLookupOutputs, name) {
			if ctx.Plan.ModelLookupAtScan {
				return fmt.Errorf("table(): model column %q is not available after the aggregation; group by it or aggregate it", name)
			}
			ctx.Plan.TableJoinedFields = append(ctx.Plan.TableJoinedFields, name)
			continue
		}
		// Not produced by the aggregated stage. It may be produced by a later
		// layer (e.g. a post-aggregation assignment computed in the outer
		// formatter); leave it to that layer rather than emitting a wrong raw
		// fields.`x` reference here.
		if entry := ctx.Registry.Get(name); entry != nil && entry.Kind != FieldKindBase && entry.Kind != FieldKindJSON {
			continue
		}
		safe, err := sanitizeIdentifier(name)
		if err != nil {
			return fmt.Errorf("table(): %w", err)
		}
		newStage.Layer.Selects = append(newStage.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", name, safe)})
	}
	return nil
}

// ptgHandler handles ptg() (Process Tree Graph): MV-backed process-lineage traversal
// over proc_lineage.
type ptgHandler struct{}

func (h *ptgHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	// Set process-tree mode early so condition routing sends _depth/_path filters to HAVING.
	ctx.Plan.IsProcessTree = true
	ctx.Registry.Register("_depth", FieldKindWindow, "_depth", ctx.CmdIndex)
	ctx.Registry.Register("_path", FieldKindWindow, "_path", ctx.CmdIndex)
	return nil
}

func (h *ptgHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if ctx.Plan.ProcessTreeStart != "" {
		return fmt.Errorf("cannot use multiple ptg() functions in the same query")
	}
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	start := b.Str("start", "")
	direction := strings.ToLower(b.Str("direction", ""))
	depth := 0
	if d := b.Int("depth", 0); d > 0 {
		depth = d
	}
	if start == "" {
		return fmt.Errorf("ptg() requires a start= parameter, e.g. ptg(start=\"<process_guid>\")")
	}
	if depth == 0 {
		depth = 10
	}
	if depth > 50 {
		depth = 50
	}
	if direction == "" {
		direction = "both"
	}
	if direction != "forward" && direction != "backward" && direction != "both" {
		return fmt.Errorf("ptg() direction= must be forward, backward, or both (got %q)", direction)
	}
	ctx.Plan.IsProcessTree = true
	ctx.Plan.ProcessTreeStart = start
	ctx.Plan.ProcessTreeDepth = depth
	ctx.Plan.ProcessTreeDirection = direction
	ctx.Registry.SetResolveExpr("_depth", "_depth")
	ctx.Registry.SetResolveExpr("_path", "_path")
	return nil
}

// analyzeFieldsMaxScan caps the rows analyzeFields() samples, including limit=max.
const analyzeFieldsMaxScan = 200000

// analyzefieldsHandler handles analyzefields(field1, field2, limit=N)
type analyzefieldsHandler struct{}

func (h *analyzefieldsHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Registry.Register("field_name", FieldKindPerRow, "field_name", ctx.CmdIndex)
	ctx.Registry.Register("_events", FieldKindPerRow, "_events", ctx.CmdIndex)
	ctx.Registry.Register("_distinct_vals", FieldKindPerRow, "_distinct_vals", ctx.CmdIndex)
	ctx.Registry.Register("_mean", FieldKindPerRow, "_mean", ctx.CmdIndex)
	ctx.Registry.Register("_min", FieldKindPerRow, "_min", ctx.CmdIndex)
	ctx.Registry.Register("_max", FieldKindPerRow, "_max", ctx.CmdIndex)
	ctx.Registry.Register("_stdev", FieldKindPerRow, "_stdev", ctx.CmdIndex)
	return nil
}

func (h *analyzefieldsHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.IsAnalyze = true
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	if limitVal := b.Str("limit", ""); limitVal != "" {
		n, err := validateInt(limitVal)
		if err != nil {
			return fmt.Errorf("analyzeFields(): %w", err)
		}
		if n > 0 {
			ctx.Plan.AnalyzeFieldsScanLimit = min(n, analyzeFieldsMaxScan)
		}
	}
	ctx.Plan.AnalyzeFieldsList = append(ctx.Plan.AnalyzeFieldsList, b.Strings("fields")...)
	return nil
}

// chainOrdered reads whether the steps must match in the order written.
// sequence=strict|any is the spelling; order=true|false is the original one and
// still parses, because saved queries carry it.
func chainOrdered(b *Bound) (bool, error) {
	if v := strings.ToLower(b.Str("sequence", "")); v != "" {
		switch v {
		case "strict":
			return true, nil
		case "any":
			return false, nil
		default:
			return false, fmt.Errorf("chain(): sequence must be strict or any, got %q", v)
		}
	}
	if v := strings.ToLower(b.Str("order", "")); v != "" {
		switch v {
		case "true", "1", "yes":
			return true, nil
		case "false", "0", "no":
			return false, nil
		default:
			return false, fmt.Errorf("chain(): sequence must be strict or any, got %q", v)
		}
	}
	return true, nil
}

// chainWithinSeconds is the chain's span, or 0 when no within= was given.
// spanToSeconds defaults an empty span to an hour, which would silently impose a
// window on every unwindowed chain.
func chainWithinSeconds(b *Bound) int {
	within := b.Str("within", "")
	if within == "" {
		return 0
	}
	return spanToSeconds(within)
}

// chainHandler handles chain(fields, steps, within=5m)
type chainHandler struct{}

func (h *chainHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Registry.Register("chain_count", FieldKindAggregate, "chain_count", ctx.CmdIndex)
	ctx.Plan.IsAggregated = true
	return nil
}

// WindowContract reports that a matched sequence spans at most within=, and that
// chainDoneColumn marks when it completed.
func (h *chainHandler) WindowContract(cmd CommandNode) (int, string) {
	b, err := BindCommand(cmd)
	if err != nil {
		return 0, chainDoneColumn
	}
	return chainWithinSeconds(b), chainDoneColumn
}

func (h *chainHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	chainFields := b.Strings("fields")
	if len(chainFields) == 0 {
		return fmt.Errorf("chain() requires grouping field(s) and step definitions")
	}
	source := ctx.Plan.CurrentStage()

	withinSeconds := chainWithinSeconds(b)
	ordered, err := chainOrdered(b)
	if err != nil {
		return err
	}
	if !ordered && withinSeconds > 0 {
		// An unordered window needs a sliding span over the matching events, which
		// no single aggregate expresses; approximating it drops real matches.
		return fmt.Errorf("chain(): within= cannot be combined with order=false")
	}

	steps, stepFields, err := parseChainSteps(cmd.BlockTokens, cmd.BlockSource, ctx.Opts, ctx.Registry)
	if err != nil {
		return fmt.Errorf("chain(): %w", err)
	}
	if len(steps) < 2 {
		return fmt.Errorf("chain() requires at least 2 steps, got %d", len(steps))
	}

	// Steps referencing model_lookup() columns need those columns per-row, which
	// only the scan-level join provides (model_lookup() before chain, no earlier
	// aggregation).
	stepsUseModel := ""
	for _, f := range stepFields {
		if e := ctx.Registry.Get(f); e != nil && e.Kind == FieldKindJoined {
			stepsUseModel = f
			break
		}
	}
	if stepsUseModel != "" && !ctx.Plan.ModelLookupAtScan {
		return fmt.Errorf("chain(): step field %q is a model_lookup() column and requires model_lookup() before chain() in the pipeline", stepsUseModel)
	}

	// Build sequenceMatch pattern. (?t<=N) uses the units of tsExpr (milliseconds).
	var pattern strings.Builder
	for i := range steps {
		if i > 0 && withinSeconds > 0 {
			pattern.WriteString(fmt.Sprintf("(?t<=%d)", withinSeconds*1000))
		}
		pattern.WriteString(fmt.Sprintf("(?%d)", i+1))
	}
	patternStr := pattern.String()
	condArgs := strings.Join(steps, ", ")
	// Prefilters the scan, and bounds the completion marker below. Each step is
	// parenthesized so the union holds whatever shape a step compiled to.
	parenSteps := make([]string, len(steps))
	for i, s := range steps {
		parenSteps[i] = "(" + s + ")"
	}
	stepUnion := strings.Join(parenSteps, " OR ")
	// Millisecond ordering: toDateTime() leaves same-second event order undefined,
	// which misorders spawn-then-connect sequences. UInt64 millis; the aggregate
	// rejects DateTime64 and Int64.
	tsExpr := "toUInt64(toUnixTimestamp64Milli(timestamp))"

	// countExpr counts matched sequences, matchExpr gates the group, anchorExpr
	// locates one event per step. Ordered mode sequences the steps; unordered mode
	// only requires each step to occur, so it counts complete co-occurrence sets and
	// anchors on each step's earliest event.
	countExpr := fmt.Sprintf("sequenceCount('%s')(%s, %s)", patternStr, tsExpr, condArgs)
	matchExpr := fmt.Sprintf("sequenceMatch('%s')(%s, %s)", patternStr, tsExpr, condArgs)
	anchorExpr := fmt.Sprintf("sequenceMatchEvents('%s')(%s, %s)", patternStr, tsExpr, condArgs)
	if !ordered {
		counts := make([]string, len(steps))
		presence := make([]string, len(steps))
		anchors := make([]string, len(steps))
		for i, cond := range steps {
			counts[i] = fmt.Sprintf("countIf(%s)", cond)
			presence[i] = fmt.Sprintf("countIf(%s) > 0", cond)
			anchors[i] = fmt.Sprintf("minIf(%s, %s)", tsExpr, cond)
		}
		countExpr = fmt.Sprintf("least(%s)", strings.Join(counts, ", "))
		matchExpr = strings.Join(presence, " AND ")
		anchorExpr = fmt.Sprintf("[%s]", strings.Join(anchors, ", "))
	}

	// Narrow the scan to rows a step can use: the sequence aggregates skip events
	// matching no condition and an entity with none fails the HAVING, so the result
	// is unchanged while GROUP BY state drops to just the matching entities.
	// Only when chain() aggregates the scan itself: behind an earlier aggregation
	// this WHERE would filter that stage's input and silently change its counts.
	// A union referencing model columns can only run after the scan-level join
	// (strict mode's key prefilter still narrows the scan itself).
	if len(ctx.Plan.Stages) == 1 {
		if stepsUseModel != "" {
			ctx.Plan.PostJoinWhere = append(ctx.Plan.PostJoinWhere, "("+stepUnion+")")
		} else {
			scan := &ctx.Plan.SourceStage().Layer
			scan.Where = append(scan.Where, "("+stepUnion+")")
		}
	}

	meta := &ChainMeta{AnchorColumn: chainAnchorColumn, StepConditions: steps, StepFields: stepFields}

	// Multi-identity mode: when multiple fields are provided, they all represent
	// the same entity (e.g., user, source_user, target_user). We use arrayJoin
	// to expand each row into one row per non-empty identity field value, so an
	// event naturally lands in every entity group it belongs to.
	if len(chainFields) > 1 {
		var arrayElems []string
		for _, f := range chainFields {
			if f == "timestamp" || f == normLogColumn || f == "log_id" || f == "normalizer" {
				arrayElems = append(arrayElems, f)
			} else if e := ctx.Registry.Get(f); e != nil && e.Kind == FieldKindJoined && ctx.Plan.ModelLookupAtScan {
				// A model column is per-row after the scan-level join; stringify it
				// for the identity array.
				arrayElems = append(arrayElems, fmt.Sprintf("toString(%s)", f))
			} else {
				// Array element feeds arrayJoin -> _entity -> GROUP BY; a bare
				// Dynamic subcolumn errors 44, so cast to ::String.
				arrayElems = append(arrayElems, groupableCast(jsonFieldRef(f)))
			}
		}
		entityExpr := fmt.Sprintf("arrayJoin(arrayFilter(x -> x != '', [%s]))", strings.Join(arrayElems, ", "))

		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
			Expr: fmt.Sprintf("%s AS _entity", entityExpr),
		})
		source.Layer.GroupBy = append(source.Layer.GroupBy, "_entity")
		ctx.Registry.Register("_entity", FieldKindPerRow, "_entity", ctx.CmdIndex)
		ctx.Registry.SetResolveExpr("_entity", entityExpr)
		meta.EntityColumn = "_entity"
		meta.MultiIdentity = true
	} else {
		// Single field: original behavior, GROUP BY that field directly.
		chainField := chainFields[0]
		safeField, err := sanitizeIdentifier(chainField)
		if err != nil {
			return fmt.Errorf("chain(): invalid field name: %w", err)
		}
		var fieldRef string
		if chainField == "timestamp" || chainField == normLogColumn || chainField == "log_id" || chainField == "normalizer" {
			fieldRef = chainField
		} else if e := ctx.Registry.Get(chainField); e != nil && e.Kind == FieldKindJoined && ctx.Plan.ModelLookupAtScan {
			// A model column is per-row after the scan-level join; group by it directly.
			fieldRef = chainField
		} else {
			// chain() groups by this field; a bare Dynamic subcolumn errors 44.
			fieldRef = groupableCast(jsonFieldRef(chainField))
		}
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fmt.Sprintf("%s AS %s", fieldRef, safeField)})
		source.Layer.GroupBy = append(source.Layer.GroupBy, fieldRef)
		ctx.Registry.SetResolveExpr(safeField, fieldRef)
		meta.EntityColumn = safeField
		meta.EntityExpr = fieldRef
	}

	// SELECT chain_count
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
		Expr: fmt.Sprintf("%s AS chain_count", countExpr),
	})

	// Millisecond timestamps of one matching sequence, aligned to the steps. Keys the
	// exact events behind a match (alert evidence, drilldown) off the logs sorting key;
	// hidden from field order. The HAVING guarantees a full-length array, since
	// sequenceMatchEvents on its own also emits partial chains.
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
		Expr: fmt.Sprintf("%s AS %s", anchorExpr, chainAnchorColumn),
	})
	ctx.Registry.Register(chainAnchorColumn, FieldKindAggregate, chainAnchorColumn, ctx.CmdIndex)
	ctx.Registry.SetResolveExpr(chainAnchorColumn, anchorExpr)

	// Completion marker for windowed evaluators, in the same time basis the query
	// is scoped by, so it is comparable with the window bounds. Latest step-matching
	// event, which errs toward re-reporting rather than dropping a real match.
	scopeTs := "timestamp"
	if ctx.Opts.UseIngestTimestamp {
		scopeTs = "ingest_timestamp"
	}
	doneExpr := fmt.Sprintf("maxIf(toUnixTimestamp64Milli(%s), %s)", scopeTs, stepUnion)
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{
		Expr: fmt.Sprintf("%s AS %s", doneExpr, chainDoneColumn),
	})
	ctx.Registry.Register(chainDoneColumn, FieldKindAggregate, chainDoneColumn, ctx.CmdIndex)
	ctx.Registry.SetResolveExpr(chainDoneColumn, doneExpr)

	// HAVING: only groups where the sequence matched
	if ordered {
		source.Layer.Having = append(source.Layer.Having, matchExpr+" = 1")
	} else {
		source.Layer.Having = append(source.Layer.Having, matchExpr)
	}

	ctx.Plan.IsAggregated = true
	ctx.Plan.IsChain = true
	ctx.Plan.Chain = meta
	ctx.Registry.SetResolveExpr("chain_count", countExpr)
	source.Layer.OrderBy = append(source.Layer.OrderBy, "chain_count DESC")
	return nil
}

// heatmapHandler handles heatmap(x=field, y=field, value=count(), limit=N)
type heatmapHandler struct{}

func (h *heatmapHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *heatmapHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	xArg, hasX := b.First("x")
	yArg, hasY := b.First("y")
	xField, yField := xArg.Value(), yArg.Value()
	if !hasX {
		xField = ""
	}
	if !hasY {
		yField = ""
	}
	value := b.Agg("value")
	limit := 50
	if n := b.Int("limit", 0); n > 0 {
		limit = min(n, 200)
	}
	if xField == "" || yField == "" {
		return fmt.Errorf("heatmap() requires x= and y= parameters")
	}

	ctx.Plan.ChartType = "heatmap"
	ctx.Plan.ChartConfig["xField"] = xField
	ctx.Plan.ChartConfig["yField"] = yField
	ctx.Plan.ChartConfig["limit"] = limit

	source := ctx.Plan.CurrentStage()

	if len(source.Layer.GroupBy) > 0 || ctx.Plan.IsAggregated {
		// Visualization-only mode: data already aggregated
		return nil
	}

	// Standalone mode: heatmap does its own aggregation
	xRef, err := ResolveArg(xArg, ctx.Registry)
	if err != nil {
		return fmt.Errorf("heatmap(): %w", err)
	}
	yRef, err := ResolveArg(yArg, ctx.Registry)
	if err != nil {
		return fmt.Errorf("heatmap(): %w", err)
	}

	source.Layer.Selects = append(source.Layer.Selects,
		SelectExpr{Expr: fmt.Sprintf("%s AS _heatmap_x", xRef)},
		SelectExpr{Expr: fmt.Sprintf("%s AS _heatmap_y", yRef)},
	)

	valueExpr := "COUNT(*)"
	if value != nil {
		switch name := strings.ToLower(value.Name); name {
		case "count":
		case "sum", "avg", "max", "min":
			if len(value.Args) == 0 {
				return fmt.Errorf("heatmap(): %s() needs a field", name)
			}
			cast, err := aggOperandNumeric(value.Args[0], ctx.Registry)
			if err != nil {
				return fmt.Errorf("heatmap(): %w", err)
			}
			valueExpr = fmt.Sprintf("%s(%s)", name, cast)
		default:
			return fmt.Errorf("heatmap(): value= accepts count(), sum(), avg(), max() or min(), got %s()", value.Name)
		}
	}
	source.Layer.Selects = append(source.Layer.Selects,
		SelectExpr{Expr: valueExpr + " AS _heatmap_value"})

	source.Layer.GroupBy = append(source.Layer.GroupBy, xRef, yRef)
	source.Layer.OrderBy = append(source.Layer.OrderBy, "_heatmap_value DESC")
	ctx.Plan.IsAggregated = true
	source.Layer.Limit = fmt.Sprintf("LIMIT %d", limit*limit)

	ctx.Registry.SetResolveExpr("_heatmap_x", xRef)
	ctx.Registry.SetResolveExpr("_heatmap_y", yRef)
	return nil
}

func init() {
	registerCommand(&tableHandler{}, "table")
	registerCommand(&ptgHandler{}, "ptg")
	registerCommand(&analyzefieldsHandler{}, "analyzefields")
	registerCommand(&chainHandler{}, "chain")
	registerAggregatingCommand(&heatmapHandler{}, "heatmap")
}

func init() {
	registerSpec(&CommandSpec{Name: "table", Params: []ParamSpec{fields("fields"), namedLit("limit")}})
	registerSpec(&CommandSpec{Name: "analyzefields", Params: []ParamSpec{fields("fields"), namedLit("limit")}})
	// chain(field, ...) { steps } and case { branches } carry a block the handler
	// parses itself.
	registerSpec(&CommandSpec{Name: "chain", FreeForm: true, Params: []ParamSpec{
		fields("fields"), namedLit("within"), namedLit("sequence"), namedLit("order"),
	}})
	registerSpec(&CommandSpec{Name: "case", FreeForm: true, Params: []ParamSpec{reqLit("branches")}})
	registerSpec(&CommandSpec{Name: "ptg", Params: []ParamSpec{
		ParamSpec{Name: "start", Kind: ParamLiteral, Required: true},
		namedLit("depth"), namedLit("direction"),
	}})
}

// tableAggregateSelects renders an aggregate written inside table(). These are
// not scalar functions: each has its own alias shape, which is part of the
// response contract, so they are spelled out rather than derived.
func tableAggregateSelects(arg Argument, ctx *CommandContext) ([]SelectExpr, bool, error) {
	name, operand, ok := aggregateCall(arg)
	if !ok {
		return nil, false, nil
	}
	if name == "count" {
		return []SelectExpr{{Expr: "COUNT(*) AS _count"}}, true, nil
	}
	if operand == nil {
		return nil, false, nil
	}
	inner := operand.FieldName()
	cast, err := aggOperandNumeric(*operand, ctx.Registry)
	if err != nil {
		return nil, false, err
	}
	sel := func(format string, args ...any) ([]SelectExpr, bool, error) {
		return []SelectExpr{{Expr: fmt.Sprintf(format, args...)}}, true, nil
	}
	// named renders an aggregate under the one alias rule every aggregate uses,
	// so table(median(x)) and multi(median(x)) name their column the same.
	named := func(agg, format string) ([]SelectExpr, bool, error) {
		alias, err := aggregateAlias("", agg)
		if err != nil {
			return nil, false, err
		}
		return sel(format, cast, alias)
	}

	switch name {
	case "sum":
		return sel("sum(%s) AS _sum", cast)
	case "avg":
		return sel("avg(%s) AS _avg", cast)
	case "max":
		if inner == "timestamp" {
			return sel("max(timestamp) AS _max")
		}
		return sel("max(%s) AS _max", cast)
	case "min":
		if inner == "timestamp" {
			return sel("min(timestamp) AS _min")
		}
		return sel("min(%s) AS _min", cast)
	case "percentile":
		return named("percentile", "quantiles(0.5, 0.75, 0.99)(%s) AS %s")
	case "stddev":
		return named("stddev", "stddevPop(%s) AS %s")
	case "median":
		return named("median", "median(%s) AS %s")
	case "mad":
		alias, err := aggregateAlias("", "mad")
		if err != nil {
			return nil, false, err
		}
		return sel("arrayReduce('median', arrayMap(x -> abs(x - arrayReduce('median', groupArray(%[1]s))), groupArray(%[1]s))) AS %[2]s", cast, alias)
	case "skew", "skewness":
		return named("skewness", "skewPop(%s) AS %s")
	case "kurt", "kurtosis":
		return named("kurtosis", "kurtPop(%s) AS %s")
	case "iqr":
		return []SelectExpr{
			{Expr: fmt.Sprintf("quantile(0.25)(%s) AS _q1", cast)},
			{Expr: fmt.Sprintf("quantile(0.75)(%s) AS _q3", cast)},
			{Expr: fmt.Sprintf("quantile(0.75)(%s) - quantile(0.25)(%s) AS _iqr", cast, cast)},
		}, true, nil
	}
	return nil, false, nil
}

// aggregateCall reads an argument written as an aggregate call, returning its
// lowercased name and first operand. The bare keyword "count" counts as count().
func aggregateCall(arg Argument) (string, *Argument, bool) {
	if arg.FieldName() == "count" {
		return "count", nil, true
	}
	switch arg.Kind {
	case ArgAggSpec:
		if arg.Agg == nil {
			return "", nil, false
		}
		if len(arg.Agg.Args) == 0 {
			return strings.ToLower(arg.Agg.Name), nil, true
		}
		operand := arg.Agg.Args[0]
		return strings.ToLower(arg.Agg.Name), &operand, true
	case ArgExpr:
		if arg.Expr == nil || arg.Expr.Kind != ExprCall {
			return "", nil, false
		}
		if len(arg.Expr.Args) == 0 {
			return strings.ToLower(arg.Expr.Value), nil, true
		}
		operand := Argument{Kind: ArgExpr, Expr: arg.Expr.Args[0]}
		if inner := arg.Expr.Args[0]; inner.Kind == ExprString {
			// sum("bytes") names the field, the same as sum(bytes): an aggregate's
			// operand is a field position, so a quoted value is not a literal.
			operand = Argument{Kind: ArgLiteral, Text: inner.Value, Quoted: true, Pos: inner.Pos}
		}
		return strings.ToLower(arg.Expr.Value), &operand, true
	}
	return "", nil, false
}
