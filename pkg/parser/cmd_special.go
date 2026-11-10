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
	ctx.Plan.table.present = true
	return nil
}

func (h *tableHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.table.present = true
	source := ctx.Plan.CurrentStage()

	// When a prior command already aggregated (groupby, multi, stats, etc.),
	// table() is a pure projection over the existing outputs: keep their SELECT
	// expressions and just restrict/reorder to the requested columns. Re-deriving
	// them as raw JSON fields (fields.`x`) or re-adding them to GROUP BY would be
	// wrong, since they are computed columns of this stage, not log columns.
	if ctx.Plan.IsAggregated || len(source.Layer.GroupBy) > 0 {
		return h.executeProjection(cmd, ctx, source)
	}

	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	if n, err := validateInt(b.Str("limit", "")); err == nil {
		source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	}
	if err := projectColumns(ctx, source, b.Flat("fields"), true); err != nil {
		return fmt.Errorf("table(): %w", err)
	}
	return nil
}

// projectColumns replaces a raw-row stage's projection with the named columns.
// With aggregates, an aggregate call (count, sum(bytes)) groups by the others;
// without, every argument is a column name.
func projectColumns(ctx *CommandContext, source *QueryStage, args []Argument, aggregates bool) error {
	readsFrom := ctx.Registry.mark()
	ctx.Plan.table.present = true
	// modifiedzscore()/madoutlier() window layers wrap this projection and read
	// their value column from it.
	var windowValue []SelectExpr
	for _, sel := range source.Layer.Selects {
		if sel.Name() == windowValueColumn {
			windowValue = append(windowValue, sel)
		}
	}
	source.Layer.Selects = nil

	var nonAggregateFields []string

	for _, arg := range args {
		ctx.Plan.table.explicit = true
		ctx.Plan.table.columns = append(ctx.Plan.table.columns, strings.Trim(arg.Value(), "`"))
		field := arg.FieldName()

		if aggregates {
			if selects, ok, err := tableAggregateSelects(arg, ctx); err != nil {
				return err
			} else if ok {
				source.Layer.Selects = append(source.Layer.Selects, selects...)
				ctx.Plan.IsAggregated = true
				continue
			}
		}

		if entry := ctx.Registry.Get(field); entry != nil && entry.Kind == FieldKindWindow {
			// A window output is added by the layer that wraps this projection.
			continue
		} else if entry != nil && entry.Kind == FieldKindBase {
			// A base column is a real column, not a fields.`x` sub-path: as JSON it read
			// empty on every row, and aliasing that empty over the column's own name
			// shadowed the column for anything selecting it later in the same SELECT.
			// An eval() rebinding the name re-registers it as per-row, so it lands below.
			sel := SelectExpr{Expr: ctx.Registry.Column(field).sql}
			if sel.Expr != field {
				sel.Alias = field
			}
			source.Layer.Selects = append(source.Layer.Selects, sel)
			nonAggregateFields = append(nonAggregateFields, field)
		} else if entry := ctx.Registry.Get(field); entry != nil && entry.Kind == FieldKindJoined {
			// Under the scan-level model join the column is per row in this SELECT,
			// so it is projected (and grouped, when table() aggregates) like any
			// other. The join() wrapper adds its columns outside this SELECT instead:
			// projecting fields.`x` here would shadow them, so they are only carried
			// in FieldOrder.
			if ctx.Plan.lookup.AtScan {
				source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: field})
				nonAggregateFields = append(nonAggregateFields, field)
				continue
			}
			ctx.Plan.table.joinedFields = append(ctx.Plan.table.joinedFields, field)
			continue
		} else if entry := ctx.Registry.Get(field); entry != nil && (entry.Kind == FieldKindPerRow || entry.Kind == FieldKindAssignment) {
			safeAlias, err := ArgAlias(arg)
			if err != nil {
				return err
			}
			computedExpr := ctx.Registry.Resolve(field)
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: computedExpr, Alias: safeAlias})
			nonAggregateFields = append(nonAggregateFields, field)
		} else if ctx.Opts.SourceSubquery != "" {
			// Over a subquery source (a source command like pgr()) every field is a flat
			// column; resolve bare via the registry rather than as a fields.`x` JSON path.
			safeAlias, err := ArgAlias(arg)
			if err != nil {
				return err
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: ctx.Registry.Resolve(field), Alias: safeAlias})
			nonAggregateFields = append(nonAggregateFields, field)
		} else {
			safeAlias, err := ArgAlias(arg)
			if err != nil {
				return err
			}
			ref := ctx.Registry.Stored(field).sql
			if field == "" {
				if ref, err = ResolveArg(arg, ctx.Registry); err != nil {
					return err
				}
				// Register the derived alias, as groupby() does. Without it a later
				// sort/dedup/filter on the name resolved it as a JSON sub-column
				// that exists on no row.
				alias := strings.Trim(safeAlias, "`")
				ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
				ctx.Registry.define(alias, alias, readsFrom)
				nonAggregateFields = append(nonAggregateFields, alias)
				source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: ref, Alias: safeAlias})
				continue
			}
			source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: ref, Alias: safeAlias})
			nonAggregateFields = append(nonAggregateFields, field)
		}
	}

	// Auto GROUP BY for non-aggregate fields when table() includes aggregations
	if ctx.Plan.IsAggregated && len(nonAggregateFields) > 0 {
		for _, field := range nonAggregateFields {
			if field == "timestamp" {
				source.Layer.groupBy(ctx.Registry.Column("timestamp").sql, field)
			} else {
				source.Layer.groupBy(ctx.Registry.Resolve(field), field)
			}
		}
	}
	if !ctx.Plan.IsAggregated {
		source.Layer.Selects = append(source.Layer.Selects, windowValue...)
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
	ctx.Plan.aggregationOutputs = make(map[string]bool)
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
		ctx.Plan.table.explicit = true
		ctx.Plan.table.columns = append(ctx.Plan.table.columns, strings.Trim(arg.Value(), "`"))

		// The bare count keyword maps to the prior stage's default _count output.
		name := strings.Trim(arg.Value(), "`")
		if aggName, _, ok := aggregateCall(arg); ok && aggName == "count" {
			name = "_count"
		}
		if prevOutputs[name] {
			newStage.Layer.Selects = append(newStage.Layer.Selects, SelectExpr{Expr: name})
			continue
		}
		// A model column the aggregation did not output: nothing re-adds it, so
		// error rather than emit a bare reference this stage cannot resolve.
		if contains(ctx.Plan.lookup.Outputs, name) {
			if ctx.Plan.lookup.AtScan {
				return fmt.Errorf("table(): model column %q is not available after the aggregation; group by it or aggregate it", name)
			}
			return fmt.Errorf("table(): column %q is read before %s() adds it", name, ctx.Plan.lookup.name)
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
		newStage.Layer.Selects = append(newStage.Layer.Selects, SelectExpr{Expr: name, Alias: safe})
	}
	return nil
}

// ptgHandler handles ptg() (Process Tree Graph): MV-backed process-lineage traversal
// over proc_lineage.
type ptgHandler struct{}

func (h *ptgHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	// Set process-tree mode early so condition routing sends _depth/_path filters to HAVING.
	if !ctx.Plan.isProcessTree() {
		ctx.Plan.special = &processTreeMode{}
	}
	ctx.Registry.Register("_depth", FieldKindWindow, "_depth", ctx.CmdIndex)
	ctx.Registry.Register("_path", FieldKindWindow, "_path", ctx.CmdIndex)
	return nil
}

func (h *ptgHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	readsFrom := ctx.Registry.mark()
	mode, _ := ctx.Plan.special.(*processTreeMode)
	if mode == nil {
		mode = &processTreeMode{}
		ctx.Plan.special = mode
	}
	if mode.start != "" {
		return fmt.Errorf("cannot use multiple ptg() functions in the same query")
	}
	p, err := ParseProcessTreeParams(cmd)
	if err != nil {
		return err
	}
	mode.start = p.Start
	ctx.Registry.define("_depth", "_depth", readsFrom)
	ctx.Registry.define("_path", "_path", readsFrom)
	return nil
}

// analyzeFieldsMaxScan caps the rows analyzeFields() samples, including limit=max.
const analyzeFieldsMaxScan = 200000

// analyzefieldsHandler handles analyzefields(field1, field2, limit=N)
type analyzefieldsHandler struct{}

func (h *analyzefieldsHandler) buildsOutputLater() {}

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
	if ctx.Plan.isProcessTree() {
		return fmt.Errorf("analyzeFields() cannot be combined with ptg()")
	}
	mode, _ := ctx.Plan.special.(*analyzeMode)
	if mode == nil {
		mode = &analyzeMode{scanLimit: analyzeDefaultScan}
		ctx.Plan.special = mode
	}
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
			mode.scanLimit = min(n, analyzeFieldsMaxScan)
		}
	}
	mode.fields = append(mode.fields, b.Strings("fields")...)
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
	// chain() matches events, which a stage after an aggregation no longer has.
	if !ctx.Plan.CurrentStage().IsSource {
		return fmt.Errorf("chain() matches events, so it must come before the aggregation it follows")
	}
	// CSV keeps the legacy quoted spelling, chain("user,host"), meaning two fields.
	chainFields := b.CSV("fields")
	if len(chainFields) == 0 {
		return fmt.Errorf("chain() requires grouping field(s) and step definitions")
	}
	withinSeconds := chainWithinSeconds(b)
	ordered, err := chainOrdered(b)
	if err != nil {
		return err
	}
	if !ordered && withinSeconds > 0 {
		// An unordered window needs a sliding span over the matching events, which
		// no single aggregate expresses; approximating it drops real matches.
		return fmt.Errorf("chain(): within= cannot be combined with sequence=any")
	}
	var steps, stepFields []string
	stepReads, err := ctx.Registry.track(func() error {
		var err error
		steps, stepFields, err = compileChainSteps(cmd.Sequence, ctx.Opts, ctx.Registry)
		return err
	})
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
		if e := ctx.declared().Get(f); e != nil && e.Kind == FieldKindJoined {
			stepsUseModel = f
			break
		}
	}
	if stepsUseModel != "" && !ctx.Plan.lookup.AtScan {
		return fmt.Errorf("chain(): step field %q is a model_lookup() column and requires model_lookup() before chain() in the pipeline", stepsUseModel)
	}

	m := newChainMatch(steps, stepReads.deps(), withinSeconds, ordered)
	// Narrow the scan to rows a step can use: the sequence aggregates skip events
	// matching no condition and an entity with none fails the HAVING, so the result
	// is unchanged while GROUP BY state drops to just the matching entities.
	// Only when chain() aggregates the scan itself: behind an earlier aggregation
	// this WHERE would filter that stage's input and silently change its counts.
	// A union referencing model columns can only run after the scan-level join
	// (strict mode's key prefilter still narrows the scan itself).
	if len(ctx.Plan.Stages) == 1 {
		if stepsUseModel != "" {
			ctx.Plan.lookup.PostJoinWhere = append(ctx.Plan.lookup.PostJoinWhere, postJoinFilter{At: ctx.CmdIndex, Pred: group(m.union)})
		} else {
			scan := &ctx.Plan.SourceStage().Layer
			scan.Where = append(scan.Where, group(m.union))
		}
	}
	meta := &ChainMeta{AnchorColumn: chainAnchorColumn, StepConditions: steps, StepFields: stepFields}
	if err := groupChainEntities(ctx, chainFields, meta); err != nil {
		return err
	}
	projectChainMatch(ctx, m)
	ctx.Plan.IsAggregated = true
	ctx.Plan.CurrentStage().IsChain = true
	ctx.Plan.Chain = meta
	return nil
}

// chainMatch is the SQL that matches a chain's steps within each entity.
type chainMatch struct {
	count  string    // matched sequences
	match  predicate // whether the entity matched
	anchor string    // the time of one event per step
	union  predicate // any step's condition
}

// newChainMatch builds the match over the compiled steps. Ordered mode sequences
// the steps, within= apart at most; unordered mode only requires each step to
// occur, so it counts complete co-occurrence sets and anchors on each step's
// earliest event.
func newChainMatch(steps []string, stepDeps deps, withinSeconds int, ordered bool) chainMatch {
	// Millisecond ordering: toDateTime() leaves same-second event order undefined,
	// which misorders spawn-then-connect sequences. UInt64 millis; the aggregate
	// rejects DateTime64 and Int64.
	const tsExpr = "toUInt64(toUnixTimestamp64Milli(timestamp))"
	parenSteps := make([]predicate, len(steps))
	for i, st := range steps {
		// Each step is parenthesized so the union holds whatever shape it compiled to.
		parenSteps[i] = atom("(" + st + ")")
		parenSteps[i].deps = stepDeps
	}
	m := chainMatch{union: orOf(parenSteps...)}
	// The match also reads the scan's timestamp.
	matchDeps := stepDeps.and(columnRef("timestamp").deps)
	if !ordered {
		counts := make([]string, len(steps))
		presence := make([]string, len(steps))
		anchors := make([]string, len(steps))
		for i, cond := range steps {
			counts[i] = fmt.Sprintf("countIf(%s)", cond)
			presence[i] = fmt.Sprintf("countIf(%s) > 0", cond)
			anchors[i] = fmt.Sprintf("minIf(%s, %s)", tsExpr, cond)
		}
		m.count = fmt.Sprintf("least(%s)", strings.Join(counts, ", "))
		m.match = andOf(atoms(presence...)...)
		m.match.deps = matchDeps
		m.anchor = fmt.Sprintf("[%s]", strings.Join(anchors, ", "))
		return m
	}
	// (?t<=N) uses the units of tsExpr (milliseconds).
	var pattern strings.Builder
	for i := range steps {
		if i > 0 && withinSeconds > 0 {
			fmt.Fprintf(&pattern, "(?t<=%d)", withinSeconds*1000)
		}
		fmt.Fprintf(&pattern, "(?%d)", i+1)
	}
	args := fmt.Sprintf("(%s)(%s, %s)", sqlString(pattern.String()), tsExpr, strings.Join(steps, ", "))
	m.count, m.match, m.anchor = "sequenceCount"+args, atom("sequenceMatch"+args+" = 1"), "sequenceMatchEvents"+args
	m.match.deps = matchDeps
	return m
}

// groupChainEntities groups the scan by the chain's entity. Several fields name
// the same entity (user, source_user, target_user): arrayJoin expands each row
// into one per non-empty value, so an event lands in every entity group it
// belongs to.
func groupChainEntities(ctx *CommandContext, chainFields []string, meta *ChainMeta) error {
	readsFrom := ctx.Registry.mark()
	source := ctx.Plan.CurrentStage()
	// ref is the entity's reference, and whether it is the stored field (a String).
	ref := func(f string) (string, bool) {
		switch e := ctx.Registry.Get(f); {
		case f == "timestamp" || f == normLogColumn || f == "log_id" || f == "normalizer":
			return ctx.Registry.Column(f).sql, false
		case e != nil && e.Kind == FieldKindJoined && ctx.Plan.lookup.AtScan:
			// A model column is per-row after the scan-level join.
			return ctx.Registry.Column(f).sql, false
		default:
			// The value is grouped by, and a bare Dynamic subcolumn errors 44.
			return ctx.Registry.jsonRef(f).sql, true
		}
	}
	if len(chainFields) == 1 {
		chainField := chainFields[0]
		safeField, err := sanitizeIdentifier(chainField)
		if err != nil {
			return fmt.Errorf("chain(): invalid field name: %w", err)
		}
		fieldRef, text := ref(chainField)
		source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: fieldRef, Alias: safeField})
		source.Layer.groupBy(fieldRef, chainField)
		ctx.Registry.define(safeField, fieldRef, readsFrom)
		if text {
			ctx.Registry.setText(safeField)
		}
		ctx.Registry.AddGroupKeyAlias(chainField, safeField)
		meta.EntityColumn, meta.EntityExpr = safeField, fieldRef
		return nil
	}
	elems := make([]string, len(chainFields))
	for i, f := range chainFields {
		elems[i], _ = ref(f)
		if e := ctx.Registry.Get(f); e != nil && e.Kind == FieldKindJoined && ctx.Plan.lookup.AtScan {
			elems[i] = fmt.Sprintf("toString(%s)", f) // the identity array holds strings
		}
	}
	entityExpr := fmt.Sprintf("arrayJoin(arrayFilter(x -> x != '', [%s]))", strings.Join(elems, ", "))
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: entityExpr, Alias: "_entity"})
	source.Layer.groupBy("_entity", "_entity")
	ctx.Registry.Register("_entity", FieldKindPerRow, "_entity", ctx.CmdIndex)
	ctx.Registry.define("_entity", entityExpr, readsFrom)
	ctx.Registry.AddGroupKeyAlias("_entity", "_entity")
	meta.EntityColumn, meta.MultiIdentity = "_entity", true
	return nil
}

// projectChainMatch projects a chain's count, anchor and completion columns and
// keeps only the entities that matched.
func projectChainMatch(ctx *CommandContext, m chainMatch) {
	readsFrom := ctx.Registry.mark()
	source := ctx.Plan.CurrentStage()
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: m.count, Alias: "chain_count"})
	// Millisecond timestamps of one matching sequence, aligned to the steps. Keys the
	// exact events behind a match (alert evidence, drilldown) off the logs sorting key;
	// hidden from field order. The HAVING guarantees a full-length array, since
	// sequenceMatchEvents on its own also emits partial chains.
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: m.anchor, Alias: chainAnchorColumn})
	ctx.Registry.Register(chainAnchorColumn, FieldKindAggregate, chainAnchorColumn, ctx.CmdIndex)
	ctx.Registry.define(chainAnchorColumn, m.anchor, readsFrom)
	// Completion marker for windowed evaluators, in the same time basis the query
	// is scoped by, so it is comparable with the window bounds. Latest step-matching
	// event, which errs toward re-reporting rather than dropping a real match.
	scopeTs := "timestamp"
	if ctx.Opts.UseIngestTimestamp {
		scopeTs = "ingest_timestamp"
	}
	doneExpr := fmt.Sprintf("maxIf(toUnixTimestamp64Milli(%s), %s)", scopeTs, m.union.sql)
	source.Layer.Selects = append(source.Layer.Selects, SelectExpr{Expr: doneExpr, Alias: chainDoneColumn})
	ctx.Registry.Register(chainDoneColumn, FieldKindAggregate, chainDoneColumn, ctx.CmdIndex)
	ctx.Registry.define(chainDoneColumn, doneExpr, readsFrom)
	// Only the groups where the sequence matched.
	source.Layer.Having = append(source.Layer.Having, m.match)
	ctx.Registry.define("chain_count", m.count, readsFrom)
	ctx.Registry.setReads("chain_count", m.match.deps) // the steps over timestamp, as the match
	source.Layer.OrderBy = append(source.Layer.OrderBy, desc("chain_count"))
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

	ctx.Plan.chart.kind = "heatmap"
	ctx.Plan.chart.config["xField"] = xField
	ctx.Plan.chart.config["yField"] = yField
	ctx.Plan.chart.config["limit"] = limit

	source := ctx.Plan.CurrentStage()

	if len(source.Layer.GroupBy) > 0 || ctx.Plan.IsAggregated {
		// Visualization-only mode: data already aggregated
		return nil
	}

	// Standalone mode: heatmap does its own aggregation
	xFrom := ctx.Registry.mark()
	xRef, err := ResolveArg(xArg, ctx.Registry)
	if err != nil {
		return fmt.Errorf("heatmap(): %w", err)
	}
	xReads := ctx.Registry.since(xFrom)
	yFrom := ctx.Registry.mark()
	yRef, err := ResolveArg(yArg, ctx.Registry)
	if err != nil {
		return fmt.Errorf("heatmap(): %w", err)
	}
	yReads := ctx.Registry.since(yFrom)

	source.Layer.Selects = append(source.Layer.Selects,
		SelectExpr{Expr: xRef, Alias: "_heatmap_x"},
		SelectExpr{Expr: yRef, Alias: "_heatmap_y"},
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
		SelectExpr{Expr: valueExpr, Alias: "_heatmap_value"})

	source.Layer.groupBy(xRef, "_heatmap_x")
	source.Layer.groupBy(yRef, "_heatmap_y")
	source.Layer.OrderBy = append(source.Layer.OrderBy, desc("_heatmap_value"))
	ctx.Registry.Register("_heatmap_value", FieldKindAggregate, "_heatmap_value", ctx.CmdIndex)
	declareChartKey(ctx, "_heatmap_x", "_heatmap_x")
	declareChartKey(ctx, "_heatmap_y", "_heatmap_y")
	ctx.Plan.IsAggregated = true
	source.Layer.Limit = fmt.Sprintf("LIMIT %d", limit*limit)

	ctx.Registry.SetResolveExpr("_heatmap_x", xRef)
	ctx.Registry.setRead("_heatmap_x", xReads)
	ctx.Registry.SetResolveExpr("_heatmap_y", yRef)
	ctx.Registry.setRead("_heatmap_y", yReads)
	return nil
}

func init() {
	registerCommand(&tableHandler{}, "table")
	registerCommand(&ptgHandler{}, "ptg")
	registerAggregatingCommand(&analyzefieldsHandler{}, "analyzefields")
	registerAggregatingCommand(&chainHandler{}, "chain")
	registerAggregatingCommand(&heatmapHandler{}, "heatmap")
}

func init() {
	registerSpec(&CommandSpec{Name: "table", Params: []ParamSpec{fields("fields"), namedLit("limit")}})
	registerSpec(&CommandSpec{Name: "analyzefields", Params: []ParamSpec{fields("fields"), namedLit("limit")}})
	// chain's step block is kept out of its arguments; case { branches } has no
	// argument list, only a block the handler parses itself.
	registerSpec(&CommandSpec{Name: "chain", Params: []ParamSpec{
		fields("fields"), namedLit("within"), namedLit("sequence"), namedLit("order"),
	}})
	registerSpec(&CommandSpec{Name: "case", FreeForm: true, Params: []ParamSpec{reqLit("branches")}})
	registerSpec(&CommandSpec{Name: "ptg", Params: []ParamSpec{
		ParamSpec{Name: "start", Kind: ParamLiteral, Required: true},
		namedLit("depth"), namedLit("direction"),
	}})
}

// tableAggregates are the aggregates table() accepts as a column.
var tableAggregates = map[string]bool{
	"count": true, "sum": true, "avg": true, "max": true, "min": true, "percentile": true, "stddev": true,
	"median": true, "mad": true, "skew": true, "skewness": true, "kurt": true, "kurtosis": true, "iqr": true,
}

// tableAggregateSelects renders an aggregate written inside table() as the same
// aggregate spec renders anywhere else, under its default name: table(median(x))
// and multi(median(x)) name their column the same. count counts rows.
func tableAggregateSelects(arg Argument, ctx *CommandContext) ([]SelectExpr, bool, error) {
	name, operand, ok := aggregateCall(arg)
	if !ok || !tableAggregates[name] || name != "count" && operand == nil {
		return nil, false, nil
	}
	if name == "count" {
		return []SelectExpr{aggSelect(countAll, "_count")}, true, nil
	}
	var selects []SelectExpr
	spec := &AggSpec{Name: name, Args: []Argument{*operand}}
	if _, err := processAggSpec(spec, &selects, map[string]bool{}, ctx.Registry); err != nil {
		return nil, false, err
	}
	return selects, true, nil
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
