package parser

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// classifyConditions routes filters into pending buckets based on
// FieldKind. Runs AFTER all Declare() calls so the registry has field metadata
// for classification. SQL generation is deferred to materializeConditions
// (after Execute) so that PerRow handlers have set their real expressions.
//
// Routing rules by FieldKind:
//   - FieldKindAggregate  -> HAVING (when aggregation present), else WHERE
//   - FieldKindWindow     -> DeferredWhere (post-window filter), or HAVING for traversal
//   - FieldKindPerRow     -> WHERE (inlined expression; string-typed, needs textToNumber)
//   - FieldKindAssignment -> WHERE (inlined expression; already numeric, no coercion needed)
//   - Base/JSON/unknown   -> WHERE
//
// The HAVING/WHERE boundary is purely about aggregation stage: only FieldKindAggregate
// fields are produced after GROUP BY and therefore require HAVING. Assignment and PerRow
// fields are both per-row scalars computed before aggregation; they differ only in whether
// the value needs numeric coercion, not in SQL placement.
func classifyConditions(conditions []Condition, registry *FieldRegistry, plan *QueryPlan) error {
	if len(conditions) == 0 {
		return nil
	}

	willHaveAggregation := plan.IsAggregated || plan.HasGroupBy

	for _, cond := range conditions {
		registry := plan.registryAt(cond.at, registry)
		// Compound nodes: inspect all leaf fields to determine the highest-priority
		// target. A compound normally stays a unit, since its children are connected
		// by AND/OR and the operators bind them together.
		if cond.IsCompound() {
			// The exception is a pure-AND compound whose conjuncts belong to
			// different stages: AND distributes, so split it and bind each
			// conjunct where its fields exist.
			if parts := splitMixedAndCompound(cond, registry, plan, willHaveAggregation); parts != nil {
				if err := classifyConditions(parts, registry, plan); err != nil {
					return err
				}
				continue
			}
			// A compound that stays whole binds to one stage, so its leaves face the
			// same constraint as a standalone one. Checking only the leaf path let an
			// OR carry a per-row column into HAVING (ClickHouse code 215), which is
			// the error this check exists to replace with a clear message.
			if err := validateCompoundOperandStages(cond, registry, plan, willHaveAggregation); err != nil {
				return err
			}
			// Compile the expression leaves nested in this compound. Only top-level
			// leaves were compiled, so a nested one reached materialisation with no
			// PredicateSQL and no Field and was dropped, taking the whole
			// parenthesised group with it: adding brackets silently widened a rule
			// to every row. The compound binds to one stage, so its leaves compile
			// at the compound's priority, not their own.
			if err := compileNestedExprLeaves(&cond, subtreePriority(cond, registry, plan, willHaveAggregation), registry, plan); err != nil {
				return err
			}
			target := classifyCompoundTarget(cond, registry, plan, willHaveAggregation)
			*target = append(*target, cond)
			continue
		}

		if cond.Expr != nil {
			compiled, priority, err := classifyExprCondition(cond, registry, plan, willHaveAggregation)
			if err != nil {
				return err
			}
			cond.PredicateSQL = compiled
			switch priority {
			case 2:
				plan.pendingHavingConditions = append(plan.pendingHavingConditions, cond)
			case 1:
				plan.pendingDeferredConditions = append(plan.pendingDeferredConditions, cond)
			default:
				plan.pendingWhereConditions = append(plan.pendingWhereConditions, cond)
			}
			continue
		}

		if err := validateOperandStages(cond, registry, plan, willHaveAggregation); err != nil {
			return err
		}

		var target *[]Condition
		switch leafPriority(cond, registry, plan, willHaveAggregation) {
		case 2:
			target = &plan.pendingHavingConditions
		case 1:
			// Window and join outputs, and fields computed from them, exist only
			// after the window or join: filter there.
			target = &plan.pendingDeferredConditions
		default:
			target = &plan.pendingWhereConditions
		}

		*target = append(*target, cond)
	}
	return nil
}

func stagePriority(c Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) int {
	if c.Expr != nil {
		return exprPriority(c.Expr, registry, plan, willHaveAggregation)
	}
	if c.IsCompound() {
		return subtreePriority(c, registry, plan, willHaveAggregation)
	}
	return leafPriority(c, registry, plan, willHaveAggregation)
}

// compileNestedExprLeaves fills in PredicateSQL for every expression leaf beneath
// a compound, at the stage the compound as a whole binds to.
func compileNestedExprLeaves(cond *Condition, priority int, registry *FieldRegistry, plan *QueryPlan) error {
	if cond.IsCompound() {
		for i := range cond.Children {
			if err := compileNestedExprLeaves(&cond.Children[i], priority, registry, plan); err != nil {
				return err
			}
		}
		return nil
	}
	if cond.Expr == nil || cond.PredicateSQL != "" {
		return nil
	}
	var sql string
	var err error
	switch {
	case cond.Operator != "":
		sql, err = exprMatchSQL(*cond, registry)
	case priority == 1:
		sql, err = exprConditionSQLDeferred(cond.Expr, registry, plan.deferredScope(), cond.Negate)
	default:
		sql, err = exprConditionSQL(cond.Expr, registry, cond.Negate)
	}
	if err != nil {
		return err
	}
	cond.PredicateSQL = sql
	return nil
}

// classifyExprCondition compiles an expression filter and reports the stage it
// binds to. One bound above the scan (a window or join() output) reads its source
// leaves through the deferredScope export.
func classifyExprCondition(cond Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) (string, int, error) {
	priority := exprPriority(cond.Expr, registry, plan, willHaveAggregation)
	if plan.afterRowKeeping(cond.at.Cmd) {
		priority = max(priority, 1)
	}
	// A match operator makes the expression the left operand of a comparison
	// rather than a condition in its own right.
	if cond.Operator != "" {
		sql, err := exprMatchSQL(cond, registry)
		return sql, priority, err
	}
	// A deferred filter sits above the source scan, so its source-scope leaves are
	// exported through the plan's scope as they compile. Classification runs before
	// exportDeferredColumns, so the aliases allocated here are projected.
	// Under the scan-level model join a post-join filter reads the joined rows
	// directly; only a window output still sits above the scan.
	if priority == 1 && !(plan.lookup.AtScan && !hasWindowField(cond.Expr, registry)) {
		sql, err := exprConditionSQLDeferred(cond.Expr, registry, plan.deferredScope(), cond.Negate)
		return sql, priority, err
	}
	sql, err := exprConditionSQL(cond.Expr, registry, cond.Negate)
	return sql, priority, err
}

// validateCompoundOperandStages checks every leaf of a compound that binds as one
// unit. The compound's stage is the highest its leaves need, so a leaf is judged
// against that rather than against its own.
func validateCompoundOperandStages(cond Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) error {
	if subtreePriority(cond, registry, plan, willHaveAggregation) != 2 {
		return nil
	}
	var walk func(Condition) error
	walk = func(c Condition) error {
		if c.IsCompound() {
			for _, child := range c.Children {
				if err := walk(child); err != nil {
					return err
				}
			}
			return nil
		}
		return validateOperandStages(c, registry, plan, willHaveAggregation)
	}
	return walk(cond)
}

// validateOperandStages rejects a field(...) comparison that straddles the
// aggregation boundary. A comparison against an aggregate binds to HAVING, where
// a per-row field no longer exists: ClickHouse rejects it with code 215, naming
// the column but not the reason.
func validateOperandStages(cond Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) error {
	if cond.ValueField == "" || !willHaveAggregation {
		return nil
	}
	if leafPriority(cond, registry, plan, willHaveAggregation) != 2 {
		return nil
	}
	for _, side := range []string{cond.Field, cond.ValueField} {
		if fieldPriority(side, registry, plan, willHaveAggregation) == 2 {
			continue
		}
		if !isGroupKey(registry, side) {
			return fmt.Errorf("cannot compare %s with field(%s): %s is a per-row field and does not survive the aggregation; add it to groupBy()",
				cond.Field, cond.ValueField, side)
		}
		// The operand has to be addressable by name after the GROUP BY. A grouping
		// key is (its alias, quoted when the name carries a dot); anything still
		// rendering as an expression is not in the GROUP BY and ClickHouse would
		// reject it with code 215, naming the column but not the reason.
		if ref := resolveValueField(side, registry).ref; !isAddressableAfterGroupBy(ref) {
			return fmt.Errorf("cannot compare %s with field(%s): %s is grouped as an expression rather than a plain column, so it cannot be referenced after the aggregation; assign it to a simple name first",
				cond.Field, cond.ValueField, side)
		}
	}
	return nil
}

// isAddressableAfterGroupBy reports whether a reference can be named in HAVING: a
// bare column, or a single backtick-quoted identifier as a dotted grouping key is
// projected under.
func isAddressableAfterGroupBy(ref string) bool {
	ref = strings.TrimSpace(ref)
	if isBareIdentifier(ref) {
		return true
	}
	if len(ref) > 2 && ref[0] == '`' && ref[len(ref)-1] == '`' {
		return !strings.Contains(ref[1:len(ref)-1], "`")
	}
	return false
}

// isBareAggregate reports whether a name is an aggregate function used without a
// registry entry, which resolves to the numeric _count/_sum/_avg alias.
func isBareAggregate(field string) bool {
	return field == "count" || field == "sum" || field == "avg"
}

// isGroupKey reports whether a field is one of the plan's grouping keys, and so
// still addressable after the aggregation.
func isGroupKey(registry *FieldRegistry, field string) bool {
	if registry == nil {
		return false
	}
	_, ok := registry.GroupKeyAlias(field)
	return ok
}

// leafPriority returns the stage bucket a single (non-compound) condition needs:
// 0=WHERE, 1=DeferredWhere, 2=HAVING. Both operands count: `a > field(_count)`
// can only be evaluated where the later-produced side exists.
func leafPriority(c Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) int {
	p := fieldsPriority(c, registry, plan, willHaveAggregation)
	// After a window score a filter keeps scored rows: it never narrows the baseline.
	if plan.afterRowKeeping(c.at.Cmd) {
		p = max(p, 1)
	}
	return p
}

// fieldsPriority is the stage the fields a leaf reads need.
func fieldsPriority(c Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) int {
	if c.Command != nil {
		// A condition command binds where the fields it reads exist.
		p := 0
		for _, f := range conditionCommandFields(*c.Command) {
			p = max(p, fieldPriority(f, registry, plan, willHaveAggregation))
		}
		return p
	}
	p := fieldPriority(c.Field, registry, plan, willHaveAggregation)
	if c.ValueField != "" {
		if rp := fieldPriority(c.ValueField, registry, plan, willHaveAggregation); rp > p {
			return rp
		}
	}
	return p
}

// fieldPriority is leafPriority for a single field name.
func fieldPriority(field string, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) int {
	entry := registry.Get(field)
	if entry == nil {
		if registry.bareAggregate(field) && willHaveAggregation {
			return 2
		}
		return 0
	}
	switch entry.ClassifyKind() {
	case FieldKindWindow:
		if plan.isProcessTree() {
			return 2
		}
		return 1
	case FieldKindJoined:
		return 1 // after the join, like window fields
	case FieldKindAggregate:
		if willHaveAggregation {
			return 2
		}
	case FieldKindPerRow, FieldKindAssignment:
		if registry.DependsOnJoin(field) {
			return 1 // computed from a join output, so post-join as well
		}
	}
	return 0
}

// subtreePriority returns the highest leafPriority within a condition subtree.
func subtreePriority(cond Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) int {
	if !cond.IsCompound() {
		return stagePriority(cond, registry, plan, willHaveAggregation)
	}
	highest := 0
	for _, child := range cond.Children {
		if p := subtreePriority(child, registry, plan, willHaveAggregation); p > highest {
			highest = p
		}
	}
	return highest
}

// splitMixedAndCompound decomposes a compound whose conjuncts bind to different
// stages (the scan, after a window or join, HAVING), so each binds where its fields
// exist: a log-field or key predicate then reaches the scan, where skip indexes can
// prune and where it resolves at all under a join() or window wrap. Distribution
// over AND is safe; a negated node or an OR connector is not, and a compound that
// needs no split is left intact so its SQL is unchanged.
func splitMixedAndCompound(cond Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) []Condition {
	if cond.Op != CondAnd || cond.Negate {
		return nil
	}
	stages := map[int]bool{}
	for _, c := range cond.Children {
		stages[subtreePriority(c, registry, plan, willHaveAggregation)] = true
	}
	if len(stages) < 2 {
		return nil
	}
	return slices.Clone(cond.Children)
}

// classifyCompoundTarget inspects all leaf fields in a compound Condition
// and returns the highest-priority target bucket. Priority: HAVING > DeferredWhere > WHERE.
func classifyCompoundTarget(cond Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) *[]Condition {
	maxPriority := subtreePriority(cond, registry, plan, willHaveAggregation)

	// A HAVING target only works if every leaf field is available at the groupby
	// (HAVING) stage. A compound that mixes an aggregate with a column produced by
	// a later projection stage (e.g. a sprintf/concat output) cannot live in that
	// stage's HAVING; route it to WHERE so it binds to the deepest stage where all
	// its fields coexist (the aggregate is carried there as a plain column).
	if maxPriority == 2 && !compoundFitsHavingStage(cond, plan) {
		maxPriority = 0
	}

	switch maxPriority {
	case 2:
		return &plan.pendingHavingConditions
	case 1:
		return &plan.pendingDeferredConditions
	default:
		return &plan.pendingWhereConditions
	}
}

// compoundFitsHavingStage reports whether every leaf field of a compound is
// available at the HAVING (groupby) stage, i.e. none is first produced by a
// projection stage deeper than it.
func compoundFitsHavingStage(cond Condition, plan *QueryPlan) bool {
	hs := plan.havingStageIndex()
	fits := true
	var walk func(c Condition)
	walk = func(c Condition) {
		if c.IsCompound() {
			for _, child := range c.Children {
				walk(child)
			}
			return
		}
		fields := []string{c.Field}
		if c.Command != nil {
			fields = conditionCommandFields(*c.Command)
		}
		for _, f := range fields {
			if fieldFirstStage(f, plan) > hs {
				fits = false
			}
		}
	}
	walk(cond)
	return fits
}

// fieldFirstStage returns the earliest stage index that PRODUCES `field` (as an
// aggregate/group-key or a computed projection column), i.e. the shallowest
// stage whose WHERE/HAVING can reference it. A carried passthrough column (whose
// SELECT is the bare field name) forwards but does not produce the field, so it
// is skipped in favor of the earlier producing stage. Fields never materialized
// in a SELECT (base log fields, JSON sub-columns) resolve to the source stage.
func fieldFirstStage(field string, plan *QueryPlan) int {
	for i := 0; i < len(plan.Stages); i++ {
		for _, sel := range plan.Stages[i].Layer.Selects {
			if sel.Alias != "" && sel.Name() == field {
				return i
			}
		}
	}
	return 0
}

// materializeAtPositions ANDs conds, each run written at one position resolved
// against the registry there.
func materializeAtPositions(conds []Condition, registry *FieldRegistry, plan *QueryPlan, atScan bool) string {
	var clauses []string
	for i := 0; i < len(conds); {
		j := i + 1
		for j < len(conds) && conds[j].at == conds[i].at {
			j++
		}
		if clause := materializeCondGroup(conds[i:j], plan.registryAt(conds[i].at, registry), nil, atScan); clause != "" {
			clauses = append(clauses, clause)
		}
		i = j
	}
	return andJoin(clauses)
}

// scanConjuncts compiles the filters before the first command as separate scan
// WHERE entries, each AND split into its operands, so whatever reads the scan's
// filters (pgr candidate narrowing, the model key prefilter) sees every conjunct.
func scanConjuncts(conds []Condition, registry *FieldRegistry, plan *QueryPlan) []string {
	var out []string
	for _, c := range conds {
		parts := []Condition{c}
		if c.Op == CondAnd && !c.Negate {
			parts = c.Children
		}
		for _, part := range parts {
			if sql := materializeCondGroup([]Condition{part}, plan.registryAt(c.at, registry), nil, true); sql != "" {
				out = append(out, sql)
			}
		}
	}
	return out
}

// materializeConditions generates SQL from the classified pending conditions
// using the fully-populated registry (after Execute). Each condition is bound to
// the query stage that owns its referenced fields: pre-aggregation filters to the
// source stage, HAVING to the groupby stage, post-aggregation projection-column
// filters to the projection stage that produces them.
func materializeConditions(registry *FieldRegistry, plan *QueryPlan) {
	// A filter after a cap on aggregated rows reads the rows the cap kept, so even
	// an aggregate one is a WHERE above the cap rather than the group's HAVING.
	having := plan.pendingHavingConditions[:0]
	for _, c := range plan.pendingHavingConditions {
		if plan.capStageAt(c.at.Cmd) > plan.havingStageIndexAt(c.at.Cmd) {
			plan.pendingWhereConditions = append(plan.pendingWhereConditions, c)
		} else {
			having = append(having, c)
		}
	}
	plan.pendingHavingConditions = having
	// WHERE conditions bind to the stage that owns the fields they reference.
	// Pre-aggregation filters (base/JSON/pre-agg scalars) bind to the source
	// stage; a filter on a column materialized only by a post-aggregation
	// projection stage (e.g. a sprintf/concat output, or a post-agg `:=`
	// assignment) binds to that stage. Without this, such a filter lands in the
	// innermost source WHERE where the column does not exist, producing a
	// ClickHouse "unknown identifier" (or, for a carried aggregate, error 184).
	byStage := make(map[int][]Condition)
	var stageOrder []int
	// Conditions before the first command filter the raw scan, ahead of anything a
	// command adds to it.
	var firstStage []Condition
	for _, cond := range plan.pendingWhereConditions {
		if cond.at.Cmd == 0 {
			firstStage = append(firstStage, cond)
			continue
		}
		idx := whereBindingStageIndex(cond, plan)
		if _, seen := byStage[idx]; !seen {
			stageOrder = append(stageOrder, idx)
		}
		byStage[idx] = append(byStage[idx], cond)
	}
	source := plan.SourceStage()
	source.Layer.Where = slices.Insert(source.Layer.Where, min(plan.baseWhereLen, len(source.Layer.Where)), scanConjuncts(firstStage, registry, plan)...)
	for _, idx := range stageOrder {
		layer := &plan.Stages[idx].Layer
		// Filters after a row cap apply to the rows it kept, so each lands with
		// the cap before it.
		dests := []*[]string{&layer.Where}
		groups := map[*[]string][]Condition{}
		for _, cond := range byStage[idx] {
			dst := &layer.Where
			if after := layer.capAfter(cond.at.Cmd); after != nil {
				dst = after
			}
			if _, seen := groups[dst]; !seen && dst != &layer.Where {
				dests = append(dests, dst)
			}
			groups[dst] = append(groups[dst], cond)
		}
		for _, dst := range dests {
			if clause := materializeAtPositions(groups[dst], registry, plan, idx == 0 && dst == &layer.Where); clause != "" {
				*dst = append(*dst, clause)
			}
		}
	}
	// HAVING binds to the aggregation stage in effect where the filter is written.
	havingByStage := make(map[int][]Condition)
	var havingOrder []int
	for _, cond := range plan.pendingHavingConditions {
		idx := plan.havingStageIndexAt(cond.at.Cmd)
		if _, seen := havingByStage[idx]; !seen {
			havingOrder = append(havingOrder, idx)
		}
		havingByStage[idx] = append(havingByStage[idx], cond)
	}
	for _, idx := range havingOrder {
		if clause := materializeAtPositions(havingByStage[idx], registry, plan, false); clause != "" {
			plan.Stages[idx].Layer.Having = append(plan.Stages[idx].Layer.Having, clause)
		}
	}
	// Deferred conditions land above the source scan, so any source expression they
	// reference must be exported there under a hidden alias (see deferredScope).
	deferred := plan.pendingDeferredConditions
	if plan.lookup.AtScan {
		// Scan-level model join: a condition reading the join filters the joined
		// rows, where both model columns and scan fields resolve directly. Only
		// window-field conditions still need the outermost deferred layer.
		var outer, postJoin []Condition
		for _, c := range deferred {
			if condTouchesWindowField(c, registry) {
				outer = append(outer, c)
			} else {
				postJoin = append(postJoin, c)
			}
		}
		materializePostJoin(postJoin, registry, plan)
		deferred = outer
	}
	if clause := materializeCondGroup(deferred, registry, plan.deferredScope(), false); clause != "" {
		plan.DeferredWhere = append(plan.DeferredWhere, clause)
	}
}

// materializePostJoin places the filters that read the scan-level model join. Each
// run written at one position resolves against the registry there. A conjunct that
// reads only model columns is a property of the key, so a strict join applies it
// to the model side, which also narrows the scan prefilter to the keys it admits:
// the scan then reads only rows the join keeps, instead of every log in the window.
// A row cap between the lookup and the filter selects rows first, so a filter after
// one stays on the joined rows.
func materializePostJoin(conds []Condition, registry *FieldRegistry, plan *QueryPlan) {
	isModelColumn := func(c string) bool { return slices.Contains(plan.lookup.Fields, c) }
	capBetween := func(at int) bool {
		return slices.ContainsFunc(plan.SourceStage().Layer.Caps, func(c RowCap) bool {
			return c.At > plan.lookup.Cmd && c.At < at
		})
	}
	for i := 0; i < len(conds); {
		j := i + 1
		for j < len(conds) && conds[j].at == conds[i].at {
			j++
		}
		run, reg, at := conds[i:j], plan.registryAt(conds[i].at, registry), conds[i].at.Cmd
		i = j
		for _, c := range run {
			clause := materializeCondGroup([]Condition{c}, reg, nil, false)
			switch {
			case clause == "":
			case plan.lookup.Strict && !capBetween(at) && readsOnly(clause, isModelColumn):
				plan.lookup.Where = append(plan.lookup.Where, clause)
				if requiresNew(c, reg) {
					plan.lookup.keys.scoreOnlyNew()
				}
			default:
				plan.lookup.PostJoinWhere = append(plan.lookup.PostJoinWhere, postJoinFilter{At: at, SQL: clause})
			}
		}
	}
}

// requiresNew reports whether the condition holds only where the model's is_new
// is '1'.
func requiresNew(c Condition, reg *FieldRegistry) bool {
	switch {
	case c.Op == CondAnd:
		return !c.Negate && slices.ContainsFunc(c.Children, func(child Condition) bool { return requiresNew(child, reg) })
	case c.Op == CondOr:
		return false
	}
	// Rendered, so an is_new reassigned after the lookup is not mistaken for it.
	if c.Field != "is_new" {
		return false
	}
	sql := materializeCondGroup([]Condition{c}, reg, nil, false)
	return sql == "is_new = '1'" || sql == "is_new != '0'"
}

// condTouchesWindowField reports whether any leaf of the condition references a
// window-layer output (z-score/outlier), which exists only above the window wrap.
func condTouchesWindowField(cond Condition, registry *FieldRegistry) bool {
	if cond.IsCompound() {
		for _, child := range cond.Children {
			if condTouchesWindowField(child, registry) {
				return true
			}
		}
		return false
	}
	entry := registry.Get(cond.Field)
	return entry != nil && entry.ClassifyKind() == FieldKindWindow
}

// whereBindingStageIndex returns the index of the deepest stage that owns the
// fields referenced by a WHERE-bucket condition. It walks compound children so a
// compound stays a unit bound to the deepest stage any leaf requires. Fields not
// materialized by a post-aggregation projection stage (base log fields, JSON
// sub-columns, pre-aggregation scalars) bind to the source stage (index 0).
func whereBindingStageIndex(cond Condition, plan *QueryPlan) int {
	maxIdx := max(0, plan.capStageAt(cond.at.Cmd))
	var walk func(c Condition)
	walk = func(c Condition) {
		if c.IsCompound() {
			for _, child := range c.Children {
				walk(child)
			}
			return
		}
		// A command's predicate reads a carried key as its source expression, so
		// only the stage computing the field, or rewriting it, binds it.
		if c.Command != nil {
			for _, f := range conditionCommandFields(*c.Command) {
				maxIdx = max(maxIdx, fieldFirstStage(f, plan))
				if plan.rewrittenAfterAggregation(f, c.at.Cmd) {
					maxIdx = max(maxIdx, plan.stageAfterCmd[c.at.Cmd-1])
				}
			}
			return
		}
		maxIdx = max(maxIdx, fieldOwningStage(c.Field, plan))
	}
	walk(cond)
	// A filter reads the rows at its position, never a stage a later command adds.
	if at, ok := plan.stageAt[cond.at]; ok {
		maxIdx = min(maxIdx, at)
	}
	return maxIdx
}

// fieldOwningStage returns the index of the deepest post-source stage whose
// SELECT list materializes the given field as a column, or 0 (source stage) when
// no projection stage produces it.
func fieldOwningStage(field string, plan *QueryPlan) int {
	for i := len(plan.Stages) - 1; i >= 1; i-- {
		if plan.Stages[i].Layer.columnIndex(field) >= 0 {
			return i
		}
	}
	return 0
}

// materializeCondGroup ANDs the conditions, each compiled against registry.
// atScan marks conditions bound to the logs scan's own WHERE, the only place a
// norm_log index hint may go.
func materializeCondGroup(conditions []Condition, registry *FieldRegistry, scope *deferredScope, atScan bool) string {
	var parts []string
	for _, c := range conditions {
		if sql := renderCondition(c, registry, scope, atScan); sql != "" {
			parts = append(parts, sql)
		}
	}
	clause := strings.Join(parts, " AND ")
	if len(parts) > 1 && strings.Contains(clause, " OR ") {
		clause = "(" + clause + ")"
	}
	return clause
}

// renderCondition compiles one condition. An AND or OR is bracketed as a unit,
// and so is each AND or OR inside it.
func renderCondition(c Condition, registry *FieldRegistry, scope *deferredScope, atScan bool) string {
	if !c.IsCompound() {
		if c.PredicateSQL != "" {
			return c.PredicateSQL
		}
		return buildConditionSQL(c, registry, scope, atScan)
	}
	sep := " AND "
	if c.Op == CondOr {
		sep = " OR "
	}
	var parts []string
	for _, child := range c.Children {
		sql := renderCondition(child, registry, scope, atScan)
		if sql == "" {
			continue
		}
		// A test whose SQL is itself a disjunction must not re-associate with the
		// conjuncts beside it.
		if c.Op == CondAnd && !child.IsCompound() && hasTopLevelOR(sql) {
			sql = "(" + sql + ")"
		}
		parts = append(parts, sql)
	}
	if len(parts) == 0 {
		return ""
	}
	inner := strings.Join(parts, sep)
	if !isParenthesized(inner) {
		inner = "(" + inner + ")"
	}
	if c.Negate {
		return "NOT " + inner
	}
	return inner
}

// isParenthesized reports whether a clause is one bracketed unit: its opening
// bracket closes at its last character. Quoted text is skipped, and a clause it
// cannot read as balanced counts as unbracketed, which costs only a redundant pair.
func isParenthesized(clause string) bool {
	if len(clause) < 2 || clause[0] != '(' || clause[len(clause)-1] != ')' {
		return false
	}
	depth := 0
	for i := 0; i < len(clause); i++ {
		switch c := clause[i]; c {
		case '\'', '`':
			for i++; i < len(clause) && clause[i] != c; i++ {
				if clause[i] == '\\' {
					i++
				}
			}
			if i >= len(clause) {
				return false
			}
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return i == len(clause)-1
			}
		}
	}
	return false
}

// buildConditionSQL builds the SQL for a single Condition using the registry,
// with a norm_log index hint when it filters a stored field or norm_log itself on
// the logs scan.
func buildConditionSQL(cond Condition, registry *FieldRegistry, scope *deferredScope, atScan bool) string {
	fieldRef, isJSONField := resolveConditionField(cond, registry)
	sql := conditionSQL(cond, registry, scope, fieldRef, isJSONField)
	onNormLog := fieldRef == normLogColumn
	if sql == "" || !atScan || !(isJSONField || onNormLog) || !registry.normLogIndexHints {
		return sql
	}
	if hint := normLogHint(cond, registry.sourceMode, onNormLog); hint != "" {
		return "(" + sql + " AND " + hint + ")"
	}
	return sql
}

// resolveConditionField returns the reference a condition's field compiles to, and
// whether that is a stored JSON path rather than a column or computed field.
func resolveConditionField(cond Condition, registry *FieldRegistry) (fieldRef string, isJSONField bool) {
	entry := registry.Get(cond.Field)
	if entry != nil {
		switch entry.Kind {
		case FieldKindPerRow, FieldKindAssignment:
			fieldRef = registry.Resolve(cond.Field)
		case FieldKindBase:
			fieldRef = entry.Expr
		case FieldKindAggregate:
			fieldRef = entry.Name
		case FieldKindWindow:
			fieldRef = entry.Name
		case FieldKindJoined:
			fieldRef = entry.Name
		default:
			fieldRef = entry.Name
		}
	} else {
		// Check aggregate function aliases
		switch {
		case registry.bareAggregate(cond.Field):
			fieldRef = "_" + cond.Field
		case cond.Field == normLogColumn:
			fieldRef = contentColMode(registry.sourceMode)
		case cond.Field == "timestamp":
			fieldRef = "timestamp"
		case cond.Field == "log_id":
			fieldRef = "log_id"
		case cond.Field == "normalizer" || cond.Field == "_normalizer":
			fieldRef = "normalizer"
		default:
			fieldRef = registry.fieldRef(cond.Field)
			isJSONField = true
		}
	}
	return fieldRef, isJSONField
}

// conditionSQL compiles a single Condition, its field already resolved.
func conditionSQL(cond Condition, registry *FieldRegistry, scope *deferredScope, fieldRef string, isJSONField bool) string {
	if cond.Negate && isOrdering(cond.Operator) {
		// An ordering is negated whole: a non-number fails both x < n and x >= n.
		cond.Negate = false
		if sql := conditionSQL(cond, registry, scope, fieldRef, isJSONField); sql != "" {
			return "NOT (" + sql + ")"
		}
		return ""
	}
	entry := registry.Get(cond.Field)
	// At a deferred layer a source expression must be read from its exported hidden
	// column; bare column names pass through untouched.
	fieldRef = scope.ref(fieldRef, cond.Field)
	mode := sourceModeOf(registry)
	switch {
	case cond.ValueField != "":
		// field(name) on the right-hand side: compare against another field.
		rhs := resolveValueField(cond.ValueField, registry)
		rhs.ref = scope.ref(rhs.ref, cond.ValueField)
		// A bare count/sum/avg has no registry entry but resolves to the numeric
		// _count/_sum/_avg alias, which textToNumber rejects (code 43).
		computed := registry.bareAggregate(cond.Field) || (entry != nil && entry.Kind != FieldKindBase && entry.Kind != FieldKindJSON)
		return buildFieldComparisonSQL(lhsOperand(cond.Field, fieldRef, isJSONField, computed), rhs, cond.Operator)
	case cond.Value == "*" && (cond.Operator == "=" || cond.Operator == "!="):
		// * means presence only as the value of = and !=; under =~, =^ and =$ it is a term.
		return presenceSQL(fieldRef, cond.Operator == "=", isJSONField)
	case cond.IsRegex:
		return buildRegexMatchSQL(fieldRef, cond.Value, cond.LiteralTerm, cond.Operator == "!=", mode)
	case cond.Operator == "=~":
		return buildContainsAnySQL(fieldRef, conditionValues(cond), cond.Negate, mode)
	case cond.Operator == "=^":
		return buildStartsWithAnySQL(fieldRef, conditionValues(cond), cond.Negate, mode)
	case cond.Operator == "=$":
		return buildEndsWithAnySQL(fieldRef, conditionValues(cond), cond.Negate, mode)
	case cond.Operator == "=" || cond.Operator == "!=":
		return equalitySQL(cond, registry, fieldRef, isJSONField)
	case isOrdering(cond.Operator):
		return orderingSQL(cond, registry, entry, fieldRef)
	}
	return ""
}

// isOrdering reports whether op is a numeric comparison.
func isOrdering(op string) bool {
	return op == ">" || op == "<" || op == ">=" || op == "<="
}

// presenceSQL tests whether a field is present and not empty, or the opposite.
func presenceSQL(fieldRef string, present, isJSONField bool) string {
	switch {
	case present:
		return fmt.Sprintf("%s != ''", fieldRef)
	case isJSONField:
		return fmt.Sprintf("(%s IS NULL OR %s = '')", fieldRef, fieldRef)
	}
	return fmt.Sprintf("%s = ''", fieldRef)
}

// equalitySQL compiles = and !=, against one value or a list. Equality is
// answered by the field's column alone, never a raw_log token prefilter: the
// value may be normalized and absent from raw_log.
func equalitySQL(cond Condition, registry *FieldRegistry, fieldRef string, isJSONField bool) string {
	negate := cond.Operator == "!="
	iceberg := registry.sourceMode == SourceIceberg && isJSONField
	if len(cond.Values) > 1 {
		// Negation is folded into the operator by negateCondition (= <-> !=).
		values := conditionValues(cond)
		if iceberg {
			return buildIcebergEqualityListSQL(cond.Field, values, negate, registry.icePromoted)
		}
		return buildEqualityListSQL(fieldRef, values, negate, isJSONField)
	}
	switch {
	case negate && isJSONField:
		return fmt.Sprintf("(%s IS NULL OR %s != '%s')", fieldRef, fieldRef, escapeString(cond.Value))
	case negate:
		return fmt.Sprintf("%s != '%s'", fieldRef, escapeString(cond.Value))
	case iceberg:
		// MAP correctness + promoted `_ice_` column pruning (icebergEqualityPredicate).
		return icebergEqualityPredicate(cond.Field, cond.Value, registry.icePromoted)
	}
	return fmt.Sprintf("%s = '%s'", fieldRef, escapeString(cond.Value))
}

// orderingSQL compiles a numeric comparison. Text is read with textToNumber; a
// value that is already a number needs no coercion, and wrapping one is rejected
// by the server (illegal type UInt64).
func orderingSQL(cond Condition, registry *FieldRegistry, entry *FieldEntry, fieldRef string) string {
	if err := validateNumeric(cond.Value); err != nil {
		return fmt.Sprintf("%s %s '%s'", fieldRef, cond.Operator, escapeString(cond.Value))
	}
	if !isNumericOperand(cond.Field, registry, entry) {
		fieldRef = textToNumber(fieldRef)
	}
	return fmt.Sprintf("%s %s %s", fieldRef, cond.Operator, cond.Value)
}

// isNumericOperand reports whether a field already compiles to a number: a bare
// aggregate name (count/sum/avg with no registry entry, read as _count/_sum/_avg),
// an aggregate, assignment, window or join output, or a numeric expression.
func isNumericOperand(field string, registry *FieldRegistry, entry *FieldEntry) bool {
	if entry == nil {
		return registry.bareAggregate(field)
	}
	switch entry.Kind {
	case FieldKindPerRow:
		return entry.Numeric
	case FieldKindAggregate, FieldKindAssignment, FieldKindWindow, FieldKindJoined:
		return true
	}
	return entry.Numeric
}

// conditionValues returns the values a condition compares against, as conditionSQL
// reads them: a contains/prefix/suffix list or its single value, an equality list
// of two or more, or else the single value.
func conditionValues(cond Condition) []string {
	switch cond.Operator {
	case "=~", "=^", "=$":
		if len(cond.Values) == 0 && cond.Value != "" {
			return []string{cond.Value}
		}
		return cond.Values
	}
	if len(cond.Values) > 1 {
		return cond.Values
	}
	return []string{cond.Value}
}

// conditionsPredicate compiles a sub-pipeline's conditions (a case branch, a chain
// step) into one predicate against registry. Such a sub-pipeline is evaluated row
// by row, so every column it references is in scope and nothing is routed to a
// later stage.
func conditionsPredicate(conds []Condition, registry *FieldRegistry, opts QueryOptions) (string, error) {
	// Compiled into a copy: the conditions belong to the parsed pipeline, which is
	// compiled again against other registries.
	conds = cloneConditions(conds)
	if err := resolveCommandConditions(conds, registry, nil, opts); err != nil {
		return "", err
	}
	for i := range conds {
		if err := compileNestedExprLeaves(&conds[i], 0, registry, nil); err != nil {
			return "", err
		}
	}
	return materializeCondGroup(conds, registry, nil, false), nil
}

// SplitFirstStage separates the filters written before a pipeline's first
// command from those after one.
func SplitFirstStage(pipeline *PipelineNode) (first, later []Condition) {
	pipeline.forEachStep(func(st Step, at position) {
		switch {
		case st.Filter == nil:
		case at.Cmd == 0:
			first = append(first, *st.Filter)
		default:
			later = append(later, *st.Filter)
		}
	})
	return first, later
}

// negateCondition flips the operator on a Condition to apply NOT.
// For compound nodes, toggles the Negate flag.
// For regex/string conditions (IsRegex=true), "=" and "~" become "!=" (which
// triggers NOT in buildRegexMatchSQL). For comparison operators, the relational
// sense is inverted (e.g. ">" becomes "<=").
func negateCondition(h *Condition) {
	if h.IsCompound() {
		h.Negate = !h.Negate
		return
	}
	if h.Expr != nil {
		// An expression carries no Operator to invert, so the negation lives on the
		// condition. Without this, !(expr) silently became expr.
		h.Negate = !h.Negate
		return
	}
	if h.Command != nil {
		// A condition function operand carries its own negation, which
		// negatedCommandPredicate applies. Without this, a="x" OR !cidr(...) lost it.
		cmd := *h.Command
		cmd.Negate = !cmd.Negate
		h.Command = &cmd
		return
	}
	switch h.Operator {
	case "=", "~":
		h.Operator = "!="
	case "!=":
		h.Operator = "="
	case "=~", "=^", "=$", ">", "<", ">=", "<=":
		// An ordering is negated whole, not flipped: a non-number fails both
		// x < n and x >= n, and NOT has to match it.
		h.Negate = !h.Negate
	}
}

// cloneConditions deep-copies a condition tree, so compiling one never writes
// into another.
func cloneConditions(conds []Condition) []Condition {
	out := slices.Clone(conds)
	for i := range out {
		out[i].Children = cloneConditions(out[i].Children)
	}
	return out
}

// resolveCommandConditions compiles condition-function operands into SQL, each
// against the registry at its position (plan.conditionRegistry when a plan is
// given). It runs before classification so the rest of the pipeline treats them
// as ordinary leaves.
func resolveCommandConditions(conditions []Condition, registry *FieldRegistry, plan *QueryPlan, opts QueryOptions) error {
	for i := range conditions {
		if conditions[i].IsCompound() {
			if err := resolveCommandConditions(conditions[i].Children, registry, plan, opts); err != nil {
				return err
			}
			continue
		}
		if conditions[i].Command == nil {
			continue
		}
		ctx := &CommandContext{Registry: registry, Plan: plan, Opts: opts, CmdIndex: conditions[i].at.Cmd}
		if plan == nil {
			ctx.Plan = NewQueryPlan()
		} else {
			ctx.Registry = plan.registryAt(conditions[i].at, registry)
			ctx.Registry = withPostSourceColumns(conditions[i], ctx.Registry, plan)
			if plan.afterRowKeeping(conditions[i].at.Cmd) {
				ctx.Registry = withDeferredColumns(*conditions[i].Command, ctx.Registry, plan)
			}
		}
		sql, err := negatedCommandPredicate(*conditions[i].Command, ctx)
		if err != nil {
			return err
		}
		conditions[i].PredicateSQL = sql
	}
	return nil
}

// withPostSourceColumns resolves the fields a stage after the scan computes, or that
// a filter after a cap reads, to their bare columns: all a later stage can read.
func withPostSourceColumns(c Condition, registry *FieldRegistry, plan *QueryPlan) *FieldRegistry {
	for _, f := range conditionCommandFields(*c.Command) {
		if fieldFirstStage(f, plan) > 0 || plan.rewrittenAfterAggregation(f, c.at.Cmd) || plan.capStageAt(c.at.Cmd) >= 0 {
			registry = registry.Clone()
			registry.Register(f, FieldKindPerRow, f, -1)
			registry.SetResolveExpr(f, f)
		}
	}
	return registry
}

// withDeferredColumns resolves the fields a command reads to the hidden columns the
// scan exports for a filter above it.
func withDeferredColumns(cmd CommandNode, registry *FieldRegistry, plan *QueryPlan) *FieldRegistry {
	registry = registry.Clone()
	for _, f := range conditionCommandFields(cmd) {
		ref := plan.deferredScope().ref(resolveFieldRef(f, registry), f)
		registry.Register(f, FieldKindPerRow, ref, -1)
		registry.SetResolveExpr(f, ref)
	}
	return registry
}

// negatedCommandPredicate compiles a condition command, applying NOT in one place:
// the handler's own negative form when it has one, NOT around it otherwise.
func negatedCommandPredicate(cmd CommandNode, ctx *CommandContext) (string, error) {
	h, ok := getCommandHandler(cmd.Name).(predicateHandler)
	if !ok {
		if cmd.Negate {
			return "", fmt.Errorf("NOT cannot apply to %s(): it is not a filter", commandDisplayName(cmd.Name))
		}
		return "", notConditionError(fmt.Sprintf("%s() is not a filter, so it cannot be combined with AND or OR: put it in a stage of its own", commandDisplayName(cmd.Name)))
	}
	negate := cmd.Negate && !commandAppliesNegate(cmd.Name)
	if negate {
		cmd.Negate = false
	}
	sql, err := h.predicate(cmd, ctx)
	switch {
	case err != nil && negate:
		return "", fmt.Errorf("NOT %s(): %w", commandDisplayName(cmd.Name), err)
	case err != nil:
		return "", err
	case negate:
		return "NOT (" + sql + ")", nil
	}
	return sql, nil
}

// isClauseSpace reports whether a byte separates SQL tokens.
func isClauseSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// isORBoundary reports whether a byte can follow the R of a standalone OR.
// "OR(" is a boundary; "ORDER" is not.
func isORBoundary(c byte) bool {
	return isClauseSpace(c) || c == '('
}

// andJoin combines WHERE parts with AND, bracketing any part with a top-level
// OR so no disjunct escapes the time-range and fractal_id guards beside it.
func andJoin(parts []string) string {
	safe := make([]string, 0, len(parts))
	for _, p := range parts {
		if hasTopLevelOR(p) {
			p = "(" + p + ")"
		}
		safe = append(safe, p)
	}
	return strings.Join(safe, " AND ")
}

// hasTopLevelOR reports whether the clause contains an OR outside every bracket
// and string literal or backtick identifier.
//
// A quoted run is skipped whole, honouring backslash escapes: escapeString
// doubles backslashes, so a Windows path ends 'C:\\' and a naive "is the
// previous byte a backslash" test reads the closing quote as escaped, never
// leaves the string, and misses every OR after it.
func hasTopLevelOR(clause string) bool {
	depth := 0
	for i := 0; i < len(clause); i++ {
		c := clause[i]
		if c == '\'' || c == '`' {
			i++
			for ; i < len(clause); i++ {
				if clause[i] == '\\' {
					i++ // whatever follows is escaped, quote included
					continue
				}
				if clause[i] == c {
					break
				}
			}
			if i >= len(clause) {
				// Unterminated: the clause is not shaped as expected, so bracket it
				// rather than risk letting an OR past the scope guards.
				return true
			}
			continue
		}
		switch {
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth < 0 {
				// Unbalanced, so the clause is not shaped as expected. Bracket rather
				// than let an OR past the scope guards.
				return true
			}
		case depth == 0 && (c == 'O' || c == 'o') &&
			i+1 < len(clause) && (clause[i+1] == 'R' || clause[i+1] == 'r') &&
			i > 0 && isClauseSpace(clause[i-1]) && i+2 < len(clause) && isORBoundary(clause[i+2]):
			return true
		}
	}
	return false
}

// statsCommands aggregate the rows before them: after chain(), the matched
// entities, in a stage of their own.
var statsCommands = map[string]bool{
	"count": true, "sum": true, "avg": true, "max": true, "min": true,
	"median": true, "percentile": true, "stddev": true, "skewness": true, "kurtosis": true,
	"multi": true, "mad": true, "iqr": true, "corr": true, "rankcorr": true,
	"selectfirst": true, "selectlast": true, "top": true,
}

// validatePostAggregationFields rejects a filter that reads anything the aggregation
// before it did not return. Such a name resolves as a log field and is pushed into
// the scan, so it would change what was counted instead of filtering the output.
func validatePostAggregationFields(pipeline *PipelineNode, registry *FieldRegistry, plan *QueryPlan) error {
	commands := pipeline.Commands()
	var check func(c Condition, pos position) error
	check = func(c Condition, pos position) error {
		if c.IsCompound() {
			for _, child := range c.Children {
				if err := check(child, pos); err != nil {
					return err
				}
			}
			return nil
		}
		at := lastAggregatingCommandIndex(commands, pos.Cmd)
		if at < 0 {
			return nil
		}
		agg := canonicalCommandName(commands[at].Name)
		if !outputIsClosed(agg) {
			return nil
		}
		if commandBuildsOutputLater(agg) {
			return fmt.Errorf("a filter cannot follow %s(), whose output is built around the finished query: filter before it", agg)
		}
		if c.Command != nil && commandReadsLogRows(c.Command.Name) {
			return fmt.Errorf("%s() reads log rows, so move it before %s()", commandDisplayName(c.Command.Name), agg)
		}
		for _, name := range conditionReads(c) {
			if availableAfterAggregation(name, pos, at, pipeline, registry, plan) {
				continue
			}
			if name == normLogColumn {
				return fmt.Errorf("a text search reads log rows, so move it before %s()", agg)
			}
			return fmt.Errorf("%s does not exist after %s(): filter on a grouping key or an aggregate output (%s), or move the filter before the aggregation",
				name, agg, strings.Join(visibleOutputs(plan.outputsAfter[at]), ", "))
		}
		return nil
	}
	var err error
	pipeline.forEachStep(func(st Step, pos position) {
		if err != nil {
			return
		}
		switch {
		case st.Filter != nil:
			err = check(*st.Filter, pos)
		case st.Command != nil:
			// A command after the aggregation reads its output the same way. One that
			// aggregates again on the same stage, as groupby(user) | sum(bytes) does,
			// reads the rows being grouped instead.
			err = checkCommandAfterAggregation(*st.Command, pos, commands, pipeline, registry, plan)
		}
	})
	return err
}

// checkCommandAfterAggregation rejects a command reading a field the aggregation
// before it did not return.
func checkCommandAfterAggregation(cmd CommandNode, pos position, commands []CommandNode, pipeline *PipelineNode, registry *FieldRegistry, plan *QueryPlan) error {
	if IsAggregatingCommand(cmd.Name) {
		return nil
	}
	at := lastAggregatingCommandIndex(commands, pos.Cmd)
	if at < 0 || !outputIsClosed(commands[at].Name) {
		return nil
	}
	for _, name := range commandFieldReads(cmd) {
		if !availableAfterAggregation(name, pos, at, pipeline, registry, plan) {
			return fmt.Errorf("%s(): %s does not exist after %s(): use a grouping key or an aggregate output (%s), or move %s() before the aggregation",
				commandDisplayName(cmd.Name), name, canonicalCommandName(commands[at].Name),
				strings.Join(visibleOutputs(plan.outputsAfter[at]), ", "), commandDisplayName(cmd.Name))
		}
	}
	return nil
}

// commandFieldReads names the fields a command's field parameters read.
func commandFieldReads(cmd CommandNode) []string {
	spec := commandSpecs[canonicalCommandName(cmd.Name)]
	if spec == nil || spec.FreeForm {
		return nil
	}
	b, err := BindCommand(cmd)
	if err != nil {
		return nil
	}
	var names []string
	for _, p := range spec.Params {
		if p.Kind != ParamField {
			continue
		}
		for _, a := range b.Flat(p.Name) {
			switch {
			case a.IsFieldName():
				names = append(names, a.FieldName())
			case a.Kind == ArgExpr && a.Expr != nil:
				names = append(names, exprFieldNames(a.Expr)...)
			}
		}
	}
	return names
}

// lastAggregatingCommandIndex finds the last aggregating command among the first n,
// or -1 when none precedes.
func lastAggregatingCommandIndex(commands []CommandNode, n int) int {
	for i := min(n, len(commands)) - 1; i >= 0; i-- {
		if IsAggregatingCommand(commands[i].Name) {
			return i
		}
	}
	return -1
}

// conditionReads names every field a leaf condition reads.
func conditionReads(c Condition) []string {
	var names []string
	if c.Command != nil {
		names = conditionCommandFields(*c.Command)
	}
	if c.Expr != nil {
		names = append(names, exprFieldNames(c.Expr)...)
	}
	if c.Field != "" {
		names = append(names, c.Field)
	}
	if c.ValueField != "" {
		names = append(names, c.ValueField)
	}
	return names
}

// availableAfterAggregation reports whether name exists at pos: the aggregation
// at index at returned it, or the pipeline computed it since. A stage keeps the
// row-level columns selected before its GROUP BY, and those do not survive.
func availableAfterAggregation(name string, pos position, at int, pipeline *PipelineNode, registry *FieldRegistry, plan *QueryPlan) bool {
	outputs := plan.outputsAfter[at]
	if alias, ok := plan.registryAt(pos, registry).GroupKeyAlias(name); ok && slices.Contains(outputs, alias) {
		return true
	}
	if plan.declaredBetween(name, at, pos.Cmd) {
		return true
	}
	assignedSince := false
	pipeline.forEachStep(func(st Step, stepAt position) {
		if st.Assign != nil && st.Assign.Field == name && stepAt.Assign < pos.Assign && stepAt.Cmd > at {
			assignedSince = true
		}
	})
	if assignedSince || isBareAggregate(name) {
		return true
	}
	return slices.Contains(outputs, name) && !plan.declaredBetween(name, 0, firstAggregatingCommandIndex(pipeline.Commands()))
}

// visibleOutputs is the column list an error shows, without chain()'s hidden ones.
func visibleOutputs(outputs []string) []string {
	out := slices.DeleteFunc(slices.Clone(outputs), func(c string) bool {
		return c == chainAnchorColumn || c == chainDoneColumn
	})
	sort.Strings(out)
	return out
}
