package parser

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// stageBucket is the clause a filter binds to, ordered by precedence: a filter
// binds to the greatest any of its operands needs (see combine).
type stageBucket int

const (
	bucketWhere    stageBucket = iota // the WHERE of the stage owning its fields
	bucketDeferred                    // the layer above the window or join outputs it reads
	bucketHaving                      // the HAVING of the aggregation it filters
)

// combine is where a filter reading operands of buckets a and b binds: the later of
// the two, except that a window output exists only above the aggregation whose
// HAVING the other needs, and the deferred layer reads that aggregation's columns.
func combine(a, b stageBucket) stageBucket {
	if min(a, b) == bucketDeferred && max(a, b) == bucketHaving {
		return bucketDeferred
	}
	return max(a, b)
}

// bucket is the pending list for a stage bucket.
func (p *pendingFilters) bucket(b stageBucket) *[]Condition {
	switch b {
	case bucketHaving:
		return &p.having
	case bucketDeferred:
		return &p.deferred
	}
	return &p.where
}

// classifyConditions routes filters into pending buckets based on
// FieldKind. Runs AFTER all Declare() calls so the registry has field metadata
// for classification. SQL generation is deferred to materializeConditions
// (after Execute) so that PerRow handlers have set their real expressions.
//
// Routing rules by FieldKind:
//   - FieldKindAggregate  -> HAVING (when aggregation present), else WHERE
//   - FieldKindWindow     -> the deferred layer (post-window filter), or HAVING for traversal
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
		if err := checkTimeFilters(cond, registry); err != nil {
			return err
		}
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
			// compiled predicate and no Field and was dropped, taking the whole
			// parenthesised group with it: adding brackets silently widened a rule
			// to every row. The compound binds to one stage, so its leaves compile
			// at the compound's priority, not their own.
			if err := compileNestedExprLeaves(&cond, subtreeStage(cond, registry, plan, willHaveAggregation), registry, plan); err != nil {
				return err
			}
			target := classifyCompoundTarget(cond, registry, plan, willHaveAggregation)
			*target = append(*target, cond)
			continue
		}

		if cond.Expr != nil {
			var stage stageBucket
			compiled, err := registry.predicate(func() (predicate, error) {
				sql, s, err := classifyExprCondition(cond, registry, plan, willHaveAggregation)
				stage = s
				return atom(sql), err
			})
			if err != nil {
				return err
			}
			cond.compiled = compiled
			target := plan.pending.bucket(stage)
			*target = append(*target, cond)
			continue
		}

		if err := validateOperandStages(cond, registry, plan, willHaveAggregation); err != nil {
			return err
		}

		// Window and join outputs, and fields computed from them, exist only after
		// the window or join: a filter on one binds there.
		target := plan.pending.bucket(leafStage(cond, registry, plan, willHaveAggregation))
		*target = append(*target, cond)
	}
	return nil
}

func conditionStage(c Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) stageBucket {
	if c.Expr != nil {
		return exprStage(c.Expr, registry, plan, willHaveAggregation)
	}
	if c.IsCompound() {
		return subtreeStage(c, registry, plan, willHaveAggregation)
	}
	return leafStage(c, registry, plan, willHaveAggregation)
}

// compileNestedExprLeaves compiles every expression leaf beneath
// a compound, at the stage the compound as a whole binds to.
func compileNestedExprLeaves(cond *Condition, stage stageBucket, registry *FieldRegistry, plan *QueryPlan) error {
	if cond.IsCompound() {
		for i := range cond.Children {
			if err := compileNestedExprLeaves(&cond.Children[i], stage, registry, plan); err != nil {
				return err
			}
		}
		return nil
	}
	if cond.Expr == nil || !cond.compiled.empty() {
		return nil
	}
	// Every expression filter compiles bracketed or negated: one operand.
	p, err := registry.predicate(func() (predicate, error) {
		var sql string
		var err error
		switch {
		case cond.Operator != "":
			sql, err = exprMatchSQL(*cond, registry)
		case stage == bucketDeferred:
			sql, err = exprConditionSQLDeferred(cond.Expr, registry, plan.deferredScope(), cond.Negate)
		default:
			sql, err = exprConditionSQL(cond.Expr, registry, cond.Negate)
		}
		return atom(sql), err
	})
	if err != nil {
		return err
	}
	cond.compiled = p
	return nil
}

// classifyExprCondition compiles an expression filter and reports the stage it
// binds to. One bound above the scan (a window or join() output) reads its source
// leaves through the deferredScope export.
func classifyExprCondition(cond Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) (string, stageBucket, error) {
	stage := exprStage(cond.Expr, registry, plan, willHaveAggregation)
	if plan.afterRowKeeping(cond.at.Cmd) {
		stage = max(stage, bucketDeferred)
	}
	// A match operator makes the expression the left operand of a comparison
	// rather than a condition in its own right.
	if cond.Operator != "" {
		sql, err := exprMatchSQL(cond, registry)
		return sql, stage, err
	}
	// A deferred filter sits above the source scan, so its source-scope leaves are
	// exported through the plan's scope as they compile. Classification runs before
	// exportDeferredColumns, so the aliases allocated here are projected.
	// Under the scan-level model join a post-join filter reads the joined rows
	// directly; only a window output still sits above the scan.
	if stage == bucketDeferred && !(plan.lookup.AtScan && !hasWindowField(cond.Expr, registry)) {
		sql, err := exprConditionSQLDeferred(cond.Expr, registry, plan.deferredScope(), cond.Negate)
		return sql, stage, err
	}
	sql, err := exprConditionSQL(cond.Expr, registry, cond.Negate)
	return sql, stage, err
}

// validateCompoundOperandStages checks every leaf of a compound that binds as one
// unit. The compound's stage is the highest its leaves need, so a leaf is judged
// against that rather than against its own.
func validateCompoundOperandStages(cond Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) error {
	if subtreeStage(cond, registry, plan, willHaveAggregation) != bucketHaving {
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
	if leafStage(cond, registry, plan, willHaveAggregation) != bucketHaving {
		return nil
	}
	for _, side := range []string{cond.Field, cond.ValueField} {
		if fieldStage(side, registry, plan, willHaveAggregation) == bucketHaving {
			continue
		}
		if !isGroupKey(registry, side) {
			return fmt.Errorf("cannot compare %s with field(%s): %s is a per-row field and does not survive the aggregation; add it to groupBy()",
				cond.Field, cond.ValueField, side)
		}
	}
	return nil
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

// leafStage returns the stage bucket a single (non-compound) condition needs.
// Both operands count: `a > field(_count)` can only be evaluated where the
// later-produced side exists.
func leafStage(c Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) stageBucket {
	b := fieldsStage(c, registry, plan, willHaveAggregation)
	// After a window score a filter keeps scored rows: it never narrows the baseline.
	if plan.afterRowKeeping(c.at.Cmd) {
		b = max(b, bucketDeferred)
	}
	return b
}

// fieldsStage is the stage the fields a leaf reads need.
func fieldsStage(c Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) stageBucket {
	if c.Command != nil {
		// A condition command binds where the fields it reads exist.
		b := bucketWhere
		for _, f := range conditionCommandFields(*c.Command) {
			b = combine(b, fieldStage(f, registry, plan, willHaveAggregation))
		}
		return b
	}
	b := fieldStage(c.Field, registry, plan, willHaveAggregation)
	if c.ValueField != "" {
		b = combine(b, fieldStage(c.ValueField, registry, plan, willHaveAggregation))
	}
	return b
}

// fieldStage is leafStage for a single field name.
func fieldStage(field string, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) stageBucket {
	entry := registry.Get(field)
	if entry == nil {
		if registry.bareAggregate(field) && willHaveAggregation {
			return bucketHaving
		}
		return bucketWhere
	}
	switch entry.ClassifyKind() {
	case FieldKindWindow:
		if plan.isProcessTree() {
			return bucketHaving
		}
		return bucketDeferred
	case FieldKindJoined:
		return bucketDeferred // after the join, like window fields
	case FieldKindAggregate:
		if willHaveAggregation {
			return bucketHaving
		}
	case FieldKindPerRow, FieldKindAssignment:
		if registry.DependsOnJoin(field) {
			return bucketDeferred // computed from a join output, so post-join as well
		}
	}
	return bucketWhere
}

// subtreeStage returns the highest leafStage within a condition subtree.
func subtreeStage(cond Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) stageBucket {
	if !cond.IsCompound() {
		return conditionStage(cond, registry, plan, willHaveAggregation)
	}
	b := bucketWhere
	for _, child := range cond.Children {
		b = combine(b, subtreeStage(child, registry, plan, willHaveAggregation))
	}
	return b
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
	stages := map[stageBucket]bool{}
	for _, c := range cond.Children {
		stages[subtreeStage(c, registry, plan, willHaveAggregation)] = true
	}
	if len(stages) < 2 {
		return nil
	}
	return slices.Clone(cond.Children)
}

// classifyCompoundTarget returns the pending list for a compound: the latest stage
// any of its leaves needs.
func classifyCompoundTarget(cond Condition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) *[]Condition {
	stage := subtreeStage(cond, registry, plan, willHaveAggregation)

	// A HAVING target only works if every leaf field is available at the groupby
	// (HAVING) stage. A compound that mixes an aggregate with a column produced by
	// a later projection stage (e.g. a sprintf/concat output) cannot live in that
	// stage's HAVING; route it to WHERE so it binds to the deepest stage where all
	// its fields coexist (the aggregate is carried there as a plain column).
	if stage == bucketHaving && !compoundFitsHavingStage(cond, plan) {
		stage = bucketWhere
	}
	return plan.pending.bucket(stage)
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
func materializeAtPositions(conds []Condition, registry *FieldRegistry, plan *QueryPlan, atScan bool) predicate {
	var clauses []predicate
	for i := 0; i < len(conds); {
		j := i + 1
		for j < len(conds) && conds[j].at == conds[i].at {
			j++
		}
		clauses = append(clauses, materializeCondGroup(conds[i:j], plan.registryAt(conds[i].at, registry), nil, atScan))
		i = j
	}
	return andOf(clauses...)
}

// scanConjuncts compiles the filters before the first command as separate scan
// WHERE entries, each AND split into its operands, so whatever reads the scan's
// filters (pgr candidate narrowing, the model key prefilter) sees every conjunct.
func scanConjuncts(conds []Condition, registry *FieldRegistry, plan *QueryPlan) []predicate {
	var out []predicate
	for _, c := range conds {
		parts := []Condition{c}
		if c.Op == CondAnd && !c.Negate {
			parts = c.Children
		}
		for _, part := range parts {
			if p := materializeCondGroup([]Condition{part}, plan.registryAt(c.at, registry), nil, true); !p.empty() {
				out = append(out, p)
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
	having := plan.pending.having[:0]
	for _, c := range plan.pending.having {
		if plan.capStageAt(c.at.Cmd) > plan.havingStageIndexAt(c.at.Cmd) {
			plan.pending.where = append(plan.pending.where, c)
		} else {
			having = append(having, c)
		}
	}
	plan.pending.having = having
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
	for _, cond := range plan.pending.where {
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
		dests := []*[]predicate{&layer.Where}
		groups := map[*[]predicate][]Condition{}
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
			if clause := materializeAtPositions(groups[dst], registry, plan, idx == 0 && dst == &layer.Where); !clause.empty() {
				*dst = append(*dst, clause)
			}
		}
	}
	// HAVING binds to the aggregation stage in effect where the filter is written.
	havingByStage := make(map[int][]Condition)
	var havingOrder []int
	for _, cond := range plan.pending.having {
		idx := plan.havingStageIndexAt(cond.at.Cmd)
		if _, seen := havingByStage[idx]; !seen {
			havingOrder = append(havingOrder, idx)
		}
		havingByStage[idx] = append(havingByStage[idx], cond)
	}
	for _, idx := range havingOrder {
		if clause := materializeAtPositions(havingByStage[idx], registry, plan, false); !clause.empty() {
			plan.Stages[idx].Layer.Having = append(plan.Stages[idx].Layer.Having, clause)
		}
	}
	// Deferred conditions land above the source scan, so any source expression they
	// reference must be exported there under a hidden alias (see deferredScope).
	deferred := plan.pending.deferred
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
	if clause := materializeCondGroup(deferred, registry, plan.deferredScope(), false); !clause.empty() {
		plan.deferred.where = append(plan.deferred.where, clause)
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
			case clause.empty():
			case plan.lookup.Strict && !capBetween(at) && clause.deps.model:
				plan.lookup.Where = append(plan.lookup.Where, clause)
				if requiresNew(c, reg) {
					plan.lookup.keys.scoreOnlyNew()
				}
			default:
				plan.lookup.PostJoinWhere = append(plan.lookup.PostJoinWhere, postJoinFilter{At: at, Pred: clause})
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
	sql := materializeCondGroup([]Condition{c}, reg, nil, false).sql
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
					maxIdx = max(maxIdx, plan.pos.stageAfter[c.at.Cmd-1])
				}
			}
			return
		}
		maxIdx = max(maxIdx, fieldOwningStage(c.Field, plan))
	}
	walk(cond)
	// A filter reads the rows at its position, never a stage a later command adds.
	if at, ok := plan.pos.stage[cond.at]; ok {
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
func materializeCondGroup(conditions []Condition, registry *FieldRegistry, scope *deferredScope, atScan bool) predicate {
	parts := make([]predicate, len(conditions))
	for i, c := range conditions {
		parts[i] = renderCondition(c, registry, scope, atScan)
	}
	return andOf(parts...)
}

// renderCondition compiles one condition. An AND or OR is one bracketed operand.
func renderCondition(c Condition, registry *FieldRegistry, scope *deferredScope, atScan bool) predicate {
	if !c.IsCompound() {
		if !c.compiled.empty() {
			return c.compiled
		}
		p, _ := registry.predicate(func() (predicate, error) { return buildConditionSQL(c, registry, scope, atScan), nil })
		return p
	}
	parts := make([]predicate, len(c.Children))
	for i, child := range c.Children {
		parts[i] = renderCondition(child, registry, scope, atScan)
	}
	join := andOf
	if c.Op == CondOr {
		join = orOf
	}
	p := join(parts...)
	if c.Negate {
		return notOf(p)
	}
	return group(p)
}

// buildConditionSQL builds the SQL for a single Condition using the registry,
// with a norm_log index hint when it filters a stored field or norm_log itself on
// the logs scan.
func buildConditionSQL(cond Condition, registry *FieldRegistry, scope *deferredScope, atScan bool) predicate {
	var isJSONField bool
	ref := registry.through(scope, cond.Field, func() sqlRef {
		var ref sqlRef
		ref, isJSONField = resolveConditionField(cond, registry)
		return ref
	})
	fieldRef := ref.sql
	p := conditionSQL(cond, registry, scope, ref, isJSONField)
	onNormLog := fieldRef == normLogColumn
	if p.empty() || !atScan || !(isJSONField || onNormLog) || !registry.normLogIndexHints {
		return p
	}
	if hint := normLogHint(cond, registry.sourceMode, onNormLog); hint != "" {
		return group(andOf(p, atom(hint)))
	}
	return p
}

// resolveConditionField returns the reference a condition's field compiles to, and
// whether that is a stored JSON path rather than a column or computed field.
func resolveConditionField(cond Condition, registry *FieldRegistry) (ref sqlRef, isJSONField bool) {
	if entry := registry.Get(cond.Field); entry != nil {
		switch entry.Kind {
		case FieldKindPerRow, FieldKindAssignment:
			return registry.Ref(cond.Field), false
		}
		return registry.Column(entry.Name), false
	}
	switch {
	case registry.bareAggregate(cond.Field):
		return registry.Column("_" + cond.Field).typed(typeNumber), false
	case cond.Field == normLogColumn, cond.Field == "timestamp", cond.Field == "log_id":
		return registry.Column(cond.Field), false
	case cond.Field == "normalizer" || cond.Field == "_normalizer":
		return registry.Column("normalizer"), false
	}
	return registry.Ref(cond.Field), true
}

// conditionSQL compiles a single Condition, its field already resolved.
func conditionSQL(cond Condition, registry *FieldRegistry, scope *deferredScope, ref sqlRef, isJSONField bool) predicate {
	if cond.Negate && isOrdering(cond.Operator) {
		// An ordering is negated whole: a non-number fails both x < n and x >= n.
		cond.Negate = false
		return notOf(conditionSQL(cond, registry, scope, ref, isJSONField))
	}
	fieldRef := ref.sql
	entry := registry.Get(cond.Field)
	mode := sourceModeOf(registry)
	switch {
	case cond.ValueField != "":
		// field(name) on the right-hand side: compare against another field.
		var rhs condOperand
		r := registry.through(scope, cond.ValueField, func() sqlRef {
			rhs = resolveValueField(cond.ValueField, registry)
			return sqlRef{sql: rhs.ref, form: rhs.form}
		})
		rhs.ref, rhs.form = r.sql, r.form
		// A bare count/sum/avg has no registry entry but resolves to the numeric
		// _count/_sum/_avg alias, which textToNumber rejects (code 43).
		computed := registry.bareAggregate(cond.Field) || (entry != nil && entry.Kind != FieldKindBase && entry.Kind != FieldKindJSON)
		return atom(buildFieldComparisonSQL(lhsOperand(cond.Field, fieldRef, isJSONField, computed), rhs, cond.Operator))
	case cond.Value == "*" && (cond.Operator == "=" || cond.Operator == "!="):
		// * means presence only as the value of = and !=; under =~, =^ and =$ it is a term.
		return atom(presenceSQL(fieldRef, cond.Operator == "=", isJSONField))
	case cond.IsRegex:
		return atom(buildRegexMatchSQL(ref.text(), cond.Value, cond.LiteralTerm, cond.Operator == "!=", mode))
	case cond.Operator == "=~":
		return atom(buildContainsAnySQL(ref.text(), conditionValues(cond), cond.Negate, mode))
	case cond.Operator == "=^":
		return atom(buildStartsWithAnySQL(ref.text(), conditionValues(cond), cond.Negate, mode))
	case cond.Operator == "=$":
		return atom(buildEndsWithAnySQL(ref.text(), conditionValues(cond), cond.Negate, mode))
	case cond.Operator == "=" || cond.Operator == "!=":
		return equalitySQL(cond, registry, fieldRef, isJSONField)
	case isOrdering(cond.Operator):
		return atom(orderingSQL(cond, ref))
	}
	return predicate{}
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
func equalitySQL(cond Condition, registry *FieldRegistry, fieldRef string, isJSONField bool) predicate {
	negate := cond.Operator == "!="
	iceberg := registry.sourceMode == SourceIceberg && isJSONField
	if len(cond.Values) > 1 {
		// Negation is folded into the operator by negateCondition (= <-> !=).
		values := conditionValues(cond)
		if iceberg {
			return buildIcebergEqualityListSQL(cond.Field, values, negate, registry.icePromoted)
		}
		return atom(buildEqualityListSQL(fieldRef, values, negate, isJSONField))
	}
	switch {
	case negate && isJSONField:
		return atom(fmt.Sprintf("(%s IS NULL OR %s != %s)", fieldRef, fieldRef, sqlString(cond.Value)))
	case negate:
		return atom(fmt.Sprintf("%s != %s", fieldRef, sqlString(cond.Value)))
	case iceberg:
		// MAP correctness + promoted `_ice_` column pruning (icebergEqualityPredicate).
		return icebergEqualityPredicate(cond.Field, cond.Value, registry.icePromoted)
	}
	return atom(fmt.Sprintf("%s = %s", fieldRef, sqlString(cond.Value)))
}

// orderingSQL compiles a numeric comparison. Text is read with textToNumber; a
// value that is already a number needs no coercion, and wrapping one is rejected
// by the server (illegal type UInt64).
func orderingSQL(cond Condition, ref sqlRef) string {
	fieldRef := ref.sql
	// A duration such as 5m compares as the seconds it holds.
	if seconds, err := durationSeconds(cond.Value); err == nil {
		cond.Value = fmt.Sprint(seconds)
	}
	if err := validateNumeric(cond.Value); err != nil {
		return fmt.Sprintf("%s %s %s", fieldRef, cond.Operator, sqlString(cond.Value))
	}
	if ref.typ != typeNumber {
		fieldRef = textToNumber(fieldRef)
	}
	return fmt.Sprintf("%s %s %s", fieldRef, cond.Operator, cond.Value)
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
func conditionsPredicate(conds []Condition, registry *FieldRegistry, opts QueryOptions) (predicate, error) {
	// Compiled into a copy: the conditions belong to the parsed pipeline, which is
	// compiled again against other registries.
	conds = cloneConditions(conds)
	if err := resolveCommandConditions(conds, registry, nil, opts); err != nil {
		return predicate{}, err
	}
	for i := range conds {
		if err := compileNestedExprLeaves(&conds[i], 0, registry, nil); err != nil {
			return predicate{}, err
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
		p, err := negatedCommandPredicate(*conditions[i].Command, ctx)
		if err != nil {
			return err
		}
		conditions[i].compiled = p
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
			registry.setReads(f, deps{})
		}
	}
	return registry
}

// withDeferredColumns resolves the fields a command reads to the hidden columns the
// scan exports for a filter above it.
func withDeferredColumns(cmd CommandNode, registry *FieldRegistry, plan *QueryPlan) *FieldRegistry {
	registry = registry.Clone()
	for _, f := range conditionCommandFields(cmd) {
		ref := registry.through(plan.deferredScope(), f, func() sqlRef { return registry.Ref(f) })
		registry.Register(f, FieldKindPerRow, ref.sql, -1)
		registry.SetResolveExpr(f, ref.sql)
		registry.setReads(f, ref.deps)
		if ref.form == refColumn {
			registry.setColumn(f)
		}
	}
	return registry
}

// negatedCommandPredicate compiles a condition command, applying NOT in one place:
// the handler's own negative form when it has one, NOT around it otherwise.
func negatedCommandPredicate(cmd CommandNode, ctx *CommandContext) (predicate, error) {
	h, ok := getCommandHandler(cmd.Name).(predicateHandler)
	if !ok {
		if cmd.Negate {
			return predicate{}, fmt.Errorf("NOT cannot apply to %s(): it is not a filter", commandDisplayName(cmd.Name))
		}
		return predicate{}, notConditionError(fmt.Sprintf("%s() is not a filter, so it cannot be combined with AND or OR: put it in a stage of its own", commandDisplayName(cmd.Name)))
	}
	negate := cmd.Negate && !commandAppliesNegate(cmd.Name)
	if negate {
		cmd.Negate = false
	}
	p, err := ctx.Registry.predicate(func() (predicate, error) { return h.predicate(cmd, ctx) })
	switch {
	case err != nil && negate:
		return predicate{}, fmt.Errorf("NOT %s(): %w", commandDisplayName(cmd.Name), err)
	case err != nil:
		return predicate{}, err
	case negate:
		return notOf(p), nil
	}
	return p, nil
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
				name, agg, strings.Join(visibleOutputs(plan.pos.outputs[at]), ", "))
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
				strings.Join(visibleOutputs(plan.pos.outputs[at]), ", "), commandDisplayName(cmd.Name))
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
			names = append(names, argFieldNames(a)...)
		}
	}
	return names
}

// argFieldNames lists the fields an argument in a field position reads: the field
// it names, quoted or not, or those its expression reads.
func argFieldNames(a Argument) []string {
	if name := a.FieldName(); name != "" {
		return []string{name}
	}
	if a.Kind == ArgExpr && a.Expr != nil {
		return exprFieldNames(a.Expr)
	}
	return nil
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
	outputs := plan.pos.outputs[at]
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
