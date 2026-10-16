package parser

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

type condGroup struct {
	sql   string
	logic string
}

// classifyConditions routes HavingConditions into pending buckets based on
// FieldKind. Runs AFTER all Declare() calls so the registry has field metadata
// for classification. SQL generation is deferred to materializeConditions
// (after Execute) so that PerRow handlers have set their real expressions.
//
// Routing rules by FieldKind:
//   - FieldKindAggregate  -> HAVING (when aggregation present), else WHERE
//   - FieldKindWindow     -> DeferredWhere (post-window filter), or HAVING for traversal
//   - FieldKindPerRow     -> WHERE (inlined expression; string-typed, needs toFloat64OrZero)
//   - FieldKindAssignment -> WHERE (inlined expression; already numeric, no coercion needed)
//   - Base/JSON/unknown   -> WHERE
//
// The HAVING/WHERE boundary is purely about aggregation stage: only FieldKindAggregate
// fields are produced after GROUP BY and therefore require HAVING. Assignment and PerRow
// fields are both per-row scalars computed before aggregation; they differ only in whether
// the value needs numeric coercion, not in SQL placement.
func classifyConditions(conditions []HavingCondition, registry *FieldRegistry, plan *QueryPlan) error {
	if len(conditions) == 0 {
		return nil
	}

	willHaveAggregation := plan.IsAggregated || plan.HasGroupBy

	for i, cond := range conditions {
		// Only an assignment separates a first-stage group's parts, so they are
		// AND-ed and each resolves at its own position.
		if cond.IsCompound && !cond.Negate && cond.CmdIndex == 0 && spansAssignment(cond) {
			if err := classifyConditions(cond.Children, registry, plan); err != nil {
				return err
			}
			continue
		}
		registry := plan.conditionRegistry(cond, registry)
		// Compound nodes: inspect all leaf fields to determine the highest-priority
		// target. A compound normally stays a unit, since its children are connected
		// by AND/OR and the operators bind them together.
		if cond.IsCompound {
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
			if err := checkDisjunctionStages(conditions, i, priority, registry, plan, willHaveAggregation); err != nil {
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

		var target *[]HavingCondition
		switch leafPriority(cond, registry, plan, willHaveAggregation) {
		case 2:
			target = &plan.pendingHavingConditions
		case 1:
			// Window and model_lookup outputs exist only after the outer wrap;
			// defer to a post-join/post-window WHERE.
			target = &plan.pendingDeferredConditions
		default:
			target = &plan.pendingWhereConditions
		}

		*target = append(*target, cond)
	}
	return nil
}

// spansAssignment reports whether a compound's parts sit on both sides of an
// assignment.
func spansAssignment(c HavingCondition) bool {
	for _, child := range c.Children {
		if child.AssignIndex != c.AssignIndex || spansAssignment(child) {
			return true
		}
	}
	return false
}

// checkDisjunctionStages rejects an expression OR-ed with a neighbour that binds
// to a different pipeline stage. Each condition is materialised into the clause
// its stage owns, so an OR split across WHERE and HAVING is evaluated as AND and
// silently returns fewer rows than asked for.
func checkDisjunctionStages(conditions []HavingCondition, i, priority int, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) error {
	neighbour := func(j int) (HavingCondition, bool) {
		if j < 0 || j >= len(conditions) {
			return HavingCondition{}, false
		}
		return conditions[j], true
	}
	// Logic sits on the condition to the LEFT of the operator.
	if prev, ok := neighbour(i - 1); ok && strings.EqualFold(prev.Logic, "OR") {
		if p := stagePriority(prev, registry, plan, willHaveAggregation); p != priority {
			return disjunctionStageError(conditions[i], prev)
		}
	}
	if strings.EqualFold(conditions[i].Logic, "OR") {
		if next, ok := neighbour(i + 1); ok {
			if p := stagePriority(next, registry, plan, willHaveAggregation); p != priority {
				return disjunctionStageError(conditions[i], next)
			}
		}
	}
	return nil
}

func stagePriority(c HavingCondition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) int {
	if c.Expr != nil {
		return exprPriority(c.Expr, registry, plan, willHaveAggregation)
	}
	if c.IsCompound {
		return subtreePriority(c, registry, plan, willHaveAggregation)
	}
	return leafPriority(c, registry, plan, willHaveAggregation)
}

func disjunctionStageError(a, b HavingCondition) error {
	return fmt.Errorf("cannot OR %s with %s: they are evaluated at different stages of the pipeline (one before the aggregation, one after), so the result would not be a disjunction. Filter them separately, or move the aggregation",
		describeCondition(a), describeCondition(b))
}

func describeCondition(c HavingCondition) string {
	if c.Expr != nil {
		return c.Expr.String()
	}
	if c.Command != nil {
		return c.Command.Name + "()"
	}
	if c.Field != "" {
		return c.Field
	}
	return "that condition"
}

// compileNestedExprLeaves fills in PredicateSQL for every expression leaf beneath
// a compound, at the stage the compound as a whole binds to.
func compileNestedExprLeaves(cond *HavingCondition, priority int, registry *FieldRegistry, plan *QueryPlan) error {
	if cond.IsCompound {
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
// binds to. A deferred stage (a window or join output) is rejected rather than
// bound: those columns exist only after an outer wrap, and reaching one from an
// expression needs the deferredScope export that plain conditions get.
func classifyExprCondition(cond HavingCondition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) (string, int, error) {
	priority := exprPriority(cond.Expr, registry, plan, willHaveAggregation)
	// A match operator makes the expression the left operand of a comparison
	// rather than a condition in its own right.
	if cond.Operator != "" {
		sql, err := exprMatchSQL(cond, registry)
		return sql, priority, err
	}
	// A deferred filter sits above the source scan, so its source-scope leaves are
	// exported through the plan's scope as they compile. Classification runs before
	// exportDeferredColumns, so the aliases allocated here are projected.
	if priority == 1 {
		sql, err := exprConditionSQLDeferred(cond.Expr, registry, plan.deferredScope(), cond.Negate)
		return sql, priority, err
	}
	sql, err := exprConditionSQL(cond.Expr, registry, cond.Negate)
	return sql, priority, err
}

// validateCompoundOperandStages checks every leaf of a compound that binds as one
// unit. The compound's stage is the highest its leaves need, so a leaf is judged
// against that rather than against its own.
func validateCompoundOperandStages(cond HavingCondition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) error {
	if subtreePriority(cond, registry, plan, willHaveAggregation) != 2 {
		return nil
	}
	var walk func(HavingCondition) error
	walk = func(c HavingCondition) error {
		if c.IsCompound {
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
func validateOperandStages(cond HavingCondition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) error {
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

// sanitizedAlias is the alias a grouping key is projected under. A name carrying a
// dot or dash is not a bare identifier, so it is quoted; sanitizeIdentifier already
// makes that decision for every other alias.
func sanitizedAlias(field string) string {
	if safe, err := sanitizeIdentifier(field); err == nil {
		return safe
	}
	return field
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
func leafPriority(c HavingCondition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) int {
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
		if plan.IsProcessTree {
			return 2
		}
		return 1
	case FieldKindJoined:
		return 1 // deferred (post-join), like window fields
	case FieldKindAggregate:
		if willHaveAggregation {
			return 2
		}
	}
	return 0
}

// subtreePriority returns the highest leafPriority within a condition subtree.
func subtreePriority(cond HavingCondition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) int {
	if !cond.IsCompound {
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

// splitMixedAndCompound decomposes a compound that mixes a deferred-stage conjunct
// (a window or model_lookup output, produced only after the outer wrap) with one
// that is not, so each binds where its fields actually exist. Without it a clause
// like `beacon_score > 0.5 AND NOT image=~foo` is routed wholesale past the
// model_lookup join, where the source-only `fields.image` no longer resolves.
// Distribution over AND is safe; a negated node or an OR connector is not, and a
// compound that needs no deferral is left intact so its SQL is unchanged.
func splitMixedAndCompound(cond HavingCondition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) []HavingCondition {
	if !cond.IsCompound || cond.Negate || len(cond.Children) < 2 {
		return nil
	}
	for _, c := range cond.Children[:len(cond.Children)-1] {
		if c.Logic != "" && c.Logic != "AND" {
			return nil
		}
	}
	deferred, other := 0, 0
	for _, c := range cond.Children {
		if subtreePriority(c, registry, plan, willHaveAggregation) == 1 {
			deferred++
		} else {
			other++
		}
	}
	if deferred == 0 || other == 0 {
		return nil
	}
	parts := make([]HavingCondition, len(cond.Children))
	copy(parts, cond.Children)
	for i := range parts {
		parts[i].Logic = "" // each conjunct now stands alone in its bucket
	}
	return parts
}

// classifyCompoundTarget inspects all leaf fields in a compound HavingCondition
// and returns the highest-priority target bucket. Priority: HAVING > DeferredWhere > WHERE.
func classifyCompoundTarget(cond HavingCondition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) *[]HavingCondition {
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
func compoundFitsHavingStage(cond HavingCondition, plan *QueryPlan) bool {
	hs := plan.havingStageIndex()
	fits := true
	var walk func(c HavingCondition)
	walk = func(c HavingCondition) {
		if c.IsCompound {
			for _, child := range c.Children {
				walk(child)
			}
			return
		}
		if fieldFirstStage(c.Field, plan) > hs {
			fits = false
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
			s := sel.String()
			if strings.Trim(extractFieldAlias(s), "`") == field && s != field {
				return i
			}
		}
	}
	return 0
}

// materializeConditions generates SQL from the classified pending conditions
// using the fully-populated registry (after Execute). Each condition is bound to
// the query stage that owns its referenced fields: pre-aggregation filters to the
// source stage, HAVING to the groupby stage, post-aggregation projection-column
// filters to the projection stage that produces them.
func materializeConditions(registry *FieldRegistry, plan *QueryPlan) {
	// WHERE conditions bind to the stage that owns the fields they reference.
	// Pre-aggregation filters (base/JSON/pre-agg scalars) bind to the source
	// stage; a filter on a column materialized only by a post-aggregation
	// projection stage (e.g. a sprintf/concat output, or a post-agg `:=`
	// assignment) binds to that stage. Without this, such a filter lands in the
	// innermost source WHERE where the column does not exist, producing a
	// ClickHouse "unknown identifier" (or, for a carried aggregate, error 184).
	byStage := make(map[int][]HavingCondition)
	var stageOrder []int
	// Conditions before the first command filter the raw scan, ahead of anything a
	// command adds to it.
	var firstStage []HavingCondition
	for _, cond := range plan.pendingWhereConditions {
		if cond.CmdIndex == 0 && len(plan.firstStageAt) > 0 {
			firstStage = append(firstStage, cond)
			continue
		}
		idx := whereBindingStageIndex(cond, plan)
		if _, seen := byStage[idx]; !seen {
			stageOrder = append(stageOrder, idx)
		}
		byStage[idx] = append(byStage[idx], cond)
	}
	var firstClauses []string
	for i := 0; i < len(firstStage); {
		j := i + 1
		for j < len(firstStage) && firstStage[j].AssignIndex == firstStage[i].AssignIndex {
			j++
		}
		if clause := materializeCondGroup(firstStage[i:j], plan.conditionRegistry(firstStage[i], registry), nil); clause != "" {
			firstClauses = append(firstClauses, clause)
		}
		i = j
	}
	if clause := andJoin(firstClauses); clause != "" {
		source := plan.SourceStage()
		source.Layer.Where = slices.Insert(source.Layer.Where, min(plan.baseWhereLen, len(source.Layer.Where)), clause)
	}
	for _, idx := range stageOrder {
		if clause := materializeCondGroup(byStage[idx], registry, nil); clause != "" {
			plan.Stages[idx].Layer.Where = append(plan.Stages[idx].Layer.Where, clause)
		}
	}
	// HAVING binds to the aggregation stage in effect where the filter is written.
	havingByStage := make(map[int][]HavingCondition)
	var havingOrder []int
	for _, cond := range plan.pendingHavingConditions {
		idx := plan.havingStageIndexAt(cond.CmdIndex)
		if _, seen := havingByStage[idx]; !seen {
			havingOrder = append(havingOrder, idx)
		}
		havingByStage[idx] = append(havingByStage[idx], cond)
	}
	for _, idx := range havingOrder {
		if clause := materializeCondGroup(havingByStage[idx], registry, nil); clause != "" {
			plan.Stages[idx].Layer.Having = append(plan.Stages[idx].Layer.Having, clause)
		}
	}
	// Deferred conditions land above the source scan, so any source expression they
	// reference must be exported there under a hidden alias (see deferredScope).
	deferred := plan.pendingDeferredConditions
	if plan.ModelLookupAtScan {
		// Scan-level model join: a condition on a model column filters rows between
		// the join and the aggregation (PostJoinWhere), where both model columns and
		// scan fields resolve directly. Only window-field conditions still need the
		// outermost deferred layer.
		var outer, postJoin []HavingCondition
		for _, c := range deferred {
			if condTouchesWindowField(c, registry) {
				outer = append(outer, c)
			} else {
				postJoin = append(postJoin, c)
			}
		}
		if clause := materializeCondGroup(postJoin, registry, nil); clause != "" {
			plan.PostJoinWhere = append(plan.PostJoinWhere, clause)
		}
		deferred = outer
	}
	if clause := materializeCondGroup(deferred, registry, plan.deferredScope()); clause != "" {
		plan.DeferredWhere = append(plan.DeferredWhere, clause)
	}
}

// condTouchesWindowField reports whether any leaf of the condition references a
// window-layer output (z-score/outlier), which exists only above the window wrap.
func condTouchesWindowField(cond HavingCondition, registry *FieldRegistry) bool {
	if cond.IsCompound {
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
func whereBindingStageIndex(cond HavingCondition, plan *QueryPlan) int {
	maxIdx := 0
	var walk func(c HavingCondition)
	walk = func(c HavingCondition) {
		if c.IsCompound {
			for _, child := range c.Children {
				walk(child)
			}
			return
		}
		if idx := fieldOwningStage(c.Field, plan); idx > maxIdx {
			maxIdx = idx
		}
	}
	walk(cond)
	return maxIdx
}

// fieldOwningStage returns the index of the deepest post-source stage whose
// SELECT list materializes the given field as a column, or 0 (source stage) when
// no projection stage produces it.
func fieldOwningStage(field string, plan *QueryPlan) int {
	for i := len(plan.Stages) - 1; i >= 1; i-- {
		for _, sel := range plan.Stages[i].Layer.Selects {
			if strings.Trim(extractFieldAlias(sel.String()), "`") == field {
				return i
			}
		}
	}
	return 0
}

// materializeCondGroup builds SQL for a group of conditions and joins them.
// Handles both flat conditions (with GroupID-based grouping) and compound
// nodes (tree-based nesting) for arbitrary expression depth.
func materializeCondGroup(conditions []HavingCondition, registry *FieldRegistry, scope *deferredScope) string {
	if len(conditions) == 0 {
		return ""
	}

	// Build each condition into a condGroup (sql + logic connector).
	var groups []condGroup
	for _, cond := range conditions {
		var condSQL string
		if cond.IsCompound {
			inner := materializeCondGroup(cond.Children, registry, scope)
			if inner == "" {
				continue
			}
			if !isParenthesized(inner) {
				inner = "(" + inner + ")"
			}
			condSQL = inner
			if cond.Negate {
				condSQL = "NOT " + inner
			}
		} else if cond.PredicateSQL != "" {
			condSQL = cond.PredicateSQL
		} else {
			condSQL = buildConditionSQL(cond, registry, scope)
			if condSQL == "" {
				continue
			}
		}
		groups = append(groups, condGroup{sql: condSQL, logic: cond.Logic})
	}
	return joinCondGroups(groups)
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

// joinCondGroups joins condition groups with their logic operators.
func joinCondGroups(groups []condGroup) string {
	if len(groups) == 0 {
		return ""
	}
	var result strings.Builder
	for i, g := range groups {
		if i > 0 {
			if groups[i-1].logic != "" {
				result.WriteString(" " + groups[i-1].logic + " ")
			} else {
				result.WriteString(" AND ")
			}
		}
		result.WriteString(g.sql)
	}
	clause := result.String()
	if len(groups) > 1 && strings.Contains(clause, " OR ") {
		clause = "(" + clause + ")"
	}
	return clause
}

// buildConditionSQL builds the SQL for a single HavingCondition using the registry.
func buildConditionSQL(cond HavingCondition, registry *FieldRegistry, scope *deferredScope) string {
	var fieldRef string
	isJSONField := false

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

	// At a deferred layer a source expression must be read from its exported hidden
	// column; bare column names pass through untouched.
	fieldRef = scope.ref(fieldRef, cond.Field)

	// field(name) on the right-hand side: compare against another field.
	if cond.ValueField != "" {
		rhs := resolveValueField(cond.ValueField, registry)
		rhs.ref = scope.ref(rhs.ref, cond.ValueField)
		// A bare count/sum/avg has no registry entry but resolves to the numeric
		// _count/_sum/_avg alias, which toFloat64OrZero rejects (code 43). Same
		// carve-out the literal comparison path makes below.
		computed := registry.bareAggregate(cond.Field) ||
			(entry != nil && entry.Kind != FieldKindBase && entry.Kind != FieldKindJSON)
		lhs := lhsOperand(cond.Field, fieldRef, isJSONField, computed)
		return buildFieldComparisonSQL(lhs, rhs, cond.Operator)
	}

	if cond.Value == "*" {
		if cond.Operator == "!=" {
			if isJSONField {
				return fmt.Sprintf("(%s IS NULL OR %s = '')", fieldRef, fieldRef)
			}
			return fmt.Sprintf("%s = ''", fieldRef)
		}
		return fmt.Sprintf("%s != ''", fieldRef)
	}

	if cond.IsRegex {
		negate := cond.Operator == "!="
		return buildRegexMatchSQL(fieldRef, cond.Value, cond.LiteralTerm, negate, sourceModeOf(registry))
	}

	if cond.Operator == "=~" || cond.Operator == "=^" || cond.Operator == "=$" {
		values := cond.Values
		if len(values) == 0 && cond.Value != "" {
			values = []string{cond.Value}
		}
		switch cond.Operator {
		case "=~":
			return buildContainsAnySQL(fieldRef, values, cond.Negate, sourceModeOf(registry))
		case "=^":
			return buildStartsWithAnySQL(fieldRef, values, cond.Negate, sourceModeOf(registry))
		case "=$":
			return buildEndsWithAnySQL(fieldRef, values, cond.Negate, sourceModeOf(registry))
		}
	}

	if (cond.Operator == "=" || cond.Operator == "!=") && len(cond.Values) > 1 {
		// Comma-separated equality list -> IN / NOT IN. Negation is folded into
		// the operator by negateHavingCondition (= <-> !=), matching single value.
		negate := cond.Operator == "!="
		if registry.sourceMode == SourceIceberg && isJSONField {
			return buildIcebergEqualityListSQL(cond.Field, cond.Values, negate, registry.icePromoted)
		}
		return buildEqualityListSQL(fieldRef, cond.Values, negate, isJSONField)
	}

	switch cond.Operator {
	case "=":
		// Equality is answered by the field's column/sub-column alone; no raw_log token
		// pre-filter (the value may be normalized and absent from raw_log -> false negatives).
		if registry.sourceMode == SourceIceberg && isJSONField {
			// MAP correctness + promoted `_ice_` column pruning (icebergEqualityPredicate).
			return icebergEqualityPredicate(cond.Field, cond.Value, registry.icePromoted)
		}
		return fmt.Sprintf("%s = '%s'", fieldRef, escapeString(cond.Value))
	case "!=":
		if isJSONField {
			return fmt.Sprintf("(%s IS NULL OR %s != '%s')", fieldRef, fieldRef, escapeString(cond.Value))
		}
		return fmt.Sprintf("%s != '%s'", fieldRef, escapeString(cond.Value))
	case ">", "<", ">=", "<=":
		if err := validateNumeric(cond.Value); err != nil {
			return fmt.Sprintf("%s %s '%s'", fieldRef, cond.Operator, escapeString(cond.Value))
		}
		// An assignment that compiles to a number needs no coercion; wrapping one
		// in toFloat64OrZero is rejected by the server (illegal type UInt64).
		isPerRow := entry != nil && entry.Kind == FieldKindPerRow && !entry.Numeric
		// Bare aggregate names (count/sum/avg with no registry entry) resolve to the
		// numeric _count/_sum/_avg aliases and must not be coerced via toFloat64OrZero.
		isAggFallback := entry == nil && registry.bareAggregate(cond.Field)
		isComputed := isAggFallback || (entry != nil && (entry.Numeric || entry.Kind == FieldKindAggregate || entry.Kind == FieldKindAssignment || entry.Kind == FieldKindWindow || entry.Kind == FieldKindJoined))
		if isPerRow {
			return fmt.Sprintf("toFloat64OrZero(%s) %s %s", fieldRef, cond.Operator, cond.Value)
		}
		if isComputed {
			return fmt.Sprintf("%s %s %s", fieldRef, cond.Operator, cond.Value)
		}
		return fmt.Sprintf("toFloat64OrZero(%s) %s %s", fieldRef, cond.Operator, cond.Value)
	}
	return ""
}

// conditionsPredicate compiles a sub-pipeline's conditions (a case branch, a chain
// step) into one predicate against registry. Such a sub-pipeline is evaluated row
// by row, so every column it references is in scope and nothing is routed to a
// later stage.
func conditionsPredicate(conds []HavingCondition, registry *FieldRegistry, opts QueryOptions) (string, error) {
	if err := resolveCommandConditions(conds, opts); err != nil {
		return "", err
	}
	for i := range conds {
		if err := compileNestedExprLeaves(&conds[i], 0, registry, nil); err != nil {
			return "", err
		}
	}
	return materializeCondGroup(conds, registry, nil), nil
}

// splitFirstStage separates the conditions written before a pipeline's first
// command from those after one.
func splitFirstStage(conds []HavingCondition) (first, later []HavingCondition) {
	for _, c := range conds {
		if c.CmdIndex == 0 {
			first = append(first, c)
		} else {
			later = append(later, c)
		}
	}
	return first, later
}

// negateHavingCondition flips the operator on a HavingCondition to apply NOT.
// For compound nodes, toggles the Negate flag.
// For regex/string conditions (IsRegex=true), "=" and "~" become "!=" (which
// triggers NOT in buildRegexMatchSQL). For comparison operators, the relational
// sense is inverted (e.g. ">" becomes "<=").
func negateHavingCondition(h *HavingCondition) {
	if h.IsCompound {
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
	case ">":
		h.Operator = "<="
	case "<":
		h.Operator = ">="
	case ">=":
		h.Operator = "<"
	case "<=":
		h.Operator = ">"
	case "=~", "=^", "=$":
		h.Negate = !h.Negate
	}
}

// cloneConditions deep-copies a condition tree, so compiling one never writes
// into another.
func cloneConditions(conds []HavingCondition) []HavingCondition {
	out := slices.Clone(conds)
	for i := range out {
		out[i].Children = cloneConditions(out[i].Children)
	}
	return out
}

// resolveCommandConditions compiles condition-function operands into SQL. It runs
// before classification so the rest of the pipeline treats them as ordinary leaves.
func resolveCommandConditions(conditions []HavingCondition, opts QueryOptions) error {
	for i := range conditions {
		if conditions[i].IsCompound {
			if err := resolveCommandConditions(conditions[i].Children, opts); err != nil {
				return err
			}
			continue
		}
		if conditions[i].Command == nil {
			continue
		}
		sql, err := negatedCommandPredicate(*conditions[i].Command, opts)
		if err != nil {
			return err
		}
		conditions[i].PredicateSQL = sql
	}
	return nil
}

// negatedCommandPredicate builds a condition function's predicate and applies its
// negation exactly once.
//
// Some handlers honour cmd.Negate themselves (in, cidr) and some do not (comment,
// tlsh). Wrapping the handler's output in NOT therefore doubled the negation on
// the first pair, so `!in(status,"200") AND x` returned exactly what was
// excluded, and dropped it entirely on the second. Building with Negate cleared
// makes this the only place negation is applied.
func negatedCommandPredicate(cmd CommandNode, opts QueryOptions) (string, error) {
	negate := cmd.Negate
	cmd.Negate = false
	sql, err := CommandPredicate(cmd, opts)
	if err != nil {
		return "", err
	}
	if negate {
		return "NOT (" + sql + ")", nil
	}
	return sql, nil
}

// andJoin combines WHERE parts with AND, bracketing any part that carries a
// top-level OR.
//
// The time-range and fractal_id guards are just entries in this list, and AND
// binds tighter than OR, so an unbracketed `a OR b` would leave the right
// disjunct outside them and read every fractal at any time. Each condition
// producer already brackets its own output, but that was an unwritten invariant
// with nothing enforcing it: a new producer that forgot re-opened the hole.
// Enforcing it here makes the guarantee structural.
// isClauseSpace reports whether a byte separates SQL tokens.
func isClauseSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// isORBoundary reports whether a byte can follow the R of a standalone OR.
// "OR(" is a boundary; "ORDER" is not.
func isORBoundary(c byte) bool {
	return isClauseSpace(c) || c == '('
}

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
// and string literal.
//
// A string literal is skipped whole, honouring backslash escapes: escapeString
// doubles backslashes, so a Windows path ends 'C:\\' and a naive "is the
// previous byte a backslash" test reads the closing quote as escaped, never
// leaves the string, and misses every OR after it.
func hasTopLevelOR(clause string) bool {
	depth := 0
	for i := 0; i < len(clause); i++ {
		c := clause[i]
		if c == '\'' {
			i++
			for ; i < len(clause); i++ {
				if clause[i] == '\\' {
					i++ // whatever follows is escaped, quote included
					continue
				}
				if clause[i] == '\'' {
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
				// Unbalanced, so the clause is not shaped as expected (a backtick
				// identifier can carry a stray paren). Bracket rather than let an
				// OR past the scope guards.
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

// closedOutputCommands produce a fully known column set: a groupby's keys and
// aggregates, or a single stats row. Other aggregating commands (chain, charts,
// window scores) pass identity or row fields through, so they are not listed.
var closedOutputCommands = map[string]bool{
	"groupby": true, "count": true, "sum": true, "avg": true, "max": true, "min": true,
	"median": true, "percentile": true, "stddev": true, "skewness": true, "kurtosis": true,
	"multi": true, "mad": true, "iqr": true, "corr": true, "rankcorr": true,
	"selectfirst": true, "selectlast": true, "top": true,
}

// validatePostAggregationFields rejects a filter on a name that does not exist
// after the aggregation before it. Such a name resolves as a log field and is
// pushed into the scan, so a typo like `_cuont > 1` silently matched nothing.
func validatePostAggregationFields(pipeline *PipelineNode, registry *FieldRegistry, declared map[string]bool) error {
	for _, cond := range pipeline.HavingConditions {
		agg := lastAggregatingCommandBefore(pipeline.Commands, cond.CmdIndex)
		if agg == "" || !closedOutputCommands[agg] {
			continue
		}
		if name := firstMissingPostAggField(cond, registry, declared); name != "" {
			return fmt.Errorf("%s does not exist after %s(): filter on a grouping key or an aggregate output (%s), or move the filter before the aggregation",
				name, agg, strings.Join(aggregateOutputNames(registry), ", "))
		}
	}
	return nil
}

// lastAggregatingCommandBefore names the last aggregating command among the
// first n, or "" when none precedes.
func lastAggregatingCommandBefore(commands []CommandNode, n int) string {
	for i := min(n, len(commands)) - 1; i >= 0; i-- {
		if IsAggregatingCommand(commands[i].Name) {
			return strings.ToLower(commands[i].Name)
		}
	}
	return ""
}

// firstMissingPostAggField returns the first operand of a condition tree that is
// a plain log field rather than a column the pipeline computes. A field()
// comparison is left to validateOperandStages, which explains it better.
func firstMissingPostAggField(cond HavingCondition, registry *FieldRegistry, declared map[string]bool) string {
	if cond.IsCompound {
		for _, child := range cond.Children {
			if name := firstMissingPostAggField(child, registry, declared); name != "" {
				return name
			}
		}
		return ""
	}
	if cond.Command != nil || cond.Expr != nil || cond.ValueField != "" || cond.Field == "" {
		return ""
	}
	if isBareAggregate(cond.Field) || isGroupKey(registry, cond.Field) || declared[cond.Field] {
		return ""
	}
	if e := registry.Get(cond.Field); e != nil && e.ClassifyKind() != FieldKindJSON {
		return ""
	}
	return cond.Field
}

// aggregateOutputNames lists the grouping keys and aggregate columns in scope,
// for an error that names what the author can filter on.
func aggregateOutputNames(registry *FieldRegistry) []string {
	var out []string
	for name := range registry.groupKeys {
		out = append(out, name)
	}
	for _, name := range registry.order {
		if e := registry.fields[name]; e != nil && e.ClassifyKind() == FieldKindAggregate {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
