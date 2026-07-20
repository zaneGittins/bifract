package parser

import (
	"fmt"
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
func classifyConditions(conditions []HavingCondition, registry *FieldRegistry, plan *QueryPlan) {
	if len(conditions) == 0 {
		return
	}

	willHaveAggregation := plan.IsAggregated || plan.HasGroupBy

	for _, cond := range conditions {
		// Compound nodes: inspect all leaf fields to determine the
		// highest-priority target. The entire compound must stay as a
		// unit since its children are connected by AND/OR.
		if cond.IsCompound {
			target := classifyCompoundTarget(cond, registry, plan, willHaveAggregation)
			*target = append(*target, cond)
			continue
		}

		entry := registry.Get(cond.Field)
		var target *[]HavingCondition

		if entry != nil {
			switch entry.ClassifyKind() {
			case FieldKindWindow:
				if plan.IsTraversal || plan.IsProcessTree {
					target = &plan.pendingHavingConditions
				} else {
					target = &plan.pendingDeferredConditions
				}
			case FieldKindModelLookup:
				// Model-lookup outputs exist only after the JOIN wrap; defer to a
				// post-join outer WHERE (same mechanism as window fields).
				target = &plan.pendingDeferredConditions
			case FieldKindAggregate:
				if willHaveAggregation {
					target = &plan.pendingHavingConditions
				} else {
					target = &plan.pendingWhereConditions
				}
			case FieldKindPerRow:
				target = &plan.pendingWhereConditions
			case FieldKindAssignment:
				target = &plan.pendingWhereConditions
			default:
				target = &plan.pendingWhereConditions
			}
		} else {
			switch cond.Field {
			case "count", "sum", "avg":
				if willHaveAggregation {
					target = &plan.pendingHavingConditions
				} else {
					target = &plan.pendingWhereConditions
				}
			default:
				target = &plan.pendingWhereConditions
			}
		}

		*target = append(*target, cond)
	}
}

// classifyCompoundTarget inspects all leaf fields in a compound HavingCondition
// and returns the highest-priority target bucket. Priority: HAVING > DeferredWhere > WHERE.
func classifyCompoundTarget(cond HavingCondition, registry *FieldRegistry, plan *QueryPlan, willHaveAggregation bool) *[]HavingCondition {
	// Priority levels: 0=WHERE, 1=DeferredWhere, 2=HAVING
	maxPriority := 0

	var walk func(c HavingCondition)
	walk = func(c HavingCondition) {
		if c.IsCompound {
			for _, child := range c.Children {
				walk(child)
			}
			return
		}
		priority := 0
		entry := registry.Get(c.Field)
		if entry != nil {
			switch entry.ClassifyKind() {
			case FieldKindWindow:
				if plan.IsTraversal || plan.IsProcessTree {
					priority = 2
				} else {
					priority = 1
				}
			case FieldKindModelLookup:
				priority = 1 // deferred (post-join), like window fields
			case FieldKindAggregate:
				if willHaveAggregation {
					priority = 2
				}
			case FieldKindAssignment:
				// per-row scalar, always WHERE
			}
		} else {
			switch c.Field {
			case "count", "sum", "avg":
				if willHaveAggregation {
					priority = 2
				}
			}
		}
		if priority > maxPriority {
			maxPriority = priority
		}
	}
	walk(cond)

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
	for _, cond := range plan.pendingWhereConditions {
		idx := whereBindingStageIndex(cond, plan)
		if _, seen := byStage[idx]; !seen {
			stageOrder = append(stageOrder, idx)
		}
		byStage[idx] = append(byStage[idx], cond)
	}
	for _, idx := range stageOrder {
		if clause := materializeCondGroup(byStage[idx], registry); clause != "" {
			plan.Stages[idx].Layer.Where = append(plan.Stages[idx].Layer.Where, clause)
		}
	}
	// HAVING binds to the outermost aggregation stage, not the innermost source
	// stage, so chained-groupby filters apply to the final aggregates.
	if clause := materializeCondGroup(plan.pendingHavingConditions, registry); clause != "" {
		havingStage := plan.havingStage()
		havingStage.Layer.Having = append(havingStage.Layer.Having, clause)
	}
	if clause := materializeCondGroup(plan.pendingDeferredConditions, registry); clause != "" {
		plan.DeferredWhere = append(plan.DeferredWhere, clause)
	}
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
func materializeCondGroup(conditions []HavingCondition, registry *FieldRegistry) string {
	if len(conditions) == 0 {
		return ""
	}

	// Build each condition into a condGroup (sql + logic connector).
	var groups []condGroup
	for _, cond := range conditions {
		var condSQL string
		if cond.IsCompound {
			inner := materializeCondGroup(cond.Children, registry)
			if inner == "" {
				continue
			}
			if cond.Negate {
				condSQL = "NOT (" + inner + ")"
			} else {
				condSQL = "(" + inner + ")"
			}
		} else {
			condSQL = buildConditionSQL(cond, registry)
			if condSQL == "" {
				continue
			}
		}
		groups = append(groups, condGroup{sql: condSQL, logic: cond.Logic})
	}
	return joinCondGroups(groups)
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
func buildConditionSQL(cond HavingCondition, registry *FieldRegistry) string {
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
		case FieldKindModelLookup:
			fieldRef = entry.Name
		default:
			fieldRef = entry.Name
		}
	} else {
		// Check aggregate function aliases
		switch cond.Field {
		case "count":
			fieldRef = "_count"
		case "sum":
			fieldRef = "_sum"
		case "avg":
			fieldRef = "_avg"
		case normLogColumn:
			fieldRef = contentColMode(registry.sourceMode)
		case "timestamp":
			fieldRef = "timestamp"
		case "log_id":
			fieldRef = "log_id"
		case "normalizer", "_normalizer":
			fieldRef = "normalizer"
		default:
			fieldRef = registry.fieldRef(cond.Field)
			isJSONField = true
		}
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
		return buildRegexMatchSQL(fieldRef, cond.Value, negate, isJSONField)
	}

	if cond.Operator == "=~" || cond.Operator == "=^" || cond.Operator == "=$" {
		values := cond.Values
		if len(values) == 0 && cond.Value != "" {
			values = []string{cond.Value}
		}
		switch cond.Operator {
		case "=~":
			return buildContainsAnySQL(fieldRef, values, cond.Negate)
		case "=^":
			return buildStartsWithAnySQL(fieldRef, values, cond.Negate)
		case "=$":
			return buildEndsWithAnySQL(fieldRef, values, cond.Negate)
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
		isPerRow := entry != nil && entry.Kind == FieldKindPerRow
		// Bare aggregate names (count/sum/avg with no registry entry) resolve to the
		// numeric _count/_sum/_avg aliases and must not be coerced via toFloat64OrZero.
		isAggFallback := entry == nil && (cond.Field == "count" || cond.Field == "sum" || cond.Field == "avg")
		isComputed := isAggFallback || (entry != nil && (entry.Kind == FieldKindAggregate || entry.Kind == FieldKindAssignment || entry.Kind == FieldKindWindow || entry.Kind == FieldKindModelLookup))
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

// negateConditionOperator flips the operator on a ConditionNode to apply NOT.
// Used by parseConditionsWithPrecedence where negation must be encoded in the
// operator itself (ConditionNode uses Negate flag for leaf conditions but
// operator-level negation for flat groups).
func negateConditionOperator(c *ConditionNode) {
	switch c.Operator {
	case "=", "~":
		c.Operator = "!="
	case "!=":
		c.Operator = "="
	case ">":
		c.Operator = "<="
	case "<":
		c.Operator = ">="
	case ">=":
		c.Operator = "<"
	case "<=":
		c.Operator = ">"
	case "=~", "=^", "=$":
		// No negated operator variant; toggle the Negate flag instead so the
		// SQL builder wraps the expression in NOT (...).
		c.Negate = !c.Negate
	}
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
