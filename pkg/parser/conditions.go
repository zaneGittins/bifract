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
//   - FieldKindPerRow    -> WHERE (with inlined expression)
//   - FieldKindAggregate -> HAVING (when aggregation present), else WHERE
//   - FieldKindWindow    -> DeferredWhere (post-window filter), or HAVING for traversal
//   - FieldKindAssignment -> HAVING (when aggregation present), else WHERE
//   - Base/JSON/unknown  -> WHERE
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
			switch entry.Kind {
			case FieldKindWindow:
				if plan.IsTraversal {
					target = &plan.pendingHavingConditions
				} else {
					target = &plan.pendingDeferredConditions
				}
			case FieldKindAggregate:
				if willHaveAggregation {
					target = &plan.pendingHavingConditions
				} else {
					target = &plan.pendingWhereConditions
				}
			case FieldKindPerRow:
				target = &plan.pendingWhereConditions
			case FieldKindAssignment:
				if willHaveAggregation {
					target = &plan.pendingHavingConditions
				} else {
					target = &plan.pendingWhereConditions
				}
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
			switch entry.Kind {
			case FieldKindWindow:
				if plan.IsTraversal {
					priority = 2
				} else {
					priority = 1
				}
			case FieldKindAggregate:
				if willHaveAggregation {
					priority = 2
				}
			case FieldKindAssignment:
				if willHaveAggregation {
					priority = 2
				}
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

	switch maxPriority {
	case 2:
		return &plan.pendingHavingConditions
	case 1:
		return &plan.pendingDeferredConditions
	default:
		return &plan.pendingWhereConditions
	}
}

// materializeConditions generates SQL from the classified pending conditions
// using the fully-populated registry (after Execute). Appends to SourceStage
// which matches the original routing target (CurrentStage before any PushStage).
func materializeConditions(registry *FieldRegistry, plan *QueryPlan) {
	source := plan.SourceStage()

	if clause := materializeCondGroup(plan.pendingWhereConditions, registry); clause != "" {
		source.Layer.Where = append(source.Layer.Where, clause)
	}
	if clause := materializeCondGroup(plan.pendingHavingConditions, registry); clause != "" {
		source.Layer.Having = append(source.Layer.Having, clause)
	}
	if clause := materializeCondGroup(plan.pendingDeferredConditions, registry); clause != "" {
		plan.DeferredWhere = append(plan.DeferredWhere, clause)
	}
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
			// Recursively render compound sub-expression.
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
		case "raw_log":
			fieldRef = "raw_log"
		case "timestamp":
			fieldRef = "timestamp"
		case "log_id":
			fieldRef = "log_id"
		default:
			fieldRef = jsonFieldRef(cond.Field)
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
		return buildRegexMatchSQL(fieldRef, cond.Value, cond.Operator == "!=")
	}

	switch cond.Operator {
	case "=":
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
		isComputed := entry != nil && (entry.Kind == FieldKindAggregate || entry.Kind == FieldKindAssignment || entry.Kind == FieldKindWindow)
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
	}
}
