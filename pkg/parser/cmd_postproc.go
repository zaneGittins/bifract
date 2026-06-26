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
	source := ctx.Plan.CurrentStage()
	if len(cmd.Arguments) == 0 {
		return nil
	}

	field := strings.TrimPrefix(cmd.Arguments[0], "field=")
	direction := "ASC"

	for _, arg := range cmd.Arguments[1:] {
		argUpper := strings.ToUpper(arg)
		if strings.HasPrefix(argUpper, "ORDER=") {
			val := strings.TrimPrefix(argUpper, "ORDER=")
			if val == "DESC" || val == "ASC" {
				direction = val
			}
		} else if argUpper == "DESC" || argUpper == "ASC" {
			direction = argUpper
		}
	}

	var fieldRef string
	if ctx.Registry.IsComputed(field) || ctx.Registry.Has(field) {
		fieldRef = field
	} else {
		switch field {
		case "timestamp", "raw_log", "log_id":
			fieldRef = field
		default:
			fieldRef = jsonFieldRef(field)
		}
	}

	source.Layer.OrderBy = append(source.Layer.OrderBy, fieldRef+" "+direction)
	return nil
}

// limitHandler handles limit(n)
type limitHandler struct{}

func (h *limitHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *limitHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		if n, err := validateInt(cmd.Arguments[0]); err == nil {
			ctx.Plan.CurrentStage().Layer.Limit = fmt.Sprintf("LIMIT %d", n)
		}
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
	n := 200
	if len(cmd.Arguments) > 0 {
		if parsed, err := validateInt(cmd.Arguments[0]); err == nil {
			n = parsed
		}
	}
	// On aggregated stages timestamp is neither grouped nor aggregated, so
	// ordering by it is invalid (ClickHouse error 215). Keep the existing
	// ordering (e.g. a prior sort) and just take its first N rows.
	if !isAggregatedStage(ctx.Plan, source) {
		source.Layer.OrderBy = []string{"timestamp ASC"}
	}
	source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	return nil
}

// tailHandler handles tail(n) - last N events ordered by timestamp DESC
type tailHandler struct{}

func (h *tailHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *tailHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	source := ctx.Plan.CurrentStage()
	n := 200
	if len(cmd.Arguments) > 0 {
		if parsed, err := validateInt(cmd.Arguments[0]); err == nil {
			n = parsed
		}
	}
	// On aggregated stages timestamp is neither grouped nor aggregated, so
	// ordering by it is invalid (ClickHouse error 215). The "last N" rows are
	// the first N of the reversed existing ordering.
	if isAggregatedStage(ctx.Plan, source) {
		source.Layer.OrderBy = reverseOrderBy(source.Layer.OrderBy)
	} else {
		source.Layer.OrderBy = []string{"timestamp DESC"}
	}
	source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	return nil
}

// isAggregatedStage reports whether the given stage produces aggregated rows,
// in which case ordering by raw columns like timestamp is invalid.
func isAggregatedStage(plan *QueryPlan, stage *QueryStage) bool {
	return plan.IsAggregated || len(stage.Layer.GroupBy) > 0
}

// reverseOrderBy flips the direction of each ORDER BY term so that the tail of
// a result set becomes its head. Terms without an explicit direction default to
// ASC in ClickHouse, so they become DESC.
func reverseOrderBy(order []string) []string {
	if len(order) == 0 {
		return order
	}
	flipped := make([]string, len(order))
	for i, term := range order {
		t := strings.TrimSpace(term)
		switch {
		case strings.HasSuffix(strings.ToUpper(t), " DESC"):
			flipped[i] = t[:len(t)-len(" DESC")] + " ASC"
		case strings.HasSuffix(strings.ToUpper(t), " ASC"):
			flipped[i] = t[:len(t)-len(" ASC")] + " DESC"
		default:
			flipped[i] = t + " DESC"
		}
	}
	return flipped
}

// dedupHandler handles dedup(field1, field2, ...) using LIMIT 1 BY
type dedupHandler struct{}

func (h *dedupHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *dedupHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		var dedupFields []string
		for _, field := range cmd.Arguments {
			if ctx.Registry.IsComputed(field) {
				dedupFields = append(dedupFields, field)
			} else {
				dedupFields = append(dedupFields, jsonFieldRef(field))
			}
		}
		ctx.Plan.CurrentStage().Layer.LimitBy = fmt.Sprintf("LIMIT 1 BY %s", strings.Join(dedupFields, ", "))
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
