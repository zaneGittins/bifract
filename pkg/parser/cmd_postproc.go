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
	if len(cmd.Arguments) > 0 {
		field := cmd.Arguments[0]
		direction := "ASC"
		if len(cmd.Arguments) > 1 {
			dir := strings.ToUpper(cmd.Arguments[1])
			if dir == "DESC" || dir == "ASC" {
				direction = dir
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
	}
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
	source.Layer.OrderBy = []string{"timestamp ASC"}
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
	source.Layer.OrderBy = []string{"timestamp DESC"}
	source.Layer.Limit = fmt.Sprintf("LIMIT %d", n)
	return nil
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
