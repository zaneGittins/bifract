package parser

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	joinDefaultMaxRows = 10000
	joinHardMaxRows    = 100000
)

// joinHandler handles join(key, type=inner|left, max=N, include=[f1,f2]) { subquery }
type joinHandler struct{}

func (h *joinHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Plan.IsJoin = true
	return nil
}

func (h *joinHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) < 2 {
		return fmt.Errorf("join() requires a join key and a subquery block")
	}

	// Arguments layout from parser:
	// [0] = subquery block body (raw BQL text)
	// [1..N] = parsed params: first positional is the join key, then type=, max=, include=
	blockBody := strings.TrimSpace(cmd.Arguments[0])
	if blockBody == "" {
		return fmt.Errorf("join() subquery cannot be empty")
	}

	var joinKey string
	joinType := "inner"
	maxRows := joinDefaultMaxRows
	var includeFields []string

	for _, arg := range cmd.Arguments[1:] {
		arg = strings.TrimSpace(arg)
		if strings.HasPrefix(arg, "type=") {
			val := strings.TrimPrefix(arg, "type=")
			switch val {
			case "inner", "left":
				joinType = val
			default:
				return fmt.Errorf("join() type must be 'inner' or 'left', got '%s'", val)
			}
		} else if strings.HasPrefix(arg, "max=") {
			val := strings.TrimPrefix(arg, "max=")
			n, err := strconv.Atoi(val)
			if err != nil || n <= 0 {
				return fmt.Errorf("join() max must be a positive integer, got '%s'", val)
			}
			if n > joinHardMaxRows {
				n = joinHardMaxRows
			}
			maxRows = n
		} else if strings.HasPrefix(arg, "include=") {
			val := strings.TrimPrefix(arg, "include=")
			val = strings.Trim(val, "[]")
			for _, f := range strings.Split(val, ",") {
				f = strings.TrimSpace(f)
				if f != "" {
					includeFields = append(includeFields, f)
				}
			}
		} else if joinKey == "" {
			// First positional argument is the join key
			joinKey = arg
		} else {
			return fmt.Errorf("join() unexpected argument: '%s'", arg)
		}
	}

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

	// Translate subquery with the same security context (fractal, time range)
	subOpts := QueryOptions{
		StartTime:             ctx.Opts.StartTime,
		EndTime:               ctx.Opts.EndTime,
		MaxRows:               maxRows,
		FractalID:             ctx.Opts.FractalID,
		FractalIDs:            ctx.Opts.FractalIDs,
		IncludeEmptyFractalID: ctx.Opts.IncludeEmptyFractalID,
		Dictionaries:          ctx.Opts.Dictionaries,
		GeoIPEnabled:          ctx.Opts.GeoIPEnabled,
		TableName:             ctx.Opts.TableName,
		UseIngestTimestamp:     ctx.Opts.UseIngestTimestamp,
	}

	subResult, err := TranslateToSQLWithOrder(subPipeline, subOpts)
	if err != nil {
		return fmt.Errorf("join() subquery translation error: %w", err)
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
	ctx.Plan.JoinMaxRows = maxRows

	return nil
}

func init() {
	registerCommand(&joinHandler{}, "join")
}
