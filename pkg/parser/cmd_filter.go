package parser

import (
	"fmt"
	"strings"
)

// inHandler handles in(field, values=[v1,v2,...])
type inHandler struct{}

func (h *inHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *inHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) < 2 {
		return nil
	}

	field := cmd.Arguments[0]
	var fieldRef string
	if ctx.Registry.IsComputed(field) {
		fieldRef = field
	} else {
		switch field {
		case "timestamp", "raw_log", "log_id":
			fieldRef = field
		default:
			fieldRef = jsonFieldRef(field)
		}
	}

	// Parse values from remaining arguments
	var values []string
	for _, arg := range cmd.Arguments[1:] {
		arg = strings.TrimSpace(arg)
		if strings.HasPrefix(arg, "values=") {
			arg = strings.TrimPrefix(arg, "values=")
		}
		// Strip surrounding brackets if present
		arg = strings.Trim(arg, "[]")
		for _, v := range strings.Split(arg, ",") {
			v = strings.TrimSpace(v)
			v = strings.Trim(v, "\"'")
			if v != "" {
				values = append(values, fmt.Sprintf("'%s'", escapeString(v)))
			}
		}
	}

	if len(values) > 0 {
		inExpr := fmt.Sprintf("%s IN (%s)", fieldRef, strings.Join(values, ", "))
		if cmd.Negate {
			inExpr = fmt.Sprintf("%s NOT IN (%s)", fieldRef, strings.Join(values, ", "))
		}
		ctx.Plan.SourceStage().Layer.Where = append(ctx.Plan.SourceStage().Layer.Where, inExpr)
	}
	return nil
}

// cidrHandler handles cidr(field, "range") - filter by CIDR range
type cidrHandler struct{}

func (h *cidrHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *cidrHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) >= 2 {
		field := cmd.Arguments[0]
		cidrRange := strings.Trim(cmd.Arguments[1], "\"'")
		fieldRef := resolveFieldRef(field, ctx.Registry)
		// isIPAddressInRange throws CANNOT_PARSE_TEXT on any value that is not a
		// valid IP literal. Type-hinted JSON fields default missing values to ''
		// (dynamic paths previously yielded NULL, which the function tolerated),
		// and some sources store non-IP values, so a single bad row aborts the
		// whole query. Guard both ends without relying on short-circuit evaluation:
		//   - feed the function a real address only (sentinel otherwise) so it can
		//     never be handed an unparseable value, and
		//   - AND the validity check so non-IP rows are always "not in range",
		//     correct for every CIDR including 0.0.0.0/0.
		// isIPv4String/isIPv6String accept any string without throwing.
		valid := fmt.Sprintf("(isIPv4String(%[1]s) OR isIPv6String(%[1]s))", fieldRef)
		safeAddr := fmt.Sprintf("if(%s, %s, '0.0.0.0')", valid, fieldRef)
		cidrExpr := fmt.Sprintf("(isIPAddressInRange(%s, '%s') AND %s)", safeAddr, escapeString(cidrRange), valid)
		if cmd.Negate {
			ctx.Plan.SourceStage().Layer.Where = append(ctx.Plan.SourceStage().Layer.Where, "NOT "+cidrExpr)
		} else {
			ctx.Plan.SourceStage().Layer.Where = append(ctx.Plan.SourceStage().Layer.Where, cidrExpr)
		}
	}
	return nil
}

// commentHandler handles comment() - filter to logs with matching comments
type commentHandler struct{}

func (h *commentHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return nil
}

func (h *commentHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if !ctx.Opts.HasCommentFilter {
		return fmt.Errorf("comment() requires server-side pre-processing")
	}

	if len(ctx.Opts.CommentLogIDs) == 0 {
		// No matching comments - return empty result
		ctx.Plan.SourceStage().Layer.Where = append(ctx.Plan.SourceStage().Layer.Where, "1 = 0")
		return nil
	}

	// Build IN clause with pre-fetched log IDs
	quoted := make([]string, len(ctx.Opts.CommentLogIDs))
	for i, id := range ctx.Opts.CommentLogIDs {
		quoted[i] = fmt.Sprintf("'%s'", escapeString(id))
	}
	ctx.Plan.SourceStage().Layer.Where = append(ctx.Plan.SourceStage().Layer.Where,
		fmt.Sprintf("log_id IN (%s)", strings.Join(quoted, ", ")))
	return nil
}

func init() {
	registerCommand(&inHandler{}, "in")
	registerCommand(&cidrHandler{}, "cidr")
	registerCommand(&commentHandler{}, "comment")
	registerCommand(&commentHandler{}, "comments")
}
