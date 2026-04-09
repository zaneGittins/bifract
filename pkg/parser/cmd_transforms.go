package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// strftimeHandler handles strftime(format, field=timestamp, timezone=UTC, as=_time)
type strftimeHandler struct{}

func (h *strftimeHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	alias := "_time"
	field := "timestamp"
	timezone := "UTC"
	var formatStr string
	for _, arg := range cmd.Arguments {
		a := strings.TrimSpace(arg)
		if strings.HasPrefix(a, "as=") {
			alias = strings.TrimPrefix(a, "as=")
		} else if strings.HasPrefix(a, "field=") {
			field = strings.TrimPrefix(a, "field=")
		} else if strings.HasPrefix(a, "timezone=") {
			timezone = strings.Trim(strings.TrimPrefix(a, "timezone="), "\"'")
		} else if formatStr == "" {
			formatStr = strings.Trim(a, "\"'")
		}
	}
	if formatStr != "" {
		chFmt := convertTimeFormat(formatStr)
		src := "timestamp"
		if field != "timestamp" {
			src = fmt.Sprintf("toDateTime(%s)", jsonFieldRef(field))
		}
		expr := fmt.Sprintf("formatDateTime(%s, '%s', '%s')", src, escapeString(chFmt), escapeString(timezone))
		ctx.Registry.Register(alias, FieldKindPerRow, expr, ctx.CmdIndex)
	} else {
		ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
	}
	return nil
}

func (h *strftimeHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return fmt.Errorf("strftime() requires a format string")
	}
	field := "timestamp"
	timezone := "UTC"
	alias := "_time"
	var formatStr string
	for _, arg := range cmd.Arguments {
		arg = strings.TrimSpace(arg)
		if strings.HasPrefix(arg, "as=") {
			alias = strings.TrimPrefix(arg, "as=")
		} else if strings.HasPrefix(arg, "field=") {
			field = strings.TrimPrefix(arg, "field=")
		} else if strings.HasPrefix(arg, "timezone=") {
			timezone = strings.Trim(strings.TrimPrefix(arg, "timezone="), "\"'")
		} else if formatStr == "" {
			formatStr = strings.Trim(arg, "\"'")
		}
	}
	if formatStr == "" {
		return fmt.Errorf("strftime() requires a format string")
	}
	safeAlias, err := sanitizeIdentifier(alias)
	if err != nil {
		return fmt.Errorf("strftime(): invalid alias: %w", err)
	}
	chFormat := convertTimeFormat(formatStr)
	var expr string
	if field == "timestamp" {
		expr = fmt.Sprintf("formatDateTime(timestamp, '%s', '%s') AS %s", escapeString(chFormat), escapeString(timezone), safeAlias)
	} else {
		expr = fmt.Sprintf("formatDateTime(toDateTime(%s), '%s', '%s') AS %s", jsonFieldRef(field), escapeString(chFormat), escapeString(timezone), safeAlias)
	}
	ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
	src := "timestamp"
	if field != "timestamp" {
		src = fmt.Sprintf("toDateTime(%s)", jsonFieldRef(field))
	}
	ctx.Registry.SetResolveExpr(safeAlias, fmt.Sprintf("formatDateTime(%s, '%s', '%s')", src, escapeString(chFormat), escapeString(timezone)))
	return nil
}

// lowercaseHandler handles lowercase(field, output_field)
type lowercaseHandler struct{}

func (h *lowercaseHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		outputField := cmd.Arguments[0]
		if len(cmd.Arguments) > 1 {
			outputField = cmd.Arguments[1]
		}
		ctx.Registry.Register(outputField, FieldKindPerRow, outputField, ctx.CmdIndex)
	}
	return nil
}

func (h *lowercaseHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		field := cmd.Arguments[0]
		outputField := field
		if len(cmd.Arguments) > 1 {
			outputField = cmd.Arguments[1]
		}
		safeOutput, err := sanitizeIdentifier(outputField)
		if err != nil {
			return fmt.Errorf("lowercase(): invalid output field: %w", err)
		}
		var expr string
		if field == "timestamp" {
			expr = fmt.Sprintf("lower(toString(timestamp)) AS %s", safeOutput)
		} else {
			expr = fmt.Sprintf("lower(%s) AS %s", resolveFieldRef(field, ctx.Registry), safeOutput)
		}
		ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr(outputField, expr)
	}
	return nil
}

// uppercaseHandler handles uppercase(field, output_field)
type uppercaseHandler struct{}

func (h *uppercaseHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		outputField := cmd.Arguments[0]
		if len(cmd.Arguments) > 1 {
			outputField = cmd.Arguments[1]
		}
		ctx.Registry.Register(outputField, FieldKindPerRow, outputField, ctx.CmdIndex)
	}
	return nil
}

func (h *uppercaseHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		field := cmd.Arguments[0]
		outputField := field
		if len(cmd.Arguments) > 1 {
			outputField = cmd.Arguments[1]
		}
		safeOutput, err := sanitizeIdentifier(outputField)
		if err != nil {
			return fmt.Errorf("uppercase(): invalid output field: %w", err)
		}
		var expr string
		if field == "timestamp" {
			expr = fmt.Sprintf("upper(toString(timestamp)) AS %s", safeOutput)
		} else {
			expr = fmt.Sprintf("upper(%s) AS %s", resolveFieldRef(field, ctx.Registry), safeOutput)
		}
		ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr(outputField, expr)
	}
	return nil
}

// evalHandler handles eval(field = expression)
type evalHandler struct{}

func (h *evalHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	for _, arg := range cmd.Arguments {
		if strings.Contains(arg, "=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) == 2 {
				fieldName := strings.TrimSpace(parts[0])
				ctx.Registry.Register(fieldName, FieldKindPerRow, fieldName, ctx.CmdIndex)
			}
		}
	}
	return nil
}

func (h *evalHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		for _, arg := range cmd.Arguments {
			if strings.Contains(arg, "=") {
				parts := strings.SplitN(arg, "=", 2)
				if len(parts) == 2 {
					fieldName := strings.TrimSpace(parts[0])
					expression := strings.TrimSpace(parts[1])

					safeFieldName, err := sanitizeIdentifier(fieldName)
					if err != nil {
						return fmt.Errorf("eval(): invalid field name: %w", err)
					}

					var sqlExpr string
					if expression == "timestamp" {
						sqlExpr = "timestamp"
					} else if strings.ContainsAny(expression, "+-*/()") {
						sqlExpr = convertMathExprToSQL(expression, ctx.Registry)
					} else {
						if strings.HasPrefix(expression, "\"") && strings.HasSuffix(expression, "\"") {
							inner := expression[1 : len(expression)-1]
							sqlExpr = fmt.Sprintf("'%s'", escapeString(inner))
						} else {
							sqlExpr = jsonFieldRef(expression)
						}
					}

					expr := fmt.Sprintf("%s AS %s", sqlExpr, safeFieldName)
					ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
					ctx.Registry.SetResolveExpr(fieldName, expr)
				}
			}
		}
	}
	return nil
}

// regexHandler handles regex(pattern, field=raw_log)
type regexHandler struct{}

func (h *regexHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		var pattern string
		for _, arg := range cmd.Arguments {
			arg = strings.TrimSpace(arg)
			if strings.HasPrefix(arg, "regex=") {
				pattern = strings.Trim(strings.TrimPrefix(arg, "regex="), `"'`)
			} else if !strings.HasPrefix(arg, "field=") && pattern == "" {
				pattern = arg
			}
		}
		namedGroupRe := regexp.MustCompile(`\(\?<([a-zA-Z_][a-zA-Z0-9_]*)>`)
		namedGroups := namedGroupRe.FindAllStringSubmatch(pattern, -1)
		if len(namedGroups) > 0 {
			for _, match := range namedGroups {
				ctx.Registry.Register(match[1], FieldKindPerRow, match[1], ctx.CmdIndex)
			}
		} else {
			ctx.Registry.Register("regex_match", FieldKindPerRow, "regex_match", ctx.CmdIndex)
		}
	}
	return nil
}

func (h *regexHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		var pattern, field string
		field = "raw_log"
		for _, arg := range cmd.Arguments {
			arg = strings.TrimSpace(arg)
			if strings.HasPrefix(arg, "field=") {
				field = strings.TrimPrefix(arg, "field=")
			} else if strings.HasPrefix(arg, "regex=") {
				pattern = strings.TrimPrefix(arg, "regex=")
				pattern = strings.Trim(pattern, `"'`)
			} else if pattern == "" {
				pattern = arg
			} else if field == "raw_log" {
				field = arg
			}
		}
		if pattern == "" {
			return fmt.Errorf("regex() requires a pattern")
		}

		fieldRef := "raw_log"
		if field != "raw_log" && field != "timestamp" {
			fieldRef = jsonFieldRef(field)
		} else if field == "timestamp" {
			fieldRef = "toString(timestamp)"
		}

		namedGroupRe := regexp.MustCompile(`\(\?<([a-zA-Z_][a-zA-Z0-9_]*)>`)
		namedGroups := namedGroupRe.FindAllStringSubmatch(pattern, -1)

		if len(namedGroups) > 0 {
			for i, match := range namedGroups {
				name := match[1]
				safeName, err := sanitizeIdentifier(name)
				if err != nil {
					return fmt.Errorf("regex(): invalid capture name %q: %w", name, err)
				}
				expr := fmt.Sprintf("extractAllGroups(%s, '%s')[1][%d] AS %s", fieldRef, escapeString(pattern), i+1, safeName)
				ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
				ctx.Registry.SetResolveExpr(safeName, fmt.Sprintf("extractAllGroups(%s, '%s')[1][%d]", fieldRef, escapeString(pattern), i+1))
			}
		} else {
			expr := fmt.Sprintf("extractAllGroups(%s, '%s') AS regex_match", fieldRef, escapeString(pattern))
			ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
			ctx.Registry.SetResolveExpr("regex_match", expr)
		}
	}
	return nil
}

// replaceHandler handles replace(regex, with, field, output_field)
type replaceHandler struct{}

func (h *replaceHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	outputField := ""
	if len(cmd.Arguments) > 2 {
		outputField = cmd.Arguments[2]
	}
	if len(cmd.Arguments) > 3 {
		outputField = cmd.Arguments[3]
	}
	if outputField != "" {
		ctx.Registry.Register(outputField, FieldKindPerRow, outputField, ctx.CmdIndex)
	}
	return nil
}

func (h *replaceHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) >= 2 {
		pattern := cmd.Arguments[0]
		replacement := cmd.Arguments[1]
		field := "raw_log"
		outputField := field
		if len(cmd.Arguments) > 2 {
			field = cmd.Arguments[2]
		}
		if len(cmd.Arguments) > 3 {
			outputField = cmd.Arguments[3]
		}

		fieldRef := "raw_log"
		if field != "raw_log" && field != "timestamp" {
			fieldRef = jsonFieldRef(field)
		} else if field == "timestamp" {
			fieldRef = "toString(timestamp)"
		}

		safeOutput, err := sanitizeIdentifier(outputField)
		if err != nil {
			return fmt.Errorf("replace(): invalid output field: %w", err)
		}

		expr := fmt.Sprintf("replaceRegexpAll(%s, '%s', '%s') AS %s",
			fieldRef, escapeString(pattern), escapeString(replacement), safeOutput)
		ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr(outputField, expr)
	}
	return nil
}

// concatHandler handles concat([field1,field2,...], as=alias)
type concatHandler struct{}

func (h *concatHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	alias := "_concat"
	for _, arg := range cmd.Arguments {
		arg = strings.TrimSpace(arg)
		if strings.HasPrefix(arg, "as=") {
			alias = strings.TrimPrefix(arg, "as=")
		}
	}
	ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
	return nil
}

func (h *concatHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return fmt.Errorf("concat() requires at least one argument with fields in brackets, e.g. concat([field1,field2])")
	}
	alias := "_concat"
	var fields []string
	for _, arg := range cmd.Arguments {
		arg = strings.TrimSpace(arg)
		if strings.HasPrefix(arg, "as=") {
			alias = strings.TrimPrefix(arg, "as=")
			continue
		}
		// Handle bracket syntax: [field1,field2,...]
		if strings.HasPrefix(arg, "[") && strings.HasSuffix(arg, "]") {
			inner := strings.Trim(arg, "[]")
			for _, f := range strings.Split(inner, ",") {
				f = strings.TrimSpace(f)
				if f == "" {
					continue
				}
				if f == "timestamp" {
					fields = append(fields, "toString(timestamp)")
				} else {
					fields = append(fields, jsonFieldRef(f))
				}
			}
		} else {
			// Single field without brackets
			if arg == "timestamp" {
				fields = append(fields, "toString(timestamp)")
			} else {
				fields = append(fields, jsonFieldRef(arg))
			}
		}
	}
	if len(fields) == 0 {
		return fmt.Errorf("concat() requires at least one field")
	}
	safeOutput, err := sanitizeIdentifier(alias)
	if err != nil {
		return fmt.Errorf("concat(): invalid output field: %w", err)
	}
	expr := fmt.Sprintf("concat(%s) AS %s", strings.Join(fields, ", "), safeOutput)
	ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
	ctx.Registry.SetResolveExpr(alias, expr)
	return nil
}

// hashHandler handles hash(field1, field2, as=alias)
type hashHandler struct{}

func (h *hashHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	alias := "hash_key"
	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(strings.TrimSpace(arg), "as=") {
			alias = strings.TrimPrefix(strings.TrimSpace(arg), "as=")
		}
	}
	ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
	return nil
}

func (h *hashHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return fmt.Errorf("hash() requires at least one field")
	}
	var hashFields []string
	alias := "hash_key"
	for _, arg := range cmd.Arguments {
		arg = strings.TrimSpace(arg)
		if strings.HasPrefix(arg, "as=") {
			alias = strings.TrimPrefix(arg, "as=")
			continue
		}
		if strings.HasPrefix(arg, "field=") {
			arg = strings.TrimPrefix(arg, "field=")
		}
		if arg == "timestamp" {
			hashFields = append(hashFields, "toString(timestamp)")
		} else {
			hashFields = append(hashFields, resolveFieldRef(arg, ctx.Registry))
		}
	}
	safeAlias, err := sanitizeIdentifier(alias)
	if err != nil {
		return fmt.Errorf("hash(): invalid alias: %w", err)
	}
	expr := fmt.Sprintf("hex(cityHash64(%s)) AS %s", strings.Join(hashFields, ", "), safeAlias)
	ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
	ctx.Registry.SetResolveExpr(safeAlias, fmt.Sprintf("hex(cityHash64(%s))", strings.Join(hashFields, ", ")))
	return nil
}

// nowHandler handles now(output_field)
type nowHandler struct{}

func (h *nowHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	outputField := "_now"
	if len(cmd.Arguments) > 0 {
		outputField = cmd.Arguments[0]
	}
	ctx.Registry.Register(outputField, FieldKindPerRow, outputField, ctx.CmdIndex)
	return nil
}

func (h *nowHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	outputField := "_now"
	if len(cmd.Arguments) > 0 {
		outputField = cmd.Arguments[0]
	}
	safeOutput, err := sanitizeIdentifier(outputField)
	if err != nil {
		return fmt.Errorf("now(): invalid output field: %w", err)
	}
	expr := fmt.Sprintf("now() AS %s", safeOutput)
	ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
	ctx.Registry.SetResolveExpr(outputField, expr)
	return nil
}

// caseHandler handles case { condition | result ; ... }
type caseHandler struct{}

func (h *caseHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		outputField := "case_result"
		if len(cmd.Arguments) > 1 {
			outputField = cmd.Arguments[1]
		}
		ctx.Registry.Register(outputField, FieldKindPerRow, outputField, ctx.CmdIndex)

		caseExpr := cmd.Arguments[0]
		_, _, caseAssignments := parseCaseConditions(caseExpr)
		for _, assignment := range caseAssignments {
			ctx.Registry.Register(assignment.Field, FieldKindPerRow, assignment.Field, ctx.CmdIndex)
		}
	}
	return nil
}

func (h *caseHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		outputField := "case_result"
		if len(cmd.Arguments) > 1 {
			outputField = cmd.Arguments[1]
		}

		caseExpr := cmd.Arguments[0]
		whenClauses, defaultClause, caseAssignments := parseCaseConditions(caseExpr)

		if len(caseAssignments) > 0 {
			for _, assignment := range caseAssignments {
				expr := fmt.Sprintf("%s AS %s", assignment.Expression, assignment.Field)
				ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
				ctx.Registry.SetResolveExpr(assignment.Field, expr)
			}
		} else if len(whenClauses) > 0 {
			caseSQL := fmt.Sprintf("CASE %s", strings.Join(whenClauses, " "))
			if defaultClause != "" {
				caseSQL += fmt.Sprintf(" ELSE %s", defaultClause)
			} else {
				caseSQL += " ELSE NULL"
			}
			caseSQL += fmt.Sprintf(" END AS %s", outputField)
			ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: caseSQL})
			ctx.Registry.SetResolveExpr(outputField, caseSQL)
		}
	}
	return nil
}

// lenHandler handles len(field)
type lenHandler struct{}

func (h *lenHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	// _len produces a numeric SELECT alias; FieldKindAssignment lets condition
	// routing reference it by name without wrapping in toFloat64OrZero.
	ctx.Registry.Register("_len", FieldKindAssignment, "_len", ctx.CmdIndex)
	return nil
}

func (h *lenHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		field := cmd.Arguments[0]
		fieldRef := resolveFieldRef(field, ctx.Registry)
		expr := fmt.Sprintf("length(%s) AS _len", fieldRef)
		ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr("_len", fmt.Sprintf("length(%s)", fieldRef))
	}
	return nil
}

// levenshteinHandler handles levenshtein(field1, field2)
type levenshteinHandler struct{}

func (h *levenshteinHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	// _distance produces a numeric SELECT alias; same reasoning as _len.
	ctx.Registry.Register("_distance", FieldKindAssignment, "_distance", ctx.CmdIndex)
	return nil
}

func (h *levenshteinHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) >= 2 {
		arg1 := cmd.Arguments[0]
		arg2 := cmd.Arguments[1]
		var ref1, ref2 string
		if strings.HasPrefix(arg1, "\"") && strings.HasSuffix(arg1, "\"") {
			ref1 = fmt.Sprintf("'%s'", escapeString(strings.Trim(arg1, "\"")))
		} else {
			ref1 = resolveFieldRef(arg1, ctx.Registry)
		}
		if strings.HasPrefix(arg2, "\"") && strings.HasSuffix(arg2, "\"") {
			ref2 = fmt.Sprintf("'%s'", escapeString(strings.Trim(arg2, "\"")))
		} else {
			ref2 = resolveFieldRef(arg2, ctx.Registry)
		}
		expr := fmt.Sprintf("damerauLevenshteinDistance(%s, %s) AS _distance", ref1, ref2)
		ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr("_distance", fmt.Sprintf("damerauLevenshteinDistance(%s, %s)", ref1, ref2))
	}
	return nil
}

// base64decodeHandler handles base64decode(field)
type base64decodeHandler struct{}

func (h *base64decodeHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Registry.Register("_decoded", FieldKindPerRow, "_decoded", ctx.CmdIndex)
	return nil
}

func (h *base64decodeHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		field := cmd.Arguments[0]
		fieldRef := resolveFieldRef(field, ctx.Registry)
		expr := fmt.Sprintf("tryBase64Decode(%s) AS _decoded", fieldRef)
		ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr("_decoded", fmt.Sprintf("tryBase64Decode(%s)", fieldRef))
	}
	return nil
}

// splitHandler handles split(field, delimiter, index)
type splitHandler struct{}

func (h *splitHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Registry.Register("_split", FieldKindPerRow, "_split", ctx.CmdIndex)
	return nil
}

func (h *splitHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) >= 3 {
		field := cmd.Arguments[0]
		delimiter := strings.Trim(cmd.Arguments[1], "\"'")
		indexStr := cmd.Arguments[2]
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return fmt.Errorf("split(): index must be numeric, got %q", indexStr)
		}
		fieldRef := resolveFieldRef(field, ctx.Registry)
		expr := fmt.Sprintf("splitByString('%s', %s)[%d] AS _split", escapeString(delimiter), fieldRef, index)
		ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr("_split", fmt.Sprintf("splitByString('%s', %s)[%d]", escapeString(delimiter), fieldRef, index))
	}
	return nil
}

// substrHandler handles substr(field, start, length)
type substrHandler struct{}

func (h *substrHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Registry.Register("_substr", FieldKindPerRow, "_substr", ctx.CmdIndex)
	return nil
}

func (h *substrHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) >= 2 {
		field := cmd.Arguments[0]
		startStr := cmd.Arguments[1]
		start, err := strconv.Atoi(startStr)
		if err != nil {
			return fmt.Errorf("substr(): start must be numeric, got %q", startStr)
		}
		fieldRef := resolveFieldRef(field, ctx.Registry)
		if len(cmd.Arguments) >= 3 {
			lengthStr := cmd.Arguments[2]
			length, err := strconv.Atoi(lengthStr)
			if err != nil {
				return fmt.Errorf("substr(): length must be numeric, got %q", lengthStr)
			}
			expr := fmt.Sprintf("substring(%s, %d, %d) AS _substr", fieldRef, start, length)
			ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
			ctx.Registry.SetResolveExpr("_substr", fmt.Sprintf("substring(%s, %d, %d)", fieldRef, start, length))
		} else {
			expr := fmt.Sprintf("substring(%s, %d) AS _substr", fieldRef, start)
			ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
			ctx.Registry.SetResolveExpr("_substr", fmt.Sprintf("substring(%s, %d)", fieldRef, start))
		}
	}
	return nil
}

// urldecodeHandler handles urldecode(field)
type urldecodeHandler struct{}

func (h *urldecodeHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Registry.Register("_urldecoded", FieldKindPerRow, "_urldecoded", ctx.CmdIndex)
	return nil
}

func (h *urldecodeHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) > 0 {
		field := cmd.Arguments[0]
		fieldRef := resolveFieldRef(field, ctx.Registry)
		expr := fmt.Sprintf("decodeURLComponent(%s) AS _urldecoded", fieldRef)
		ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr("_urldecoded", fmt.Sprintf("decodeURLComponent(%s)", fieldRef))
	}
	return nil
}

// coalesceHandler handles coalesce(field1, field2, ...)
type coalesceHandler struct{}

func (h *coalesceHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	ctx.Registry.Register("_coalesced", FieldKindPerRow, "_coalesced", ctx.CmdIndex)
	return nil
}

func (h *coalesceHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) >= 2 {
		var conditions []string
		for _, field := range cmd.Arguments {
			ref := resolveFieldRef(field, ctx.Registry)
			conditions = append(conditions, fmt.Sprintf("%s != '' AND %s IS NOT NULL, %s", ref, ref, ref))
		}
		expr := fmt.Sprintf("multiIf(%s, '') AS _coalesced", strings.Join(conditions, ", "))
		ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr("_coalesced", fmt.Sprintf("multiIf(%s, '')", strings.Join(conditions, ", ")))
	}
	return nil
}

// sprintfHandler handles sprintf(format, field1, field2, ..., as=alias)
type sprintfHandler struct{}

func (h *sprintfHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	alias := "_sprintf"
	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(strings.TrimSpace(arg), "as=") {
			alias = strings.Trim(strings.TrimPrefix(strings.TrimSpace(arg), "as="), "\"'")
		}
	}
	ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
	return nil
}

func (h *sprintfHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if len(cmd.Arguments) == 0 {
		return fmt.Errorf("sprintf() requires a format string")
	}
	alias := "_sprintf"
	formatStr := strings.Trim(cmd.Arguments[0], "\"'")
	var fieldRefs []string
	for _, arg := range cmd.Arguments[1:] {
		arg = strings.TrimSpace(arg)
		if strings.HasPrefix(arg, "as=") {
			alias = strings.Trim(strings.TrimPrefix(arg, "as="), "\"'")
			continue
		}
		ref := resolveFieldRef(arg, ctx.Registry)
		fieldRefs = append(fieldRefs, fmt.Sprintf("ifNull(%s, '')", ref))
	}
	safeAlias, err := sanitizeIdentifier(alias)
	if err != nil {
		return fmt.Errorf("sprintf(): invalid alias: %w", err)
	}
	var printfArgs string
	if len(fieldRefs) > 0 {
		printfArgs = fmt.Sprintf("'%s', %s", escapeString(formatStr), strings.Join(fieldRefs, ", "))
	} else {
		printfArgs = fmt.Sprintf("'%s'", escapeString(formatStr))
	}
	expr := fmt.Sprintf("printf(%s) AS %s", printfArgs, safeAlias)
	ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
	ctx.Registry.SetResolveExpr(safeAlias, fmt.Sprintf("printf(%s)", printfArgs))
	return nil
}

// matchHandler handles match(dict="name", field=logfield, column=keycolumn, include=[col1,col2])
type matchHandler struct{}

func (h *matchHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	var logField, keyColumn, dictName string
	var includeColumns []string
	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "include=") {
			val := strings.TrimPrefix(arg, "include=")
			val = strings.Trim(val, "[]")
			for _, c := range strings.Split(val, ",") {
				c = strings.TrimSpace(c)
				if c != "" {
					includeColumns = append(includeColumns, c)
				}
			}
		} else if strings.HasPrefix(arg, "field=") {
			logField = strings.TrimPrefix(arg, "field=")
		} else if strings.HasPrefix(arg, "column=") {
			keyColumn = strings.TrimPrefix(arg, "column=")
		} else if strings.HasPrefix(arg, "dict=") {
			dictName = strings.Trim(strings.TrimPrefix(arg, "dict="), `"'`)
		}
	}

	// Resolve the ClickHouse dictionary name so we can build the real expression.
	chLookupName := ""
	if ctx.Opts.Dictionaries != nil && dictName != "" && keyColumn != "" {
		if colMap, ok := ctx.Opts.Dictionaries[dictName]; ok {
			chLookupName = colMap[keyColumn]
		}
	}

	var fieldRef string
	if logField == "timestamp" {
		fieldRef = "toString(timestamp)"
	} else if logField != "" {
		fieldRef = jsonFieldRef(logField)
	}

	for _, c := range includeColumns {
		if chLookupName != "" && fieldRef != "" {
			expr := fmt.Sprintf("dictGetOrDefault('%s', '%s', %s, '')",
				escapeString(chLookupName), escapeString(c), fieldRef)
			ctx.Registry.Register(c, FieldKindPerRow, expr, ctx.CmdIndex)
		} else {
			ctx.Registry.Register(c, FieldKindPerRow, c, ctx.CmdIndex)
		}
	}
	return nil
}

func (h *matchHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	var dictName, logField, keyColumn string
	var includeColumns []string
	strict := false

	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "dict=") {
			dictName = strings.Trim(strings.TrimPrefix(arg, "dict="), `"'`)
		} else if strings.HasPrefix(arg, "field=") {
			logField = strings.TrimPrefix(arg, "field=")
		} else if strings.HasPrefix(arg, "column=") {
			keyColumn = strings.TrimPrefix(arg, "column=")
		} else if strings.HasPrefix(arg, "include=") {
			val := strings.TrimPrefix(arg, "include=")
			val = strings.Trim(val, "[]")
			for _, c := range strings.Split(val, ",") {
				c = strings.TrimSpace(c)
				if c != "" {
					includeColumns = append(includeColumns, c)
				}
			}
		} else if strings.HasPrefix(arg, "strict=") {
			strict = strings.ToLower(strings.TrimPrefix(arg, "strict=")) == "true"
		}
	}

	if dictName == "" {
		return fmt.Errorf("match() requires dict= parameter")
	}
	if logField == "" {
		return fmt.Errorf("match() requires field= parameter")
	}
	if len(includeColumns) == 0 {
		return fmt.Errorf("match() requires include= parameter with at least one column")
	}
	if keyColumn == "" {
		return fmt.Errorf("match() requires column= parameter to specify the lookup key column")
	}

	chLookupName := ""
	if ctx.Opts.Dictionaries != nil {
		if colMap, ok := ctx.Opts.Dictionaries[dictName]; ok {
			chLookupName = colMap[keyColumn]
		}
	}
	if chLookupName == "" {
		return fmt.Errorf("dictionary %q with key column %q not found - use the key toggle in the Dicts UI to enable it", dictName, keyColumn)
	}

	var fieldRef string
	if logField == "timestamp" {
		fieldRef = "toString(timestamp)"
	} else {
		fieldRef = jsonFieldRef(logField)
	}

	for _, col := range includeColumns {
		safeCol, colErr := sanitizeIdentifier(col)
		if colErr != nil {
			return fmt.Errorf("match(): invalid include column: %w", colErr)
		}
		expr := fmt.Sprintf("dictGetOrDefault('%s', '%s', %s, '') AS %s",
			escapeString(chLookupName), escapeString(col), fieldRef, safeCol)
		ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr(col, expr)
	}

	if strict {
		ctx.Plan.SourceStage().Layer.Where = append(ctx.Plan.SourceStage().Layer.Where,
			fmt.Sprintf("dictHas('%s', %s)", escapeString(chLookupName), fieldRef))
	}
	return nil
}

// lookupIPHandler handles lookupIP(field=src_ip, include=[country,city,asn,as_org])
// Enriches logs with GeoIP and ASN data from MaxMind GeoLite2 dictionaries.
type lookupIPHandler struct{}

var geoIPCityFields = map[string]string{
	"country":     "String",
	"city":        "String",
	"subdivision": "String",
	"continent":   "String",
	"timezone":    "String",
	"latitude":    "Float64",
	"longitude":   "Float64",
	"postal_code": "String",
}

var geoIPASNFields = map[string]string{
	"asn":    "UInt32",
	"as_org": "String",
}

func geoipDictName(field string) string {
	if _, ok := geoIPASNFields[field]; ok {
		return "geoip_asn_lookup"
	}
	return "geoip_city_lookup"
}

func geoipDefaultValue(field string) string {
	if t, ok := geoIPASNFields[field]; ok && t == "UInt32" {
		return "toUInt32(0)"
	}
	if t, ok := geoIPCityFields[field]; ok && t == "Float64" {
		return "toFloat64(0)"
	}
	return "''"
}

// geoipIsNumeric returns true if the field type is not String and needs toString() wrapping.
func geoipIsNumeric(field string) bool {
	if t, ok := geoIPASNFields[field]; ok && t != "String" {
		return true
	}
	if t, ok := geoIPCityFields[field]; ok && t != "String" {
		return true
	}
	return false
}

// geoipLookupExpr builds the full dictGetOrDefault expression, wrapping numeric
// results in toString() so the query handler can scan them as strings.
func geoipLookupExpr(field, fieldRef string) string {
	dictName := geoipDictName(field)
	defVal := geoipDefaultValue(field)
	inner := fmt.Sprintf("dictGetOrDefault('%s', '%s', toIPv4OrDefault(%s), %s)",
		escapeString(dictName), escapeString(field), fieldRef, defVal)
	if geoipIsNumeric(field) {
		return fmt.Sprintf("toString(%s)", inner)
	}
	return inner
}

func (h *lookupIPHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	var ipField string
	var includeColumns []string
	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "include=") {
			val := strings.TrimPrefix(arg, "include=")
			val = strings.Trim(val, "[]")
			for _, c := range strings.Split(val, ",") {
				c = strings.TrimSpace(c)
				if c != "" {
					includeColumns = append(includeColumns, c)
				}
			}
		} else if strings.HasPrefix(arg, "field=") {
			ipField = strings.TrimPrefix(arg, "field=")
		}
	}

	var fieldRef string
	if ipField != "" {
		fieldRef = jsonFieldRef(ipField)
	}

	for _, c := range includeColumns {
		if fieldRef != "" && ctx.Opts.GeoIPEnabled {
			expr := geoipLookupExpr(c, fieldRef)
			ctx.Registry.Register(c, FieldKindPerRow, expr, ctx.CmdIndex)
		} else {
			ctx.Registry.Register(c, FieldKindPerRow, c, ctx.CmdIndex)
		}
	}
	return nil
}

func (h *lookupIPHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	var ipField string
	var includeColumns []string

	for _, arg := range cmd.Arguments {
		if strings.HasPrefix(arg, "field=") {
			ipField = strings.TrimPrefix(arg, "field=")
		} else if strings.HasPrefix(arg, "include=") {
			val := strings.TrimPrefix(arg, "include=")
			val = strings.Trim(val, "[]")
			for _, c := range strings.Split(val, ",") {
				c = strings.TrimSpace(c)
				if c != "" {
					includeColumns = append(includeColumns, c)
				}
			}
		}
	}

	if ipField == "" {
		return fmt.Errorf("lookupIP() requires field= parameter specifying the IP address field")
	}
	if len(includeColumns) == 0 {
		return fmt.Errorf("lookupIP() requires include= parameter with at least one column (country, city, asn, as_org, etc.)")
	}
	if !ctx.Opts.GeoIPEnabled {
		return fmt.Errorf("lookupIP() requires MaxMind GeoLite2 configuration (set MAXMIND_LICENSE_KEY and MAXMIND_ACCOUNT_ID)")
	}

	fieldRef := jsonFieldRef(ipField)

	for _, col := range includeColumns {
		if _, okCity := geoIPCityFields[col]; !okCity {
			if _, okASN := geoIPASNFields[col]; !okASN {
				return fmt.Errorf("lookupIP(): unknown column %q (available: country, city, subdivision, continent, timezone, latitude, longitude, postal_code, asn, as_org)", col)
			}
		}

		safeCol, colErr := sanitizeIdentifier(col)
		if colErr != nil {
			return fmt.Errorf("lookupIP(): invalid include column: %w", colErr)
		}

		lookupExpr := geoipLookupExpr(col, fieldRef)
		expr := fmt.Sprintf("%s AS %s", lookupExpr, safeCol)
		ctx.Plan.CurrentStage().Layer.Selects = append(ctx.Plan.CurrentStage().Layer.Selects, SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr(col, expr)
	}

	return nil
}

func init() {
	registerCommand(&strftimeHandler{}, "strftime")
	registerCommand(&lowercaseHandler{}, "lowercase")
	registerCommand(&uppercaseHandler{}, "uppercase")
	registerCommand(&evalHandler{}, "eval")
	registerCommand(&regexHandler{}, "regex")
	registerCommand(&replaceHandler{}, "replace")
	registerCommand(&concatHandler{}, "concat")
	registerCommand(&hashHandler{}, "hash")
	registerCommand(&nowHandler{}, "now")
	registerCommand(&caseHandler{}, "case")
	registerCommand(&lenHandler{}, "len")
	registerCommand(&levenshteinHandler{}, "levenshtein")
	registerCommand(&base64decodeHandler{}, "base64decode")
	registerCommand(&splitHandler{}, "split")
	registerCommand(&substrHandler{}, "substr")
	registerCommand(&urldecodeHandler{}, "urldecode")
	registerCommand(&coalesceHandler{}, "coalesce")
	registerCommand(&sprintfHandler{}, "sprintf")
	registerCommand(&matchHandler{}, "match")
	registerCommand(&lookupIPHandler{}, "lookupIP", "lookupip", "geoip")
}
