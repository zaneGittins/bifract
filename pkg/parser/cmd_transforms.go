package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// strftimeHandler handles strftime(format, field=timestamp, timezone=UTC, as=_time)
type strftimeHandler struct{}

func (h *strftimeHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	p, err := strftimeParams(cmd)
	if err != nil {
		return nil
	}
	if p.format != "" {
		expr, err := timeFormatExpr(p.field, convertTimeFormat(p.format), p.timezone, ctx.Registry)
		if err != nil {
			return nil
		}
		ctx.Registry.Register(p.alias, FieldKindPerRow, expr, ctx.CmdIndex)
	} else {
		ctx.Registry.Register(p.alias, FieldKindPerRow, p.alias, ctx.CmdIndex)
	}
	return nil
}

func (h *strftimeHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	p, err := strftimeParams(cmd)
	if err != nil {
		return err
	}
	if p.format == "" {
		return fmt.Errorf("strftime() requires a format string")
	}
	safeAlias, err := sanitizeIdentifier(p.alias)
	if err != nil {
		return fmt.Errorf("strftime(): invalid alias: %w", err)
	}
	formatted, err := timeFormatExpr(p.field, convertTimeFormat(p.format), p.timezone, ctx.Registry)
	if err != nil {
		return fmt.Errorf("strftime(): %w", err)
	}
	ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: fmt.Sprintf("%s AS %s", formatted, safeAlias)})
	ctx.Registry.SetResolveExpr(safeAlias, formatted)
	return nil
}

type strftimeArgs struct {
	format, timezone, alias string
	field                   Argument
}

// strftimeParams reads strftime()'s arguments once, so Declare and Execute agree.
func strftimeParams(cmd CommandNode) (strftimeArgs, error) {
	p := strftimeArgs{
		timezone: "UTC",
		alias:    "_time",
		field:    Argument{Kind: ArgLiteral, Text: "timestamp"},
	}
	b, err := BindCommand(cmd)
	if err != nil {
		return p, err
	}
	p.format = b.Str("format", "")
	if a, ok := b.First("field"); ok {
		p.field = a
	}
	p.timezone = b.Str("timezone", p.timezone)
	p.alias = b.Str("as", p.alias)
	return p, nil
}

// lowercaseHandler handles lowercase(field, output_field)
type lowercaseHandler struct{}

func (h *lowercaseHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return declareCaseFold(cmd, ctx)
}

func (h *lowercaseHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	return executeCaseFold(cmd, ctx, "lowercase", "lower")
}

// uppercaseHandler handles uppercase(field, output_field)
type uppercaseHandler struct{}

func (h *uppercaseHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return declareCaseFold(cmd, ctx)
}

func (h *uppercaseHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	return executeCaseFold(cmd, ctx, "uppercase", "upper")
}

// caseFoldOutput is the column lowercase()/uppercase() writes: the output= name
// when given, otherwise the input field rebound in place.
func caseFoldOutput(cmd CommandNode) (arg Argument, output string, ok bool) {
	b, err := BindCommand(cmd)
	if err != nil {
		return Argument{}, "", false
	}
	arg, ok = b.First("field")
	if !ok {
		return Argument{}, "", false
	}
	output = b.Str("output", arg.Value())
	return arg, output, output != ""
}

func declareCaseFold(cmd CommandNode, ctx *CommandContext) error {
	if _, output, ok := caseFoldOutput(cmd); ok {
		ctx.Registry.Register(output, FieldKindPerRow, output, ctx.CmdIndex)
	}
	return nil
}

func executeCaseFold(cmd CommandNode, ctx *CommandContext, name, chFunc string) error {
	arg, output, ok := caseFoldOutput(cmd)
	if !ok {
		return nil
	}
	safeOutput, err := sanitizeIdentifier(output)
	if err != nil {
		return fmt.Errorf("%s(): invalid output field: %w", name, err)
	}
	ref := "toString(timestamp)"
	if arg.FieldName() != "timestamp" {
		if ref, err = ResolveArg(arg, ctx.Registry); err != nil {
			return fmt.Errorf("%s(): %w", name, err)
		}
	}
	expr := fmt.Sprintf("%s(%s) AS %s", chFunc, ref, safeOutput)
	ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: expr})
	ctx.Registry.SetResolveExpr(output, expr)
	return nil
}

// evalHandler handles eval(field = expression)
type evalHandler struct{}

func (h *evalHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return nil
	}
	for _, a := range b.Positional() {
		name := a.Name
		if name == "" {
			if before, _, found := strings.Cut(a.Value(), "="); found {
				name = strings.TrimSpace(before)
			}
		}
		if name != "" {
			ctx.Registry.Register(name, FieldKindPerRow, name, ctx.CmdIndex)
		}
	}
	return nil
}

func (h *evalHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	source := ctx.Plan.CurrentStage()
	for _, a := range b.Positional() {
		name := evalTarget(a)
		if name == "" {
			continue
		}
		// Assigning a column this stage already computes replaces that SELECT, so
		// a self-reference would end up pointing at itself. Inline what the column
		// currently is instead: eval(t = b*2) | eval(t = t*3) is (b*2)*3.
		if prior := stageSelectExpr(source, name); prior != "" {
			ctx.Registry.RegisterInlineExpr(name, prior, ctx.CmdIndex)
		}
		sqlExpr, err := evalAssignmentSQL(a, ctx.Registry)
		if err != nil {
			return fmt.Errorf("eval(): %w", err)
		}
		safeFieldName, err := sanitizeIdentifier(name)
		if err != nil {
			return fmt.Errorf("eval(): invalid field name: %w", err)
		}
		expr := fmt.Sprintf("%s AS %s", sqlExpr, safeFieldName)
		source.Layer.UpsertSelect(SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr(name, expr)
	}
	return nil
}

// evalTarget is the column one eval() assignment writes. eval(name = expr)
// parses as a named expression; eval("name = expr") is one quoted assignment,
// so its text is split here.
func evalTarget(a Argument) string {
	if a.Name != "" {
		return a.Name
	}
	name, _, found := strings.Cut(a.Value(), "=")
	if !found {
		return ""
	}
	return strings.TrimSpace(name)
}

// evalAssignmentSQL compiles the right-hand side of one eval() assignment. No
// selfField: unlike `x := x * 100`, where x means the log field, eval() assigns
// into the same SELECT.
func evalAssignmentSQL(a Argument, registry *FieldRegistry) (string, error) {
	if a.Name != "" {
		return ResolveArg(a, registry)
	}
	_, expression, found := strings.Cut(a.Value(), "=")
	if !found {
		return "", nil
	}
	return compileExpressionText(strings.TrimSpace(expression), registry, "")
}

// stageSelectExpr is the expression a stage already projects under alias, or ""
// when it projects none.
func stageSelectExpr(stage *QueryStage, alias string) string {
	for _, sel := range stage.Layer.Selects {
		s := sel.String()
		if strings.Trim(extractFieldAlias(s), "`") != alias {
			continue
		}
		if idx := strings.LastIndex(s, " AS "); idx >= 0 {
			return s[:idx]
		}
	}
	return ""
}

// regexDefaultColumn holds the match when the pattern names no capture group
// and no as= was given.
const regexDefaultColumn = "_regex"

// regexHandler handles regex(pattern, field=norm_log)
type regexHandler struct{}

func (h *regexHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	p, err := regexParams(cmd)
	if err != nil || p.pattern == "" {
		return nil
	}
	names := NamedCaptureGroups(p.pattern)
	switch {
	case len(names) > 0:
		// Named capture groups take precedence over as=.
		for _, name := range names {
			ctx.Registry.Register(name, FieldKindPerRow, name, ctx.CmdIndex)
		}
	case p.asName != "":
		ctx.Registry.Register(p.asName, FieldKindPerRow, p.asName, ctx.CmdIndex)
	default:
		ctx.Registry.Register(regexDefaultColumn, FieldKindPerRow, regexDefaultColumn, ctx.CmdIndex)
	}
	return nil
}

func (h *regexHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	p, err := regexParams(cmd)
	if err != nil {
		return err
	}
	if p.pattern == "" {
		if len(cmd.Args) == 0 {
			return nil
		}
		return fmt.Errorf("regex() requires a pattern")
	}

	fieldRef, err := contentFieldRef(p.field, ctx.Registry)
	if err != nil {
		return fmt.Errorf("regex(): %w", err)
	}

	sqlPattern, names := rewriteCaptureGroups(p.pattern)

	switch {
	case len(names) > 0:
		for i, name := range names {
			safeName, err := sanitizeIdentifier(name)
			if err != nil {
				return fmt.Errorf("regex(): invalid capture name %q: %w", name, err)
			}
			scalarExpr := fmt.Sprintf("extractAllGroups(%s, '%s')[1][%d]", fieldRef, escapeString(sqlPattern), i+1)
			ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: fmt.Sprintf("%s AS %s", scalarExpr, safeName)})
			ctx.Registry.SetResolveExpr(safeName, scalarExpr)
		}
	case p.asName != "":
		// Single unnamed capture group aliased via as=. extract() returns the first
		// capturing group as a scalar string, matching the model materialized-view
		// semantics in pkg/models/ddl.go.
		safeName, err := sanitizeIdentifier(p.asName)
		if err != nil {
			return fmt.Errorf("regex(): invalid as name %q: %w", p.asName, err)
		}
		scalarExpr := fmt.Sprintf("extract(%s, '%s')", fieldRef, escapeString(sqlPattern))
		ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: fmt.Sprintf("%s AS %s", scalarExpr, safeName)})
		ctx.Registry.SetResolveExpr(safeName, scalarExpr)
	default:
		// extractAllGroups requires at least one capturing group; without a name
		// or as= there is also nothing to call the output but _regex.
		if captureGroupCount(sqlPattern) == 0 {
			return fmt.Errorf("regex(): pattern has no capture group; wrap the part to extract in (?<name>...)")
		}
		scalarExpr := fmt.Sprintf("extractAllGroups(%s, '%s')", fieldRef, escapeString(sqlPattern))
		ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: scalarExpr + " AS " + regexDefaultColumn})
		ctx.Registry.SetResolveExpr(regexDefaultColumn, scalarExpr)
	}
	return nil
}

type regexArgs struct {
	pattern, asName string
	field           Argument
}

// regexParams reads regex()'s arguments once, so Declare and Execute agree on
// which columns the command produces.
func regexParams(cmd CommandNode) (regexArgs, error) {
	p := regexArgs{field: Argument{Kind: ArgLiteral, Text: normLogColumn}}
	b, err := BindCommand(cmd)
	if err != nil {
		return p, err
	}
	p.pattern = b.StrOf("pattern", "regex")
	if a, ok := b.First("field"); ok {
		p.field = a
	}
	p.asName = b.Str("as", "")
	return p, nil
}

// replaceHandler handles replace(regex, with, field, output_field)
type replaceHandler struct{}

func (h *replaceHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return nil
	}
	if output := replaceOutput(b); output != "" && output != normLogColumn {
		ctx.Registry.Register(output, FieldKindPerRow, output, ctx.CmdIndex)
	}
	return nil
}

// replaceField is the column replace() reads, defaulting to the event text.
func replaceField(b *Bound) Argument {
	if a, ok := b.First("field"); ok {
		return a
	}
	return Argument{Kind: ArgLiteral, Text: normLogColumn}
}

// replaceOutput returns the column replace() writes: the one it was named, or
// the field it reads, rebound in place. A computed source has no name to rebind,
// so it gets a derived one.
func replaceOutput(b *Bound) string {
	if output := b.Str("as", ""); output != "" {
		return output
	}
	field := replaceField(b)
	if name := field.FieldName(); name != "" {
		return name
	}
	alias, err := ArgAlias(field)
	if err != nil {
		return normLogColumn
	}
	return strings.Trim(alias, "`")
}

// contentFieldRef resolves a field position whose default is the event text.
// timestamp is rendered as text because these commands match or rewrite strings.
func contentFieldRef(a Argument, registry *FieldRegistry) (string, error) {
	switch a.FieldName() {
	case normLogColumn:
		return normLogColumn, nil
	case "timestamp":
		return "toString(timestamp)", nil
	}
	return ResolveArg(a, registry)
}

func (h *replaceHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	pattern, hasPattern := b.First("pattern")
	replacement, hasReplacement := b.First("replacement")
	if !hasPattern || !hasReplacement {
		return nil
	}
	outputField := replaceOutput(b)

	fieldRef, err := contentFieldRef(replaceField(b), ctx.Registry)
	if err != nil {
		return fmt.Errorf("replace(): %w", err)
	}

	safeOutput, err := sanitizeIdentifier(outputField)
	if err != nil {
		return fmt.Errorf("replace(): invalid output field: %w", err)
	}

	expr := fmt.Sprintf("replaceRegexpAll(%s, '%s', '%s') AS %s",
		fieldRef, escapeString(pattern.Value()), escapeString(replacement.Value()), safeOutput)
	ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: expr})
	ctx.Registry.SetResolveExpr(outputField, expr)
	return nil
}

// concatHandler handles concat([field1,field2,...], as=alias)
type concatHandler struct{}

func (h *concatHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return declareFieldListAlias(cmd, ctx, "_concat")
}

func (h *concatHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	alias, fields, err := fieldListArgs(cmd, ctx, "concat", "_concat")
	if err != nil {
		return err
	}
	safeOutput, err := sanitizeIdentifier(alias)
	if err != nil {
		return fmt.Errorf("concat(): invalid output field: %w", err)
	}
	expr := fmt.Sprintf("concat(%s) AS %s", strings.Join(fields, ", "), safeOutput)
	ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: expr})
	ctx.Registry.SetResolveExpr(alias, expr)
	return nil
}

// hashHandler handles hash(field1, field2, as=alias)
type hashHandler struct{}

func (h *hashHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return declareFieldListAlias(cmd, ctx, "_hash")
}

func (h *hashHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	alias, fields, err := fieldListArgs(cmd, ctx, "hash", "_hash")
	if err != nil {
		return err
	}
	safeAlias, err := sanitizeIdentifier(alias)
	if err != nil {
		return fmt.Errorf("hash(): invalid alias: %w", err)
	}
	sqlExpr := fmt.Sprintf("hex(cityHash64(%s))", strings.Join(fields, ", "))
	ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: fmt.Sprintf("%s AS %s", sqlExpr, safeAlias)})
	ctx.Registry.SetResolveExpr(safeAlias, sqlExpr)
	return nil
}

// declareFieldListAlias registers the single column a field-list command emits.
func declareFieldListAlias(cmd CommandNode, ctx *CommandContext, defaultAlias string) error {
	alias := defaultAlias
	if b, err := BindCommand(cmd); err == nil {
		alias = b.Str("as", defaultAlias)
	}
	ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
	return nil
}

// csvArguments splits a quoted comma-separated value into one argument per
// field. concat("a,b") has always meant two fields, not a field named "a,b".
func csvArguments(args []Argument) []Argument {
	var out []Argument
	for _, a := range args {
		if !a.Quoted || !strings.Contains(a.Text, ",") {
			out = append(out, a)
			continue
		}
		for _, part := range strings.Split(a.Text, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, Argument{Kind: ArgLiteral, Text: part, Quoted: true, Pos: a.Pos})
			}
		}
	}
	return out
}

// fieldListArgs reads the alias and resolved field references of a command that
// combines several fields into one column (concat, hash).
func fieldListArgs(cmd CommandNode, ctx *CommandContext, name, defaultAlias string) (string, []string, error) {
	b, err := BindCommand(cmd)
	if err != nil {
		return "", nil, err
	}
	args := csvArguments(b.Flat("fields"))
	if len(args) == 0 {
		return "", nil, fmt.Errorf("%s() requires at least one field", name)
	}
	for _, a := range args {
		if a.Value() == "" {
			return "", nil, fmt.Errorf("%s() requires at least one field", name)
		}
	}
	fields := make([]string, 0, len(args))
	for _, a := range args {
		if a.FieldName() == "timestamp" {
			fields = append(fields, "toString(timestamp)")
			continue
		}
		ref, err := ResolveArg(a, ctx.Registry)
		if err != nil {
			return "", nil, fmt.Errorf("%s(): %w", name, err)
		}
		fields = append(fields, ref)
	}
	return b.Str("as", defaultAlias), fields, nil
}

// nowHandler handles now(output_field)
type nowHandler struct{}

func (h *nowHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	outputField := "_now"
	if b, err := BindCommand(cmd); err == nil {
		outputField = b.Str("outputField", outputField)
	}
	ctx.Registry.Register(outputField, FieldKindPerRow, outputField, ctx.CmdIndex)
	return nil
}

func (h *nowHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	outputField := b.Str("outputField", "_now")
	safeOutput, err := sanitizeIdentifier(outputField)
	if err != nil {
		return fmt.Errorf("now(): invalid output field: %w", err)
	}
	expr := fmt.Sprintf("now() AS %s", safeOutput)
	ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: expr})
	ctx.Registry.SetResolveExpr(outputField, expr)
	return nil
}

// caseOutputColumn is the column the bare-result form of case { ... } writes.
const caseOutputColumn = "_case"

// caseHandler handles case { condition | result ; ... }
type caseHandler struct{}

func (h *caseHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	if cmd.Block != "" {
		outputField := caseOutputColumn
		ctx.Registry.Register(outputField, FieldKindPerRow, outputField, ctx.CmdIndex)

		compiled, err := compileCase(cmd.Block, ctx.Registry, ctx.Opts)
		if err != nil {
			return err
		}
		for _, assignment := range compiled.assignments {
			ctx.Registry.Register(assignment.Field, FieldKindPerRow, assignment.Field, ctx.CmdIndex)
		}
		// Conditional aggregations (count()/sum()/... per branch) are aggregates of
		// the current stage; register them so downstream groupby/HAVING compose.
		for _, agg := range compiled.aggregates {
			ctx.Registry.Register(agg.alias, FieldKindAggregate, agg.alias, ctx.CmdIndex)
			ctx.Plan.IsAggregated = true
		}
	}
	return nil
}

func (h *caseHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if cmd.Block != "" {
		outputField := caseOutputColumn

		compiled, err := compileCase(cmd.Block, ctx.Registry, ctx.Opts)
		if err != nil {
			return err
		}
		source := ctx.Plan.CurrentStage()

		// Per-row field assignments: each becomes a CASE ... END column.
		for _, col := range compiled.assignments {
			expr := fmt.Sprintf("%s AS %s", col.SQL, col.Field)
			source.Layer.UpsertSelect(SelectExpr{Expr: expr})
			ctx.Registry.SetResolveExpr(col.Field, expr)
		}

		// Legacy bare-result form: a single output field.
		if len(compiled.whenClauses) > 0 {
			caseSQL := fmt.Sprintf("CASE %s", strings.Join(compiled.whenClauses, " "))
			if compiled.defaultClause != "" {
				caseSQL += fmt.Sprintf(" ELSE %s", compiled.defaultClause)
			} else {
				caseSQL += " ELSE NULL"
			}
			caseSQL += fmt.Sprintf(" END AS %s", outputField)
			source.Layer.UpsertSelect(SelectExpr{Expr: caseSQL})
			ctx.Registry.SetResolveExpr(outputField, caseSQL)
		}

		// Conditional aggregations via -If combinators (single-pass, N per branch).
		for _, agg := range compiled.aggregates {
			source.Layer.UpsertSelect(SelectExpr{Expr: fmt.Sprintf("%s AS %s", agg.expr, agg.alias)})
			ctx.Registry.Register(agg.alias, FieldKindAggregate, agg.alias, ctx.CmdIndex)
			ctx.Registry.SetResolveExpr(agg.alias, agg.alias)
			ctx.Plan.aggregationOutputs[agg.alias] = agg.expr
			ctx.Plan.IsAggregated = true
		}
	}
	return nil
}

// lenHandler handles len(field) and len(field, as=name).
// Without as=, the result is the shared field _len; with as=, it is the given
// name, so multiple len() calls in one pipeline do not collide on _len.
type lenHandler struct{}

// lenArgs returns the source field and output name for a len() command.
func lenArgs(cmd CommandNode) (field Argument, outName string, ok bool) {
	b, err := BindCommand(cmd)
	if err != nil {
		return Argument{}, "_len", false
	}
	field, ok = b.First("field")
	return field, b.Str("as", "_len"), ok
}

func (h *lenHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	// The numeric SELECT alias is registered as FieldKindAssignment so condition
	// routing can reference it by name without wrapping in toFloat64OrZero.
	_, outName, _ := lenArgs(cmd)
	if outName != "" {
		ctx.Registry.Register(outName, FieldKindAssignment, outName, ctx.CmdIndex)
	}
	return nil
}

func (h *lenHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	field, outName, ok := lenArgs(cmd)
	if ok && outName != "" {
		fieldRef, err := ResolveArg(field, ctx.Registry)
		if err != nil {
			return fmt.Errorf("len(): %w", err)
		}
		safeName, err := sanitizeIdentifier(outName)
		if err != nil {
			return fmt.Errorf("len(): invalid as name %q: %w", outName, err)
		}
		expr := fmt.Sprintf("length(%s) AS %s", fieldRef, safeName)
		ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr(safeName, fmt.Sprintf("length(%s)", fieldRef))
	}
	return nil
}

// logSizeHandler handles logSize() and logSize(field, as=name).
// With no field it measures the event via byteSize(norm_log); pass a
// field to size a specific column instead. The result is the shared field _size
// (or the as= name), so it can be summed/aggregated downstream to diagnose log
// growth, e.g. | logSize() | groupby(channel, function=sum(_size)).
type logSizeHandler struct{}

// logSizeArgs returns the source field and output name for a logSize() command.
// The field defaults to norm_log (the normalized event text).
func logSizeArgs(cmd CommandNode) (field Argument, outName string) {
	field = Argument{Kind: ArgLiteral, Text: normLogColumn}
	b, err := BindCommand(cmd)
	if err != nil {
		return field, "_size"
	}
	if a, ok := b.First("field"); ok {
		field = a
	}
	return field, b.Str("as", "_size")
}

func (h *logSizeHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	// Numeric SELECT alias, registered like _len so condition/aggregation routing
	// can reference it by name without re-wrapping in toFloat64OrZero.
	_, outName := logSizeArgs(cmd)
	if outName != "" {
		ctx.Registry.Register(outName, FieldKindAssignment, outName, ctx.CmdIndex)
	}
	return nil
}

func (h *logSizeHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	field, outName := logSizeArgs(cmd)
	if outName == "" {
		return nil
	}
	fieldRef, err := ResolveArg(field, ctx.Registry)
	if err != nil {
		return fmt.Errorf("logSize(): %w", err)
	}
	safeName, err := sanitizeIdentifier(outName)
	if err != nil {
		return fmt.Errorf("logSize(): invalid as name %q: %w", outName, err)
	}
	// byteSize() returns the estimated uncompressed in-memory byte size per row.
	expr := fmt.Sprintf("byteSize(%s) AS %s", fieldRef, safeName)
	ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: expr})
	ctx.Registry.SetResolveExpr(safeName, fmt.Sprintf("byteSize(%s)", fieldRef))
	return nil
}

// levenshteinHandler handles levenshtein(field1, field2)
type levenshteinHandler struct{}

func (h *levenshteinHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	// _distance produces a numeric SELECT alias; same reasoning as _len.
	alias, err := transformAlias(cmd, "_distance")
	if err != nil {
		return nil
	}
	ctx.Registry.Register(alias, FieldKindAssignment, alias, ctx.CmdIndex)
	return nil
}

func (h *levenshteinHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	a1, ok1 := b.First("s1")
	a2, ok2 := b.First("s2")
	if ok1 && ok2 {
		ref1, err := levenshteinOperand(a1, ctx.Registry)
		if err != nil {
			return fmt.Errorf("levenshtein(): %w", err)
		}
		ref2, err := levenshteinOperand(a2, ctx.Registry)
		if err != nil {
			return fmt.Errorf("levenshtein(): %w", err)
		}
		alias, err := transformAlias(cmd, "_distance")
		if err != nil {
			return err
		}
		sqlExpr := fmt.Sprintf("damerauLevenshteinDistance(%s, %s)", ref1, ref2)
		ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: sqlExpr + " AS " + alias})
		ctx.Registry.SetResolveExpr(alias, sqlExpr)
	}
	return nil
}

// levenshteinOperand resolves one side of levenshtein(): a quoted value is the
// string to compare against, anything else is a field or an expression over one.
func levenshteinOperand(a Argument, registry *FieldRegistry) (string, error) {
	if a.Quoted {
		return fmt.Sprintf("'%s'", escapeString(a.Text)), nil
	}
	return ResolveArg(a, registry)
}

// base64decodeHandler handles base64decode(field)
type base64decodeHandler struct{}

func (h *base64decodeHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	alias, err := transformAlias(cmd, "_decoded")
	if err != nil {
		return nil
	}
	ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
	return nil
}

func (h *base64decodeHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if ok {
		fieldRef, err := ResolveArg(arg, ctx.Registry)
		if err != nil {
			return err
		}
		alias, err := transformAlias(cmd, "_decoded")
		if err != nil {
			return err
		}
		sqlExpr := fmt.Sprintf("tryBase64Decode(%s)", fieldRef)
		ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: sqlExpr + " AS " + alias})
		ctx.Registry.SetResolveExpr(alias, sqlExpr)
	}
	return nil
}

// splitHandler handles split(field, delimiter, index)
type splitHandler struct{}

func (h *splitHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	alias, err := transformAlias(cmd, "_split")
	if err != nil {
		return nil
	}
	ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
	return nil
}

func (h *splitHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if delimiter, indexStr := b.Str("delimiter", ""), b.Str("index", ""); ok && indexStr != "" {
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return fmt.Errorf("split(): index must be numeric, got %q", indexStr)
		}
		fieldRef, err := ResolveArg(arg, ctx.Registry)
		if err != nil {
			return fmt.Errorf("split(): %w", err)
		}
		alias, err := transformAlias(cmd, "_split")
		if err != nil {
			return err
		}
		sqlExpr := fmt.Sprintf("splitByString('%s', %s)[%d]", escapeString(delimiter), fieldRef, index)
		ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: sqlExpr + " AS " + alias})
		ctx.Registry.SetResolveExpr(alias, sqlExpr)
	}
	return nil
}

// substrHandler handles substr(field, start, length)
type substrHandler struct{}

func (h *substrHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	alias, err := transformAlias(cmd, "_substr")
	if err != nil {
		return nil
	}
	ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
	return nil
}

func (h *substrHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if startStr := b.Str("start", ""); ok && startStr != "" {
		start, err := strconv.Atoi(startStr)
		if err != nil {
			return fmt.Errorf("substr(): start must be numeric, got %q", startStr)
		}
		fieldRef, err := ResolveArg(arg, ctx.Registry)
		if err != nil {
			return fmt.Errorf("substr(): %w", err)
		}
		var sqlExpr string
		if lengthStr := b.Str("length", ""); lengthStr != "" {
			length, err := strconv.Atoi(lengthStr)
			if err != nil {
				return fmt.Errorf("substr(): length must be numeric, got %q", lengthStr)
			}
			sqlExpr = fmt.Sprintf("substring(%s, %d, %d)", fieldRef, start, length)
		} else {
			sqlExpr = fmt.Sprintf("substring(%s, %d)", fieldRef, start)
		}
		alias, err := transformAlias(cmd, "_substr")
		if err != nil {
			return err
		}
		ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: sqlExpr + " AS " + alias})
		ctx.Registry.SetResolveExpr(alias, sqlExpr)
	}
	return nil
}

// urldecodeHandler handles urldecode(field)
type urldecodeHandler struct{}

func (h *urldecodeHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	alias, err := transformAlias(cmd, "_urldecoded")
	if err != nil {
		return nil
	}
	ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
	return nil
}

func (h *urldecodeHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if ok {
		fieldRef, err := ResolveArg(arg, ctx.Registry)
		if err != nil {
			return err
		}
		alias, err := transformAlias(cmd, "_urldecoded")
		if err != nil {
			return err
		}
		sqlExpr := fmt.Sprintf("decodeURLComponent(%s)", fieldRef)
		ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: sqlExpr + " AS " + alias})
		ctx.Registry.SetResolveExpr(alias, sqlExpr)
	}
	return nil
}

// coalesceHandler handles coalesce(field1, field2, ...)
type coalesceHandler struct{}

func (h *coalesceHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	alias, err := transformAlias(cmd, "_coalesced")
	if err != nil {
		return nil
	}
	ctx.Registry.Register(alias, FieldKindPerRow, alias, ctx.CmdIndex)
	return nil
}

func (h *coalesceHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	if args := b.Flat("fields"); len(args) >= 2 {
		var conditions []string
		for _, a := range args {
			ref, err := ResolveArg(a, ctx.Registry)
			if err != nil {
				return fmt.Errorf("coalesce(): %w", err)
			}
			conditions = append(conditions, fmt.Sprintf("%s != '' AND %s IS NOT NULL, %s", ref, ref, ref))
		}
		alias, err := transformAlias(cmd, "_coalesced")
		if err != nil {
			return err
		}
		sqlExpr := fmt.Sprintf("multiIf(%s, '')", strings.Join(conditions, ", "))
		ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: sqlExpr + " AS " + alias})
		ctx.Registry.SetResolveExpr(alias, sqlExpr)
	}
	return nil
}

// sprintfHandler handles sprintf(format, field1, field2, ..., as=alias)
type sprintfHandler struct{}

func (h *sprintfHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	return declareFieldListAlias(cmd, ctx, "_sprintf")
}

func (h *sprintfHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	formatStr := b.Str("format", "")
	if formatStr == "" {
		return fmt.Errorf("sprintf() requires a format string")
	}
	alias := b.Str("as", "_sprintf")
	var fieldRefs []string
	for _, a := range b.Flat("fields") {
		ref, err := ResolveArg(a, ctx.Registry)
		if err != nil {
			return fmt.Errorf("sprintf(): %w", err)
		}
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
	ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: expr})
	ctx.Registry.SetResolveExpr(safeAlias, fmt.Sprintf("printf(%s)", printfArgs))
	return nil
}

// matchFieldRef renders the value a dictionary lookup is made with. dictGet
// compares against a String key, so the reference is cast either way.
func matchFieldRef(a Argument, registry *FieldRegistry) string {
	if a.FieldName() == "timestamp" {
		return "toString(timestamp)"
	}
	ref, err := ResolveArg(a, registry)
	if err != nil {
		return ""
	}
	return "toString(" + ref + ")"
}

// matchLookupExpr returns the value expression for one include column. dictRef is the
// already-escaped dictionary name.
//
// The key column is not an attribute of a ClickHouse dictionary: it is declared
// separately, so dictGet cannot read it back and fails with code 36 ("No such
// attribute"). Asking for the key is the natural shape for a watchlist, a single
// column of names to match against, so it resolves to the looked-up value itself
// whenever the dictionary holds that key, matching what dictGet returns for a hit on
// any other column.
// dictProbe is the value a match() lookup is made with. A case-insensitive
// dictionary hashed lower(key), so the probe has to be lowered to meet it; an
// unlowered probe would simply miss every mixed-case key.
func dictProbe(fieldRef, dictName string, opts QueryOptions) string {
	if fieldRef == "" {
		return fieldRef
	}
	// A network list is an IP_TRIE, whose key is a range: it is probed with an
	// address, and the server rejects a string outright. OrDefault rather than a
	// plain cast, because the field is whatever the log carried and an
	// unparseable value has to miss rather than fail the query.
	if opts.NetworkDicts[dictName] {
		return "toIPv6OrDefault(" + fieldRef + ")"
	}
	if !opts.CaseInsensitiveDicts[dictName] {
		return fieldRef
	}
	return "lower(" + fieldRef + ")"
}

// probeRef is the value looked up (lowercased for a case-insensitive dictionary);
// displayRef is the value as the log carries it. They differ only for the key
// column, which is echoed back rather than read from the dictionary: echoing the
// probe would show a lowercased value the event never contained.
func matchLookupExpr(dictRef, col, keyColumn, probeRef, displayRef string, pattern bool) string {
	if col == keyColumn {
		// A REGEXP_TREE has no dictHas at all ("does not support method hasKeys"),
		// so membership is read from the marker attribute every pattern list
		// carries. Reading some other attribute instead could not tell a row that
		// matched but holds an empty value from a row that did not match.
		if pattern {
			return fmt.Sprintf("if(dictGetOrDefault('%s', '%s', %s, '') = '1', %s, '')",
				dictRef, PatternMatchAttr, probeRef, displayRef)
		}
		// dictHas works on a HASHED and on an IP_TRIE dictionary alike, so asking
		// for the key column reports membership either way.
		return fmt.Sprintf("if(dictHas('%s', %s), %s, '')", dictRef, probeRef, displayRef)
	}
	return fmt.Sprintf("dictGetOrDefault('%s', '%s', %s, '')", dictRef, escapeString(col), probeRef)
}

// PatternMatchAttr mirrors dictionaries.PatternMatchAttr. Named here rather than
// imported because the parser cannot depend on the dictionary package without a
// cycle; pkg/query sees both and asserts they agree.
const PatternMatchAttr = "_match"

// matchHandler handles match(dict="name", field=logfield, column=keycolumn, include=[col1,col2])
type matchHandler struct{}

func (h *matchHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return nil
	}
	fieldArg, hasField := b.First("field")
	keyColumn := b.Str("column", "")
	dictName := b.Str("dict", "")
	includeColumns := b.Strings("include")

	// Resolve the ClickHouse dictionary name so we can build the real expression.
	chLookupName := ""
	if ctx.Opts.Dictionaries != nil && dictName != "" && keyColumn != "" {
		if colMap, ok := ctx.Opts.Dictionaries[dictName]; ok {
			chLookupName = colMap[keyColumn]
		}
	}

	fieldRef := matchFieldRef(fieldArg, ctx.Registry)
	probeRef := dictProbe(fieldRef, dictName, ctx.Opts)

	for _, c := range includeColumns {
		if chLookupName != "" && hasField && fieldRef != "" {
			expr := matchLookupExpr(escapeString(dictRef(ctx.Opts.DictionaryDatabase, chLookupName)), c, keyColumn, probeRef, fieldRef, ctx.Opts.PatternDicts[dictName])
			ctx.Registry.Register(c, FieldKindPerRow, expr, ctx.CmdIndex)
		} else {
			ctx.Registry.Register(c, FieldKindPerRow, c, ctx.CmdIndex)
		}
	}
	return nil
}

func (h *matchHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	dictName := b.Str("dict", "")
	fieldArg, hasField := b.First("field")
	keyColumn := b.Str("column", "")
	includeColumns := b.Strings("include")
	strict := b.Flag("strict", false)

	if dictName == "" {
		return fmt.Errorf("match() requires dict= parameter")
	}
	if !hasField {
		return fmt.Errorf("match() requires field= parameter")
	}
	if len(includeColumns) == 0 {
		return fmt.Errorf("match() requires include= parameter with at least one column")
	}
	if keyColumn == "" {
		return fmt.Errorf("match() requires column= parameter to specify the lookup key column")
	}

	// The three ways this resolves to nothing are different problems with different
	// fixes, and one message for all of them sent authors to the key toggle for a
	// context that never had dictionaries at all.
	if ctx.Opts.Dictionaries == nil {
		return fmt.Errorf("match() is not available here: this context has no dictionaries")
	}
	colMap, known := ctx.Opts.Dictionaries[dictName]
	if !known {
		return fmt.Errorf("dictionary %q not found in this fractal or prism", dictName)
	}
	chLookupName := colMap[keyColumn]
	if chLookupName == "" {
		return fmt.Errorf("dictionary %q has no key column %q - enable that column as a key in the Context tab", dictName, keyColumn)
	}

	// Resolve through the registry so an earlier command that rewrote this field
	// (lowercase, eval, regex...) is what gets looked up, not the raw stored value.
	fieldRef := matchFieldRef(fieldArg, ctx.Registry)
	probeRef := dictProbe(fieldRef, dictName, ctx.Opts)

	chDictRef := escapeString(dictRef(ctx.Opts.DictionaryDatabase, chLookupName))
	for _, col := range includeColumns {
		safeCol, colErr := sanitizeIdentifier(col)
		if colErr != nil {
			return fmt.Errorf("match(): invalid include column: %w", colErr)
		}
		scalarExpr := matchLookupExpr(chDictRef, col, keyColumn, probeRef, fieldRef, ctx.Opts.PatternDicts[dictName])
		ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: fmt.Sprintf("%s AS %s", scalarExpr, safeCol)})
		ctx.Registry.SetResolveExpr(col, scalarExpr)
	}

	if strict {
		ctx.Plan.SourceStage().Layer.Where = append(ctx.Plan.SourceStage().Layer.Where,
			fmt.Sprintf("dictHas('%s', %s)", chDictRef, probeRef))
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
func geoipLookupExpr(db, field, fieldRef string) string {
	dictName := dictRef(db, geoipDictName(field))
	defVal := geoipDefaultValue(field)
	inner := fmt.Sprintf("dictGetOrDefault('%s', '%s', toIPv4OrDefault(%s), %s)",
		escapeString(dictName), escapeString(field), fieldRef, defVal)
	if geoipIsNumeric(field) {
		return fmt.Sprintf("toString(%s)", inner)
	}
	return inner
}

func (h *lookupIPHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return nil
	}
	ipField, hasField := b.First("field")
	includeColumns := b.Strings("include")

	var fieldRef string
	if hasField {
		// geoip wraps the value in IP functions (isIPv4String/IPv4StringToNum)
		// that reject a bare Dynamic subcolumn; ResolveArg casts to ::String.
		if ref, err := ResolveArg(ipField, ctx.Registry); err == nil {
			fieldRef = ref
		}
	}

	for _, c := range includeColumns {
		if fieldRef != "" && ctx.Opts.GeoIPEnabled {
			expr := geoipLookupExpr(ctx.Opts.DictionaryDatabase, c, fieldRef)
			ctx.Registry.Register(c, FieldKindPerRow, expr, ctx.CmdIndex)
		} else {
			ctx.Registry.Register(c, FieldKindPerRow, c, ctx.CmdIndex)
		}
	}
	return nil
}

func (h *lookupIPHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	ipField, hasField := b.First("field")
	includeColumns := b.Strings("include")

	if !hasField {
		return fmt.Errorf("lookupIP() requires field= parameter specifying the IP address field")
	}
	if len(includeColumns) == 0 {
		return fmt.Errorf("lookupIP() requires include= parameter with at least one column (country, city, asn, as_org, etc.)")
	}
	if !ctx.Opts.GeoIPEnabled {
		return fmt.Errorf("lookupIP() requires MaxMind GeoLite2 configuration (set MAXMIND_LICENSE_KEY and MAXMIND_ACCOUNT_ID)")
	}

	fieldRef, err := ResolveArg(ipField, ctx.Registry)
	if err != nil {
		return fmt.Errorf("lookupIP(): %w", err)
	}

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

		lookupExpr := geoipLookupExpr(ctx.Opts.DictionaryDatabase, col, fieldRef)
		expr := fmt.Sprintf("%s AS %s", lookupExpr, safeCol)
		ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: expr})
		ctx.Registry.SetResolveExpr(col, expr)
	}

	return nil
}

func init() {
	registerTransformCommand(&strftimeHandler{}, "strftime")
	registerTransformCommand(&lowercaseHandler{}, "lowercase")
	registerTransformCommand(&uppercaseHandler{}, "uppercase")
	registerTransformCommand(&evalHandler{}, "eval")
	registerTransformCommand(&regexHandler{}, "regex")
	registerTransformCommand(&replaceHandler{}, "replace")
	registerTransformCommand(&concatHandler{}, "concat")
	registerTransformCommand(&hashHandler{}, "hash")
	registerTransformCommand(&nowHandler{}, "now")
	registerTransformCommand(&caseHandler{}, "case")
	registerTransformCommand(&lenHandler{}, "len")
	registerTransformCommand(&logSizeHandler{}, "logsize")
	registerTransformCommand(&levenshteinHandler{}, "levenshtein")
	registerTransformCommand(&base64decodeHandler{}, "base64decode")
	registerTransformCommand(&splitHandler{}, "split")
	registerTransformCommand(&substrHandler{}, "substr")
	registerTransformCommand(&urldecodeHandler{}, "urldecode")
	registerTransformCommand(&coalesceHandler{}, "coalesce")
	registerTransformCommand(&sprintfHandler{}, "sprintf")
	registerTransformCommand(&matchHandler{}, "match")
	registerTransformCommand(&lookupIPHandler{}, "lookupIP", "lookupip", "geoip")
}

func init() {
	registerSpec(&CommandSpec{Name: "strftime", Params: []ParamSpec{
		reqLit("format"), namedField("field"), namedLit("timezone"), as(),
	}})
	// lowercase(field) and lowercase(field, output) rebind in place by default.
	registerSpec(&CommandSpec{Name: "lowercase", Params: []ParamSpec{reqField("field"), lit("output")}})
	registerSpec(&CommandSpec{Name: "uppercase", Params: []ParamSpec{reqField("field"), lit("output")}})
	// eval("name = expression") is one free-form assignment per argument.
	registerSpec(&CommandSpec{Name: "eval", FreeForm: true, Params: []ParamSpec{reqLit("expression")}})
	// Only the first positional is read as the pattern; a second was silently
	// ignored, so regex(commandline, "p") used commandline AS the pattern.
	// The pattern comes from the first positional, pattern= or regex=; the
	// handler reports the case where none is given.
	registerSpec(&CommandSpec{Name: "regex", Params: []ParamSpec{
		lit("pattern"), namedLit("regex"), namedField("field"), as(),
	}})
	// The field comes first, as it does in every other transform. as= names the
	// output column; without it the field is rewritten in place.
	registerSpec(&CommandSpec{Name: "replace", Params: []ParamSpec{
		reqField("field"), reqLit("pattern"), reqLit("replacement"), as(),
	}})
	registerSpec(&CommandSpec{Name: "concat", Params: []ParamSpec{fields("fields"), as()}})
	registerSpec(&CommandSpec{Name: "hash", Params: []ParamSpec{fields("fields"), as()}})
	registerSpec(&CommandSpec{Name: "now", Params: []ParamSpec{lit("outputField")}})
	registerSpec(&CommandSpec{Name: "len", Params: []ParamSpec{reqField("field"), as()}})
	registerSpec(&CommandSpec{Name: "logsize", Params: []ParamSpec{field("field"), as()}})
	registerSpec(&CommandSpec{Name: "levenshtein", Params: []ParamSpec{reqField("s1"), reqField("s2"), as()}})
	registerSpec(&CommandSpec{Name: "base64decode", Params: []ParamSpec{reqField("field"), as()}})
	registerSpec(&CommandSpec{Name: "split", Params: []ParamSpec{
		reqField("field"), reqLit("delimiter"), reqLit("index"), as(),
	}})
	registerSpec(&CommandSpec{Name: "substr", Params: []ParamSpec{
		reqField("field"), reqLit("start"), lit("length"), as(),
	}})
	registerSpec(&CommandSpec{Name: "urldecode", Params: []ParamSpec{reqField("field"), as()}})
	registerSpec(&CommandSpec{Name: "coalesce", Params: []ParamSpec{fields("fields"), as()}})
	registerSpec(&CommandSpec{Name: "sprintf", Params: []ParamSpec{
		reqLit("format"), fields("fields"), as(),
	}})
	registerSpec(&CommandSpec{Name: "match", Params: []ParamSpec{
		ParamSpec{Name: "dict", Kind: ParamLiteral, Required: true},
		ParamSpec{Name: "field", Kind: ParamField, Required: true},
		ParamSpec{Name: "column", Kind: ParamLiteral, Required: true},
		ParamSpec{Name: "include", Kind: ParamList, Required: true},
		namedLit("strict"),
	}})
	registerSpec(&CommandSpec{Name: "lookupip", Params: []ParamSpec{
		ParamSpec{Name: "field", Kind: ParamField, Required: true},
		ParamSpec{Name: "include", Kind: ParamList, Required: true},
	}}, "lookupip", "lookupIP", "geoip")
}
