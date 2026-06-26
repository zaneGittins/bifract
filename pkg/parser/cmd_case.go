package parser

import (
	"fmt"
	"strings"
	"unicode"
)

// caseAgg is a single conditional aggregation produced by a case branch, e.g.
// `sum(bytes)` in a branch with predicate P compiles to {alias, "sumIf(<x>, P)"}.
type caseAgg struct {
	alias string
	expr  string
}

// compiledCase is the full result of compiling a case { ... } block under the
// single-pass conditional model:
//   - branch conditions (filters and condition-commands like in()/cidr()) become a
//     SQL predicate P, compiled by the real handlers (every operator works);
//   - per-row transforms (regex/eval/lowercase/concat/...) become CASE WHEN P THEN
//     <expr> END columns, harvested from the real handler output, N per branch;
//   - aggregations (count/sum/avg/quantile/...) become ClickHouse -If combinators
//     scoped by P, N per branch, in a single table scan.
//
// Structural commands (groupby/sort/limit/join/chain/window/viz) cannot be
// branch-local in one SQL pass and are rejected with a clear error.
type compiledCase struct {
	assignments   []AssignmentNode // per-row output fields; Expression is a CASE ... END
	whenClauses   []string         // legacy bare-result WHEN arms
	defaultClause string           // legacy bare-result ELSE value (already quoted), or ""
	aggregates    []caseAgg        // conditional aggregations (-If combinators)
}

// caseDisallowedCommands are commands whose effect is whole-result/structural and
// therefore cannot differ per case branch in a single SQL pass.
var caseDisallowedCommands = map[string]string{
	"groupby": "groupby", "sort": "sort", "limit": "limit", "head": "head",
	"tail": "tail", "dedup": "dedup", "join": "join", "chain": "chain",
	"bfs": "bfs", "dfs": "dfs", "analyzefields": "analyzefields",
	"table": "table", "piechart": "piechart", "barchart": "barchart",
	"heatmap": "heatmap", "singleval": "singleval", "histogram": "histogram",
	"modifiedzscore": "modifiedzscore", "modifiedz": "modifiedzscore", "mzscore": "modifiedzscore",
	"madoutlier": "madoutlier", "outlier": "madoutlier", "model_lookup": "model_lookup",
	"timechart": "timechart",
}

// caseSel is a harvested (alias, expression) pair from a branch segment.
type caseSel struct {
	field string
	expr  string
}

// segEffect is the harvested effect of one branch segment after running it through
// the real handler pipeline.
type segEffect struct {
	where  []string  // predicate conjuncts (filters, in/cidr/comment)
	perRow []caseSel // per-row transform / assignment outputs
	aggs   []caseSel // aggregate outputs (raw agg SQL, pre -If injection)
}

// harvestSegment runs a single branch segment ("image=~ps", "in(prog,[sshd])",
// "regex(...)", "count()", "valid:=true") through the canonical engine against a
// throwaway sub-context, then partitions its effect into predicate / per-row /
// aggregate contributions. Structural effects produce an error.
func harvestSegment(segText string, opts QueryOptions, parentReg *FieldRegistry) (segEffect, error) {
	var eff segEffect
	segText = strings.TrimSpace(segText)
	if segText == "" {
		return eff, nil
	}

	pl, err := ParseQuery(segText)
	if err != nil {
		return eff, fmt.Errorf("case branch: %w", err)
	}

	// Reject structural commands by name up front for a clear message.
	for _, cmd := range pl.Commands {
		if disp, bad := caseDisallowedCommands[cmd.Name]; bad {
			return eff, fmt.Errorf("%s() cannot be used inside a case branch; place it after the case", disp)
		}
	}

	plan := NewQueryPlan()
	reg := NewFieldRegistry()
	ctx := &CommandContext{Registry: reg, Plan: plan, Opts: opts, Pipeline: pl}

	if pl.Filter != nil {
		// Resolve branch-condition fields against the parent pipeline registry so
		// a computed column (e.g. a prior assignment or aggregate) is referenced
		// directly instead of as a raw JSON sub-column.
		w, err := buildWhereClauseCtx(pl.Filter.Conditions, parentReg)
		if err != nil {
			return eff, err
		}
		if w != "" {
			plan.SourceStage().Layer.Where = append(plan.SourceStage().Layer.Where, w)
		}
	}

	for i, cmd := range pl.Commands {
		ctx.CmdIndex = i
		h := getCommandHandler(cmd.Name)
		if h == nil {
			return eff, fmt.Errorf("unsupported command in case branch: %s()", cmd.Name)
		}
		if err := h.Declare(cmd, ctx); err != nil {
			return eff, err
		}
	}
	for _, a := range pl.Assignments {
		reg.Register(a.Field, FieldKindAssignment, a.Field, -1)
	}
	for i, cmd := range pl.Commands {
		ctx.CmdIndex = i
		if err := getCommandHandler(cmd.Name).Execute(cmd, ctx); err != nil {
			return eff, err
		}
	}

	src := &plan.SourceStage().Layer
	// Backstop: reject any structural effect a handler may have produced (groupby,
	// sort, limit, join, chain, window layers, extra stages).
	if len(src.GroupBy) > 0 || len(src.OrderBy) > 0 || src.Limit != "" || src.LimitBy != "" ||
		plan.IsJoin || plan.IsTraversal || plan.IsChain || len(plan.WindowLayers) > 0 ||
		plan.ModelLookupSQL != "" || len(plan.Stages) > 1 {
		return eff, fmt.Errorf("structural command (groupby/sort/limit/join/chain/window) cannot be used inside a case branch")
	}

	eff.where = append(eff.where, src.Where...)
	for _, sel := range src.Selects {
		expr, alias := splitSelectAlias(sel.String())
		if alias == "" {
			continue
		}
		if reg.IsAggregate(alias) {
			eff.aggs = append(eff.aggs, caseSel{field: alias, expr: expr})
		} else {
			eff.perRow = append(eff.perRow, caseSel{field: alias, expr: expr})
		}
	}
	return eff, nil
}

// splitSelectAlias splits "expr AS alias" into (expr, alias). When there is no AS
// clause it returns (s, "").
func splitSelectAlias(s string) (string, string) {
	if idx := strings.LastIndex(s, " AS "); idx != -1 {
		return strings.TrimSpace(s[:idx]), strings.TrimSpace(s[idx+4:])
	}
	return s, ""
}

// matchParen returns the index of the ')' matching the '(' at runes[open], or -1.
func matchParen(runes []rune, open int) int {
	depth := 0
	for i := open; i < len(runes); i++ {
		switch runes[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// aggExprToIf rewrites a harvested aggregate expression into its conditional -If
// combinator scoped by cond. Handles COUNT(*), count(field) (non-null semantics
// preserved), parametric aggregates (quantiles(p)(x)), and the general f(args)
// case.
func aggExprToIf(expr, cond string) string {
	e := strings.TrimSpace(expr)
	if strings.TrimSpace(cond) == "" {
		cond = "1"
	}
	runes := []rune(e)
	open := strings.IndexByte(e, '(')
	if open <= 0 {
		return e // not a function call; leave untouched
	}
	name := strings.TrimSpace(e[:open])
	closeIdx := matchParen(runes, open)
	if closeIdx < 0 {
		return e
	}
	innerArg := strings.TrimSpace(string(runes[open+1 : closeIdx]))

	// count(): rows in branch; count(field): non-null field in branch.
	if strings.EqualFold(name, "count") {
		if innerArg == "*" || innerArg == "" {
			return fmt.Sprintf("countIf(%s)", cond)
		}
		return fmt.Sprintf("countIf((%s) IS NOT NULL AND (%s))", innerArg, cond)
	}

	// Parametric aggregate: f(params)(args) -> fIf(params)(args, cond).
	if closeIdx+1 < len(runes) && runes[closeIdx+1] == '(' {
		close2 := matchParen(runes, closeIdx+1)
		if close2 >= 0 {
			params := string(runes[open : closeIdx+1]) // "(p1, p2)"
			args := strings.TrimSpace(string(runes[closeIdx+2 : close2]))
			return fmt.Sprintf("%sIf%s(%s, %s)", name, params, args, cond)
		}
	}

	return fmt.Sprintf("%sIf(%s, %s)", name, innerArg, cond)
}

// caseValueSQL renders an assignment/result value as a quoted SQL string literal.
func caseValueSQL(raw string) string {
	v := strings.TrimSpace(raw)
	v = strings.Trim(v, `"'`)
	return "'" + escapeString(v) + "'"
}

// looksLikeCommand reports whether a segment is a function-call command
// (count(), in(...), regex(...), eval(...)) rather than an assignment or value.
func looksLikeCommand(seg string) bool {
	s := strings.TrimSpace(seg)
	open := strings.IndexByte(s, '(')
	if open <= 0 || !strings.HasSuffix(s, ")") {
		return false
	}
	for _, r := range strings.TrimSpace(s[:open]) {
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
			return false
		}
	}
	return true
}

// branchData holds one branch's compiled predicate and value/aggregate contributions.
type branchData struct {
	isDefault bool
	preds     []string  // predicate conjuncts (AND-ed)
	vals      []caseSel // per-row field contributions (literals + transform exprs)
	aggs      []caseSel // aggregate contributions (raw agg SQL)
	bares     []string  // legacy bare-result values
}

// compileCase compiles a full case { ... } block under the single-pass conditional
// model. registry resolves field references; opts feeds the sub-handlers.
func compileCase(block string, registry *FieldRegistry, opts QueryOptions) (compiledCase, error) {
	var out compiledCase

	inner := strings.TrimSpace(block)
	inner = strings.TrimPrefix(inner, "{")
	inner = strings.TrimSuffix(inner, "}")

	// ---- Pass 1: parse branches, harvest each segment -----------------------
	var branches []branchData
	for _, br := range splitTopLevelByToken(inner, TokenSemicolon) {
		if strings.TrimSpace(br) == "" {
			continue
		}
		segs := splitTopLevelByToken(br, TokenPipe)
		condText := strings.TrimSpace(segs[0])

		bd := branchData{}
		if condText == "*" {
			bd.isDefault = true
		} else {
			eff, err := harvestSegment(condText, opts, registry)
			if err != nil {
				return out, err
			}
			bd.preds = append(bd.preds, eff.where...)
			bd.vals = append(bd.vals, eff.perRow...)
			bd.aggs = append(bd.aggs, eff.aggs...)
		}

		for _, seg := range segs[1:] {
			seg = strings.TrimSpace(seg)
			if seg == "" {
				continue
			}
			switch {
			case strings.Contains(seg, ":="):
				parts := strings.SplitN(seg, ":=", 2)
				rhs := strings.TrimSpace(parts[1])
				if looksLikeCommand(rhs) {
					// `field := regex(...)` is a common mistake: transforms and
					// aggregations are pipe commands, not assignment values. Guide the
					// user to the bare-command form rather than quoting it as a literal.
					return out, fmt.Errorf("case: assign a command result with a bare pipe, not ':='; write `| %s` instead of `%s := %s`", rhs, strings.TrimSpace(parts[0]), rhs)
				}
				bd.vals = append(bd.vals, caseSel{field: strings.TrimSpace(parts[0]), expr: caseValueSQL(rhs)})
			case looksLikeCommand(seg):
				eff, err := harvestSegment(seg, opts, registry)
				if err != nil {
					return out, err
				}
				bd.preds = append(bd.preds, eff.where...)
				bd.vals = append(bd.vals, eff.perRow...)
				bd.aggs = append(bd.aggs, eff.aggs...)
			case strings.Contains(seg, "="):
				parts := strings.SplitN(seg, "=", 2)
				bd.vals = append(bd.vals, caseSel{field: strings.TrimSpace(parts[0]), expr: caseValueSQL(parts[1])})
			default:
				bd.bares = append(bd.bares, caseValueSQL(seg))
			}
		}
		branches = append(branches, bd)
	}

	// Default predicate: NOT (p1 OR p2 OR ... OR pk).
	var nonDefaultPreds []string
	for _, bd := range branches {
		if bd.isDefault {
			continue
		}
		if p := andConjuncts(bd.preds); p != "" {
			nonDefaultPreds = append(nonDefaultPreds, p)
		}
	}
	defaultPred := ""
	if len(nonDefaultPreds) > 0 {
		quoted := make([]string, len(nonDefaultPreds))
		for i, p := range nonDefaultPreds {
			quoted[i] = "(" + p + ")"
		}
		defaultPred = "NOT (" + strings.Join(quoted, " OR ") + ")"
	}

	// ---- Pass 2: conditionalize each branch by its predicate ----------------
	fieldArms := make(map[string][]string)
	fieldOrder := []string{}
	fieldDefaults := make(map[string]string)
	aggIdx := 0

	for _, bd := range branches {
		pred := andConjuncts(bd.preds)
		if bd.isDefault {
			pred = defaultPred
		}
		for _, v := range bd.vals {
			if bd.isDefault {
				fieldDefaults[v.field] = v.expr
			} else {
				if _, seen := fieldArms[v.field]; !seen {
					fieldOrder = append(fieldOrder, v.field)
				}
				fieldArms[v.field] = append(fieldArms[v.field], fmt.Sprintf("WHEN %s THEN %s", pred, v.expr))
			}
		}
		for _, a := range bd.aggs {
			aggIdx++
			// Branches reuse the same natural alias (_count, _sum, ...), so suffix
			// with a per-case counter to keep each conditional aggregate distinct.
			base := strings.TrimPrefix(a.field, "_")
			if base == "" {
				base = "agg"
			}
			alias := fmt.Sprintf("%s_%d", base, aggIdx)
			out.aggregates = append(out.aggregates, caseAgg{alias: alias, expr: aggExprToIf(a.expr, pred)})
		}
		for _, bare := range bd.bares {
			if bd.isDefault {
				out.defaultClause = bare
			} else {
				out.whenClauses = append(out.whenClauses, fmt.Sprintf("WHEN %s THEN %s", pred, bare))
			}
		}
	}

	// ---- Build per-field CASE assignments -----------------------------------
	for _, field := range fieldOrder {
		var b strings.Builder
		b.WriteString("CASE ")
		b.WriteString(strings.Join(fieldArms[field], " "))
		if def, ok := fieldDefaults[field]; ok {
			b.WriteString(" ELSE ")
			b.WriteString(def)
		} else {
			b.WriteString(" ELSE NULL")
		}
		b.WriteString(" END")
		out.assignments = append(out.assignments, AssignmentNode{Field: field, Expression: b.String()})
	}
	for field, def := range fieldDefaults {
		if _, hasArms := fieldArms[field]; !hasArms {
			out.assignments = append(out.assignments, AssignmentNode{Field: field, Expression: def})
		}
	}

	return out, nil
}

// andConjuncts joins predicate conjuncts with AND, parenthesizing each.
func andConjuncts(conjuncts []string) string {
	var parts []string
	for _, c := range conjuncts {
		if strings.TrimSpace(c) != "" {
			parts = append(parts, c)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	wrapped := make([]string, len(parts))
	for i, p := range parts {
		wrapped[i] = "(" + p + ")"
	}
	return strings.Join(wrapped, " AND ")
}

// splitTopLevelByToken splits raw at top-level occurrences of sepType (a single-rune
// separator: TokenPipe or TokenSemicolon). It lexes raw so that separators nested
// inside (), [], {} or contained within regex/quoted tokens do not split. Falls back
// to returning raw unsplit if lexing fails.
func splitTopLevelByToken(raw string, sepType TokenType) []string {
	lex := NewLexer(raw)
	toks, err := lex.Tokenize()
	if err != nil {
		return []string{raw}
	}
	runes := []rune(raw)
	var segs []string
	depth := 0
	start := 0
	for _, tk := range toks {
		switch tk.Type {
		case TokenLParen, TokenLBracket, TokenLBrace:
			depth++
		case TokenRParen, TokenRBracket, TokenRBrace:
			if depth > 0 {
				depth--
			}
		case sepType:
			if depth == 0 && tk.Pos >= start && tk.Pos <= len(runes) {
				segs = append(segs, string(runes[start:tk.Pos]))
				start = tk.Pos + 1 // separators are a single rune
			}
		}
	}
	if start <= len(runes) {
		segs = append(segs, string(runes[start:]))
	}
	return segs
}
