package parser

import (
	"fmt"
	"regexp"
	"slices"
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
	assignments   []caseColumn // per-row output fields, each a CASE ... END
	whenClauses   []string     // legacy bare-result WHEN arms
	defaultClause string       // legacy bare-result ELSE value (already quoted), or ""
	aggregates    []caseAgg    // conditional aggregations (-If combinators)
}

// caseColumn is a column a case block produces: the output field and the
// CASE ... END that computes it.
type caseColumn struct {
	Field string
	SQL   string
}

// caseDisallowedCommands are commands whose effect is whole-result/structural and
// therefore cannot differ per case branch in a single SQL pass. Keyed by canonical
// command name, so every alias of a command is covered.
var caseDisallowedCommands = map[string]bool{
	"groupby": true, "sort": true, "limit": true, "head": true, "tail": true, "dedup": true,
	"join": true, "chain": true, "analyzefields": true, "table": true, "piechart": true,
	"barchart": true, "heatmap": true, "singleval": true, "histogram": true,
	"modifiedzscore": true, "madoutlier": true, "modellookup": true, "timechart": true,
	"mitre": true, "boxplot": true, "scatter": true,
}

// caseAggSel is a harvested aggregate from a branch segment.
type caseAggSel struct {
	field string
	agg   aggregate
}

// caseSel is a harvested (alias, expression) pair from a branch segment.
type caseSel struct {
	field string
	expr  string
}

// segEffect is the harvested effect of one branch segment after running it through
// the real handler pipeline.
type segEffect struct {
	where  []predicate  // predicate conjuncts (filters, in/cidr/comment)
	perRow []caseSel    // per-row transform / assignment outputs
	aggs   []caseAggSel // aggregate outputs, scoped to the branch later
}

// harvestSegment runs a single branch segment (a condition such as image=~ps or
// in(prog,[sshd]), or a command such as regex(...) or count()) through the
// canonical engine against a throwaway sub-context, then partitions its effect
// into predicate / per-row / aggregate contributions. Structural effects produce
// an error.
func harvestSegment(pl *PipelineNode, opts QueryOptions, parentReg *FieldRegistry) (segEffect, error) {
	var eff segEffect
	if pl == nil || len(pl.Steps) == 0 {
		return eff, nil
	}

	// Reject structural commands by name up front for a clear message.
	for _, cmd := range pl.Commands() {
		if caseDisallowedCommands[canonicalCommandName(cmd.Name)] {
			return eff, fmt.Errorf("%s() cannot be used inside a case branch; place it after the case", commandDisplayName(cmd.Name))
		}
	}

	plan := NewQueryPlan()
	reg := NewFieldRegistry(opts.SourceMode, opts.IcePromoted)
	if parentReg != nil {
		reg.rec = parentReg.rec // what the branch reads, the case command reads
	}
	ctx := &CommandContext{Registry: reg, Plan: plan, Opts: opts, Pipeline: pl}

	// Conditions before the segment's first command resolve against the parent
	// pipeline too, ahead of what the segment's commands add; later ones see those
	// commands' outputs.
	first, later := SplitFirstStage(pl)
	condReg := reg
	if parentReg != nil {
		condReg = parentReg
	}
	if w, err := conditionsPredicate(first, condReg.rowLevelClone(), opts); err != nil {
		return eff, err
	} else if !w.empty() {
		plan.SourceStage().Layer.Where = append(plan.SourceStage().Layer.Where, w)
	}

	for i, cmd := range pl.Commands() {
		ctx.CmdIndex = i
		h := getCommandHandler(cmd.Name)
		if h == nil {
			return eff, fmt.Errorf("unsupported command in case branch: %s()", cmd.Name)
		}
		if err := h.Declare(cmd, ctx); err != nil {
			return eff, err
		}
	}
	for _, a := range pl.Assignments() {
		reg.Register(a.Field, FieldKindAssignment, a.Field, -1)
	}
	for i, cmd := range pl.Commands() {
		ctx.CmdIndex = i
		if _, err := runTracked(ctx, cmd, reg.Clone()); err != nil {
			return eff, err
		}
	}

	if w, err := conditionsPredicate(later, reg.rowLevelClone(), opts); err != nil {
		return eff, err
	} else if !w.empty() {
		plan.SourceStage().Layer.Where = append(plan.SourceStage().Layer.Where, w)
	}

	src := &plan.SourceStage().Layer
	// Backstop: reject any structural effect a handler may have produced (groupby,
	// sort, limit, join, chain, window layers, extra stages).
	if len(src.GroupBy) > 0 || len(src.OrderBy) > 0 || src.Limit != "" || src.LimitBy != "" ||
		plan.join != nil || plan.Chain != nil || len(plan.window.layers) > 0 ||
		plan.hasModelLookup() || len(plan.Stages) > 1 {
		return eff, fmt.Errorf("structural command (groupby/sort/limit/join/chain/window) cannot be used inside a case branch")
	}

	eff.where = append(eff.where, src.Where...)
	for _, sel := range src.Selects {
		expr, alias := sel.Expr, sel.Name()
		if sel.Alias == "" {
			continue
		}
		if reg.IsAggregate(alias) {
			if sel.agg == nil {
				return eff, fmt.Errorf("%s cannot be computed per case branch; compute it after the case", alias)
			}
			eff.aggs = append(eff.aggs, caseAggSel{field: alias, agg: sel.agg})
		} else {
			eff.perRow = append(eff.perRow, caseSel{field: alias, expr: expr})
		}
	}
	return eff, nil
}

// caseValueSQL renders an assignment/result value as a quoted SQL string literal.
func caseValueSQL(raw string) string {
	v := strings.TrimSpace(raw)
	v = strings.Trim(v, `"'`)
	return sqlString(v)
}

// caseAssignName matches a field a case branch may assign. The name becomes a
// bare SQL alias and GROUP BY key, so anything beyond a plain identifier is
// rejected rather than quoted.
var caseAssignName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// looksLikeCommand reports whether text is a function call (count(), in(...),
// regex(...)) rather than a value.
func looksLikeCommand(text string) bool {
	s := strings.TrimSpace(text)
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
	preds     []predicate  // predicate conjuncts (AND-ed)
	vals      []caseSel    // per-row field contributions (literals + transform exprs)
	aggs      []caseAggSel // aggregate contributions
	bares     []string     // legacy bare-result values
}

// compileCase compiles a full case { ... } block under the single-pass conditional
// model. registry resolves field references; opts feeds the sub-handlers.
func compileCase(block []CaseBranch, registry *FieldRegistry, opts QueryOptions) (compiledCase, error) {
	branches, err := harvestBranches(block, registry, opts)
	if err != nil {
		return compiledCase{}, err
	}
	return conditionalize(branches, defaultPredicate(branches)), nil
}

// harvestBranches compiles each branch's condition and results.
func harvestBranches(block []CaseBranch, registry *FieldRegistry, opts QueryOptions) ([]branchData, error) {
	branches := make([]branchData, 0, len(block))
	for _, br := range block {
		bd := branchData{isDefault: br.When == nil}
		harvest := func(seg *PipelineNode) error {
			eff, err := harvestSegment(seg, opts, registry)
			bd.preds = append(bd.preds, eff.where...)
			bd.vals = append(bd.vals, eff.perRow...)
			bd.aggs = append(bd.aggs, eff.aggs...)
			return err
		}
		if err := harvest(br.When); err != nil {
			return nil, err
		}
		for _, r := range br.Results {
			switch {
			case r.Command != nil:
				if err := harvest(r.Command); err != nil {
					return nil, err
				}
			case r.Field != "":
				bd.vals = append(bd.vals, caseSel{field: r.Field, expr: caseValueSQL(r.Value)})
			default:
				bd.bares = append(bd.bares, caseValueSQL(r.Value))
			}
		}
		branches = append(branches, bd)
	}
	return branches, nil
}

// defaultPredicate is the default branch's condition: NOT (p1 OR p2 OR ... OR pk).
func defaultPredicate(branches []branchData) predicate {
	var preds []predicate
	for _, bd := range branches {
		if !bd.isDefault {
			preds = append(preds, andOf(bd.preds...))
		}
	}
	return notOf(orOf(preds...))
}

// conditionalize scopes each branch's contributions to its condition: a field it
// sets becomes a CASE arm, an aggregate its -If form.
func conditionalize(branches []branchData, defaultPred predicate) compiledCase {
	var out compiledCase
	arms := map[string][]string{}
	defaults := map[string]string{}
	var fields []string // in the order first set
	aggIdx := 0
	for _, bd := range branches {
		p := andOf(bd.preds...)
		if bd.isDefault {
			p = defaultPred
		}
		pred := p.sql
		for _, v := range bd.vals {
			if _, seen := arms[v.field]; !seen && !slices.Contains(fields, v.field) {
				fields = append(fields, v.field)
			}
			if bd.isDefault {
				defaults[v.field] = v.expr
			} else {
				arms[v.field] = append(arms[v.field], fmt.Sprintf("WHEN %s THEN %s", pred, v.expr))
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
			cond := pred
			if cond == "" {
				cond = "1"
			}
			out.aggregates = append(out.aggregates, caseAgg{alias: fmt.Sprintf("%s_%d", base, aggIdx), expr: a.agg.scoped(cond)})
		}
		for _, bare := range bd.bares {
			if bd.isDefault {
				out.defaultClause = bare
			} else {
				out.whenClauses = append(out.whenClauses, fmt.Sprintf("WHEN %s THEN %s", pred, bare))
			}
		}
	}
	// Fields with arms first, as written; a field only the default sets is that value.
	var defaultOnly []caseColumn
	for _, field := range fields {
		def, hasDefault := defaults[field]
		if len(arms[field]) == 0 {
			defaultOnly = append(defaultOnly, caseColumn{Field: field, SQL: def})
			continue
		}
		if !hasDefault {
			def = "NULL"
		}
		out.assignments = append(out.assignments, caseColumn{Field: field, SQL: "CASE " + strings.Join(arms[field], " ") + " ELSE " + def + " END"})
	}
	out.assignments = append(out.assignments, defaultOnly...)
	return out
}
