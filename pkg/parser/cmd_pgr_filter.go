package parser

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// pgrFilterField is the log field the filters test.
const pgrFilterField = "process_guid"

const (
	// defaultPgrDepth covers a typical attack chain; maxPgrDepth bounds the walk.
	defaultPgrDepth = 5
	maxPgrDepth     = 8
	// MaxPgrDepth is maxPgrDepth for the ancestry builder, which records ancestors that deep.
	MaxPgrDepth = maxPgrDepth
	// maxPgrLookups bounds a query's provenance lookups, and pgrFilterLookups is what any
	// filter costs at least, since each one also scans the time range for candidates.
	maxPgrLookups    = 40
	pgrFilterLookups = 5
)

// pgrFilter is a row filter that looks a row's process_guid up in the provenance tables by key,
// so the answer for a process does not depend on the time range and retention adds no cost.
type pgrFilter interface {
	CommandHandler
	// lookups is what the command costs against maxPgrLookups.
	lookups(cmd CommandNode) int
}

// pgrHasAncestorHandler compiles pgrHasAncestor(), which reads proc_ancestry and walks
// proc_lineage for the processes it does not answer yet.
type pgrHasAncestorHandler struct{}

func (h *pgrHasAncestorHandler) appliesNegate() {}

func (h *pgrHasAncestorHandler) Declare(cmd CommandNode, ctx *CommandContext) error { return nil }

func (h *pgrHasAncestorHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if err := checkPgrSource("pgrHasAncestor", ctx); err != nil {
		return err
	}
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	arg, ok := b.First("value")
	if !ok {
		return fmt.Errorf("pgrHasAncestor() needs a name, path or /regex/ to look for")
	}
	walkMatch, err := pgrImageMatch("pgrHasAncestor", arg, "parent_image")
	if err != nil {
		return err
	}
	ancMatch, _ := pgrImageMatch("pgrHasAncestor", arg, "anc_image") // the same value, already checked
	depth, err := pgrDepth("pgrHasAncestor", b)
	if err != nil {
		return err
	}
	addPgrFilter(cmd, ctx, jsonFieldRef(pgrFilterField), pgrAncestorSet(ctx, walkMatch, ancMatch, depth)...)
	return nil
}

func (h *pgrHasAncestorHandler) lookups(cmd CommandNode) int { return pgrWalkLookups(cmd) }

// pgrWalkLookups is a walk's cost. Each level repeats the levels below it, so it is the
// triangle of the depth.
func pgrWalkLookups(cmd CommandNode) int {
	depth := defaultPgrDepth
	if b, err := BindCommand(cmd); err == nil {
		if d, err := pgrDepth(cmd.Name, b); err == nil {
			depth = d
		}
	}
	return max(pgrFilterLookups, depth*(depth+1)/2)
}

// pgrDepth is how many levels a filter covers.
func pgrDepth(name string, b *Bound) (int, error) {
	if !b.Has("depth") {
		return defaultPgrDepth, nil
	}
	depth := b.Int("depth", 0)
	if depth < 1 || depth > maxPgrDepth {
		return 0, fmt.Errorf("%s() depth= must be between 1 and %d", name, maxPgrDepth)
	}
	return depth, nil
}

// pgrAncestorSet selects the candidates with a matching ancestor within depth levels. A process
// whose ancestry is built, and whose chain cannot grow, answers from its rows; the rest, new or
// stopped at an ancestor that has since arrived, walk proc_lineage as before. Ancestry is built
// per fractal, so a scope over several walks, where a parent in another member still counts.
func pgrAncestorSet(ctx *CommandContext, walkMatch, ancMatch string, depth int) []string {
	in, notIn := ctx.Opts.subqueryIn(false), ctx.Opts.subqueryIn(true)
	lineage, anc := procLineageTable(ctx.Opts), procAncestryTable(ctx.Opts)
	cand := pgrCandidates(ctx)
	if pgrMultiFractal(ctx.Opts) {
		return []string{pgrWalkUp(ctx, walkMatch, depth, cand)}
	}
	status := fmt.Sprintf("SELECT proc_key, argMax(open_key, version) AS open_key FROM %s WHERE %s GROUP BY proc_key", anc,
		andJoin(pgrScope(ctx, "depth = 0", fmt.Sprintf("proc_key %s (%s)", in, cand))))
	arrived := fmt.Sprintf("SELECT proc_key FROM %s WHERE %s", lineage,
		andJoin(pgrScope(ctx, fmt.Sprintf("proc_key %s (SELECT open_key FROM (%s) WHERE open_key != '')", in, status))))
	closed := fmt.Sprintf("SELECT proc_key FROM (%s) WHERE open_key = '' OR open_key %s (%s)", status, notIn, arrived)
	hits := fmt.Sprintf("SELECT process_guid FROM %s WHERE %s", anc,
		andJoin(pgrScope(ctx, fmt.Sprintf("depth BETWEEN 1 AND %d", depth), ancMatch, fmt.Sprintf("proc_key %s (%s)", in, closed))))
	rest := fmt.Sprintf("SELECT proc_key FROM %s WHERE %s", lineage,
		andJoin(pgrScope(ctx, fmt.Sprintf("proc_key %s (%s)", in, cand), fmt.Sprintf("proc_key %s (%s)", notIn, closed))))
	return []string{hits, pgrWalkUp(ctx, walkMatch, depth, rest)}
}

// pgrMultiFractal reports whether a scope spans more than one fractal.
func pgrMultiFractal(o QueryOptions) bool {
	n := len(o.FractalIDs)
	if n == 0 && o.FractalID != "" {
		n = 1
	}
	if o.IncludeEmptyFractalID {
		n++
	}
	return n > 1
}

// pgrDescendantMaxPending is how many lineage rows the ancestry may not yet reflect before
// pgrHasDescendant() refuses, since it walks them all, as during the first build of a large fleet.
const pgrDescendantMaxPending = 1_000_000

// pgrWalkUp selects the processes keyed by start with a matching ancestor within depth levels:
// parents are collected upward, then matches carried back down, one key lookup per level. Inner
// levels carry keys, and the outermost returns GUIDs for the log rows.
func pgrWalkUp(ctx *CommandContext, match string, depth int, start string) string {
	in := ctx.Opts.subqueryIn(false)
	table := procLineageTable(ctx.Opts)
	level := make([]string, depth)
	level[0] = start
	for i := 1; i < depth; i++ {
		level[i] = fmt.Sprintf("SELECT parent_key FROM %s WHERE %s", table,
			andJoin(pgrScope(ctx, fmt.Sprintf("proc_key %s (%s)", in, level[i-1]))))
	}
	set := ""
	for i := depth - 1; i >= 0; i-- {
		hit := match
		if set != "" {
			hit = fmt.Sprintf("(%s OR parent_key %s (%s))", match, in, set)
		}
		col := "proc_key"
		if i == 0 {
			col = "process_guid"
		}
		set = fmt.Sprintf("SELECT %s FROM %s WHERE %s", col, table,
			andJoin(pgrScope(ctx, hit, fmt.Sprintf("proc_key %s (%s)", in, level[i]))))
	}
	return set
}

// pgrImageMatch tests an image column against the value: a file name, a whole path or a regex.
func pgrImageMatch(name string, arg Argument, col string) (string, error) {
	if arg.Kind == ArgRegex {
		return fmt.Sprintf("match(%s, %s)", col, escapeRegexForClickHouse("(?i)"+arg.Text)), nil
	}
	if arg.Kind != ArgLiteral {
		return "", fmt.Errorf("%s() takes one name, path or /regex/, not %s", name, arg.String())
	}
	text := arg.Text
	if text == "" || text == "*" {
		return "", fmt.Errorf("%s() was given no value to match", name)
	}
	v := escapeString(text)
	if strings.ContainsAny(text, `\/`) {
		return fmt.Sprintf("lower(%s) = lower('%s')", col, v), nil
	}
	if strings.Contains(text, ":") {
		return "", fmt.Errorf(`%s(): %s looks like a path that lost its backslashes: double them, as in "C:\\Windows\\cmd.exe"`, name, text)
	}
	return fmt.Sprintf(`(lower(%[2]s) = lower('%[1]s') OR endsWith(lower(%[2]s), lower('\\%[1]s')) OR endsWith(lower(%[2]s), lower('/%[1]s')))`, v, col), nil
}

// pgrHasDescendantHandler compiles pgrHasDescendant(), which reads proc_ancestry by ancestor.
type pgrHasDescendantHandler struct{}

func (h *pgrHasDescendantHandler) appliesNegate()   {}
func (h *pgrHasDescendantHandler) readsProvenance() {}

func (h *pgrHasDescendantHandler) Declare(cmd CommandNode, ctx *CommandContext) error { return nil }

func (h *pgrHasDescendantHandler) lookups(cmd CommandNode) int { return pgrWalkLookups(cmd) }

func (h *pgrHasDescendantHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if err := checkPgrSource("pgrHasDescendant", ctx); err != nil {
		return err
	}
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	arg, ok := b.First("value")
	if !ok {
		return fmt.Errorf("pgrHasDescendant() needs a name, path or /regex/ to look for")
	}
	match, err := pgrImageMatch("pgrHasDescendant", arg, "image")
	if err != nil {
		return err
	}
	depth, err := pgrDepth("pgrHasDescendant", b)
	if err != nil {
		return err
	}
	addPgrFilter(cmd, ctx, procKeyExpr(jsonFieldRef(pgrFilterField)), pgrDescendantSet(ctx, match, depth)...)
	return nil
}

// pgrDescendantSet selects the keys of candidates with a matching descendant within depth levels.
// The ancestry names every descendant it has built; a process it may not have reflected yet, one
// added since the builder's watermark or below one, walks up proc_lineage to its ancestors.
func pgrDescendantSet(ctx *CommandContext, match string, depth int) []string {
	in := ctx.Opts.subqueryIn(false)
	lineage, anc := procLineageTable(ctx.Opts), procAncestryTable(ctx.Opts)
	built := fmt.Sprintf("SELECT anc_key FROM %s WHERE %s", anc,
		andJoin(pgrScope(ctx, fmt.Sprintf("depth BETWEEN 1 AND %d", depth), match, "anc_key != ''", fmt.Sprintf("anc_key %s (%s)", in, pgrCandidates(ctx)))))
	since := fmt.Sprintf("ingest_timestamp >= (SELECT argMax(at, set_at) FROM %s)", procAncestryMarkTable(ctx.Opts))
	ready := fmt.Sprintf("(SELECT throwIf(count() > %d, 'pgrHasDescendant() is not available yet: process ancestry is still being recorded. Try again later.') "+
		"FROM %s WHERE %s) = 0", pgrDescendantMaxPending, lineage, andJoin(pgrScope(ctx, since)))
	recent := fmt.Sprintf("SELECT proc_key FROM %s WHERE %s", lineage, andJoin(pgrScope(ctx, ready, since)))
	below := fmt.Sprintf("SELECT proc_key FROM %s WHERE %s", anc, andJoin(pgrScope(ctx, fmt.Sprintf("anc_key %s (%s)", in, recent))))
	level := fmt.Sprintf("SELECT proc_key FROM %s WHERE %s", lineage,
		andJoin(pgrScope(ctx, match, fmt.Sprintf("(proc_key %s (%s) OR proc_key %s (%s))", in, recent, in, below))))
	parts := []string{built}
	for i := 0; i < depth; i++ {
		level = fmt.Sprintf("SELECT parent_key FROM %s WHERE %s", lineage, andJoin(pgrScope(ctx, "parent_key != ''", fmt.Sprintf("proc_key %s (%s)", in, level))))
		parts = append(parts, level)
	}
	return parts
}

// pgrHasNetworkHandler compiles pgrHasNetwork(), which reads process_edges.
type pgrHasNetworkHandler struct{}

func (h *pgrHasNetworkHandler) appliesNegate() {}

func (h *pgrHasNetworkHandler) Declare(cmd CommandNode, ctx *CommandContext) error { return nil }

func (h *pgrHasNetworkHandler) lookups(CommandNode) int { return pgrFilterLookups }

func (h *pgrHasNetworkHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	if err := checkPgrSource("pgrHasNetwork", ctx); err != nil {
		return err
	}
	b, err := BindCommand(cmd)
	if err != nil {
		return err
	}
	conds := pgrScope(ctx, "event_type = 'net_connect'")
	if arg, ok := b.First("value"); ok {
		cond, err := pgrNetworkMatch(arg)
		if err != nil {
			return err
		}
		conds = append(conds, cond)
	}
	if b.Has("external") {
		switch strings.ToLower(b.Str("external", "")) {
		case "true":
			conds = append(conds, pgrExternalTarget)
		case "false":
		default:
			return fmt.Errorf("pgrHasNetwork() external= must be true or false")
		}
	}
	conds = append(conds, fmt.Sprintf("proc_key %s (%s)", ctx.Opts.subqueryIn(false), pgrCandidates(ctx)))
	addPgrFilter(cmd, ctx, jsonFieldRef(pgrFilterField), fmt.Sprintf("SELECT process_guid FROM %s WHERE %s", procEdgesTable(ctx.Opts), andJoin(conds)))
	return nil
}

// pgrNetworkMatch tests a stored destination against the value, compared as ingest stored it.
func pgrNetworkMatch(arg Argument) (string, error) {
	if arg.Kind == ArgRegex {
		return fmt.Sprintf("match(fkey_tgt, %s)", escapeRegexForClickHouse(arg.Text)), nil
	}
	if arg.Kind != ArgLiteral {
		return "", fmt.Errorf("pgrHasNetwork() takes one address or /regex/, not %s", arg.String())
	}
	text := arg.Text
	if text == "" || text == "*" {
		return "", fmt.Errorf("pgrHasNetwork() was given no value to match: leave the parentheses empty to match anything")
	}
	if stored := internalTargetForm(text); stored != "" {
		return "", fmt.Errorf("pgrHasNetwork(): internal addresses are stored as %s, so %s would never match", stored, text)
	}
	if strings.ContainsAny(text, "/*") && !isStoredSubnet(text) {
		return "", fmt.Errorf("pgrHasNetwork() compares one stored address, so %s would never match: use a /regex/ for a range", text)
	}
	return fmt.Sprintf("fkey_tgt = '%s'", escapeString(text)), nil
}

// The internal ranges of ipAbstractTmpl, case included; a test holds them to it.
const (
	internalIPv4Prefix = `10\.|172\.(1[6-9]|2[0-9]|3[01])\.|192\.168\.|127\.|169\.254\.`
	internalIPv6Prefix = `::1$|fe80:|fc|fd`
)

var (
	internalIPv4Re = regexp.MustCompile(`^(` + internalIPv4Prefix + `)[0-9.]*$`)
	internalIPv6Re = regexp.MustCompile(`^(` + internalIPv6Prefix + `)`)
)

// internalTargetForm returns how ingest stores an internal address, or "" for anything else.
func internalTargetForm(text string) string {
	switch {
	case internalIPv4Re.MatchString(text):
		return text[:strings.LastIndex(text, ".")] + ".0/24"
	case internalIPv6Re.MatchString(text):
		return "internal"
	}
	return ""
}

// isStoredSubnet reports the /24 form ingest gives an internal IPv4 address.
func isStoredSubnet(text string) bool {
	prefix, ok := strings.CutSuffix(text, ".0/24")
	return ok && internalIPv4Re.MatchString(prefix+".0")
}

// What pgrHasNetwork(external=true) keeps. Internal ranges are stored as a /24 or "internal", so
// they already fail; this also drops 0/8, CGNAT, multicast, reserved, broadcast and non-global IPv6.
const (
	pgrGlobalIPv6      = `^[23][0-9a-fA-F]{3}:`
	pgrPlainIPv4       = `^[0-9.]+$`
	pgrNonRoutableIPv4 = `^(0\.|100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\.|22[4-9]\.|2[3-5][0-9]\.)`
)

var pgrExternalTarget = fmt.Sprintf("(match(fkey_tgt, %s) OR (match(fkey_tgt, %s) AND NOT match(fkey_tgt, %s)))",
	escapeRegexForClickHouse(pgrGlobalIPv6), escapeRegexForClickHouse(pgrPlainIPv4), escapeRegexForClickHouse(pgrNonRoutableIPv4))

// checkPgrSource refuses rows the filters cannot test: a generated source has no logs, and a
// reassigned process_guid no longer names the process.
func checkPgrSource(name string, ctx *CommandContext) error {
	if ctx.Opts.SourceSubquery != "" {
		return fmt.Errorf("%s() cannot follow a generated source such as pgr(): its rows are not logs", name)
	}
	if ctx.Registry.IsComputed(pgrFilterField) {
		return errPgrAssignedGUID
	}
	return nil
}

var errPgrAssignedGUID = fmt.Errorf("pgrHas filters and pgrContext() read the logged process_guid, so it cannot be assigned in the same query")

// addPgrFilter tests whether the row's process, as row, is in any of sets, recording the entry
// for narrowing. Negated, it is in none of them.
func addPgrFilter(cmd CommandNode, ctx *CommandContext, row string, sets ...string) {
	op, join := ctx.Opts.subqueryIn(cmd.Negate), " OR "
	if cmd.Negate {
		join = " AND "
	}
	tests := make([]string, len(sets))
	for i, set := range sets {
		tests[i] = fmt.Sprintf("%s %s (%s)", row, op, set)
	}
	entry := tests[0]
	if len(tests) > 1 {
		entry = "(" + strings.Join(tests, join) + ")"
	}
	ctx.Plan.SourceStage().Layer.Where = append(ctx.Plan.SourceStage().Layer.Where, entry)
	ctx.Plan.pgrWhere = append(ctx.Plan.pgrWhere, entry)
}

// pgrScope prefixes conditions with the fractal scope, which leads every provenance key.
func pgrScope(ctx *CommandContext, conds ...string) []string {
	if fc := fractalScopeCond(ctx.Opts, ""); fc != "" {
		return append([]string{fc}, conds...)
	}
	return conds
}

// pgrCandidates selects the process keys of every log in the window: a superset of the rows a
// filter tests, which narrowPgrCandidates may shrink.
func pgrCandidates(ctx *CommandContext) string {
	conds := windowConditions(ctx.Opts)
	if fc := fractalScopeCond(ctx.Opts, ""); fc != "" {
		conds = append(conds, fc)
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", procKeyExpr(jsonFieldRef(pgrFilterField)), ctx.Opts.EffectiveTableName(), andJoin(conds))
}

// narrowPgrCandidates adds the plan's other scan filters to its own filters' candidates. Only
// entries this plan wrote are touched: the same text inlined from elsewhere is another query's.
func narrowPgrCandidates(ctx *CommandContext) {
	if len(ctx.Plan.pgrWhere) == 0 && ctx.Plan.lookupName != pgrContextName {
		return
	}
	src := &ctx.Plan.SourceStage().Layer
	var others []string
	for _, w := range src.Where[ctx.Plan.baseWhereLen:] {
		if readsOnlyScanColumns(w) {
			others = append(others, w)
		}
	}
	if len(others) == 0 {
		return
	}
	candidates := pgrCandidates(ctx)
	narrowed := candidates + " AND " + andJoin(others)
	for i, w := range src.Where {
		if slices.Contains(ctx.Plan.pgrWhere, w) {
			src.Where[i] = strings.ReplaceAll(w, candidates, narrowed)
		}
	}
	if ks := ctx.Plan.modelKeys; ks != nil && ctx.Plan.lookupName == pgrContextName {
		ks.scoringSQL = strings.ReplaceAll(ks.scoringSQL, candidates, narrowed)
	}
}

// provenanceReader marks a command that reads the provenance tables, which only a live
// deployment fills.
type provenanceReader interface{ readsProvenance() }

func (h *pgrHasAncestorHandler) readsProvenance() {}
func (h *pgrHasNetworkHandler) readsProvenance()  {}
func (h *pgrContextHandler) readsProvenance()     {}

// forEachCommandIn visits every command a pipeline runs, saying whether it sits in a let binding.
func forEachCommandIn(pipeline *PipelineNode, inBinding bool, fn func(cmd CommandNode, inBinding bool)) {
	if pipeline == nil {
		return
	}
	visit := func(cmd CommandNode) { fn(cmd, inBinding) }
	for _, cmd := range pipeline.Commands {
		visit(cmd)
		for _, sub := range nestedPipelines(cmd) {
			forEachCommandIn(sub, inBinding, fn)
		}
	}
	forEachHavingCommand(pipeline.HavingConditions, visit)
	for _, b := range pipeline.Bindings {
		forEachCommandIn(b.Pipe, true, fn)
	}
}

// forEachPgrFilter visits every provenance filter a pipeline runs, saying whether it sits in a
// let binding.
func forEachPgrFilter(pipeline *PipelineNode, inBinding bool, fn func(cmd CommandNode, f pgrFilter, inBinding bool)) {
	forEachCommandIn(pipeline, inBinding, func(cmd CommandNode, inBinding bool) {
		if f, ok := getCommandHandler(cmd.Name).(pgrFilter); ok {
			fn(cmd, f, inBinding)
		}
	})
}

// FirstPgrFilter returns the display name of the first command a pipeline uses that reads the
// provenance tables, or "".
func FirstPgrFilter(pipeline *PipelineNode) string {
	first := ""
	forEachCommandIn(pipeline, false, func(cmd CommandNode, _ bool) {
		if _, ok := getCommandHandler(cmd.Name).(provenanceReader); ok && first == "" {
			first = CommandDisplayName(cmd.Name)
		}
	})
	return first
}

// ValidatePgrFilters refuses what the filters cannot do. They test log rows at the source scan,
// so past an aggregation they would change the input instead of the output. A binding is
// refused because each read repeats the walk, and ClickHouse could not cancel stacked reads.
func ValidatePgrFilters(pipeline *PipelineNode) error {
	name, total, bound := "", 0, false
	forEachPgrFilter(pipeline, false, func(cmd CommandNode, f pgrFilter, inBinding bool) {
		name = CommandDisplayName(cmd.Name)
		total += f.lookups(cmd)
		bound = bound || inBinding
	})
	if name == "" {
		return nil
	}
	if bound {
		return fmt.Errorf("%s() cannot be used in a let binding: write it in the query", name)
	}
	if total > maxPgrLookups {
		return fmt.Errorf("this query needs %d pgrHas lookups, and the limit is %d: use fewer filters or a smaller depth=", total, maxPgrLookups)
	}
	for _, a := range pipeline.Assignments {
		if a.Field == pgrFilterField {
			return errPgrAssignedGUID
		}
	}
	blocked := firstAggregatingCommandIndex(pipeline.Commands)
	late := false
	note := func(cmd CommandNode, _ pgrFilter, _ bool) { late = true }
	for i, cmd := range pipeline.Commands {
		if i > blocked {
			forEachPgrFilter(&PipelineNode{Commands: []CommandNode{cmd}}, false, note)
		}
	}
	for _, c := range pipeline.HavingConditions {
		if c.CmdIndex > blocked {
			forEachPgrFilter(&PipelineNode{HavingConditions: []HavingCondition{c}}, false, note)
		}
	}
	if late {
		return fmt.Errorf("pgrHas filters test log rows, so put them before %s()", CommandDisplayName(pipeline.Commands[blocked].Name))
	}
	return nil
}

func init() {
	registerCommand(&pgrHasAncestorHandler{}, "pgrHasAncestor")
	registerSpec(&CommandSpec{Name: "pgrHasAncestor", Params: []ParamSpec{lit("value"), namedLit("depth")}})
	registerCommand(&pgrHasDescendantHandler{}, "pgrHasDescendant")
	registerSpec(&CommandSpec{Name: "pgrHasDescendant", Params: []ParamSpec{lit("value"), namedLit("depth")}})
	registerCommand(&pgrHasNetworkHandler{}, "pgrHasNetwork")
	registerSpec(&CommandSpec{Name: "pgrHasNetwork", Params: []ParamSpec{lit("value"), namedLit("external")}})
}
