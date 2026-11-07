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

// pgrHasAncestorHandler compiles pgrHasAncestor(), which walks up through proc_lineage.
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
	match, err := pgrAncestorMatch(arg)
	if err != nil {
		return err
	}
	depth, err := pgrDepth(b)
	if err != nil {
		return err
	}
	addPgrFilter(cmd, ctx, pgrAncestorSet(ctx, match, depth))
	return nil
}

// lookups is a walk's cost. Each level repeats the levels below it, so it is the triangle of
// the depth.
func (h *pgrHasAncestorHandler) lookups(cmd CommandNode) int {
	depth := defaultPgrDepth
	if b, err := BindCommand(cmd); err == nil {
		if d, err := pgrDepth(b); err == nil {
			depth = d
		}
	}
	return max(pgrFilterLookups, depth*(depth+1)/2)
}

// pgrDepth is how many levels the walk covers.
func pgrDepth(b *Bound) (int, error) {
	if !b.Has("depth") {
		return defaultPgrDepth, nil
	}
	depth := b.Int("depth", 0)
	if depth < 1 || depth > maxPgrDepth {
		return 0, fmt.Errorf("pgrHasAncestor() depth= must be between 1 and %d", maxPgrDepth)
	}
	return depth, nil
}

// pgrAncestorSet selects the candidates with a matching ancestor within depth levels: parents
// are collected upward, then matches carried back down, one key lookup per level.
func pgrAncestorSet(ctx *CommandContext, match string, depth int) string {
	in := ctx.Opts.subqueryIn(false)
	table := procLineageTable(ctx.Opts)
	level := make([]string, depth)
	level[0] = pgrCandidates(ctx)
	for i := 1; i < depth; i++ {
		level[i] = fmt.Sprintf("SELECT parent_guid FROM %s WHERE %s", table,
			andJoin(pgrScope(ctx, fmt.Sprintf("process_guid %s (%s)", in, level[i-1]))))
	}
	set := ""
	for i := depth - 1; i >= 0; i-- {
		hit := match
		if set != "" {
			hit = fmt.Sprintf("(%s OR parent_guid %s (%s))", match, in, set)
		}
		set = fmt.Sprintf("SELECT process_guid FROM %s WHERE %s", table,
			andJoin(pgrScope(ctx, hit, fmt.Sprintf("process_guid %s (%s)", in, level[i]))))
	}
	return set
}

// pgrAncestorMatch tests a lineage row's parent_image against the value.
func pgrAncestorMatch(arg Argument) (string, error) {
	if arg.Kind == ArgRegex {
		return fmt.Sprintf("match(parent_image, %s)", escapeRegexForClickHouse("(?i)"+arg.Text)), nil
	}
	if arg.Kind != ArgLiteral {
		return "", fmt.Errorf("pgrHasAncestor() takes one name, path or /regex/, not %s", arg.String())
	}
	text := arg.Text
	if text == "" || text == "*" {
		return "", fmt.Errorf("pgrHasAncestor() was given no value to match")
	}
	v := escapeString(text)
	if strings.ContainsAny(text, `\/`) {
		return fmt.Sprintf("lower(parent_image) = lower('%s')", v), nil
	}
	if strings.Contains(text, ":") {
		return "", fmt.Errorf(`pgrHasAncestor(): %s looks like a path that lost its backslashes: double them, as in "C:\\Windows\\cmd.exe"`, text)
	}
	return fmt.Sprintf(`(lower(parent_image) = lower('%[1]s') OR endsWith(lower(parent_image), lower('\\%[1]s')) OR endsWith(lower(parent_image), lower('/%[1]s')))`, v), nil
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
	conds = append(conds, fmt.Sprintf("process_guid %s (%s)", ctx.Opts.subqueryIn(false), pgrCandidates(ctx)))
	addPgrFilter(cmd, ctx, fmt.Sprintf("SELECT process_guid FROM %s WHERE %s", procEdgesTable(ctx.Opts), andJoin(conds)))
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

var errPgrAssignedGUID = fmt.Errorf("pgrHas filters read the logged process_guid, so it cannot be assigned in the same query")

// addPgrFilter tests the row's process_guid against set, recording the entry for narrowing.
func addPgrFilter(cmd CommandNode, ctx *CommandContext, set string) {
	entry := fmt.Sprintf("%s %s (%s)", jsonFieldRef(pgrFilterField), ctx.Opts.subqueryIn(cmd.Negate), set)
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

// pgrCandidates selects the GUIDs of every log in the window: a superset of the rows a filter
// tests, which narrowPgrCandidates may shrink.
func pgrCandidates(ctx *CommandContext) string {
	conds := windowConditions(ctx.Opts)
	if fc := fractalScopeCond(ctx.Opts, ""); fc != "" {
		conds = append(conds, fc)
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", jsonFieldRef(pgrFilterField), ctx.Opts.EffectiveTableName(), andJoin(conds))
}

// narrowPgrCandidates adds the plan's other scan filters to its own filters' candidates. Only
// entries this plan wrote are touched: the same text inlined from elsewhere is another query's.
func narrowPgrCandidates(ctx *CommandContext) {
	if len(ctx.Plan.pgrWhere) == 0 {
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
}

// forEachPgrFilter visits every provenance filter a pipeline runs, saying whether it sits in a
// let binding.
func forEachPgrFilter(pipeline *PipelineNode, inBinding bool, fn func(cmd CommandNode, f pgrFilter, inBinding bool)) {
	if pipeline == nil {
		return
	}
	visit := func(cmd CommandNode) {
		if f, ok := getCommandHandler(cmd.Name).(pgrFilter); ok {
			fn(cmd, f, inBinding)
		}
	}
	for _, cmd := range pipeline.Commands {
		visit(cmd)
		for _, sub := range nestedPipelines(cmd) {
			forEachPgrFilter(sub, inBinding, fn)
		}
	}
	forEachHavingCommand(pipeline.HavingConditions, visit)
	for _, b := range pipeline.Bindings {
		forEachPgrFilter(b.Pipe, true, fn)
	}
}

// FirstPgrFilter returns the display name of the first provenance filter a pipeline uses, or "".
func FirstPgrFilter(pipeline *PipelineNode) string {
	first := ""
	forEachPgrFilter(pipeline, false, func(cmd CommandNode, _ pgrFilter, _ bool) {
		if first == "" {
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
	registerCommand(&pgrHasNetworkHandler{}, "pgrHasNetwork")
	registerSpec(&CommandSpec{Name: "pgrHasNetwork", Params: []ParamSpec{lit("value"), namedLit("external")}})
}
