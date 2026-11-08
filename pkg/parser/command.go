package parser

import (
	"fmt"
	"sort"
	"strings"
)

// CommandContext provides shared state for command handlers during declare and execute phases.
type CommandContext struct {
	Registry *FieldRegistry
	Plan     *QueryPlan
	Opts     QueryOptions
	Pipeline *PipelineNode
	CmdIndex int
	// Declared holds every command's declarations, for checks that look ahead.
	// Registry holds only what precedes the running command.
	Declared *FieldRegistry
}

// declared is Declared, or Registry where nothing was declared ahead of it.
func (c *CommandContext) declared() *FieldRegistry {
	if c.Declared != nil {
		return c.Declared
	}
	return c.Registry
}

// CommandHandler defines the interface for a pipeline command.
// Declare registers fields that the command will produce.
// Execute reads the registry and writes structured data into the plan.
type CommandHandler interface {
	Declare(cmd CommandNode, ctx *CommandContext) error
	Execute(cmd CommandNode, ctx *CommandContext) error
}

// predicateHandler is a command that can stand as a row condition: predicate
// compiles it, where it is written, to a boolean SQL expression. A handler that
// applies NOT itself (negatingHandler) reads cmd.Negate; any other is negated
// around its predicate, so NOT is never silently dropped.
type predicateHandler interface {
	predicate(cmd CommandNode, ctx *CommandContext) (string, error)
}

// filterHandler is a command that does nothing but filter rows. It is a filter
// condition wherever it is written, a step of its own or an operand of AND, OR and
// NOT, so it has the one compile path.
type filterHandler interface {
	predicateHandler
	filtersRows()
}

// rowFilter is embedded by filter handlers: a filter declares no fields and never
// runs as a pipeline step.
type rowFilter struct{}

func (rowFilter) filtersRows()                               {}
func (rowFilter) Declare(CommandNode, *CommandContext) error { return nil }
func (rowFilter) Execute(cmd CommandNode, _ *CommandContext) error {
	return fmt.Errorf("%s() is a filter and runs as a condition", cmd.Name)
}

// isPredicateCommand reports whether the command can stand as a row condition.
func isPredicateCommand(name string) bool {
	_, ok := getCommandHandler(name).(predicateHandler)
	return ok
}

// isFilterCommand reports whether the command only filters rows.
func isFilterCommand(name string) bool {
	_, ok := getCommandHandler(name).(filterHandler)
	return ok
}

// negatingHandler is a predicate handler that applies cmd.Negate itself, to a
// canonical negative form such as NOT IN.
type negatingHandler interface {
	appliesNegate()
}

// commandRegistry maps command names to their handlers.
var commandHandlers = map[string]CommandHandler{}

// canonicalCommandNames maps every registered name to the first name its handler
// was registered under, so a command is recognised whichever alias it is written as.
var canonicalCommandNames = map[string]string{}

// commandDisplayNames maps a canonical name to the spelling it was registered
// under, for messages about a command the parser has lower-cased.
var commandDisplayNames = map[string]string{}

// aggregatingCommandNames holds the names of commands that collapse rows via
// GROUP BY or aggregate functions. It is the single source of truth for that
// property: registered alongside the handler (see registerAggregatingCommand)
// so it cannot drift from the handler set. Consumed by
// firstAggregatingCommandIndex to tell a pre-aggregation field assignment
// (inlined per-row) from a post-aggregation one (computed after the GROUP BY).
var aggregatingCommandNames = map[string]bool{}

// registerCommand registers a handler for one or more command names. Names are
// folded to lower case here and looked up the same way, so a command written
// modelLookup() resolves whichever case the registration used. Storing them
// verbatim meant a mixed-case registration silently matched nothing.
func registerCommand(handler CommandHandler, names ...string) {
	for _, name := range names {
		commandHandlers[strings.ToLower(name)] = handler
		canonicalCommandNames[strings.ToLower(name)] = strings.ToLower(names[0])
	}
	commandDisplayNames[strings.ToLower(names[0])] = names[0]
}

// commandDisplayName is a command's registered spelling, whichever alias it was
// written as.
func commandDisplayName(name string) string {
	if d, ok := commandDisplayNames[canonicalCommandName(name)]; ok {
		return d
	}
	return name
}

// canonicalCommandName folds a command name, as written or as registered, to the
// one name its handler answers to. An unregistered name folds to itself.
func canonicalCommandName(name string) string {
	name = strings.ToLower(name)
	if c, ok := canonicalCommandNames[name]; ok {
		return c
	}
	return name
}

// CanonicalCommandName folds any spelling of a command to the one name its handler
// answers to, for callers that look a command up by name.
func CanonicalCommandName(name string) string { return canonicalCommandName(name) }

// CommandDisplayName is a command's registered spelling, for messages.
func CommandDisplayName(name string) string { return commandDisplayName(name) }

// commandAppliesNegate reports whether the command's handler applies NOT itself.
func commandAppliesNegate(name string) bool {
	_, ok := getCommandHandler(name).(negatingHandler)
	return ok
}

// rowKeepingHandler marks an aggregating command that scores rows rather than
// collapsing them, so its output is every input row plus its own columns.
type rowKeepingHandler interface {
	keepsRows()
}

// windowScoreHandler marks a row-keeping command that scores each row against all
// of them, so a filter after it must not narrow that baseline.
type windowScoreHandler interface {
	rowKeepingHandler
	scoresAgainstAllRows()
}

func commandScoresAgainstAllRows(name string) bool {
	_, ok := getCommandHandler(name).(windowScoreHandler)
	return ok
}

// outputIsClosed reports whether a filter after the named aggregating command
// can read only what it returns.
func outputIsClosed(name string) bool {
	_, keeps := getCommandHandler(name).(rowKeepingHandler)
	return IsAggregatingCommand(name) && !keeps
}

// laterOutputHandler marks an aggregating command whose output columns are built
// in layers around the finished query, which no filter after it can reach.
type laterOutputHandler interface {
	buildsOutputLater()
}

func commandBuildsOutputLater(name string) bool {
	_, ok := getCommandHandler(name).(laterOutputHandler)
	return ok
}

// logRowFilterHandler marks a filter that tests log rows themselves, so after an
// aggregation nothing it could test remains.
type logRowFilterHandler interface {
	readsLogRows()
}

func commandReadsLogRows(name string) bool {
	_, ok := getCommandHandler(name).(logRowFilterHandler)
	return ok
}

// registerAggregatingCommand registers a handler whose command aggregates
// (collapses rows). It registers the handler normally and records every name as
// aggregating.
func registerAggregatingCommand(handler CommandHandler, names ...string) {
	registerCommand(handler, names...)
	for _, name := range names {
		aggregatingCommandNames[strings.ToLower(name)] = true
	}
}

// transformCommandNames holds the names of per-row transform commands: those
// that add a computed column to the current row set (sprintf, concat, regex,
// case, ...). Recorded at registration so the Execute phase can detect when one
// runs after an aggregation and must operate on a post-aggregation projection
// stage rather than the GROUP BY stage (where a non-grouped column is invalid).
var transformCommandNames = map[string]bool{}

// registerTransformCommand registers a per-row transform handler and records
// every name as a transform.
func registerTransformCommand(handler CommandHandler, names ...string) {
	registerCommand(handler, names...)
	for _, name := range names {
		transformCommandNames[strings.ToLower(name)] = true
	}
}

// RegisteredCommandNames returns every registered command name, including aliases.
// Exported so the built-in query reference can be tested against the real registry
// rather than against a hand-maintained list that silently drifts.
func RegisteredCommandNames() []string {
	names := make([]string, 0, len(commandHandlers))
	for name := range commandHandlers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// IsAggregatingCommand reports whether a command collapses rows. Callers hold a
// name as the author wrote it, so the case is folded here rather than at each
// call site.
func IsAggregatingCommand(name string) bool { return aggregatingCommandNames[strings.ToLower(name)] }

// IsTransformCommand reports whether a command adds a per-row computed column.
func IsTransformCommand(name string) bool { return transformCommandNames[strings.ToLower(name)] }

// rewriteInPlaceCommandNames holds the transforms that write their result back to
// the field they read, rather than adding an underscore-prefixed column of their
// own. The distinction matters to any caller that keeps a query's predicates but
// not its projection: the column keeps a log field's name while no longer holding
// that field's stored value. TestTransformOutputsAreDeclared fails the build if a
// command joins or leaves this set.
var rewriteInPlaceCommandNames = map[string]bool{
	"lowercase": true,
	"uppercase": true,
	"replace":   true,
}

// RewritesFieldInPlace reports whether a command overwrites the field it reads.
func RewritesFieldInPlace(name string) bool { return rewriteInPlaceCommandNames[strings.ToLower(name)] }

// getCommandHandler returns the handler for a command name, or nil if not found.
func getCommandHandler(name string) CommandHandler {
	return commandHandlers[strings.ToLower(name)]
}
