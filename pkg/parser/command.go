package parser

// CommandContext provides shared state for command handlers during declare and execute phases.
type CommandContext struct {
	Registry *FieldRegistry
	Plan     *QueryPlan
	Opts     QueryOptions
	Pipeline *PipelineNode
	CmdIndex int
}

// CommandHandler defines the interface for a pipeline command.
// Declare registers fields that the command will produce.
// Execute reads the registry and writes structured data into the plan.
type CommandHandler interface {
	Declare(cmd CommandNode, ctx *CommandContext) error
	Execute(cmd CommandNode, ctx *CommandContext) error
}

// commandRegistry maps command names to their handlers.
var commandHandlers = map[string]CommandHandler{}

// aggregatingCommandNames holds the names of commands that collapse rows via
// GROUP BY or aggregate functions. It is the single source of truth for that
// property: registered alongside the handler (see registerAggregatingCommand)
// so it cannot drift from the handler set. Consumed by
// firstAggregatingCommandIndex to tell a pre-aggregation field assignment
// (inlined per-row) from a post-aggregation one (computed after the GROUP BY).
var aggregatingCommandNames = map[string]bool{}

// registerCommand registers a handler for one or more command names.
func registerCommand(handler CommandHandler, names ...string) {
	for _, name := range names {
		commandHandlers[name] = handler
	}
}

// registerAggregatingCommand registers a handler whose command aggregates
// (collapses rows). It registers the handler normally and records every name as
// aggregating.
func registerAggregatingCommand(handler CommandHandler, names ...string) {
	registerCommand(handler, names...)
	for _, name := range names {
		aggregatingCommandNames[name] = true
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
		transformCommandNames[name] = true
	}
}

// getCommandHandler returns the handler for a command name, or nil if not found.
func getCommandHandler(name string) CommandHandler {
	return commandHandlers[name]
}
