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

// registerCommand registers a handler for one or more command names.
func registerCommand(handler CommandHandler, names ...string) {
	for _, name := range names {
		commandHandlers[name] = handler
	}
}

// getCommandHandler returns the handler for a command name, or nil if not found.
func getCommandHandler(name string) CommandHandler {
	return commandHandlers[name]
}
