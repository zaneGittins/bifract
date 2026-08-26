package parser

import "fmt"

// Source commands GENERATE the pipeline's source rather than filtering/transforming an
// existing one. They cannot be expressed as pure SQL over the logs table (they need Go
// orchestration -- e.g. pgr()'s two-pass tree traversal + edge scoring), so when one appears
// it MUST be the first command. The query layer resolves it into a SQL subquery
// (QueryOptions.SourceSubquery) that the rest of the pipeline composes over via the normal
// translator. Adding a new source command = register its name here + a resolver in the query
// layer; the core translate/handler path needs no per-command special-casing.
var sourceCommandNames = map[string]bool{
	"pgr": true,
}

// FirstSourceCommand returns the first source command in the pipeline, if any.
func FirstSourceCommand(pipeline *PipelineNode) (CommandNode, bool) {
	if pipeline == nil {
		return CommandNode{}, false
	}
	for _, c := range pipeline.Commands {
		if sourceCommandNames[c.Name] {
			return c, true
		}
	}
	return CommandNode{}, false
}

// ValidateSourceCommandPlacement enforces that a source command is the FIRST thing in the
// pipeline. It is scoped only by its own arguments, the time range, and the fractal, so any
// filter or command placed before it would be silently ignored -- return a clear error
// instead. Also rejects combining two source commands.
func ValidateSourceCommandPlacement(pipeline *PipelineNode) error {
	if pipeline == nil {
		return nil
	}
	srcIdx, srcName := -1, ""
	for i, c := range pipeline.Commands {
		if !sourceCommandNames[c.Name] {
			continue
		}
		if srcIdx != -1 {
			return fmt.Errorf("%s() and %s() cannot be combined: a query has at most one source command", srcName, c.Name)
		}
		srcIdx, srcName = i, c.Name
	}
	if srcIdx == -1 {
		return nil
	}
	if pipeline.Filter != nil && len(pipeline.Filter.Conditions) > 0 {
		return fmt.Errorf("%s() must be the first command: a filter before it (e.g. `event_id=1 | %s(...)`) is ignored, because %s() is scoped only by its arguments, the time range, and the fractal. Filter after it instead: `%s(...) | field=value`", srcName, srcName, srcName, srcName)
	}
	if srcIdx != 0 {
		before := pipeline.Commands[0].Name
		return fmt.Errorf("%s() must be the first command; `%s()` before it is ignored. Move it after: `%s(...) | %s(...)`", srcName, before, srcName, before)
	}
	return nil
}

// StripCommand returns a shallow copy of the pipeline with every occurrence of the named
// command removed. Used to drop a source command once it has been resolved into a subquery
// source, leaving the downstream commands (filters/aggregations/charts) to translate over it.
func StripCommand(pipeline *PipelineNode, name string) *PipelineNode {
	out := *pipeline
	out.Commands = nil
	for _, c := range pipeline.Commands {
		if c.Name == name {
			continue
		}
		out.Commands = append(out.Commands, c)
	}
	return &out
}
