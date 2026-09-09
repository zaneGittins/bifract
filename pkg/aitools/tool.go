// Package aitools defines the operations an AI agent may perform against
// Bifract. Every tool reaches the instance through the same HTTP API the UI
// uses and is authorized by the router's own guards, so a tool can never do
// more than the caller it runs for is permitted. Two callers exist: the MCP
// server, which carries an analyst's API key over the network, and the chat
// backend, which dispatches in process under the signed-in user's session.
package aitools

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"bifract/pkg/api"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// APIPrefix is the versioned mount every tool path is relative to.
const APIPrefix = "/api/v1"

// Client performs API calls for a tool. Implementations differ only in how the
// caller is authenticated, never in what a call is allowed to do.
type Client interface {
	Get(ctx context.Context, path string, query url.Values) (any, error)
	Post(ctx context.Context, path string, body any) (any, error)
	Put(ctx context.Context, path string, body any) (any, error)
	Delete(ctx context.Context, path string) (any, error)
	// Static is a Get whose answer is fixed by the build and may be cached.
	Static(ctx context.Context, path string) (any, error)
	// FractalID is the fractal this session acts in, for the endpoints that
	// name it in the path.
	FractalID(ctx context.Context) (string, error)
}

// FractalArg is the argument a scoped tool takes to say which fractal it acts
// in. It is added to the MCP schema here rather than to each tool's own
// arguments, so no tool can ship without it and none has to retype it.
//
// It is absent from the schema chat renders: there the scope is the one the user
// has selected on screen, and it is not the model's to pick.
const FractalArg = "fractal_id"

const fractalArgDescription = "The fractal to act in, from list_fractals. Needed only for an " +
	"instance-wide key, which belongs to no fractal of its own; a key issued for one fractal " +
	"or prism ignores it."

// fractalArgIgnored is what the argument means to a tool that acts across the
// instance rather than in one fractal.
const fractalArgIgnored = "Ignored: this tool reports on the instance rather than acting in one fractal."

// ScopeResolver is implemented by a client whose credential may belong to no
// scope, and so has to be told which one a call acts in. It returns the context
// the call runs on, and refuses a call that names none where none can be
// inferred: a write that lands in whichever fractal the server falls back to
// reads as a success and detects nothing.
type ScopeResolver interface {
	ResolveScope(ctx context.Context, fractalID string) (context.Context, error)
}

// noArgs is the argument type of a tool that takes none. It still has to be a
// struct, because the MCP spec requires an object input schema.
type noArgs struct{}

// handler is a tool body. It receives the client and its arguments already
// unmarshaled and validated against the schema inferred from In.
type handler[In any] func(ctx context.Context, c Client, in In) (any, error)

// declaration collects the options a tool is declared with.
type declaration struct {
	enums     map[string][]string
	needs     api.Access
	confirm   *bool
	scopeFree bool
}

// option adjusts a tool as it is declared.
type option func(*declaration)

// enumFor lists an argument's accepted values, taken from the server's own type
// so a value added in Go reaches the tool schema without being retyped here and
// cannot drift from what the API will accept.
func enumFor[T api.Enumerator](property string) option {
	var zero T
	values := zero.EnumValues()
	return func(d *declaration) { d.enums[property] = values }
}

// enumOf lists an argument's accepted values from a slice the tool already
// defines, where they are not a server type's own enum.
func enumOf(property string, values []string) option {
	return func(d *declaration) { d.enums[property] = values }
}

// needsAccess declares the authority a tool's own calls require, for the cases
// where the annotation does not imply it: Recall's reads sit behind analyst
// routes because an archive scan costs real money to run.
func needsAccess(a api.Access) option {
	return func(d *declaration) { d.needs = a }
}

// noConfirm exempts a writing tool from the user's confirmation. Only for a
// write that can neither create nor alter anything: cancelling a job the user
// already approved is the one case.
func noConfirm() option {
	return func(d *declaration) { no := false; d.confirm = &no }
}

// scopeFree marks a tool that acts outside any one fractal, so it neither takes
// a fractal argument nor requires a scope to be resolvable. Discovery is the
// case: naming the fractal cannot be a precondition of finding out which
// fractals there are.
func scopeFree() option {
	return func(d *declaration) { d.scopeFree = true }
}

// readOnly marks a tool that cannot change anything. Chat runs these without
// asking the user, so it is an authorization fact rather than decoration: an
// unannotated tool reads as a write, which is the safe way to be wrong.
func readOnly() *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true}
}

// mutates marks a tool that changes state without removing anything.
func mutates() *mcp.ToolAnnotations {
	no := false
	return &mcp.ToolAnnotations{DestructiveHint: &no}
}

// destroys marks a tool whose effect cannot be undone.
func destroys() *mcp.ToolAnnotations {
	yes := true
	return &mcp.ToolAnnotations{DestructiveHint: &yes}
}

// Tool is one AI-callable operation: its schema, its body, and whether calling
// it can change anything.
type Tool struct {
	Def *mcp.Tool

	// needs is the most authority a call may use, and confirm whether a user
	// has to approve the call before it runs.
	needs   api.Access
	confirm bool
	// window records that the tool accepts a start/end range, so a caller that
	// governs the range centrally knows to supply it.
	window bool
	// scopeFree records that the tool acts across the instance, so no scope is
	// resolved before it runs.
	scopeFree bool

	addTo func(*mcp.Server, Client)
	run   func(context.Context, Client, json.RawMessage) (any, error)
}

// Name is the tool's identifier, as the model sees it.
func (t Tool) Name() string { return t.Def.Name }

// ReadOnly reports whether calling this tool can only read.
func (t Tool) ReadOnly() bool {
	return t.Def.Annotations != nil && t.Def.Annotations.ReadOnlyHint
}

// Ceiling is the most authority a call to this tool may use. It caps the
// request regardless of what the caller's own role would allow, so a tool body
// that reaches past what it declared is refused by the router rather than by
// review.
func (t Tool) Ceiling() api.Access { return t.needs }

// NeedsConfirmation reports whether a user has to approve this call before it
// runs. Every write does, bar the narrow exemptions noConfirm names.
func (t Tool) NeedsConfirmation() bool { return t.confirm }

// TakesWindow reports whether the tool accepts a start/end time range.
func (t Tool) TakesWindow() bool { return t.window }

// ScopeFree reports whether the tool acts across the instance rather than in one
// fractal, and so runs without a scope being resolved. Exported so a test can
// hold the set to an explicit list: a scoped tool marked scope-free by mistake
// would write wherever the server falls back to, and nothing else would say so.
func (t Tool) ScopeFree() bool { return t.scopeFree }

// Call runs the tool with arguments as the model produced them.
func (t Tool) Call(ctx context.Context, c Client, args json.RawMessage) (any, error) {
	return t.run(ctx, c, args)
}

// set collects the tools as they are declared.
type set struct {
	tools []Tool
	seen  map[string]bool
}

// add wires one tool into the set. Schema faults panic: they are programming
// errors that must not reach a running server.
func add[In any](d *set, t *mcp.Tool, h handler[In], opts ...option) {
	if d.seen[t.Name] {
		panic(fmt.Sprintf("aitools: duplicate tool %s", t.Name))
	}
	if t.Annotations == nil {
		panic(fmt.Sprintf("aitools: %s declares no annotations, so nothing says whether it writes", t.Name))
	}

	spec := &declaration{enums: map[string][]string{}}
	for _, o := range opts {
		o(spec)
	}

	build := func() *jsonschema.Schema {
		schema, err := jsonschema.For[In](nil)
		if err != nil {
			panic(fmt.Sprintf("%s: input schema: %v", t.Name, err))
		}
		for name, values := range spec.enums {
			property, ok := schema.Properties[name]
			if !ok {
				panic(fmt.Sprintf("%s: no argument %q to constrain", t.Name, name))
			}
			property.Enum = make([]any, len(values))
			for i, v := range values {
				property.Enum[i] = v
			}
		}
		return schema
	}

	schema := build()
	t.InputSchema = schema

	resolved, err := schema.Resolve(nil)
	if err != nil {
		panic(fmt.Sprintf("%s: input schema does not resolve: %v", t.Name, err))
	}

	// The MCP surface takes one argument more than chat's, so the two schemas are
	// built separately rather than one being mutated after the fact.
	//
	// Every tool takes it, a scope-free one included: the schemas refuse an
	// argument they do not declare, and a model told to name the fractal on every
	// call must not be answered with a validation error by the very tool it calls
	// to find the id.
	served := *t
	scoped := build()
	if scoped.Properties == nil {
		// A tool that takes no arguments of its own still takes this one.
		scoped.Properties = map[string]*jsonschema.Schema{}
		scoped.Type = "object"
	}
	if _, taken := scoped.Properties[FractalArg]; taken {
		panic(fmt.Sprintf("%s declares its own %s, which the scope argument would shadow", t.Name, FractalArg))
	}
	description := fractalArgDescription
	if spec.scopeFree {
		description = fractalArgIgnored
	}
	scoped.Properties[FractalArg] = &jsonschema.Schema{Type: "string", Description: description}
	served.InputSchema = scoped

	if d.seen == nil {
		d.seen = map[string]bool{}
	}
	d.seen[t.Name] = true

	readOnlyTool := t.Annotations.ReadOnlyHint
	needs := spec.needs
	if needs == "" {
		needs = api.AccessAnalyst
		if readOnlyTool {
			needs = api.AccessViewer
		}
	}
	confirm := !readOnlyTool
	if spec.confirm != nil {
		confirm = *spec.confirm
	}

	_, windowed := schema.Properties["start"]

	d.tools = append(d.tools, Tool{
		Def:       t,
		needs:     needs,
		confirm:   confirm,
		window:    windowed,
		scopeFree: spec.scopeFree,
		addTo: func(s *mcp.Server, c Client) {
			mcp.AddTool(s, &served, func(ctx context.Context, req *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
				ctx, err := scopedCall(ctx, c, spec.scopeFree, req)
				if err != nil {
					return nil, nil, err
				}
				out, err := h(ctx, c, in)
				if err != nil {
					return nil, nil, err
				}
				text, err := json.MarshalIndent(out, "", "  ")
				if err != nil {
					return nil, nil, fmt.Errorf("could not encode the result of %s: %w", t.Name, err)
				}
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(text)}}}, nil, nil
			})
		},
		run: func(ctx context.Context, c Client, raw json.RawMessage) (any, error) {
			value := any(map[string]any{})
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &value); err != nil {
					return nil, fmt.Errorf("%s: the arguments are not valid JSON: %w", t.Name, err)
				}
			}
			// Validated against the same schema the MCP transport enforces, so a
			// caller that does not speak MCP cannot reach a body with input the
			// tool never declared.
			if err := resolved.Validate(value); err != nil {
				return nil, fmt.Errorf("%s was called with arguments it does not accept: %w", t.Name, err)
			}
			var in In
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &in); err != nil {
					return nil, fmt.Errorf("%s: the arguments could not be decoded: %w", t.Name, err)
				}
			}
			return h(ctx, c, in)
		},
	})
}

// All returns every tool defined here, in a stable order.
func All() []Tool {
	d := &set{seen: map[string]bool{}}
	registerQueryTools(d)
	registerAlertTools(d)
	registerGovernanceTools(d)
	registerCatalogTools(d)
	registerDictionaryTools(d)
	registerCommentTools(d)
	registerNotebookTools(d)
	registerModelTools(d)
	registerAttackTools(d)
	registerRecallTools(d)
	registerInstructionTools(d)
	registerProvenanceTools(d)
	return d.tools
}

// scopedCall settles which scope a tool call acts in, for a client whose
// credential may belong to none.
func scopedCall(ctx context.Context, c Client, isScopeFree bool, req *mcp.CallToolRequest) (context.Context, error) {
	resolver, ok := c.(ScopeResolver)
	if !ok || isScopeFree {
		return ctx, nil
	}
	return resolver.ResolveScope(ctx, fractalArgument(req))
}

// fractalArgument reads the fractal a call named. It comes off the raw arguments
// because the tool's own argument type does not carry it, and it is keyed on
// FractalArg rather than on a struct tag: a name that drifted from the schema
// would read as a call that named no fractal, which is the failure this whole
// argument exists to prevent.
func fractalArgument(req *mcp.CallToolRequest) string {
	if req == nil || req.Params == nil || len(req.Params.Arguments) == 0 {
		return ""
	}
	var named map[string]json.RawMessage
	if err := json.Unmarshal(req.Params.Arguments, &named); err != nil {
		return ""
	}
	var id string
	if err := json.Unmarshal(named[FractalArg], &id); err != nil {
		return ""
	}
	return id
}

// Serve registers every tool on an MCP server backed by c.
func Serve(s *mcp.Server, c Client, tools []Tool) {
	for _, t := range tools {
		t.addTo(s, c)
	}
}

// summarize keeps only the fields worth spending the model's context on. Empty
// values are dropped; a zero is kept, since a row count of 0 is an answer.
func summarize(rows any, fields ...string) []map[string]any {
	list, ok := rows.([]any)
	if !ok {
		return []map[string]any{}
	}
	out := make([]map[string]any, 0, len(list))
	for _, row := range list {
		object, ok := row.(map[string]any)
		if !ok {
			continue
		}
		kept := make(map[string]any, len(fields))
		for _, name := range fields {
			if value, present := object[name]; present && value != nil && value != "" {
				kept[name] = value
			}
		}
		out = append(out, kept)
	}
	return out
}

// Field reads a named value from a decoded API response.
func Field[T any](payload any, name string) T {
	var zero T
	object, ok := payload.(map[string]any)
	if !ok {
		return zero
	}
	value, ok := object[name].(T)
	if !ok {
		return zero
	}
	return value
}

// clamp keeps a caller-supplied count inside what an endpoint will accept, and
// substitutes the default for the zero value an omitted argument leaves behind.
func clamp(value, fallback, low, high int) int {
	if value == 0 {
		value = fallback
	}
	return max(low, min(value, high))
}
