package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"

	"bifract/pkg/api"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// noArgs is the argument type of a tool that takes none. It still has to be a
// struct, because the MCP spec requires an object input schema.
type noArgs struct{}

// handler is a tool body. It receives the shared client and its arguments already
// unmarshaled and validated against the schema inferred from In.
type handler[In any] func(ctx context.Context, c *Client, in In) (any, error)

// choices names an argument whose accepted values the schema should list.
type choices struct {
	property string
	values   []string
}

// enumFor takes the values from the server's own type, so a value added in Go
// reaches the tool schema without being retyped here and cannot drift from what
// the API will accept.
func enumFor[T api.Enumerator](property string) choices {
	var zero T
	return choices{property: property, values: zero.EnumValues()}
}

// register wires a handler onto the server. The result is JSON so the model reads
// the API's own field names, and an error is returned as an MCP tool error rather
// than prose the model has to recognise as a failure.
func register[In any](s *mcp.Server, c *Client, t *mcp.Tool, h handler[In], enums ...choices) {
	if len(enums) > 0 {
		schema, err := jsonschema.For[In](nil)
		if err != nil {
			panic(fmt.Sprintf("%s: input schema: %v", t.Name, err))
		}
		for _, e := range enums {
			property, ok := schema.Properties[e.property]
			if !ok {
				panic(fmt.Sprintf("%s: no argument %q to constrain", t.Name, e.property))
			}
			property.Enum = make([]any, len(e.values))
			for i, v := range e.values {
				property.Enum[i] = v
			}
		}
		t.InputSchema = schema
	}
	mcp.AddTool(s, t, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
		out, err := h(ctx, c, in)
		if err != nil {
			return nil, nil, err
		}
		text, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return nil, nil, fmt.Errorf("could not encode the result of %s: %w", t.Name, err)
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(text)}},
		}, nil, nil
	})
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

// field reads a named value from a decoded API response.
func field[T any](payload any, name string) T {
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
