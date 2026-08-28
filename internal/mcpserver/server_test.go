package mcpserver

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// connect brings up the server over an in-memory transport and returns a client
// session, which is the only way to see the tools exactly as a model does.
func connect(t *testing.T, c *Client) *mcp.ClientSession {
	t.Helper()
	clientSide, serverSide := mcp.NewInMemoryTransports()

	ctx := context.Background()
	server, err := New(c).Connect(ctx, serverSide, nil)
	if err != nil {
		t.Fatalf("server could not connect: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	session, err := mcp.NewClient(&mcp.Implementation{Name: "test"}, nil).Connect(ctx, clientSide, nil)
	if err != nil {
		t.Fatalf("client could not connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func tools(t *testing.T) []*mcp.Tool {
	t.Helper()
	session := connect(t, NewClient(Config{URL: "http://example.invalid", APIKey: "k"}))
	result, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("tools/list failed: %v", err)
	}
	return result.Tools
}

func TestEveryToolDescribesItselfAndItsArguments(t *testing.T) {
	for _, tool := range tools(t) {
		if tool.Description == "" {
			t.Errorf("%s has no description", tool.Name)
		}
		schema, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatalf("%s: input schema does not marshal: %v", tool.Name, err)
		}
		var decoded struct {
			Properties map[string]struct {
				Description string `json:"description"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(schema, &decoded); err != nil {
			t.Fatalf("%s: input schema is not an object schema: %v", tool.Name, err)
		}
		for name, property := range decoded.Properties {
			if property.Description == "" {
				t.Errorf("%s argument %q has no description, so a model has only its name to go on", tool.Name, name)
			}
		}
	}
}

// The time range is an embedded struct. If it ever stopped being flattened, the
// arguments would silently move under a nested object and every windowed tool
// would reject the range a model sent.
func TestTheTimeRangeIsFlattenedIntoWindowedTools(t *testing.T) {
	windowed := map[string]bool{"query_logs": true, "get_field_stats": true}
	seen := 0
	for _, tool := range tools(t) {
		if !windowed[tool.Name] {
			continue
		}
		seen++
		schema, _ := json.Marshal(tool.InputSchema)
		var decoded struct {
			Properties map[string]any `json:"properties"`
			Required   []string       `json:"required"`
		}
		if err := json.Unmarshal(schema, &decoded); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"start", "end", "query"} {
			if _, ok := decoded.Properties[name]; !ok {
				t.Errorf("%s is missing the %q argument: %s", tool.Name, name, schema)
			}
		}
		if len(decoded.Required) != 1 || decoded.Required[0] != "query" {
			t.Errorf("%s should require only query, got %v", tool.Name, decoded.Required)
		}
	}
	if seen != len(windowed) {
		t.Fatalf("expected %d windowed tools, found %d", len(windowed), seen)
	}
}

func TestToolNamesAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, tool := range tools(t) {
		if seen[tool.Name] {
			t.Errorf("%s is registered twice", tool.Name)
		}
		seen[tool.Name] = true
	}
	if len(seen) == 0 {
		t.Fatal("no tools registered")
	}
}
