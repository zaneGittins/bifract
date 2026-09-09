package aitools

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// errUnscoped stands in for the refusal a client gives when nothing says which
// fractal a call means.
var errUnscoped = errors.New("this key is instance-wide: pass fractal_id")

// scopeFreeTools act outside any one fractal. Naming a fractal cannot be a
// precondition of finding out which fractals exist, or of reading a reference
// the build ships. Every other tool acts in a fractal and has its scope resolved
// before it runs.
var scopeFreeTools = map[string]bool{"list_fractals": true, "get_bql_reference": true}

// A tool marked scope-free skips the guard that settles which fractal it acts
// in, so the set is held to this list rather than to whatever the declarations
// happen to say.
func TestOnlyDiscoveryToolsSkipTheScopeGuard(t *testing.T) {
	for _, tool := range All() {
		if tool.ScopeFree() != scopeFreeTools[tool.Name()] {
			t.Errorf("%s declares scope-free %v, want %v: a scoped tool that skips the guard "+
				"writes wherever the server falls back to",
				tool.Name(), tool.ScopeFree(), scopeFreeTools[tool.Name()])
		}
	}
}

func schemaProperties(t *testing.T, schema any) map[string]bool {
	t.Helper()
	encoded, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("schema does not marshal: %v", err)
	}
	var decoded struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("schema is not an object schema: %v", err)
	}
	names := make(map[string]bool, len(decoded.Properties))
	for name := range decoded.Properties {
		names[name] = true
	}
	return names
}

// servedSchemas are the schemas a model is actually offered, which only a
// listing of the running server reports.
func servedSchemas(t *testing.T) map[string]any {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "test"}, nil)
	Serve(server, &scopeClient{}, All())

	result, err := connectTo(t, server).ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	schemas := map[string]any{}
	for _, tool := range result.Tools {
		schemas[tool.Name] = tool.InputSchema
	}
	return schemas
}

func connectTo(t *testing.T, server *mcp.Server) *mcp.ClientSession {
	t.Helper()
	clientSide, serverSide := mcp.NewInMemoryTransports()
	ctx := context.Background()
	running, err := server.Connect(ctx, serverSide, nil)
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	t.Cleanup(func() { _ = running.Close() })
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test"}, nil).Connect(ctx, clientSide, nil)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

// scopeClient records the scope each call resolved to.
type scopeClient struct {
	resolved []string
	refuse   error
}

func (s *scopeClient) ResolveScope(ctx context.Context, fractalID string) (context.Context, error) {
	if s.refuse != nil {
		return nil, s.refuse
	}
	s.resolved = append(s.resolved, fractalID)
	return ctx, nil
}

func (s *scopeClient) Get(context.Context, string, url.Values) (any, error) {
	return map[string]any{}, nil
}
func (s *scopeClient) Post(context.Context, string, any) (any, error) { return map[string]any{}, nil }
func (s *scopeClient) Put(context.Context, string, any) (any, error)  { return map[string]any{}, nil }
func (s *scopeClient) Delete(context.Context, string) (any, error)    { return map[string]any{}, nil }
func (s *scopeClient) Static(context.Context, string) (any, error)    { return map[string]any{}, nil }
func (s *scopeClient) FractalID(context.Context) (string, error)      { return "f1", nil }

// Every tool takes the argument that names a fractal. An instance-wide key
// belongs to none, and a tool it cannot target is a tool that writes wherever
// the server falls back to.
//
// The scope-free tools take it too, and ignore it. Schemas refuse an argument
// they do not declare, so a model told to name the fractal on every call would
// otherwise be answered with a validation error by the very tool it calls to
// find the id.
func TestEveryToolTakesTheFractalArgument(t *testing.T) {
	served := servedSchemas(t)
	for _, tool := range All() {
		schema, ok := served[tool.Name()]
		if !ok {
			t.Errorf("%s is not registered on the server", tool.Name())
			continue
		}
		if !schemaProperties(t, schema)[FractalArg] {
			t.Errorf("%s cannot be told which fractal to act in", tool.Name())
		}
	}
}

// Naming a fractal on a tool that acts across the instance is accepted and
// ignored, never refused.
func TestNamingAFractalOnADiscoveryToolIsHarmless(t *testing.T) {
	client := &scopeClient{}
	server := mcp.NewServer(&mcp.Implementation{Name: "test"}, nil)
	Serve(server, client, All())
	session := connectTo(t, server)

	for name := range scopeFreeTools {
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
			Name:      name,
			Arguments: map[string]any{FractalArg: "f1"},
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if result.IsError {
			t.Errorf("%s refused a fractal it only had to ignore: %v", name, result.Content)
		}
	}
	if len(client.resolved) != 0 {
		t.Errorf("a scope-free tool asked the client to resolve a scope: %v", client.resolved)
	}
}

// Chat resolves the scope from the signed-in user's session, which the model may
// not override. The argument must therefore never reach the schema chat renders.
func TestChatNeverSeesTheFractalArgument(t *testing.T) {
	for _, tool := range All() {
		if schemaProperties(t, tool.Def.InputSchema)[FractalArg] {
			t.Errorf("%s offers %s to chat, where the user's selection governs the scope", tool.Name(), FractalArg)
		}
	}
}

// The argument is answered by the client, not by the tool body, so a tool cannot
// forget to apply it.
func TestTheNamedFractalReachesTheClient(t *testing.T) {
	client := &scopeClient{}
	server := mcp.NewServer(&mcp.Implementation{Name: "test"}, nil)
	Serve(server, client, All())
	session := connectTo(t, server)

	if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "list_alerts",
		Arguments: map[string]any{FractalArg: "velociraptor-1"},
	}); err != nil {
		t.Fatalf("call: %v", err)
	}
	if len(client.resolved) != 1 || client.resolved[0] != "velociraptor-1" {
		t.Fatalf("the client resolved %v, want the fractal the call named", client.resolved)
	}

	// Discovery runs without one, or an instance-wide key could never find the id
	// it is being asked for.
	if _, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "list_fractals"}); err != nil {
		t.Fatalf("list_fractals: %v", err)
	}
	if len(client.resolved) != 1 {
		t.Errorf("a scope-free tool asked the client to resolve one: %v", client.resolved)
	}
}

// A refusal has to stop the call, not be reported alongside a result.
func TestAnUnresolvableScopeRefusesTheCall(t *testing.T) {
	client := &scopeClient{refuse: errUnscoped}
	server := mcp.NewServer(&mcp.Implementation{Name: "test"}, nil)
	Serve(server, client, All())
	session := connectTo(t, server)

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "list_alerts"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !result.IsError {
		t.Fatal("a call that cannot be scoped must fail")
	}
}
