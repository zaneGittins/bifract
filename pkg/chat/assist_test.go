package chat

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"bifract/pkg/fractals"
	"bifract/pkg/llm"
)

// scriptedModel answers each completion with the next reply in turn and keeps
// every request it was sent.
type scriptedModel struct {
	mu       sync.Mutex
	replies  []string
	requests []map[string]any
}

func (s *scriptedModel) serve(t *testing.T) *llm.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req)
		s.mu.Lock()
		s.requests = append(s.requests, req)
		n := len(s.requests)
		s.mu.Unlock()
		if n > len(s.replies) {
			t.Errorf("model called %d times, only %d replies scripted", n, len(s.replies))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte(`{"choices":[{"message":` + s.replies[n-1] + `}]}`))
	}))
	t.Cleanup(srv.Close)
	client, err := llm.New(llm.Config{Provider: llm.ProviderOpenAI, APIKey: "k", Model: "m", BaseURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func toolReply(id, name string, args map[string]any) string {
	encoded, _ := json.Marshal(args)
	call, _ := json.Marshal(map[string]any{
		"id": id, "type": "function",
		"function": map[string]any{"name": name, "arguments": string(encoded)},
	})
	return `{"content":null,"tool_calls":[` + string(call) + `]}`
}

// validatingRouter stands in for the server's own router: /query/validate
// rejects any query containing "bad", and every other path is recorded.
func validatingRouter(paths *[]string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*paths = append(*paths, r.URL.Path)
		if r.URL.Path != "/api/v1/query/validate" {
			http.NotFound(w, r)
			return
		}
		var body struct {
			Query string `json:"query"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if strings.Contains(body.Query, "bad") {
			json.NewEncoder(w).Encode(map[string]any{"valid": false, "error": "unknown function bad()"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"valid": true})
	})
}

func assistEvents(t *testing.T, body string) []StreamEvent {
	t.Helper()
	var events []StreamEvent
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev StreamEvent
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
			t.Fatalf("bad event %q: %v", line, err)
		}
		events = append(events, ev)
	}
	return events
}

func runAssist(t *testing.T, m *Manager, req QueryAssistRequest) []StreamEvent {
	t.Helper()
	rec := httptest.NewRecorder()
	origin := httptest.NewRequest(http.MethodPost, "/api/v1/query/assist/stream", nil)
	m.AssistQuery(origin, rec, rec, &fractals.Fractal{ID: "f1", Name: "corp"}, "f1", req)
	return assistEvents(t, rec.Body.String())
}

// The server checks the proposed query itself rather than trusting the model,
// and hands a rejection back so the model can fix it before the user sees it.
func TestAssistQueryValidatesBeforeAnswering(t *testing.T) {
	model := &scriptedModel{replies: []string{
		toolReply("c1", "propose_query", map[string]any{"query": "bad(x)", "explanation": "first try"}),
		toolReply("c2", "propose_query", map[string]any{"query": "event_id=1 | groupBy(host)", "explanation": "PowerShell by host"}),
	}}
	var paths []string
	m := &Manager{llm: model.serve(t), tools: chatTools(t), router: validatingRouter(&paths)}

	events := runAssist(t, m, QueryAssistRequest{Prompt: "powershell by host", CurrentQuery: "event_id=4625", TimeRange: "24h"})

	last := events[len(events)-1]
	if last.Type != "result" {
		t.Fatalf("last event = %+v, want a result", last)
	}
	result, _ := last.ToolArgs.(map[string]any)
	if result["query"] != "event_id=1 | groupBy(host)" || result["explanation"] != "PowerShell by host" {
		t.Errorf("result = %v", result)
	}

	// The second request carries the validator's complaint about the first.
	second, _ := json.Marshal(model.requests[1]["messages"])
	if !strings.Contains(string(second), "unknown function bad()") {
		t.Errorf("the rejection never reached the model: %s", second)
	}
	// And the user's editor query reached the first prompt.
	first, _ := json.Marshal(model.requests[0]["messages"])
	if !strings.Contains(string(first), "event_id=4625") {
		t.Errorf("the current query was not in the prompt: %s", first)
	}
}

// Only read tools that learn the fractal's shape are offered. Running the
// user's query is the user's decision, and nothing here may write.
func TestAssistQueryOffersOnlyReadTools(t *testing.T) {
	model := &scriptedModel{replies: []string{
		toolReply("c1", "query_logs", map[string]any{"query": "*"}),
		toolReply("c2", "propose_query", map[string]any{"query": "*", "explanation": "all"}),
	}}
	var paths []string
	m := &Manager{llm: model.serve(t), tools: chatTools(t), router: validatingRouter(&paths)}

	runAssist(t, m, QueryAssistRequest{Prompt: "everything"})

	offered := map[string]bool{}
	tools, _ := model.requests[0]["tools"].([]any)
	for _, raw := range tools {
		fn, _ := raw.(map[string]any)["function"].(map[string]any)
		offered[fn["name"].(string)] = true
	}
	for _, name := range []string{"query_logs", "create_alert", "add_comment", "search_archive"} {
		if offered[name] {
			t.Errorf("%s was offered to the query writer", name)
		}
	}
	if !offered["get_fields"] || !offered["propose_query"] {
		t.Errorf("offered %v, want get_fields and propose_query among them", offered)
	}
	for _, p := range paths {
		if p == "/api/v1/query" {
			t.Errorf("query_logs ran through the router")
		}
	}
}

// A model that answers in prose instead of proposing a query ends with an error
// the palette can show, not a silent empty result.
func TestAssistQueryWithoutProposalIsAnError(t *testing.T) {
	model := &scriptedModel{replies: []string{`{"content":"I am not sure what you mean."}`}}
	var paths []string
	m := &Manager{llm: model.serve(t), tools: chatTools(t), router: validatingRouter(&paths)}

	events := runAssist(t, m, QueryAssistRequest{Prompt: "hmm"})

	last := events[len(events)-1]
	if last.Type != "error" || !strings.Contains(last.Content, "I am not sure what you mean.") {
		t.Fatalf("last event = %+v, want an error carrying the model's reply", last)
	}
}

// Every tool the query writer is offered must exist in chat's allowlist and
// only read. A name that drifted out would silently vanish from the model's
// tools, and a write added here would run without the approval chat requires.
func TestAssistToolsAreExposedReadTools(t *testing.T) {
	if err := checkAssistTools(chatTools(t)); err != nil {
		t.Fatal(err)
	}
	tools := chatTools(t)
	for name := range assistTools {
		tool, ok := tools.lookup(name)
		if !ok || !tool.ReadOnly() || tool.NeedsConfirmation() {
			t.Errorf("%s: exposed=%v readOnly=%v", name, ok, ok && tool.ReadOnly())
		}
	}
	if !assistTools[assistValidateTool] {
		t.Errorf("%s must be available to check proposals", assistValidateTool)
	}
}

// A validator that cannot answer is not a verdict on the query, so the model is
// not sent round after round to fix a query that may be fine.
func TestAssistQueryStopsWhenTheValidatorFails(t *testing.T) {
	model := &scriptedModel{replies: []string{
		toolReply("c1", "propose_query", map[string]any{"query": "event_id=1", "explanation": "x"}),
	}}
	forbidden := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"success":false,"error":"no access"}`, http.StatusForbidden)
	})
	m := &Manager{llm: model.serve(t), tools: chatTools(t), router: forbidden}

	events := runAssist(t, m, QueryAssistRequest{Prompt: "x"})

	if last := events[len(events)-1]; last.Type != "error" {
		t.Fatalf("last event = %+v, want an error", last)
	}
	if len(model.requests) != 1 {
		t.Errorf("model called %d times after the validator failed, want 1", len(model.requests))
	}
}

// A proposal that is only comments validates as empty, which is not a query.
func TestAssistQueryRejectsACommentOnlyProposal(t *testing.T) {
	model := &scriptedModel{replies: []string{
		toolReply("c1", "propose_query", map[string]any{"query": "// TODO", "explanation": "x"}),
		toolReply("c2", "propose_query", map[string]any{"query": "event_id=1", "explanation": "y"}),
	}}
	var paths []string
	m := &Manager{llm: model.serve(t), tools: chatTools(t), router: validatingRouter(&paths)}

	events := runAssist(t, m, QueryAssistRequest{Prompt: "x"})

	result, _ := events[len(events)-1].ToolArgs.(map[string]any)
	if result["query"] != "event_id=1" {
		t.Fatalf("result = %v, want the second proposal", result)
	}
}

// Out of rounds, the last invalid query is still worth showing, but a proposal
// with no query never is.
func TestAssistQueryOutOfRounds(t *testing.T) {
	var bad, empty []string
	for i := 0; i < assistMaxRounds; i++ {
		bad = append(bad, toolReply("c", "propose_query", map[string]any{"query": "bad(x)", "explanation": "x"}))
		empty = append(empty, toolReply("c", "propose_query", map[string]any{"query": " ", "explanation": "x"}))
	}
	var paths []string

	m := &Manager{llm: (&scriptedModel{replies: bad}).serve(t), tools: chatTools(t), router: validatingRouter(&paths)}
	last := runAssist(t, m, QueryAssistRequest{Prompt: "x"})
	ev := last[len(last)-1]
	result, _ := ev.ToolArgs.(map[string]any)
	if ev.Type != "result" || result["query"] != "bad(x)" || result["valid"] != false {
		t.Errorf("bad proposals ended with %+v, want the invalid query", ev)
	}

	m = &Manager{llm: (&scriptedModel{replies: empty}).serve(t), tools: chatTools(t), router: validatingRouter(&paths)}
	last = runAssist(t, m, QueryAssistRequest{Prompt: "x"})
	if ev := last[len(last)-1]; ev.Type != "error" {
		t.Errorf("empty proposals ended with %+v, want an error", ev)
	}
}

// One reply cannot fan out into an unbounded number of scans.
func TestAssistQueryCapsToolCallsPerRound(t *testing.T) {
	var calls []string
	for i := 0; i < assistMaxCallsPerRound+4; i++ {
		encoded, _ := json.Marshal(map[string]any{"id": "c" + strings.Repeat("x", i), "type": "function",
			"function": map[string]any{"name": "get_bql_reference", "arguments": "{}"}})
		calls = append(calls, string(encoded))
	}
	model := &scriptedModel{replies: []string{
		`{"content":null,"tool_calls":[` + strings.Join(calls, ",") + `]}`,
		toolReply("p", "propose_query", map[string]any{"query": "event_id=1", "explanation": "x"}),
	}}
	var paths []string
	m := &Manager{llm: model.serve(t), tools: chatTools(t), router: validatingRouter(&paths)}

	runAssist(t, m, QueryAssistRequest{Prompt: "x"})

	reached := 0
	for _, p := range paths {
		if p != "/api/v1/query/validate" {
			reached++
		}
	}
	if reached != assistMaxCallsPerRound {
		t.Errorf("%d tool calls reached the router, want %d", reached, assistMaxCallsPerRound)
	}
	// Every call is still answered, or the provider rejects the next turn.
	second, _ := model.requests[1]["messages"].([]any)
	answered := 0
	for _, raw := range second {
		if msg, _ := raw.(map[string]any); msg["role"] == "tool" {
			answered++
		}
	}
	if answered != len(calls) {
		t.Errorf("%d of %d calls answered", answered, len(calls))
	}
}

// A prism request on a server with no prism resolver is a missing scope, as it
// was before the scope lookup was shared: a 400, not a server error.
func TestScopeFractalWithoutScopeIsAClientError(t *testing.T) {
	h := &Handler{}
	if _, err := h.scopeFractal(context.Background(), "", "p1"); !errors.Is(err, errNoScope) {
		t.Fatalf("err = %v, want errNoScope", err)
	}
}
