package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(t *testing.T, provider string, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := New(Config{Provider: provider, APIKey: "test-key", Model: "test-model", BaseURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func writeSSE(w http.ResponseWriter, events ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	for _, e := range events {
		fmt.Fprintf(w, "data: %s\n\n", e)
	}
}

var searchTool = Tool{Type: "function", Function: Function{Name: "search", Description: "Search logs", Parameters: map[string]interface{}{"type": "object"}}}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := New(Config{Provider: "bedrock", APIKey: "k"}); err == nil {
		t.Error("unknown provider accepted")
	}
	if _, err := New(Config{Provider: "openai", APIKey: "k"}); err == nil {
		t.Error("openai without a model accepted")
	}
	c, err := New(Config{APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Provider() != ProviderAnthropic || c.Model() != DefaultAnthropicModel {
		t.Errorf("defaults: got %s/%s", c.Provider(), c.Model())
	}
}

func TestClientWithoutKeyIsNotConfigured(t *testing.T) {
	c, err := New(Config{Provider: "gemini"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Enabled() {
		t.Error("client without a key reports enabled")
	}
	if _, err := c.Complete(context.Background(), Request{}); !errors.Is(err, ErrNotConfigured) {
		t.Errorf("got %v, want ErrNotConfigured", err)
	}
	var nilClient *Client
	if _, err := nilClient.Stream(context.Background(), Request{}, nil); !errors.Is(err, ErrNotConfigured) {
		t.Errorf("nil client: got %v, want ErrNotConfigured", err)
	}
}

func TestRejectedKeyNamesTheSetting(t *testing.T) {
	c := newTestClient(t, "openai", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
	})
	_, err := c.Complete(context.Background(), Request{Messages: []Message{{Role: "user", Content: "hi"}}})
	if err == nil || !strings.Contains(err.Error(), "BIFRACT_LLM_API_KEY") {
		t.Fatalf("got %v", err)
	}
}

func TestOpenAIStreamAssemblesTextAndToolCalls(t *testing.T) {
	var got openAIRequest
	c := newTestClient(t, "openai", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" || r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("unexpected request %s auth=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		json.NewDecoder(r.Body).Decode(&got)
		writeSSE(w,
			`{"choices":[{"delta":{"role":"assistant","content":"Let me "}}]}`,
			`{"choices":[{"delta":{"content":"look."}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"search","arguments":"{\"q\":"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"x\"}"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_2","type":"function","function":{"name":"think","arguments":"{}"}}]}}]}`,
			`[DONE]`,
		)
	})

	var streamed strings.Builder
	resp, err := c.Stream(context.Background(), Request{
		Messages: []Message{{Role: "user", Content: "find x"}, {Role: "tool", ToolCallID: "c0", Name: "search", Content: "{}"}},
		Tools:    []Tool{searchTool},
	}, func(s string) { streamed.WriteString(s) })
	if err != nil {
		t.Fatal(err)
	}
	if got.Messages[1].Name != "" {
		t.Errorf("tool message sent with name %q", got.Messages[1].Name)
	}
	if !got.Stream || got.Model != "test-model" || got.ToolChoice != "auto" {
		t.Errorf("request: stream=%v model=%q tool_choice=%v", got.Stream, got.Model, got.ToolChoice)
	}
	if resp.Content != "Let me look." || streamed.String() != resp.Content {
		t.Errorf("content %q, streamed %q", resp.Content, streamed.String())
	}
	want := []ToolCall{
		{Index: 0, ID: "call_1", Type: "function", Function: FunctionCall{Name: "search", Arguments: `{"q":"x"}`}},
		{Index: 1, ID: "call_2", Type: "function", Function: FunctionCall{Name: "think", Arguments: `{}`}},
	}
	if !reflect.DeepEqual(resp.ToolCalls, want) {
		t.Errorf("tool calls:\n got %+v\nwant %+v", resp.ToolCalls, want)
	}
}

// Gemini's OpenAI endpoint can send several complete calls, each at index 0.
func TestOpenAIStreamSeparatesWholeCallsSharingAnIndex(t *testing.T) {
	c := newTestClient(t, "gemini", func(w http.ResponseWriter, r *http.Request) {
		writeSSE(w,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"a","type":"function","function":{"name":"search","arguments":"{\"q\":1}"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"b","type":"function","function":{"name":"search","arguments":"{\"q\":2}"}}]}}]}`,
			`[DONE]`,
		)
	})
	resp, err := c.Stream(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.ToolCalls) != 2 || resp.ToolCalls[0].Function.Arguments != `{"q":1}` || resp.ToolCalls[1].ID != "b" {
		t.Fatalf("got %+v", resp.ToolCalls)
	}
}

func TestOpenAICompleteForcesTool(t *testing.T) {
	c := newTestClient(t, "openai", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]interface{}
		json.NewDecoder(r.Body).Decode(&req)
		choice, _ := req["tool_choice"].(map[string]interface{})
		fn, _ := choice["function"].(map[string]interface{})
		if fn["name"] != "search" || req["stream"] != nil {
			t.Errorf("tool_choice=%v stream=%v", req["tool_choice"], req["stream"])
		}
		w.Write([]byte(`{"choices":[{"message":{"content":null,"tool_calls":[{"id":"c","type":"function","function":{"name":"search","arguments":"{}"}}]}}]}`))
	})
	resp, err := c.Complete(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}, Tools: []Tool{searchTool}, ForceTool: "search"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Function.Name != "search" {
		t.Fatalf("got %+v", resp)
	}
}

func TestToAnthropicTranslatesConversation(t *testing.T) {
	calls, _ := json.Marshal([]ToolCall{
		{ID: "t1", Type: "function", Function: FunctionCall{Name: "search", Arguments: `{"q":"x"}`}},
		{ID: "t2", Type: "function", Function: FunctionCall{Name: "think", Arguments: ``}},
	})
	system, msgs := toAnthropic([]Message{
		{Role: "system", Content: "be brief"},
		{Role: "user", Content: "find x"},
		{Role: "assistant", Content: "", ToolCalls: calls},
		{Role: "tool", ToolCallID: "t1", Content: `{"rows":1}`},
		{Role: "tool", ToolCallID: "t2", Content: `{}`},
		{Role: "user", Content: "and y?"},
	})
	if system != "be brief" {
		t.Errorf("system %q", system)
	}
	want := []anthropicMessage{
		{Role: "user", Content: []anthropicBlock{{Type: "text", Text: "find x"}}},
		{Role: "assistant", Content: []anthropicBlock{
			{Type: "tool_use", ID: "t1", Name: "search", Input: json.RawMessage(`{"q":"x"}`)},
			{Type: "tool_use", ID: "t2", Name: "think", Input: json.RawMessage(`{}`)},
		}},
		{Role: "user", Content: []anthropicBlock{
			{Type: "tool_result", ToolUseID: "t1", Content: `{"rows":1}`},
			{Type: "tool_result", ToolUseID: "t2", Content: `{}`},
			{Type: "text", Text: "and y?"},
		}},
	}
	if !reflect.DeepEqual(msgs, want) {
		g, _ := json.Marshal(msgs)
		w, _ := json.Marshal(want)
		t.Errorf("\n got %s\nwant %s", g, w)
	}
}

func TestAnthropicStreamAssemblesTextAndToolCalls(t *testing.T) {
	var got anthropicRequest
	c := newTestClient(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" || r.Header.Get("x-api-key") != "test-key" || r.Header.Get("anthropic-version") == "" {
			t.Errorf("unexpected request %s headers=%v", r.URL.Path, r.Header)
		}
		json.NewDecoder(r.Body).Decode(&got)
		writeSSE(w,
			`{"type":"message_start","message":{}}`,
			`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Checking"}}`,
			`{"type":"content_block_stop","index":0}`,
			`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"search","input":{}}}`,
			`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"q\":"}}`,
			`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"x\"}"}}`,
			`{"type":"content_block_stop","index":1}`,
			`{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_2","name":"think","input":{}}}`,
			`{"type":"content_block_stop","index":2}`,
			`{"type":"message_stop"}`,
		)
	})
	var streamed strings.Builder
	resp, err := c.Stream(context.Background(), Request{
		Messages: []Message{{Role: "system", Content: "sys"}, {Role: "user", Content: "find x"}},
		Tools:    []Tool{searchTool},
	}, func(s string) { streamed.WriteString(s) })
	if err != nil {
		t.Fatal(err)
	}
	if !got.Stream || got.System != "sys" || got.MaxTokens == 0 || len(got.Tools) != 1 || got.Tools[0].Name != "search" {
		t.Errorf("request: %+v", got)
	}
	if resp.Content != "Checking" || streamed.String() != "Checking" {
		t.Errorf("content %q streamed %q", resp.Content, streamed.String())
	}
	want := []ToolCall{
		{Index: 0, ID: "toolu_1", Type: "function", Function: FunctionCall{Name: "search", Arguments: `{"q":"x"}`}},
		{Index: 1, ID: "toolu_2", Type: "function", Function: FunctionCall{Name: "think", Arguments: `{}`}},
	}
	if !reflect.DeepEqual(resp.ToolCalls, want) {
		t.Errorf("tool calls:\n got %+v\nwant %+v", resp.ToolCalls, want)
	}
}

func TestAnthropicStreamErrorKeepsPartialText(t *testing.T) {
	c := newTestClient(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		writeSSE(w,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}`,
			`{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`,
		)
	})
	resp, err := c.Stream(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}}, nil)
	if err == nil || !strings.Contains(err.Error(), "overloaded_error") {
		t.Fatalf("got %v", err)
	}
	if resp.Content != "partial" {
		t.Errorf("content %q", resp.Content)
	}
}

func TestAnthropicCompleteForcesTool(t *testing.T) {
	c := newTestClient(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		var req anthropicRequest
		json.NewDecoder(r.Body).Decode(&req)
		if choice, _ := req.ToolChoice.(map[string]interface{}); choice["type"] != "tool" || choice["name"] != "search" || req.Stream {
			t.Errorf("tool_choice=%v stream=%v", req.ToolChoice, req.Stream)
		}
		w.Write([]byte(`{"content":[{"type":"text","text":"ok"},{"type":"tool_use","id":"u","name":"search","input":{"q":"x"}}]}`))
	})
	resp, err := c.Complete(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}, Tools: []Tool{searchTool}, ForceTool: "search"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Content != "ok" || len(resp.ToolCalls) != 1 || resp.ToolCalls[0].Function.Arguments != `{"q":"x"}` {
		t.Fatalf("got %+v", resp)
	}
}

func TestStreamStopReasonsAreErrors(t *testing.T) {
	cases := []struct {
		name, provider string
		events         []string
		want           error
	}{
		{"anthropic max_tokens", "anthropic", []string{
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"half"}}`,
			`{"type":"message_delta","delta":{"stop_reason":"max_tokens"}}`,
			`{"type":"message_stop"}`,
		}, ErrTruncated},
		{"anthropic refusal", "anthropic", []string{
			`{"type":"message_delta","delta":{"stop_reason":"refusal"}}`,
			`{"type":"message_stop"}`,
		}, ErrRefused},
		{"anthropic no message_stop", "anthropic", []string{
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"half"}}`,
		}, errIncomplete},
		{"openai length", "openai", []string{
			`{"choices":[{"delta":{"content":"half"}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"length"}]}`,
		}, ErrTruncated},
		{"openai content_filter", "openai", []string{
			`{"choices":[{"delta":{},"finish_reason":"content_filter"}]}`,
		}, ErrRefused},
		{"openai no terminator", "openai", []string{
			`{"choices":[{"delta":{"content":"half"}}]}`,
		}, errIncomplete},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(t, tc.provider, func(w http.ResponseWriter, r *http.Request) {
				writeSSE(w, tc.events...)
			})
			_, err := c.Stream(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}}, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

// A clean finish without [DONE] is still a finished reply.
func TestOpenAIFinishReasonEndsStream(t *testing.T) {
	c := newTestClient(t, "gemini", func(w http.ResponseWriter, r *http.Request) {
		writeSSE(w, `{"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}`)
	})
	resp, err := c.Stream(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}}, nil)
	if err != nil || resp.Content != "ok" {
		t.Fatalf("got %q, %v", resp.Content, err)
	}
}

func TestCompleteStopReasonsAreErrors(t *testing.T) {
	c := newTestClient(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"content":[{"type":"tool_use","id":"u","name":"search","input":{}}],"stop_reason":"max_tokens"}`))
	})
	if _, err := c.Complete(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}}); !errors.Is(err, ErrTruncated) {
		t.Fatalf("anthropic: got %v", err)
	}
	c = newTestClient(t, "openai", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"choices":[{"message":{"content":""},"finish_reason":"content_filter"}]}`))
	})
	if _, err := c.Complete(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}}); !errors.Is(err, ErrRefused) {
		t.Fatalf("openai: got %v", err)
	}
}

func TestOpenAIStreamErrorChunkKeepsPartialText(t *testing.T) {
	c := newTestClient(t, "openai", func(w http.ResponseWriter, r *http.Request) {
		writeSSE(w,
			`{"choices":[{"delta":{"content":"partial"}}]}`,
			`{"error":{"message":"Rate limit reached","type":"rate_limit_error"}}`,
		)
	})
	resp, err := c.Stream(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}}, nil)
	if err == nil || !strings.Contains(err.Error(), "Rate limit reached") {
		t.Fatalf("got %v", err)
	}
	if resp.Content != "partial" {
		t.Errorf("content %q", resp.Content)
	}
}

// A stream that goes quiet is abandoned instead of holding the chat open.
func TestStalledStreamIsAbandoned(t *testing.T) {
	c := newTestClient(t, "openai", func(w http.ResponseWriter, r *http.Request) {
		writeSSE(w, `{"choices":[{"delta":{"content":"partial"}}]}`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	c.idleTimeout = 50 * time.Millisecond
	resp, err := c.Stream(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}}, nil)
	if !errors.Is(err, errStalled) {
		t.Fatalf("got %v, want errStalled", err)
	}
	if resp.Content != "partial" {
		t.Errorf("content %q", resp.Content)
	}
}

// Following a redirect would hand the key to whichever host it points at.
func TestRedirectsAreNotFollowed(t *testing.T) {
	var leaked atomic.Bool
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked.Store(true)
	}))
	defer elsewhere.Close()
	c := newTestClient(t, "anthropic", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+r.URL.Path, http.StatusTemporaryRedirect)
	})
	if _, err := c.Complete(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}}); err == nil {
		t.Fatal("redirect treated as success")
	}
	if leaked.Load() {
		t.Fatal("request, and its API key, followed the redirect")
	}
}

// Users see the provider's message, never the raw body around it.
func TestErrorBodiesStayOutOfErrors(t *testing.T) {
	cases := []struct{ body, want, hidden string }{
		{`{"error":{"message":"model not found"},"request_id":"req-internal-42"}`, "AI provider error 404: model not found", "req-internal-42"},
		{`[{"error":{"code":404,"message":"models/x is not found"}}]`, "AI provider error 404: models/x is not found", "code"},
		{`<html>gateway 10.0.0.7</html>`, "AI provider error 404", "10.0.0.7"},
	}
	for _, tc := range cases {
		c := newTestClient(t, "openai", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(tc.body))
		})
		_, err := c.Complete(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}})
		if err == nil || err.Error() != tc.want || strings.Contains(err.Error(), tc.hidden) {
			t.Errorf("body %s: got %v, want %q", tc.body, err, tc.want)
		}
	}
}

func TestUnreachableProviderHidesEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	c, _ := New(Config{Provider: "openai", APIKey: "k", Model: "m", BaseURL: url})
	_, err := c.Complete(context.Background(), Request{Messages: []Message{{Role: "user", Content: "x"}}})
	if err == nil || strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("got %v", err)
	}
}
