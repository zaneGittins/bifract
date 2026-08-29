package chat

import (
	"encoding/json"
	"testing"
)

// answeredCalls reports which tool calls in a rebuilt history have a result.
func assertEveryToolCallIsAnswered(t *testing.T, msgs []llmMessage) {
	t.Helper()
	answered := map[string]bool{}
	for _, msg := range msgs {
		if msg.Role == "tool" {
			answered[msg.ToolCallID] = true
		}
	}
	for _, msg := range msgs {
		if msg.Role != "assistant" || msg.ToolCalls == nil {
			continue
		}
		var calls []llmToolCall
		if err := json.Unmarshal(msg.ToolCalls, &calls); err != nil {
			t.Fatal(err)
		}
		for _, call := range calls {
			if !answered[call.ID] {
				t.Errorf("tool_call %s (%s) has no tool result; the provider rejects the whole turn",
					call.ID, call.Function.Name)
			}
		}
	}
}

// A turn the user resumes is rebuilt from the database, so a call that saved no
// result of its own leaves the assistant turn unanswerable.
func TestResumedHistoryAnswersEveryToolCall(t *testing.T) {
	history := []*Message{
		{Role: "user", Content: "add a note about this host"},
		{Role: "assistant", Content: "", ToolCalls: json.RawMessage(`[
			{"id":"c1","type":"function","function":{"name":"think","arguments":"{\"reasoning\":\"plan\"}"}},
			{"id":"c2","type":"function","function":{"name":"add_comment","arguments":"{\"log_id\":\"l1\",\"text\":\"x\"}"}}
		]`)},
		{Role: "tool", Content: `{"id":"k1"}`, ToolResults: json.RawMessage(`[{"tool_call_id":"c2","tool_name":"add_comment"}]`)},
	}

	m := &Manager{}
	assertEveryToolCallIsAnswered(t, m.buildLLMMessages("system", m.trimHistory(history)))
}

// A confirmation the user never answered leaves one call open beside answered
// siblings. Only the open one may be dropped: dropping the whole set would take
// the assistant's own reasoning with it.
func TestAnUnansweredConfirmationDropsOnlyItsOwnCall(t *testing.T) {
	history := []*Message{
		{Role: "user", Content: "note it and alert on it"},
		{Role: "assistant", ToolCalls: json.RawMessage(`[
			{"id":"c1","type":"function","function":{"name":"think","arguments":"{}"}},
			{"id":"c2","type":"function","function":{"name":"create_alert","arguments":"{}"}}
		]`)},
		{Role: "tool", Content: `{"ok":true}`, ToolResults: json.RawMessage(`[{"tool_call_id":"c1","tool_name":"think"}]`)},
		{Role: "user", Content: "actually never mind"},
	}

	m := &Manager{}
	msgs := m.buildLLMMessages("system", m.trimHistory(history))
	assertEveryToolCallIsAnswered(t, msgs)

	var kept []llmToolCall
	for _, msg := range msgs {
		if msg.Role == "assistant" && msg.ToolCalls != nil {
			if err := json.Unmarshal(msg.ToolCalls, &kept); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(kept) != 1 || kept[0].ID != "c1" {
		t.Errorf("kept %v, want only the answered call", kept)
	}
}
