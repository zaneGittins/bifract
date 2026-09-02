package chat

import (
	"encoding/json"
	"fmt"
	"strings"
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

// assertNoOrphanedToolResults is the mirror of the check above: a tool result
// whose assistant turn was trimmed away has no call to answer, and the provider
// rejects it just as readily.
func assertNoOrphanedToolResults(t *testing.T, msgs []llmMessage) {
	t.Helper()
	offered := map[string]bool{}
	for _, msg := range msgs {
		if msg.Role != "assistant" || msg.ToolCalls == nil {
			continue
		}
		var calls []llmToolCall
		if err := json.Unmarshal(msg.ToolCalls, &calls); err != nil {
			t.Fatal(err)
		}
		for _, call := range calls {
			offered[call.ID] = true
		}
	}
	for _, msg := range msgs {
		if msg.Role == "tool" && !offered[msg.ToolCallID] {
			t.Errorf("tool result %s answers no tool_call in the rebuilt history", msg.ToolCallID)
		}
	}
}

// toolHeavyTurn is what one question really writes: the agent loop runs a tool
// call per round, and each round stores an assistant row and a tool row.
func toolHeavyTurn(n, rounds int) []*Message {
	turn := []*Message{{Role: "user", Content: fmt.Sprintf("question %d", n)}}
	for r := 0; r < rounds; r++ {
		id := fmt.Sprintf("t%d_%d", n, r)
		turn = append(turn,
			&Message{Role: "assistant", ToolCalls: json.RawMessage(fmt.Sprintf(
				`[{"id":%q,"type":"function","function":{"name":"run_query","arguments":"{}"}}]`, id))},
			&Message{Role: "tool", Content: `{"count":3}`, ToolResults: json.RawMessage(fmt.Sprintf(
				`[{"tool_call_id":%q,"tool_name":"run_query"}]`, id))})
	}
	return append(turn, &Message{Role: "assistant", Content: fmt.Sprintf("answer %d", n)})
}

func rebuild(history []*Message) []llmMessage {
	m := &Manager{}
	return m.buildLLMMessages("system", m.trimHistory(history))
}

// The history budget is counted in turns because a single tool-heavy answer
// writes more rows than a whole conversation of short ones. Counting rows let
// one such answer push every earlier question out of the model's context.
func TestToolHeavyTurnsDoNotEvictEarlierQuestions(t *testing.T) {
	for _, rounds := range []int{1, 5, 15} {
		var history []*Message
		for i := 1; i <= 6; i++ {
			history = append(history, toolHeavyTurn(i, rounds)...)
		}

		msgs := rebuild(history)
		assertEveryToolCallIsAnswered(t, msgs)
		assertNoOrphanedToolResults(t, msgs)
		assertNoSilentAssistantTurn(t, msgs)
		assertNoAdjacentUserTurns(t, msgs)

		var asked, answered int
		for _, msg := range msgs {
			switch text, _ := msg.Content.(string); {
			case msg.Role == "user":
				asked++
			case msg.Role == "assistant" && strings.HasPrefix(text, "answer "):
				answered++
			}
		}
		if asked != 6 || answered != 6 {
			t.Errorf("%d tool rounds per turn: kept %d questions and %d answers, want 6 of each",
				rounds, asked, answered)
		}
	}
}

// Only the recent turns need their tool traffic. Older ones keep what was asked
// and what was concluded, which is what the user is referring back to.
func TestOlderTurnsKeepTheAnswerAndDropTheToolTraffic(t *testing.T) {
	var history []*Message
	for i := 1; i <= verbatimTurns+2; i++ {
		history = append(history, toolHeavyTurn(i, 3)...)
	}

	msgs := rebuild(history)
	for _, msg := range msgs {
		if msg.Role != "tool" {
			continue
		}
		if strings.HasPrefix(msg.ToolCallID, "t1_") || strings.HasPrefix(msg.ToolCallID, "t2_") {
			t.Errorf("tool result %s from an old turn survived the trim", msg.ToolCallID)
		}
	}
	if want := "answer 1"; msgs[1].Role != "user" || msgs[2].Content != want {
		t.Errorf("oldest turn rebuilt as %+v, want its question followed by %q", msgs[1:3], want)
	}
}

// Turns past the cap fall off the front, oldest first.
func TestHistoryKeepsOnlyTheMostRecentTurns(t *testing.T) {
	var history []*Message
	for i := 1; i <= maxHistoryTurns+4; i++ {
		history = append(history, toolHeavyTurn(i, 1)...)
	}

	msgs := rebuild(history)
	assertNoOrphanedToolResults(t, msgs)
	if got, want := msgs[1].Content, "question 5"; got != want {
		t.Errorf("oldest kept question is %q, want %q", got, want)
	}
}

// A single huge turn must not be able to spend the whole context window.
func TestHistoryFallsBackToTheCharacterBudget(t *testing.T) {
	var history []*Message
	for i := 1; i <= 6; i++ {
		turn := toolHeavyTurn(i, 1)
		turn[len(turn)-1].Content = strings.Repeat("x", maxHistoryChars/3)
		history = append(history, turn...)
	}

	if chars := historyChars((&Manager{}).trimHistory(history)); chars > maxHistoryChars {
		t.Errorf("kept %d characters, want at most %d", chars, maxHistoryChars)
	}
}

// Anthropic rejects an assistant message whose content is empty, so a rebuilt
// history must never contain one.
func assertNoSilentAssistantTurn(t *testing.T, msgs []llmMessage) {
	t.Helper()
	for _, msg := range msgs {
		if msg.Role != "assistant" || msg.ToolCalls != nil {
			continue
		}
		if text, _ := msg.Content.(string); strings.TrimSpace(text) == "" {
			t.Error("an assistant message carries neither text nor a tool call; the provider rejects it")
		}
	}
}

// A turn that concluded nothing contributes only its question. Providers differ
// on whether they merge the two questions that leaves side by side.
func assertNoAdjacentUserTurns(t *testing.T, msgs []llmMessage) {
	t.Helper()
	for i := 1; i < len(msgs); i++ {
		if msgs[i].Role == "user" && msgs[i-1].Role == "user" {
			t.Errorf("messages %d and %d are both user turns", i-1, i)
		}
	}
}

// Offered a write, the user types a new question instead of answering. The
// offer is superseded, so its call is never answered and is stripped; the round
// that made it had no prose of its own, which used to leave {"role":"assistant",
// "content":""} in the history.
func TestASupersededOfferLeavesNoSilentAssistantTurn(t *testing.T) {
	history := []*Message{
		{Role: "user", Content: "comment on that host"},
		{Role: "assistant", Content: "", ToolCalls: json.RawMessage(
			`[{"id":"c1","type":"function","function":{"name":"add_comment","arguments":"{}"}}]`)},
		{Role: "user", Content: "actually, show me the logins"},
	}

	msgs := rebuild(history)
	assertNoSilentAssistantTurn(t, msgs)
	assertNoAdjacentUserTurns(t, msgs)
	assertEveryToolCallIsAnswered(t, msgs)

	// Both questions are still there: the user changing their mind is context.
	joined := fmt.Sprint(msgs[1].Content)
	for _, want := range []string{"comment on that host", "show me the logins"} {
		if !strings.Contains(joined, want) {
			t.Errorf("history lost %q", want)
		}
	}
}

// An old turn interrupted before it concluded anything summarizes to its
// question alone, which lands next to the following question.
func TestAnOldTurnThatConcludedNothingKeepsItsQuestion(t *testing.T) {
	history := []*Message{
		{Role: "user", Content: "question 1"},
		{Role: "assistant", Content: "", ToolCalls: json.RawMessage(
			`[{"id":"a1","type":"function","function":{"name":"query_logs","arguments":"{}"}}]`)},
		{Role: "tool", Content: `{"count":1}`, ToolResults: json.RawMessage(`[{"tool_call_id":"a1","tool_name":"query_logs"}]`)},
	}
	for i := 2; i <= verbatimTurns+2; i++ {
		history = append(history,
			&Message{Role: "user", Content: fmt.Sprintf("question %d", i)},
			&Message{Role: "assistant", Content: fmt.Sprintf("answer %d", i)})
	}

	msgs := rebuild(history)
	assertNoSilentAssistantTurn(t, msgs)
	assertNoAdjacentUserTurns(t, msgs)
	if !strings.Contains(fmt.Sprint(msgs[1].Content), "question 1") {
		t.Errorf("the interrupted question was dropped: %v", msgs[1].Content)
	}
}
