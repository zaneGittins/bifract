package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	anthropicVersion = "2023-06-01"
	// The Messages API requires a ceiling; this one fits every reply Bifract asks for.
	anthropicMaxTokens = 8192
)

type anthropicBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   string          `json:"content,omitempty"`
}

type anthropicMessage struct {
	Role    string           `json:"role"`
	Content []anthropicBlock `json:"content"`
}

type anthropicTool struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	InputSchema interface{} `json:"input_schema"`
}

type anthropicRequest struct {
	Model      string             `json:"model"`
	MaxTokens  int                `json:"max_tokens"`
	System     string             `json:"system,omitempty"`
	Messages   []anthropicMessage `json:"messages"`
	Tools      []anthropicTool    `json:"tools,omitempty"`
	ToolChoice interface{}        `json:"tool_choice,omitempty"`
	Stream     bool               `json:"stream,omitempty"`
}

// toAnthropic translates OpenAI-shaped messages. System messages become the
// system prompt, tool results become user turns, and adjacent turns from the
// same role merge because the API requires them to alternate.
func toAnthropic(msgs []Message) (string, []anthropicMessage) {
	var system []string
	var out []anthropicMessage
	add := func(role string, blocks ...anthropicBlock) {
		if len(blocks) == 0 {
			return
		}
		if n := len(out); n > 0 && out[n-1].Role == role {
			out[n-1].Content = append(out[n-1].Content, blocks...)
			return
		}
		out = append(out, anthropicMessage{Role: role, Content: blocks})
	}
	for _, m := range msgs {
		text := textOf(m.Content)
		switch m.Role {
		case "system":
			if text != "" {
				system = append(system, text)
			}
		case "tool":
			add("user", anthropicBlock{Type: "tool_result", ToolUseID: m.ToolCallID, Content: text})
		case "assistant":
			var blocks []anthropicBlock
			if strings.TrimSpace(text) != "" {
				blocks = append(blocks, anthropicBlock{Type: "text", Text: text})
			}
			var calls []ToolCall
			if len(m.ToolCalls) > 0 {
				_ = json.Unmarshal(m.ToolCalls, &calls)
			}
			for _, tc := range calls {
				input := json.RawMessage(tc.Function.Arguments)
				if !json.Valid(input) {
					input = json.RawMessage("{}")
				}
				blocks = append(blocks, anthropicBlock{Type: "tool_use", ID: tc.ID, Name: tc.Function.Name, Input: input})
			}
			add("assistant", blocks...)
		default:
			if strings.TrimSpace(text) != "" {
				add("user", anthropicBlock{Type: "text", Text: text})
			}
		}
	}
	return strings.Join(system, "\n\n"), out
}

func (c *Client) anthropic(ctx context.Context, req Request, onText func(string)) (Response, error) {
	system, msgs := toAnthropic(req.Messages)
	body := anthropicRequest{
		Model:     c.model,
		MaxTokens: anthropicMaxTokens,
		System:    system,
		Messages:  msgs,
		Stream:    onText != nil,
	}
	for _, t := range req.Tools {
		body.Tools = append(body.Tools, anthropicTool{Name: t.Function.Name, Description: t.Function.Description, InputSchema: t.Function.Parameters})
	}
	if req.ForceTool != "" {
		body.ToolChoice = map[string]string{"type": "tool", "name": req.ForceTool}
	}
	url := c.baseURL + "/v1/messages"
	headers := map[string]string{
		"x-api-key":         c.apiKey,
		"anthropic-version": anthropicVersion,
	}

	var out Response
	addCall := func(id, name, args string) {
		if strings.TrimSpace(args) == "" {
			args = "{}"
		}
		out.ToolCalls = append(out.ToolCalls, ToolCall{
			Index: len(out.ToolCalls), ID: id, Type: "function",
			Function: FunctionCall{Name: name, Arguments: args},
		})
	}

	if onText == nil {
		resp, err := c.post(ctx, url, body, headers)
		if err != nil {
			return Response{}, err
		}
		defer resp.Body.Close()
		var parsed struct {
			Content    []anthropicBlock `json:"content"`
			StopReason string           `json:"stop_reason"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
			return Response{}, fmt.Errorf("decode AI response: %w", err)
		}
		var text strings.Builder
		for _, b := range parsed.Content {
			switch b.Type {
			case "text":
				text.WriteString(b.Text)
			case "tool_use":
				addCall(b.ID, b.Name, string(b.Input))
			}
		}
		out.Content = text.String()
		return out, stopError(parsed.StopReason)
	}

	type pendingCall struct {
		id, name string
		args     strings.Builder
	}
	var text strings.Builder
	var order []int
	calls := map[int]*pendingCall{}
	var stopReason string

	err := c.streamEvents(ctx, url, body, headers, func(data string) (bool, error) {
		var ev struct {
			Type         string         `json:"type"`
			Index        int            `json:"index"`
			ContentBlock anthropicBlock `json:"content_block"`
			Delta        struct {
				Type        string `json:"type"`
				Text        string `json:"text"`
				PartialJSON string `json:"partial_json"`
				StopReason  string `json:"stop_reason"`
			} `json:"delta"`
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return false, nil
		}
		switch ev.Type {
		case "content_block_start":
			if ev.ContentBlock.Type == "tool_use" {
				calls[ev.Index] = &pendingCall{id: ev.ContentBlock.ID, name: ev.ContentBlock.Name}
				order = append(order, ev.Index)
			}
		case "content_block_delta":
			switch ev.Delta.Type {
			case "text_delta":
				if ev.Delta.Text != "" {
					text.WriteString(ev.Delta.Text)
					onText(ev.Delta.Text)
				}
			case "input_json_delta":
				if pc := calls[ev.Index]; pc != nil {
					pc.args.WriteString(ev.Delta.PartialJSON)
				}
			}
		case "message_delta":
			if ev.Delta.StopReason != "" {
				stopReason = ev.Delta.StopReason
			}
		case "message_stop":
			return true, stopError(stopReason)
		case "error":
			return true, fmt.Errorf("AI provider error: %s: %s", ev.Error.Type, clip(ev.Error.Message, 300))
		}
		return false, nil
	})
	out.Content = text.String()
	if err != nil {
		return out, err
	}
	for _, idx := range order {
		pc := calls[idx]
		addCall(pc.id, pc.name, pc.args.String())
	}
	return out, nil
}
