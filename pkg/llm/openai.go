package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type openAIRequest struct {
	Model      string      `json:"model"`
	Messages   []Message   `json:"messages"`
	Tools      []Tool      `json:"tools,omitempty"`
	ToolChoice interface{} `json:"tool_choice,omitempty"`
	Stream     bool        `json:"stream,omitempty"`
}

type openAIMessage struct {
	Content   *string    `json:"content"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
}

type openAIChoice struct {
	Message      openAIMessage `json:"message"`
	Delta        openAIMessage `json:"delta"`
	FinishReason string        `json:"finish_reason"`
}

type openAIError struct {
	Message string `json:"message"`
}

func (c *Client) openAI(ctx context.Context, req Request, onText func(string)) (Response, error) {
	// Stored tool results carry the tool's name, which the API does not accept there.
	msgs := make([]Message, len(req.Messages))
	for i, m := range req.Messages {
		if m.Role == "tool" {
			m.Name = ""
		}
		msgs[i] = m
	}
	body := openAIRequest{Model: c.model, Messages: msgs, Tools: req.Tools, Stream: onText != nil}
	if req.ForceTool != "" {
		body.ToolChoice = map[string]interface{}{"type": "function", "function": map[string]string{"name": req.ForceTool}}
	} else if len(req.Tools) > 0 {
		body.ToolChoice = "auto"
	}
	url := c.baseURL + "/chat/completions"
	headers := map[string]string{"Authorization": "Bearer " + c.apiKey}

	if onText == nil {
		resp, err := c.post(ctx, url, body, headers)
		if err != nil {
			return Response{}, err
		}
		defer resp.Body.Close()
		var parsed struct {
			Choices []openAIChoice `json:"choices"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
			return Response{}, fmt.Errorf("decode AI response: %w", err)
		}
		if len(parsed.Choices) == 0 {
			return Response{}, fmt.Errorf("AI provider returned an empty answer")
		}
		choice := parsed.Choices[0]
		out := Response{ToolCalls: choice.Message.ToolCalls}
		if choice.Message.Content != nil {
			out.Content = *choice.Message.Content
		}
		return out, stopError(choice.FinishReason)
	}

	var text strings.Builder
	var calls []*ToolCall
	var args []*strings.Builder
	byIndex := map[int]int{}

	err := c.streamEvents(ctx, url, body, headers, func(data string) (bool, error) {
		if data == "[DONE]" {
			return true, nil
		}
		var chunk struct {
			Choices []openAIChoice `json:"choices"`
			Error   *openAIError   `json:"error"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return false, nil
		}
		if chunk.Error != nil {
			return true, fmt.Errorf("AI provider error: %s", clip(chunk.Error.Message, 300))
		}
		for _, choice := range chunk.Choices {
			if d := choice.Delta.Content; d != nil && *d != "" {
				text.WriteString(*d)
				onText(*d)
			}
			for _, tc := range choice.Delta.ToolCalls {
				// Some OpenAI-compatible servers send each call whole, all at
				// index 0, so a new id also starts a new call.
				pos, seen := byIndex[tc.Index]
				if seen && tc.ID != "" && calls[pos].ID != "" && tc.ID != calls[pos].ID {
					seen = false
				}
				if !seen {
					pos = len(calls)
					byIndex[tc.Index] = pos
					calls = append(calls, &ToolCall{Index: pos, Type: "function"})
					args = append(args, &strings.Builder{})
				}
				if tc.ID != "" {
					calls[pos].ID = tc.ID
				}
				if tc.Function.Name != "" {
					calls[pos].Function.Name = tc.Function.Name
				}
				args[pos].WriteString(tc.Function.Arguments)
			}
			// The finish reason ends the reply; [DONE] may follow, or not.
			if choice.FinishReason != "" {
				return true, stopError(choice.FinishReason)
			}
		}
		return false, nil
	})
	out := Response{Content: text.String()}
	if err != nil {
		return out, err
	}
	for i, tc := range calls {
		tc.Function.Arguments = args[i].String()
		out.ToolCalls = append(out.ToolCalls, *tc)
	}
	return out, nil
}
