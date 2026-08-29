package aitools

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// MaxErrorBody caps how much of a failed response is quoted back. An HTML error
// page from a proxy in front of Bifract would otherwise fill the model's context.
const MaxErrorBody = 400

// Decode turns a response into either the payload the tools want or an
// explanation a model can act on. Shared by both clients, so an error reads the
// same whether the call crossed the network or not.
func Decode(method, path string, status int, payload []byte) (any, error) {
	switch {
	case status == http.StatusUnauthorized:
		// Neutral on purpose: the same text reaches an MCP client holding an API
		// key and a chat user holding a session.
		return nil, errors.New("not authenticated: the credential was rejected as missing, invalid, or expired")
	case status == http.StatusForbidden:
		return nil, fmt.Errorf("forbidden: %s. This account's permissions do not cover it", detail(payload))
	case status == http.StatusNotFound:
		return nil, fmt.Errorf("%s %s: not found. Check the id, and that it exists in this fractal", method, path)
	case status >= 400:
		return nil, fmt.Errorf("%s %s failed (%d): %s", method, path, status, detail(payload))
	}

	if len(bytes.TrimSpace(payload)) == 0 {
		return map[string]any{}, nil
	}
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, fmt.Errorf("%s %s returned a non-JSON response: %s", method, path, truncate(string(payload)))
	}

	// Belt and braces: failures answer 4xx now, but an endpoint that has not been
	// migrated to the envelope could still report one in the body.
	if envelope, ok := decoded.(map[string]any); ok {
		if success, present := envelope["success"].(bool); present && !success {
			if message := failure(envelope); message != "" {
				return nil, errors.New(message)
			}
			return nil, fmt.Errorf("%s %s was rejected by the server", method, path)
		}
	}
	return Unwrap(decoded), nil
}

// Unwrap drops the {"success", "data"} envelope, which spends the model's context
// teaching it nothing. Responses with their own shape pass through untouched.
//
// A truncated page keeps its counts: a model shown 100 of 4,000 alerts and told
// nothing will reason as though it saw all of them.
func Unwrap(payload any) any {
	envelope, ok := payload.(map[string]any)
	if !ok {
		return payload
	}
	if _, wrapped := envelope["success"]; !wrapped {
		return payload
	}

	data, present := envelope["data"]
	if !present {
		rest := make(map[string]any, len(envelope))
		for k, v := range envelope {
			if k != "success" {
				rest[k] = v
			}
		}
		return rest
	}

	items, isList := data.([]any)
	page, paged := envelope["page"].(map[string]any)
	if isList && paged {
		total, offset := number(page["total"]), number(page["offset"])
		if total > offset+len(items) {
			return map[string]any{
				"items":       items,
				"showing":     fmt.Sprintf("%d-%d of %d", offset+1, offset+len(items), total),
				"more":        true,
				"next_offset": offset + len(items),
				"note":        "This is one page. Ask for the next with offset, or narrow the request.",
			}
		}
	}
	return data
}

// failure is the message an error envelope carries, with its machine-readable
// code, which is what a caller should branch on.
func failure(envelope map[string]any) string {
	message, _ := envelope["error"].(string)
	if message == "" {
		message, _ = envelope["message"].(string)
	}
	code, _ := envelope["code"].(string)
	if code != "" && message != "" {
		return message + " [" + code + "]"
	}
	return message
}

// detail pulls the message out of a Bifract error body. Handlers answer with
// either an envelope or bare text, so try both.
func detail(payload []byte) string {
	text := strings.TrimSpace(string(payload))
	var decoded any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		return truncate(text)
	}
	envelope, ok := decoded.(map[string]any)
	if !ok {
		return truncate(text)
	}
	if message := failure(envelope); message != "" {
		return truncate(message)
	}
	for _, key := range []string{"message", "detail"} {
		if value, _ := envelope[key].(string); value != "" {
			return truncate(value)
		}
	}
	return truncate(text)
}

func truncate(s string) string {
	if len(s) <= MaxErrorBody {
		return s
	}
	return s[:MaxErrorBody]
}

// number reads a JSON number, which always decodes as float64, as an int.
func number(v any) int {
	f, _ := v.(float64)
	return int(f)
}
