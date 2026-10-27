// Package llm talks to hosted model providers directly. Requests and replies use
// the OpenAI chat-completions shape, which is what conversations are stored in;
// providers with another wire format translate at the edge.
package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	ProviderAnthropic = "anthropic"
	ProviderOpenAI    = "openai"
	ProviderGemini    = "gemini"
)

// DefaultAnthropicModel keeps installs that never chose a model on the one they had.
const DefaultAnthropicModel = "claude-haiku-4-5-20251001"

var defaultBaseURLs = map[string]string{
	ProviderAnthropic: "https://api.anthropic.com",
	ProviderOpenAI:    "https://api.openai.com/v1",
	// Gemini serves the OpenAI wire format, so it shares that client.
	ProviderGemini: "https://generativelanguage.googleapis.com/v1beta/openai",
}

// ErrNotConfigured is returned by every call on a client built without a key.
var ErrNotConfigured = errors.New("AI is not configured: set BIFRACT_LLM_API_KEY")

var (
	// ErrTruncated means the reply hit the provider's length limit, so any tool
	// call in it may be incomplete.
	ErrTruncated  = errors.New("the AI reply was cut off at the provider's length limit")
	ErrRefused    = errors.New("the AI provider declined to answer")
	errIncomplete = errors.New("the AI provider closed the stream before the reply finished")
	errStalled    = errors.New("the AI provider stopped responding")
)

// responseTimeout bounds the wait for a response to start and, when streaming,
// the gap between lines. The caller's context bounds the call as a whole.
const responseTimeout = 120 * time.Second

type Message struct {
	Role       string          `json:"role"`
	Content    interface{}     `json:"content"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolCalls  json.RawMessage `json:"tool_calls,omitempty"`
	Name       string          `json:"name,omitempty"`
}

type ToolCall struct {
	Index    int          `json:"index"`
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function FunctionCall `json:"function"`
}

type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type Tool struct {
	Type     string   `json:"type"`
	Function Function `json:"function"`
}

type Function struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Parameters  interface{} `json:"parameters"`
}

type Request struct {
	Messages []Message
	Tools    []Tool
	// ForceTool names the tool the model must call; empty lets it choose.
	ForceTool string
}

type Response struct {
	Content   string
	ToolCalls []ToolCall
}

type Config struct {
	Provider string
	APIKey   string
	Model    string
	// BaseURL overrides the provider endpoint, e.g. for an OpenAI-compatible gateway.
	BaseURL string
}

type Client struct {
	provider    string
	apiKey      string
	model       string
	baseURL     string
	http        *http.Client
	idleTimeout time.Duration
}

// New validates cfg. A client without a key is valid and answers ErrNotConfigured,
// so AI features stay off rather than stopping the server.
func New(cfg Config) (*Client, error) {
	provider := strings.ToLower(strings.TrimSpace(cfg.Provider))
	if provider == "" {
		provider = ProviderAnthropic
	}
	baseURL, ok := defaultBaseURLs[provider]
	if !ok {
		return nil, fmt.Errorf("unknown LLM provider %q (want anthropic, openai or gemini)", cfg.Provider)
	}
	if cfg.BaseURL != "" {
		baseURL = cfg.BaseURL
	}
	model := strings.TrimSpace(cfg.Model)
	if model == "" && provider == ProviderAnthropic {
		model = DefaultAnthropicModel
	}
	if model == "" && cfg.APIKey != "" {
		return nil, fmt.Errorf("BIFRACT_LLM_MODEL is required for provider %s", provider)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = responseTimeout
	return &Client{
		provider: provider,
		apiKey:   cfg.APIKey,
		model:    model,
		baseURL:  strings.TrimRight(baseURL, "/"),
		// No overall timeout: it would also cut off a long streamed reply.
		// Redirects are refused because they would carry the key to another host.
		http: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		idleTimeout: responseTimeout,
	}, nil
}

// Enabled reports whether calls can reach a provider.
func (c *Client) Enabled() bool { return c != nil && c.apiKey != "" }

func (c *Client) Provider() string { return c.provider }
func (c *Client) Model() string    { return c.model }

// Complete returns the whole reply at once.
func (c *Client) Complete(ctx context.Context, req Request) (Response, error) {
	if !c.Enabled() {
		return Response{}, ErrNotConfigured
	}
	if c.provider == ProviderAnthropic {
		return c.anthropic(ctx, req, nil)
	}
	return c.openAI(ctx, req, nil)
}

// Stream calls onText with each piece of text as it arrives. On error the
// response still holds whatever text came before it.
func (c *Client) Stream(ctx context.Context, req Request, onText func(string)) (Response, error) {
	if !c.Enabled() {
		return Response{}, ErrNotConfigured
	}
	if onText == nil {
		onText = func(string) {}
	}
	if c.provider == ProviderAnthropic {
		return c.anthropic(ctx, req, onText)
	}
	return c.openAI(ctx, req, onText)
}

func (c *Client) post(ctx context.Context, url string, body interface{}, headers map[string]string) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		// The transport error names the endpoint, which users should not see.
		log.Printf("[AI] request to %s provider failed: %v", c.provider, err)
		return nil, errors.New("AI provider unreachable")
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		log.Printf("[AI] %s provider returned %d: %s", c.provider, resp.StatusCode, clip(string(raw), 2000))
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("AI provider rejected the API key (%d): check BIFRACT_LLM_API_KEY", resp.StatusCode)
		}
		if msg := providerMessage(raw); msg != "" {
			return nil, fmt.Errorf("AI provider error %d: %s", resp.StatusCode, msg)
		}
		return nil, fmt.Errorf("AI provider error %d", resp.StatusCode)
	}
	return resp, nil
}

// streamEvents posts a streaming request and hands each server-sent event's data
// to onData until it reports the reply done. A stream that goes quiet for
// idleTimeout, or ends before onData is done, is an error.
func (c *Client) streamEvents(ctx context.Context, url string, body interface{}, headers map[string]string, onData func(data string) (done bool, err error)) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	watchdog := time.AfterFunc(c.idleTimeout, func() { cancel(errStalled) })
	defer watchdog.Stop()

	resp, err := c.post(ctx, url, body, headers)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	for scanner.Scan() {
		watchdog.Reset(c.idleTimeout)
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		done, err := onData(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		if err != nil || done {
			return err
		}
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("stream read error: %w", err)
	}
	return errIncomplete
}

// stopError maps a provider's stop reason to an error when the reply did not
// finish on its own terms.
func stopError(reason string) error {
	switch reason {
	case "max_tokens", "length":
		return ErrTruncated
	case "refusal", "content_filter":
		return ErrRefused
	}
	return nil
}

// providerMessage pulls the human-readable message out of a provider error body,
// leaving the rest (request IDs, gateway pages) in the server log only.
func providerMessage(raw []byte) string {
	type body struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	var one body
	if json.Unmarshal(raw, &one) == nil && one.Error.Message != "" {
		return clip(one.Error.Message, 300)
	}
	// Gemini wraps its error in a one-element array.
	var many []body
	if json.Unmarshal(raw, &many) == nil && len(many) > 0 {
		return clip(many[0].Error.Message, 300)
	}
	return ""
}

func clip(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "..."
}

// textOf flattens message content, which is a string for everything Bifract sends.
func textOf(content interface{}) string {
	switch v := content.(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}
