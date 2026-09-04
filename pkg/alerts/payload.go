package alerts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"text/template"
	"time"
)

// Body modes control how a webhook request body is built.
const (
	// BodyModeEnvelope sends the built-in WebhookPayload JSON object.
	BodyModeEnvelope = "envelope"
	// BodyModeTemplate renders a user-supplied template, letting the action
	// match a destination's wire format (Splunk HEC events, Slack blocks, NDJSON).
	BodyModeTemplate = "template"
)

const (
	defaultContentType = "application/json"
	// maxRenderedBody bounds template output so a range over a large result
	// set cannot balloon into an unbounded allocation.
	maxRenderedBody = 8 << 20
	// maxTemplateSize bounds the template source itself.
	maxTemplateSize = 64 << 10
)

var errBodyTooLarge = fmt.Errorf("rendered body exceeds %d bytes", maxRenderedBody)

// TemplateContext is the data a body template executes against. Field names are
// the Go-side equivalents of the envelope JSON keys.
type TemplateContext struct {
	AlertName    string
	OriginalName string
	AlertID      string
	Description  string
	Severity     string
	Labels       []string
	TriggeredAt  time.Time
	QueryString  string
	MatchCount   int
	AlertLink    string
	Results      []map[string]interface{}
}

// NormalizeBodyMode maps stored/legacy values onto a known mode.
func NormalizeBodyMode(mode string) string {
	if strings.ToLower(strings.TrimSpace(mode)) == BodyModeTemplate {
		return BodyModeTemplate
	}
	return BodyModeEnvelope
}

// templateFuncs are the helpers available inside a body template. The set is
// deliberately small: enough to build any JSON shape, with nothing that reaches
// outside the payload.
func templateFuncs() template.FuncMap {
	return template.FuncMap{
		// toJSON marshals any value, and is how a whole result row is embedded.
		"toJSON": func(v interface{}) (string, error) {
			b, err := json.Marshal(v)
			if err != nil {
				return "", err
			}
			return string(b), nil
		},
		// field reads one key from a result row. Log field names contain dots and
		// dashes, so they cannot be reached with the template's .Name syntax.
		"field": func(m map[string]interface{}, key string) interface{} {
			if m == nil {
				return nil
			}
			return m[key]
		},
		"unixSeconds": func(t time.Time) int64 { return t.Unix() },
		"unixMillis":  func(t time.Time) int64 { return t.UnixMilli() },
		"rfc3339":     func(t time.Time) string { return t.UTC().Format(time.RFC3339) },
		"join":        func(sep string, v []string) string { return strings.Join(v, sep) },
		"lower":       strings.ToLower,
		"upper":       strings.ToUpper,
		"default": func(fallback, v interface{}) interface{} {
			if v == nil {
				return fallback
			}
			if s, ok := v.(string); ok && s == "" {
				return fallback
			}
			return v
		},
	}
}

// ParseBodyTemplate compiles a body template, returning a user-facing error on
// bad syntax. Callers validate at save time so a broken template cannot reach
// the alert engine.
func ParseBodyTemplate(src string) (*template.Template, error) {
	if strings.TrimSpace(src) == "" {
		return nil, errors.New("body template is required when body mode is 'template'")
	}
	if len(src) > maxTemplateSize {
		return nil, fmt.Errorf("body template exceeds %d bytes", maxTemplateSize)
	}
	// text/template, not html/template: the output is JSON or line protocol, and
	// HTML escaping would corrupt it.
	tmpl, err := template.New("webhook_body").Funcs(templateFuncs()).Parse(src)
	if err != nil {
		return nil, fmt.Errorf("template syntax error: %w", err)
	}
	return tmpl, nil
}

// ValidateWebhookBody checks a webhook's body configuration without sending.
func ValidateWebhookBody(mode, src string) error {
	if NormalizeBodyMode(mode) != BodyModeTemplate {
		return nil
	}
	_, err := ParseBodyTemplate(src)
	return err
}

// boundedBuffer fails the render once output passes the cap, rather than
// letting a template allocate without limit.
type boundedBuffer struct {
	buf bytes.Buffer
	max int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.buf.Len()+len(p) > b.max {
		return 0, errBodyTooLarge
	}
	return b.buf.Write(p)
}

// templateContext projects a payload into the data a template sees.
func templateContext(payload WebhookPayload) TemplateContext {
	results := payload.Results
	if results == nil {
		results = []map[string]interface{}{}
	}
	return TemplateContext{
		AlertName:    payload.AlertName,
		OriginalName: payload.OriginalName,
		AlertID:      payload.AlertID,
		Description:  payload.Description,
		Severity:     payload.Severity,
		Labels:       payload.Labels,
		TriggeredAt:  payload.TriggeredAt,
		QueryString:  payload.QueryString,
		MatchCount:   payload.MatchCount,
		AlertLink:    payload.AlertLink,
		Results:      results,
	}
}

// RenderWebhookBody builds the request body and content type for an action.
// Envelope mode marshals the built-in payload; template mode renders the
// configured template against the same data.
func RenderWebhookBody(webhook WebhookAction, payload WebhookPayload) ([]byte, string, error) {
	contentType := strings.TrimSpace(webhook.ContentType)
	if contentType == "" {
		contentType = defaultContentType
	}

	if NormalizeBodyMode(webhook.BodyMode) != BodyModeTemplate {
		body, err := json.Marshal(payload)
		if err != nil {
			return nil, contentType, fmt.Errorf("failed to marshal payload: %w", err)
		}
		return body, contentType, nil
	}

	tmpl, err := ParseBodyTemplate(webhook.BodyTemplate)
	if err != nil {
		return nil, contentType, err
	}

	out := &boundedBuffer{max: maxRenderedBody}
	if err := tmpl.Execute(out, templateContext(payload)); err != nil {
		if errors.Is(err, errBodyTooLarge) {
			return nil, contentType, errBodyTooLarge
		}
		return nil, contentType, fmt.Errorf("template render error: %w", err)
	}
	return out.buf.Bytes(), contentType, nil
}
