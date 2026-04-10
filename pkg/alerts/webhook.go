package alerts

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// WebhookClient handles HTTP webhook delivery with retry logic and authentication
type WebhookClient struct {
	client  *http.Client
	baseURL string
}

// WebhookAction represents a webhook endpoint configuration
type WebhookAction struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	URL              string            `json:"url"`
	Method           string            `json:"method"`
	Headers          map[string]string `json:"headers"`
	AuthType         string            `json:"auth_type"`      // 'none', 'bearer', 'basic'
	AuthConfig       map[string]string `json:"auth_config"`    // Contains auth-specific config
	TimeoutSecs      int               `json:"timeout_seconds"`
	RetryCount       int               `json:"retry_count"`
	IncludeAlertLink bool              `json:"include_alert_link"`
	Enabled          bool              `json:"enabled"`
}

// WebhookPayload is the JSON structure sent to webhook endpoints
type WebhookPayload struct {
	AlertName    string                   `json:"alert_name"`
	OriginalName string                   `json:"original_name,omitempty"`
	AlertID      string                   `json:"alert_id"`
	Description  string                   `json:"description"`
	Severity     string                   `json:"severity"`
	Labels       []string                 `json:"labels"`
	TriggeredAt  time.Time                `json:"triggered_at"`
	QueryString  string                   `json:"query_string"`
	MatchCount   int                      `json:"match_count"`
	AlertLink    string                   `json:"alert_link,omitempty"`
	Results      []map[string]interface{} `json:"results"`
}

// WebhookResult tracks the outcome of a webhook delivery attempt
type WebhookResult struct {
	WebhookID    string        `json:"webhook_id"`
	WebhookName  string        `json:"webhook_name"`
	Success      bool          `json:"success"`
	StatusCode   int           `json:"status_code"`
	Error        string        `json:"error,omitempty"`
	Duration     time.Duration `json:"duration"`
	AttemptCount int           `json:"attempt_count"`
	Timestamp    time.Time     `json:"timestamp"`
}

// NewWebhookClient creates a new webhook client with default settings
func NewWebhookClient(baseURL string) *WebhookClient {
	return &WebhookClient{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL: strings.TrimRight(baseURL, "/"),
	}
}

// encodeQueryForShareLink mirrors JavaScript encodeURIComponent then base64-encodes
// the result. The frontend decodes via decodeURIComponent(atob(...)), which only
// understands %XX sequences, so url.QueryEscape (which encodes spaces as '+') would
// leave stray '+' in the decoded query. Replacing '+' with '%20' fixes the mismatch.
func encodeQueryForShareLink(query string) string {
	escaped := strings.ReplaceAll(url.QueryEscape(query), "+", "%20")
	return base64.StdEncoding.EncodeToString([]byte(escaped))
}

// shareLinkWindow is the half-width of the time window centered on the alert
// trigger time. A ±1h window gives users context before and after the event.
const shareLinkWindow = time.Hour

// buildShareLink constructs a share link URL for the alert query results.
// It handles both fractal-scoped alerts (sets 'f') and prism-scoped alerts
// (sets 'p'). The time range is a ±1h custom window centered on triggeredAt
// so the link remains valid regardless of when the recipient clicks it.
// Returns an empty string if baseURL is empty or neither scope is set.
func buildShareLink(baseURL string, alert *Alert, triggeredAt time.Time) string {
	if baseURL == "" {
		return ""
	}
	if alert.FractalID == "" && alert.PrismID == "" {
		return ""
	}
	params := url.Values{}
	params.Set("q", encodeQueryForShareLink(alert.QueryString))

	if triggeredAt.IsZero() {
		// Fallback: no trigger time available, use a relative window.
		params.Set("tr", "1h")
	} else {
		// Custom ±1h window centered on the trigger time. Format matches
		// what the frontend produces via JavaScript Date.toISOString()
		// (RFC 3339 with millisecond precision and 'Z' suffix).
		start := triggeredAt.Add(-shareLinkWindow).UTC()
		end := triggeredAt.Add(shareLinkWindow).UTC()
		params.Set("tr", "custom")
		params.Set("ts", start.Format("2006-01-02T15:04:05.000Z"))
		params.Set("te", end.Format("2006-01-02T15:04:05.000Z"))
	}

	if alert.PrismID != "" {
		params.Set("p", alert.PrismID)
	} else {
		params.Set("f", alert.FractalID)
	}
	return baseURL + "/?" + params.Encode()
}

// buildAlertLink constructs a share link URL for the alert query results
func (wc *WebhookClient) buildAlertLink(alert *Alert, triggeredAt time.Time) string {
	return buildShareLink(wc.baseURL, alert, triggeredAt)
}

// Send delivers a webhook payload to the configured endpoint with retry logic.
// resolvedName is the alert name with any {{field}} templates replaced.
func (wc *WebhookClient) Send(ctx context.Context, webhook WebhookAction, alert *Alert, resolvedName string, results []map[string]interface{}) WebhookResult {
	triggeredAt := time.Now()
	payload := WebhookPayload{
		AlertName:   resolvedName,
		AlertID:     alert.ID,
		Description: alert.Description,
		Severity:    alert.Severity,
		Labels:      alert.Labels,
		TriggeredAt: triggeredAt,
		QueryString: alert.QueryString,
		MatchCount:  len(results),
		Results:     results,
	}
	if resolvedName != alert.Name {
		payload.OriginalName = alert.Name
	}

	if webhook.IncludeAlertLink && wc.baseURL != "" && (alert.FractalID != "" || alert.PrismID != "") {
		payload.AlertLink = wc.buildAlertLink(alert, triggeredAt)
	}

	result := WebhookResult{
		WebhookID:   webhook.ID,
		WebhookName: webhook.Name,
		Timestamp:   time.Now(),
	}

	start := time.Now()
	defer func() {
		result.Duration = time.Since(start)
	}()

	// Retry logic with exponential backoff
	maxRetries := webhook.RetryCount
	if maxRetries < 1 {
		maxRetries = 1
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		result.AttemptCount = attempt

		if err := wc.sendAttempt(ctx, webhook, payload, &result); err == nil {
			result.Success = true
			return result
		}

		// Don't sleep after the last attempt
		if attempt < maxRetries {
			// Exponential backoff: 1s, 2s, 4s, 8s...
			backoffDuration := time.Duration(1<<uint(attempt-1)) * time.Second
			select {
			case <-ctx.Done():
				result.Error = "context cancelled during retry backoff"
				return result
			case <-time.After(backoffDuration):
				// Continue to next retry
			}
		}
	}

	return result
}

// sendAttempt makes a single webhook delivery attempt
func (wc *WebhookClient) sendAttempt(ctx context.Context, webhook WebhookAction, payload WebhookPayload, result *WebhookResult) error {
	// Validate webhook URL
	if err := wc.validateURL(webhook.URL); err != nil {
		result.Error = fmt.Sprintf("invalid webhook URL: %v", err)
		result.StatusCode = 0
		return fmt.Errorf("invalid URL: %w", err)
	}

	// Marshal payload to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		result.Error = fmt.Sprintf("failed to marshal payload: %v", err)
		result.StatusCode = 0
		return fmt.Errorf("payload marshal error: %w", err)
	}

	// Create HTTP request
	method := strings.ToUpper(webhook.Method)
	if method == "" {
		method = "POST"
	}

	req, err := http.NewRequestWithContext(ctx, method, webhook.URL, bytes.NewReader(payloadBytes))
	if err != nil {
		result.Error = fmt.Sprintf("failed to create request: %v", err)
		result.StatusCode = 0
		return fmt.Errorf("request creation error: %w", err)
	}

	// Set default content type
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Bifract-Alert-System/1.0")

	// Add custom headers
	for key, value := range webhook.Headers {
		req.Header.Set(key, value)
	}

	// Apply authentication
	if err := wc.applyAuth(req, webhook); err != nil {
		result.Error = fmt.Sprintf("authentication error: %v", err)
		result.StatusCode = 0
		return fmt.Errorf("auth error: %w", err)
	}

	// Set custom timeout if specified
	client := wc.client
	if webhook.TimeoutSecs > 0 && webhook.TimeoutSecs != 30 {
		client = &http.Client{
			Timeout: time.Duration(webhook.TimeoutSecs) * time.Second,
		}
	}

	// Execute request
	resp, err := client.Do(req)
	if err != nil {
		result.Error = fmt.Sprintf("request failed: %v", err)
		result.StatusCode = 0
		return fmt.Errorf("request execution error: %w", err)
	}
	defer resp.Body.Close()

	result.StatusCode = resp.StatusCode

	// Check if response indicates success (2xx status codes)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil // Success
	}

	// Non-2xx status code is considered an error
	result.Error = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, resp.Status)
	return fmt.Errorf("HTTP error: %s", resp.Status)
}

// validateURL performs basic validation on webhook URLs.
func (wc *WebhookClient) validateURL(webhookURL string) error {
	parsedURL, err := url.Parse(webhookURL)
	if err != nil {
		return fmt.Errorf("invalid URL format: %w", err)
	}

	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("unsupported URL scheme: %s (only http/https allowed)", parsedURL.Scheme)
	}

	if parsedURL.Host == "" {
		return fmt.Errorf("URL must have a host")
	}

	return nil
}

// applyAuth applies the configured authentication method to the HTTP request
func (wc *WebhookClient) applyAuth(req *http.Request, webhook WebhookAction) error {
	switch strings.ToLower(webhook.AuthType) {
	case "none", "":
		// No authentication required
		return nil

	case "bearer":
		token, exists := webhook.AuthConfig["token"]
		if !exists || token == "" {
			return fmt.Errorf("bearer token not configured")
		}
		req.Header.Set("Authorization", "Bearer "+token)
		return nil

	case "basic":
		username, hasUsername := webhook.AuthConfig["username"]
		password, hasPassword := webhook.AuthConfig["password"]
		if !hasUsername || !hasPassword {
			return fmt.Errorf("basic auth username/password not configured")
		}
		req.SetBasicAuth(username, password)
		return nil

	default:
		return fmt.Errorf("unsupported auth type: %s", webhook.AuthType)
	}
}