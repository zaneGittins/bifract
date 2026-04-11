package alerts

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"html"
	"net"
	"net/smtp"
	"strings"
	"time"

	"bifract/pkg/storage"
)

// EmailAction represents an email notification endpoint configuration
type EmailAction struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Recipients      []string `json:"recipients"`
	SubjectTemplate string `json:"subject_template"`
	BodyTemplate    string `json:"body_template"`
	Enabled         bool   `json:"enabled"`
	FractalID       string `json:"fractal_id,omitempty"`
	PrismID         string `json:"prism_id,omitempty"`
}

// EmailResult tracks the outcome of an email delivery attempt
type EmailResult struct {
	EmailActionID   string        `json:"email_action_id"`
	EmailActionName string        `json:"email_action_name"`
	Success         bool          `json:"success"`
	Error           string        `json:"error,omitempty"`
	Duration        time.Duration `json:"duration"`
	AttemptCount    int           `json:"attempt_count"`
	Timestamp       time.Time     `json:"timestamp"`
}

// SMTPConfig holds the SMTP server configuration loaded from settings
type SMTPConfig struct {
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	FromAddress string `json:"from_address"`
	TLSMode     string `json:"tls_mode"` // "none", "starttls", "implicit"
}

// EmailClient handles email delivery with retry logic
type EmailClient struct {
	pg      *storage.PostgresClient
	baseURL string
}

// NewEmailClient creates a new email client
func NewEmailClient(pg *storage.PostgresClient, baseURL string) *EmailClient {
	return &EmailClient{
		pg:      pg,
		baseURL: strings.TrimRight(baseURL, "/"),
	}
}

// loadSMTPConfig reads SMTP configuration from the settings table
func (ec *EmailClient) loadSMTPConfig(ctx context.Context) (*SMTPConfig, error) {
	raw, err := ec.pg.GetSetting(ctx, "smtp_config")
	if err != nil {
		return nil, fmt.Errorf("SMTP not configured: %w", err)
	}
	var config SMTPConfig
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		return nil, fmt.Errorf("invalid SMTP config: %w", err)
	}
	if config.Host == "" {
		return nil, fmt.Errorf("SMTP host is required")
	}
	if config.Port == 0 {
		config.Port = 587
	}
	if config.FromAddress == "" {
		return nil, fmt.Errorf("SMTP from_address is required")
	}
	if config.TLSMode == "" {
		config.TLSMode = "starttls"
	}
	return &config, nil
}

// Send delivers an email notification with retry logic.
func (ec *EmailClient) Send(ctx context.Context, action EmailAction, alert *Alert, resolvedName string, results []map[string]interface{}) EmailResult {
	triggeredAt := time.Now()
	result := EmailResult{
		EmailActionID:   action.ID,
		EmailActionName: action.Name,
		Timestamp:       triggeredAt,
	}

	start := triggeredAt
	defer func() {
		result.Duration = time.Since(start)
	}()

	smtpConfig, err := ec.loadSMTPConfig(ctx)
	if err != nil {
		result.Error = err.Error()
		return result
	}

	subject := ec.resolveTemplate(action.SubjectTemplate, alert, resolvedName, results, triggeredAt)
	body := ec.resolveTemplate(action.BodyTemplate, alert, resolvedName, results, triggeredAt)

	if subject == "" {
		subject = fmt.Sprintf("[Bifract Alert] %s", resolvedName)
	}
	if body == "" {
		body = ec.defaultBody(alert, resolvedName, results, triggeredAt)
	}

	// Retry with exponential backoff (max 3 attempts)
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		result.AttemptCount = attempt

		if err := ec.sendMail(smtpConfig, action.Recipients, subject, body); err == nil {
			result.Success = true
			return result
		} else {
			result.Error = err.Error()
		}

		if attempt < maxRetries {
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			select {
			case <-ctx.Done():
				result.Error = "context cancelled during retry backoff"
				return result
			case <-time.After(backoff):
			}
		}
	}

	return result
}

// sendMail performs the actual SMTP delivery
func (ec *EmailClient) sendMail(config *SMTPConfig, recipients []string, subject, body string) error {
	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)

	// Build MIME message
	msg := ec.buildMIMEMessage(config.FromAddress, recipients, subject, body)

	switch strings.ToLower(config.TLSMode) {
	case "implicit":
		return ec.sendImplicitTLS(config, addr, recipients, msg)
	case "starttls":
		return ec.sendSTARTTLS(config, addr, recipients, msg)
	default:
		return ec.sendPlain(config, addr, recipients, msg)
	}
}

func (ec *EmailClient) sendImplicitTLS(config *SMTPConfig, addr string, recipients []string, msg []byte) error {
	tlsConfig := &tls.Config{ServerName: config.Host}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 15 * time.Second}, "tcp", addr, tlsConfig)
	if err != nil {
		return fmt.Errorf("TLS dial failed: %w", err)
	}

	client, err := smtp.NewClient(conn, config.Host)
	if err != nil {
		conn.Close()
		return fmt.Errorf("SMTP client creation failed: %w", err)
	}
	defer client.Close()

	return ec.deliverMessage(client, config, recipients, msg)
}

func (ec *EmailClient) sendSTARTTLS(config *SMTPConfig, addr string, recipients []string, msg []byte) error {
	client, err := smtp.Dial(addr)
	if err != nil {
		return fmt.Errorf("SMTP dial failed: %w", err)
	}
	defer client.Close()

	tlsConfig := &tls.Config{ServerName: config.Host}
	if err := client.StartTLS(tlsConfig); err != nil {
		return fmt.Errorf("STARTTLS failed: %w", err)
	}

	return ec.deliverMessage(client, config, recipients, msg)
}

func (ec *EmailClient) sendPlain(config *SMTPConfig, addr string, recipients []string, msg []byte) error {
	client, err := smtp.Dial(addr)
	if err != nil {
		return fmt.Errorf("SMTP dial failed: %w", err)
	}
	defer client.Close()

	return ec.deliverMessage(client, config, recipients, msg)
}

func (ec *EmailClient) deliverMessage(client *smtp.Client, config *SMTPConfig, recipients []string, msg []byte) error {
	// Authenticate if credentials provided
	if config.Username != "" && config.Password != "" {
		auth := smtp.PlainAuth("", config.Username, config.Password, config.Host)
		if err := client.Auth(auth); err != nil {
			return fmt.Errorf("SMTP auth failed: %w", err)
		}
	}

	if err := client.Mail(config.FromAddress); err != nil {
		return fmt.Errorf("MAIL FROM failed: %w", err)
	}

	for _, rcpt := range recipients {
		if err := client.Rcpt(strings.TrimSpace(rcpt)); err != nil {
			return fmt.Errorf("RCPT TO <%s> failed: %w", rcpt, err)
		}
	}

	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("DATA failed: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		return fmt.Errorf("message write failed: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("message close failed: %w", err)
	}

	return client.Quit()
}

func (ec *EmailClient) buildMIMEMessage(from string, to []string, subject, body string) []byte {
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf("From: %s\r\n", from))
	buf.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(to, ", ")))
	buf.WriteString(fmt.Sprintf("Subject: %s\r\n", subject))
	buf.WriteString("MIME-Version: 1.0\r\n")
	buf.WriteString("Content-Type: text/html; charset=\"UTF-8\"\r\n")
	buf.WriteString(fmt.Sprintf("Date: %s\r\n", time.Now().Format(time.RFC1123Z)))
	buf.WriteString("\r\n")
	buf.WriteString(body)
	return []byte(buf.String())
}

// resolveTemplate replaces {{placeholders}} in email templates
func (ec *EmailClient) resolveTemplate(tmpl string, alert *Alert, resolvedName string, results []map[string]interface{}, triggeredAt time.Time) string {
	if tmpl == "" {
		return ""
	}

	alertLink := buildShareLink(ec.baseURL, alert, triggeredAt)

	replacements := map[string]string{
		"{{alert_name}}":  html.EscapeString(resolvedName),
		"{{alert_id}}":    alert.ID,
		"{{description}}": html.EscapeString(alert.Description),
		"{{severity}}":    html.EscapeString(alert.Severity),
		"{{query}}":       html.EscapeString(alert.QueryString),
		"{{match_count}}": fmt.Sprintf("%d", len(results)),
		"{{alert_link}}":  alertLink,
		"{{labels}}":      html.EscapeString(strings.Join(alert.Labels, ", ")),
	}

	result := tmpl
	for placeholder, value := range replacements {
		result = strings.ReplaceAll(result, placeholder, value)
	}
	return result
}

// defaultBody generates an HTML email body when no template is configured
func (ec *EmailClient) defaultBody(alert *Alert, resolvedName string, results []map[string]interface{}, triggeredAt time.Time) string {
	alertLink := buildShareLink(ec.baseURL, alert, triggeredAt)

	severityColor := map[string]string{
		"critical": "#ef4444",
		"high":     "#f97316",
		"medium":   "#eab308",
		"low":      "#3b82f6",
		"info":     "#6b7280",
	}
	color := severityColor[alert.Severity]
	if color == "" {
		color = "#eab308"
	}

	var b strings.Builder
	b.WriteString(`<!DOCTYPE html><html><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#1a1a2e;max-width:600px;margin:0 auto;padding:20px;">`)
	b.WriteString(fmt.Sprintf(`<div style="border-left:4px solid %s;padding:12px 16px;margin-bottom:20px;background:#f8f9fa;">`, color))
	b.WriteString(fmt.Sprintf(`<h2 style="margin:0 0 4px 0;font-size:18px;">%s</h2>`, html.EscapeString(resolvedName)))
	b.WriteString(fmt.Sprintf(`<span style="display:inline-block;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600;color:white;background:%s;">%s</span>`, color, html.EscapeString(strings.ToUpper(alert.Severity))))
	b.WriteString(`</div>`)

	if alert.Description != "" {
		b.WriteString(fmt.Sprintf(`<p style="color:#555;margin:0 0 16px 0;">%s</p>`, html.EscapeString(alert.Description)))
	}

	b.WriteString(`<table style="width:100%%;border-collapse:collapse;font-size:14px;margin-bottom:16px;">`)
	b.WriteString(fmt.Sprintf(`<tr><td style="padding:6px 12px;color:#777;">Query</td><td style="padding:6px 12px;"><code>%s</code></td></tr>`, html.EscapeString(alert.QueryString)))
	b.WriteString(fmt.Sprintf(`<tr><td style="padding:6px 12px;color:#777;">Matches</td><td style="padding:6px 12px;">%d</td></tr>`, len(results)))
	if len(alert.Labels) > 0 {
		b.WriteString(fmt.Sprintf(`<tr><td style="padding:6px 12px;color:#777;">Labels</td><td style="padding:6px 12px;">%s</td></tr>`, html.EscapeString(strings.Join(alert.Labels, ", "))))
	}
	b.WriteString(`</table>`)

	if alertLink != "" {
		b.WriteString(fmt.Sprintf(`<a href="%s" style="display:inline-block;padding:10px 20px;background:#9c6ade;color:white;text-decoration:none;border-radius:6px;font-weight:500;">View in Bifract</a>`, alertLink))
	}

	b.WriteString(`<p style="color:#999;font-size:12px;margin-top:24px;">Sent by Bifract Alert System</p>`)
	b.WriteString(`</body></html>`)
	return b.String()
}
