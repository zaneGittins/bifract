package ingestcli

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxRetries     = 5
	initialBackoff = 1 * time.Second
	maxBackoff     = 30 * time.Second
	httpTimeout    = 120 * time.Second
)

// Config holds all settings for an ingestion run.
type Config struct {
	Files     []string
	Token     string
	URL       string
	BatchSize int
	Workers   int
	Limit     int
	Insecure  bool
	Adaptive  bool // true when auto mode is active (no manual flags)
}

// Stats tracks ingestion progress with atomic counters.
type Stats struct {
	LogsSent   atomic.Int64
	Errors     atomic.Int64
	Retries    atomic.Int64
	Batches    atomic.Int64
	TotalLogs  atomic.Int64
	BytesSent  atomic.Int64
	Throttled  atomic.Int64
	StartTime  time.Time
	Pacer      *AdaptivePacer
	mu         sync.Mutex
	FilesDone  int
	FilesTotal int
	CurrentFile string
}

func (s *Stats) LogsPerSec() float64 {
	elapsed := time.Since(s.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(s.LogsSent.Load()) / elapsed
}

// Batch represents a chunk of logs to send.
type Batch struct {
	Logs []map[string]interface{}
}

// IngestResult is the API response from Bifract.
type IngestResult struct {
	Success bool   `json:"success"`
	Count   int    `json:"count"`
	Message string `json:"message"`
	Error   string `json:"error"`
}

// Client handles HTTP communication with Bifract.
type Client struct {
	httpClient *http.Client
	url        string
	token      string
}

func NewClient(cfg *Config) *Client {
	transport := &http.Transport{}
	if cfg.Insecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}
	}
	return &Client{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   httpTimeout,
		},
		url:   cfg.URL,
		token: cfg.Token,
	}
}

// SendBatch sends a batch of logs with retry on 429/5xx/connection errors.
// Returns the error (if any) and whether server pressure was detected
// (429, 5xx, or connection failures) so the adaptive pacer can throttle.
func (c *Client) SendBatch(logs []map[string]interface{}, stats *Stats) (error, bool) {
	body, err := json.Marshal(logs)
	if err != nil {
		return fmt.Errorf("marshal: %w", err), false
	}

	var sawThrottle bool
	backoff := initialBackoff
	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest("POST", c.url+"/api/v1/ingest", bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create request: %w", err), false
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+c.token)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Connection error signals server pressure.
			sawThrottle = true
			stats.Retries.Add(1)
			if attempt < maxRetries {
				time.Sleep(backoff)
				backoff = nextBackoff(backoff)
				continue
			}
			return fmt.Errorf("connection failed after %d retries: %w", maxRetries, err), sawThrottle
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode == 429:
			sawThrottle = true
			stats.Retries.Add(1)
			stats.Throttled.Add(1)
			if attempt < maxRetries {
				time.Sleep(backoff)
				backoff = nextBackoff(backoff)
				continue
			}
			return fmt.Errorf("rate limited after %d retries", maxRetries), sawThrottle

		case resp.StatusCode == 413:
			return fmt.Errorf("payload too large (reduce --batch-size)"), false

		case resp.StatusCode >= 500:
			sawThrottle = true
			stats.Retries.Add(1)
			if attempt < maxRetries {
				time.Sleep(backoff)
				backoff = nextBackoff(backoff)
				continue
			}
			return fmt.Errorf("server error %d after %d retries", resp.StatusCode, maxRetries), sawThrottle

		case resp.StatusCode >= 400:
			var result IngestResult
			if json.Unmarshal(respBody, &result) == nil && result.Error != "" {
				return fmt.Errorf("HTTP %d: %s", resp.StatusCode, result.Error), false
			}
			return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody)), false

		default:
			// Success
			stats.LogsSent.Add(int64(len(logs)))
			stats.Batches.Add(1)
			stats.BytesSent.Add(int64(len(body)))
			return nil, sawThrottle
		}
	}
	return fmt.Errorf("exhausted retries"), sawThrottle
}

// TestConnection verifies the server is reachable and the token is valid.
func (c *Client) TestConnection() error {
	req, err := http.NewRequest("POST", c.url+"/api/v1/ingest", bytes.NewReader([]byte("[]")))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return fmt.Errorf("authentication failed (HTTP %d), check your token", resp.StatusCode)
	}
	return nil
}

// RunWorkers starts a pool of workers that drain the batch channel.
// Workers gate on the pacer before sending, feeding 429 signals back
// to the AIMD algorithm for adaptive concurrency control.
func RunWorkers(client *Client, batchCh <-chan Batch, stats *Stats, workers int, pacer *AdaptivePacer) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchCh {
				pacer.Acquire()
				err, throttled := client.SendBatch(batch.Logs, stats)
				pacer.Release(throttled)
				if err != nil {
					stats.Errors.Add(int64(len(batch.Logs)))
				}
			}
		}()
	}
	return &wg
}

func nextBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * 2)
	if next > maxBackoff {
		return maxBackoff
	}
	// Add small jitter
	jitter := time.Duration(float64(next) * 0.1)
	return next + jitter
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	m := int(d.Minutes())
	s := int(math.Mod(d.Seconds(), 60))
	return fmt.Sprintf("%dm%ds", m, s)
}

func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1_000_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
}
