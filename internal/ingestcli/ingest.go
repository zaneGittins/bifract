package ingestcli

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxRetries     = 5
	initialBackoff = 1 * time.Second
	maxBackoff     = 30 * time.Second
	httpTimeout    = 120 * time.Second

	// throttleBudget bounds how long one batch keeps retrying 429s. A 429 is
	// backpressure, not bad data, so giving up early discards good logs.
	throttleBudget = 15 * time.Minute

	// gzipMinBytes is the payload size above which requests are compressed.
	gzipMinBytes = 4096
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
	LogsSent    atomic.Int64
	Errors      atomic.Int64
	Retries     atomic.Int64
	Batches     atomic.Int64
	TotalLogs   atomic.Int64
	BytesSent   atomic.Int64
	Throttled   atomic.Int64
	StartTime   time.Time
	Pacer       *AdaptivePacer
	mu          sync.Mutex
	FilesDone   int
	FilesTotal  int
	CurrentFile string

	lastError  string
	errorTally map[string]int64
}

func (s *Stats) LogsPerSec() float64 {
	elapsed := time.Since(s.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(s.LogsSent.Load()) / elapsed
}

// maxTrackedErrors bounds distinct messages kept, so a server returning a
// unique error per request cannot grow the tally without limit.
const maxTrackedErrors = 8

// RecordError tallies a failure so the run can report why logs were lost
// instead of only how many.
func (s *Stats) RecordError(err error, logs int64) {
	msg := err.Error()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastError = msg
	if s.errorTally == nil {
		s.errorTally = make(map[string]int64)
	}
	if _, seen := s.errorTally[msg]; !seen && len(s.errorTally) >= maxTrackedErrors {
		msg = "(other errors)"
	}
	s.errorTally[msg] += logs
}

// LastError returns the most recent failure message, or "" if none.
func (s *Stats) LastError() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastError
}

// ErrorBreakdown returns tallied failures, most logs lost first.
func (s *Stats) ErrorBreakdown() []ErrorTally {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ErrorTally, 0, len(s.errorTally))
	for msg, n := range s.errorTally {
		out = append(out, ErrorTally{Message: msg, Logs: n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Logs > out[j].Logs })
	return out
}

// ErrorTally is a failure message and the number of logs it cost.
type ErrorTally struct {
	Message string
	Logs    int64
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

	// Retry policy. Held on the client rather than read from consts so tests
	// can exercise the loop without waiting out real backoff.
	initialBackoff time.Duration
	maxBackoff     time.Duration
	throttleBudget time.Duration

	// noGzip latches once a server is found not to support compressed bodies,
	// so the rest of the run skips compression instead of retrying every batch.
	noGzip atomic.Bool
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
		url:            cfg.URL,
		token:          cfg.Token,
		initialBackoff: initialBackoff,
		maxBackoff:     maxBackoff,
		throttleBudget: throttleBudget,
	}
}

// SendBatch sends a batch of logs with retry on 429/5xx/connection errors.
// Returns the error (if any) and whether server pressure was detected
// (429, 5xx, or connection failures) so the adaptive pacer can throttle.
func (c *Client) SendBatch(logs []map[string]interface{}, stats *Stats) (error, bool) {
	raw, err := json.Marshal(logs)
	if err != nil {
		return fmt.Errorf("marshal: %w", err), false
	}

	body, encoding := raw, ""
	if len(raw) >= gzipMinBytes && !c.noGzip.Load() {
		if compressed, err := gzipBytes(raw); err == nil {
			body, encoding = compressed, "gzip"
		}
	}

	var sawThrottle bool
	backoff := c.initialBackoff
	attempt := 0 // counts connection and 5xx retries only
	throttleDeadline := time.Now().Add(c.throttleBudget)

	for {
		req, err := http.NewRequest("POST", c.url+"/api/v1/ingest", bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create request: %w", err), sawThrottle
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+c.token)
		if encoding != "" {
			req.Header.Set("Content-Encoding", encoding)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Connection error signals server pressure.
			sawThrottle = true
			stats.Retries.Add(1)
			attempt++
			if attempt > maxRetries {
				return fmt.Errorf("connection failed after %d retries: %w", maxRetries, err), sawThrottle
			}
			time.Sleep(backoff)
			backoff = c.nextBackoff(backoff)
			continue
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode == 429:
			// Retry until the budget expires. Dropping here would discard the
			// whole batch over transient backpressure.
			sawThrottle = true
			stats.Retries.Add(1)
			stats.Throttled.Add(1)
			if time.Now().After(throttleDeadline) {
				return fmt.Errorf("rate limited for %s, batch abandoned", formatDuration(c.throttleBudget)), sawThrottle
			}
			time.Sleep(c.retryAfter(resp, backoff))
			backoff = c.nextBackoff(backoff)
			continue

		case resp.StatusCode == 413:
			return fmt.Errorf("payload too large (reduce --batch-size)"), false

		case resp.StatusCode >= 500:
			sawThrottle = true
			stats.Retries.Add(1)
			attempt++
			if attempt > maxRetries {
				return fmt.Errorf("server error %d after %d retries", resp.StatusCode, maxRetries), sawThrottle
			}
			time.Sleep(backoff)
			backoff = c.nextBackoff(backoff)
			continue

		case resp.StatusCode >= 400:
			// A server predating gzip support here parses the compressed bytes
			// as JSON and rejects them. Resend plain and stop compressing.
			if encoding == "gzip" {
				c.noGzip.Store(true)
				body, encoding = raw, ""
				continue
			}

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
}

// retryAfter honors the server's Retry-After hint, never backing off less than
// the current backoff so repeated throttling still escalates.
func (c *Client) retryAfter(resp *http.Response, backoff time.Duration) time.Duration {
	wait := backoff
	if v := resp.Header.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			if d := time.Duration(secs) * time.Second; d > wait {
				wait = d
			}
		}
	}
	if wait > c.maxBackoff {
		return c.maxBackoff
	}
	return wait
}

func gzipBytes(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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
					stats.RecordError(err, int64(len(batch.Logs)))
				}
			}
		}()
	}
	return &wg
}

func (c *Client) nextBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * 2)
	if next > c.maxBackoff {
		return c.maxBackoff
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
