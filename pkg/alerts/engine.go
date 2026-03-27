package alerts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"bifract/pkg/dictionaries"
	"bifract/pkg/parser"
	"bifract/pkg/settings"
	"bifract/pkg/storage"

	"github.com/lib/pq"
	"github.com/robfig/cron/v3"
)

// Engine is the core alert processing engine.
//
// It runs a background ticker that evaluates all enabled alerts using a
// cursor-based approach: each alert tracks the ingest_timestamp it was
// last evaluated up to, so every log is guaranteed to be seen at least
// once. The cursor is only advanced after a successful query, making
// evaluation crash-safe.
//
// Concurrency model:
//   - A single evaluation cycle runs at a time (overlap protection).
//   - Within a cycle, up to maxConcurrent alerts evaluate in parallel.
//   - A circuit breaker stops the cycle if consecutive failures accumulate.
//   - During active ingestion, evaluation is fully deferred to free
//     ClickHouse resources for writes. Cursors catch up once pressure lifts.
type Engine struct {
	pg                  *storage.PostgresClient
	ch                  *storage.ClickHouseClient
	dictManager         *dictionaries.Manager
	webhookClient       *WebhookClient
	emailClient         *EmailClient
	fractalActionClient *FractalActionClient
	throttleCache       *ThrottleCache

	// Alert cache with TTL-based refresh.
	alertsCache      map[string]*Alert
	cacheRefreshedAt time.Time
	mu               sync.RWMutex

	// System alerts fractal (lazy-resolved, cached).
	alertsFractalID string
	alertsFractalMu sync.RWMutex

	// Background evaluator.
	stopCh  chan struct{}
	evalWg  sync.WaitGroup
	running atomic.Bool // prevents overlapping evaluation cycles

	// Retention cleanup tracking.
	lastCleanupAt time.Time

	// ingestActive returns true when heavy ingestion is in progress.
	// Evaluation is fully deferred during active ingestion.
	ingestActive func() bool

	// lastIngested returns the most recent insert time for a fractal.
	// Used to skip evaluation when no new data has arrived.
	lastIngested func(fractalID string) time.Time

	// startedAt records when the engine started so shouldSkipAlert can
	// distinguish "no data since startup" from "never tracked."
	startedAt time.Time
}

const (
	// maxConcurrent bounds the number of alert evaluations running in
	// parallel within a single cycle.
	maxConcurrent = 5

	// circuitBreakerThreshold stops a cycle after this many consecutive
	// failures, freeing ClickHouse for user queries.
	circuitBreakerThreshold = 5

	// maxEvalWindow caps the time range a single event alert can scan.
	// Evaluation is already deferred during active ingestion, so this
	// only applies when the system is idle and catching up.
	maxEvalWindow = 30 * time.Minute

	// minEvalWindow is the minimum gap between cursor and now before an
	// evaluation runs. Must exceed the default eval interval (30s) so
	// caught-up alerts skip most ticks.
	minEvalWindow = 60 * time.Second

	// cacheRefreshInterval controls how often the alerts cache is
	// refreshed from PostgreSQL. Picks up external changes.
	cacheRefreshInterval = 5 * time.Minute

	// retentionDays is how long alert_executions rows are kept.
	retentionDays = 30

	// retentionInterval is how often the retention cleanup runs.
	retentionInterval = 1 * time.Hour
)

// DictionaryActionRef is a lightweight reference for API listing responses.
type DictionaryActionRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Alert represents an alert definition with its parsed query and actions.
type Alert struct {
	ID                   string                           `json:"id"`
	Name                 string                           `json:"name"`
	Description          string                           `json:"description"`
	QueryString          string                           `json:"query_string"`
	AlertType            string                           `json:"alert_type"`
	ParsedQuery          *parser.PipelineNode             `json:"-"`
	CronSchedule         cron.Schedule                    `json:"-"`
	Enabled              bool                             `json:"enabled"`
	ThrottleTimeSeconds  int                              `json:"throttle_time_seconds"`
	ThrottleField        string                           `json:"throttle_field"`
	Labels               []string                         `json:"labels"`
	References           []string                         `json:"references"`
	Severity             string                           `json:"severity"`
	WebhookActions       []WebhookAction                  `json:"webhook_actions"`
	FractalActions       []FractalAction                  `json:"fractal_actions"`
	EmailActions         []EmailAction                    `json:"email_actions"`
	EmailActionIDs       []string                         `json:"email_action_ids,omitempty"`
	DictionaryActions    []*dictionaries.DictionaryAction `json:"-"`
	DictionaryActionRefs []DictionaryActionRef            `json:"dictionary_actions,omitempty"`
	DictionaryActionIDs  []string                         `json:"dictionary_action_ids,omitempty"`
	FeedID              string                           `json:"feed_id,omitempty"`
	FeedRulePath        string                           `json:"feed_rule_path,omitempty"`
	FeedRuleHash        string                           `json:"feed_rule_hash,omitempty"`
	FractalID           string                           `json:"fractal_id,omitempty"`
	PrismID             string                           `json:"prism_id,omitempty"`
	CreatedBy           string                           `json:"created_by"`
	UpdatedBy           *string                          `json:"updated_by,omitempty"`
	CreatedAt           time.Time                        `json:"created_at"`
	UpdatedAt           time.Time                        `json:"updated_at"`
	LastTriggered       *time.Time                       `json:"last_triggered,omitempty"`
	LastEvaluatedAt     time.Time                        `json:"last_evaluated_at"`
	LastExecutionTimeMs *int                             `json:"last_execution_time_ms,omitempty"`
	DisabledReason      string                           `json:"disabled_reason,omitempty"`
	WindowDuration      *int                             `json:"window_duration,omitempty"`
	ScheduleCron        *string                          `json:"schedule_cron,omitempty"`
	QueryWindowSeconds  *int                             `json:"query_window_seconds,omitempty"`
}

// NewEngine creates a new alert processing engine.
func NewEngine(pg *storage.PostgresClient, ch *storage.ClickHouseClient) *Engine {
	return NewEngineWithDicts(pg, ch, nil, "")
}

// NewEngineWithDicts creates a new alert engine with dictionary action support.
func NewEngineWithDicts(pg *storage.PostgresClient, ch *storage.ClickHouseClient, dictManager *dictionaries.Manager, baseURL string) *Engine {
	return &Engine{
		pg:                  pg,
		ch:                  ch,
		dictManager:         dictManager,
		webhookClient:       NewWebhookClient(baseURL),
		emailClient:         NewEmailClient(pg, baseURL),
		fractalActionClient: NewFractalActionClient(ch, pg),
		throttleCache:       NewThrottleCache(),
		alertsCache:         make(map[string]*Alert),
	}
}

// SetIngestPressureFunc registers a callback that the engine checks before
// each evaluation cycle. When it returns true, evaluation is fully deferred.
func (e *Engine) SetIngestPressureFunc(f func() bool) {
	e.ingestActive = f
}

// SetLastIngestedFunc registers a callback that returns the most recent
// insert time for a fractal. Used to skip alert evaluation when no new
// data has arrived since the alert's cursor position.
func (e *Engine) SetLastIngestedFunc(f func(fractalID string) time.Time) {
	e.lastIngested = f
}

// shouldSkipAlert returns true if no new data has been ingested for the
// alert's fractal since its cursor position. The cursor is never advanced
// when skipped, so no logs can be missed.
func (e *Engine) shouldSkipAlert(alert *Alert) bool {
	if e.lastIngested == nil {
		return false
	}
	// Scheduled alerts run on a cron schedule regardless of ingest
	// activity. They may check for the absence of data or aggregate
	// over fixed windows, so skipping them defeats their purpose.
	if alert.AlertType == "scheduled" {
		return false
	}
	// Prism alerts span multiple fractals; skip logic would need all
	// member IDs resolved. Not worth the complexity -- don't skip.
	if alert.PrismID != "" || alert.FractalID == "" {
		return false
	}
	if alert.LastEvaluatedAt.IsZero() {
		return false // never evaluated, must run
	}
	lastData := e.lastIngested(alert.FractalID)
	if lastData.IsZero() {
		// No ingestion tracked for this fractal since process start.
		// If the alert has been evaluated since we started, there is
		// nothing new to find -- safe to skip.
		return alert.LastEvaluatedAt.After(e.startedAt)
	}
	return lastData.Before(alert.LastEvaluatedAt)
}

// Start launches the background alert evaluation loop.
func (e *Engine) Start(interval time.Duration) {
	e.startedAt = time.Now()
	e.stopCh = make(chan struct{})
	e.evalWg.Add(1)
	go e.evaluationLoop(interval)
	log.Printf("[Alert Engine] Started (interval: %v, max concurrent: %d)", interval, maxConcurrent)
}

// Stop halts the background evaluation loop and waits for it to finish.
func (e *Engine) Stop() {
	if e.stopCh != nil {
		close(e.stopCh)
		e.evalWg.Wait()
		log.Println("[Alert Engine] Stopped")
	}
}

// Metrics source methods (satisfy metrics.AlertSource interface).

func (e *Engine) CachedAlertCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.alertsCache)
}

func (e *Engine) IsRunning() bool {
	return e.running.Load()
}

func (e *Engine) evaluationLoop(interval time.Duration) {
	defer e.evalWg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.evaluateAllAlerts()
			e.maybeRunRetention()
		}
	}
}

// alertEngineLockID is the Postgres advisory lock ID used to ensure only one
// Bifract replica evaluates alerts at a time. The value is arbitrary but must
// be consistent across all replicas.
const alertEngineLockID int64 = 0x6269667261637401 // "bifract\x01"

// evaluateAllAlerts runs one evaluation cycle for every enabled alert.
// Only one cycle runs at a time; if the previous cycle hasn't finished
// when the next tick fires, the tick is skipped. A Postgres advisory lock
// ensures only one replica in a multi-instance deployment evaluates alerts.
func (e *Engine) evaluateAllAlerts() {
	if !e.running.CompareAndSwap(false, true) {
		return
	}
	defer e.running.Store(false)

	// Distributed lock: only one replica evaluates alerts at a time.
	unlock, acquired := e.pg.TryAdvisoryLock(context.Background(), alertEngineLockID)
	if !acquired {
		return
	}
	defer unlock()

	if e.ingestActive != nil && e.ingestActive() {
		return
	}

	alerts, err := e.getEnabledAlerts(context.Background())
	if err != nil {
		log.Printf("[Alert Engine] Failed to get enabled alerts: %v", err)
		return
	}
	if len(alerts) == 0 {
		return
	}
	log.Printf("[Alert Engine] Evaluating %d alert(s)", len(alerts))

	// Scale timeout with alert count so large deployments don't hit the
	// deadline before all alerts have been evaluated.
	timeout := time.Duration(len(alerts)/maxConcurrent+1) * 10 * time.Second
	if timeout < 2*time.Minute {
		timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Prism resolution cache scoped to this cycle.
	prismCache := &prismResolveCache{entries: make(map[string][]string)}

	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	var consecutiveFailures atomic.Int64
	var tripped atomic.Bool

	for _, alert := range alerts {
		if tripped.Load() {
			break
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(a *Alert) {
			defer wg.Done()
			defer func() { <-sem }()

			if tripped.Load() {
				return
			}

			if e.shouldSkipAlert(a) {
				log.Printf("[Alert Engine] Skipped alert '%s' (no new data)", a.Name)
				return
			}

			var evalErr error
			switch a.AlertType {
			case "compound":
				evalErr = e.evaluateCompoundAlert(ctx, a, prismCache)
			case "scheduled":
				evalErr = e.evaluateScheduledAlert(ctx, a, prismCache)
			default:
				evalErr = e.evaluateAlertCursor(ctx, a, prismCache)
			}
			if evalErr != nil {
				log.Printf("[Alert Engine] Alert '%s' error: %v", a.Name, evalErr)
				if consecutiveFailures.Add(1) >= circuitBreakerThreshold {
					if tripped.CompareAndSwap(false, true) {
						log.Printf("[Alert Engine] Circuit breaker tripped after %d consecutive failures", circuitBreakerThreshold)
					}
				}
			} else {
				consecutiveFailures.Store(0)
			}
		}(alert)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Prism resolution cache (cycle-scoped, thread-safe)
// ---------------------------------------------------------------------------

type prismResolveCache struct {
	mu      sync.Mutex
	entries map[string][]string
}

func (e *Engine) resolvePrismFractalIDs(ctx context.Context, prismID string, cache *prismResolveCache) ([]string, error) {
	if cache != nil {
		cache.mu.Lock()
		if ids, ok := cache.entries[prismID]; ok {
			cache.mu.Unlock()
			return ids, nil
		}
		cache.mu.Unlock()
	}

	rows, err := e.pg.Query(ctx, "SELECT fractal_id FROM prism_members WHERE prism_id = $1", prismID)
	if err != nil {
		return nil, fmt.Errorf("resolve prism members: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("prism %s has no member fractals", prismID)
	}

	if cache != nil {
		cache.mu.Lock()
		cache.entries[prismID] = ids
		cache.mu.Unlock()
	}
	return ids, nil
}

// ---------------------------------------------------------------------------
// Shared helpers (deduplicate query building, execution, timeout)
// ---------------------------------------------------------------------------

// buildQueryOpts constructs parser.QueryOptions with fractal/prism scoping.
func (e *Engine) buildQueryOpts(ctx context.Context, alert *Alert, from, to time.Time, cache *prismResolveCache) (parser.QueryOptions, error) {
	tableName := "logs"
	if e.ch != nil && e.ch.IsCluster() {
		tableName = "logs_distributed"
	}
	opts := parser.QueryOptions{
		StartTime:         from,
		EndTime:           to,
		MaxRows:           10000,
		UseIngestTimestamp: true,
		TableName:         tableName,
	}
	if alert.PrismID != "" {
		ids, err := e.resolvePrismFractalIDs(ctx, alert.PrismID, cache)
		if err != nil {
			return opts, err
		}
		opts.FractalIDs = ids
	} else if alert.FractalID != "" {
		opts.FractalID = alert.FractalID
	}
	// Collect extra fields needed by the alert processing pipeline so
	// auto-projection includes them even if they aren't in the WHERE clause.
	opts.AlertExtraFields = collectAlertExtraFields(alert)
	return opts, nil
}

// collectAlertExtraFields returns field names that the alert processing
// pipeline reads from results (throttle field, template name placeholders).
func collectAlertExtraFields(alert *Alert) []string {
	var fields []string
	if alert.ThrottleField != "" {
		fields = append(fields, alert.ThrottleField)
	}
	// Extract {{field}} references from the alert name template
	if strings.Contains(alert.Name, "{{") {
		for _, match := range templatePattern.FindAllStringSubmatch(alert.Name, -1) {
			if len(match) > 1 {
				fields = append(fields, match[1])
			}
		}
	}
	return fields
}

// runAlertQuery translates and executes an alert query with timeout handling.
// On timeout the alert is auto-disabled and a descriptive error is returned.
func (e *Engine) runAlertQuery(ctx context.Context, alert *Alert, opts parser.QueryOptions, timeoutSec int) ([]map[string]interface{}, error) {
	sql, err := parser.TranslateToSQL(alert.ParsedQuery, opts)
	if err != nil {
		return nil, fmt.Errorf("translate query: %w", err)
	}

	alertCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()

	results, err := e.ch.Query(alertCtx, sql)
	if err != nil {
		if alertCtx.Err() == context.DeadlineExceeded {
			reason := fmt.Sprintf("Auto-disabled: query exceeded %ds timeout", timeoutSec)
			log.Printf("[Alert Engine] Alert '%s' exceeded %ds timeout, disabling", alert.Name, timeoutSec)
			if disableErr := e.disableAlertWithReason(ctx, alert.ID, reason); disableErr != nil {
				log.Printf("[Alert Engine] Failed to disable alert '%s': %v", alert.Name, disableErr)
			}
			return nil, fmt.Errorf("query timed out after %ds", timeoutSec)
		}
		return nil, fmt.Errorf("query failed: %w", err)
	}
	return results, nil
}

// getAlertTimeout returns the per-alert query timeout from global settings.
func getAlertTimeout() int {
	t := settings.Get().AlertTimeoutSeconds
	if t <= 0 {
		return 5
	}
	return t
}

// advanceCursor writes the new cursor to PostgreSQL and updates the
// in-memory cache so the next tick starts from the right position.
func (e *Engine) advanceCursor(ctx context.Context, alert *Alert, t time.Time) {
	if err := e.updateLastEvaluated(ctx, alert.ID, t); err != nil {
		log.Printf("[Alert Engine] Failed to advance cursor for alert %s: %v", alert.Name, err)
	} else {
		alert.LastEvaluatedAt = t
	}
}

// ---------------------------------------------------------------------------
// Alert type evaluators
// ---------------------------------------------------------------------------

// evaluateAlertCursor evaluates an event alert from its cursor forward.
// The cursor advances only after a successful query. If the process
// crashes between runs, the window is re-scanned (no missed logs).
func (e *Engine) evaluateAlertCursor(ctx context.Context, alert *Alert, cache *prismResolveCache) error {
	start := time.Now()

	fromTime := alert.LastEvaluatedAt
	if fromTime.IsZero() {
		fromTime = time.Now().Add(-5 * time.Minute)
	}
	toTime := time.Now()

	if toTime.Sub(fromTime) < minEvalWindow {
		return nil
	}
	if toTime.Sub(fromTime) > maxEvalWindow {
		toTime = fromTime.Add(maxEvalWindow)
	}

	opts, err := e.buildQueryOpts(ctx, alert, fromTime, toTime, cache)
	if err != nil {
		return err
	}

	results, err := e.runAlertQuery(ctx, alert, opts, getAlertTimeout())
	if err != nil {
		return err
	}

	e.advanceCursor(ctx, alert, toTime)
	return e.processAlertResults(ctx, alert, results, int(time.Since(start).Milliseconds()))
}

// evaluateCompoundAlert evaluates a compound alert using tumbling windows.
// The alert only evaluates when the current window is complete.
func (e *Engine) evaluateCompoundAlert(ctx context.Context, alert *Alert, cache *prismResolveCache) error {
	if alert.WindowDuration == nil || *alert.WindowDuration <= 0 {
		return fmt.Errorf("compound alert %s has no valid window_duration", alert.Name)
	}

	windowDuration := time.Duration(*alert.WindowDuration) * time.Second
	fromTime := alert.LastEvaluatedAt
	if fromTime.IsZero() {
		fromTime = time.Now().Add(-windowDuration)
	}

	windowEnd := fromTime.Add(windowDuration)
	if time.Now().Before(windowEnd) {
		return nil
	}

	start := time.Now()

	opts, err := e.buildQueryOpts(ctx, alert, fromTime, windowEnd, cache)
	if err != nil {
		return err
	}

	results, err := e.runAlertQuery(ctx, alert, opts, getAlertTimeout())
	if err != nil {
		return err
	}

	e.advanceCursor(ctx, alert, windowEnd)
	return e.processAlertResults(ctx, alert, results, int(time.Since(start).Milliseconds()))
}

// evaluateScheduledAlert evaluates a scheduled alert based on its cron
// expression. The cron schedule is parsed once at cache load time.
func (e *Engine) evaluateScheduledAlert(ctx context.Context, alert *Alert, cache *prismResolveCache) error {
	if alert.CronSchedule == nil {
		return fmt.Errorf("scheduled alert %s has no parsed cron schedule", alert.Name)
	}
	if alert.QueryWindowSeconds == nil || *alert.QueryWindowSeconds <= 0 {
		return fmt.Errorf("scheduled alert %s has no valid query_window_seconds", alert.Name)
	}

	lastEval := alert.LastEvaluatedAt
	if lastEval.IsZero() {
		lastEval = time.Now().Add(-time.Duration(*alert.QueryWindowSeconds) * time.Second)
	}
	if time.Now().Before(alert.CronSchedule.Next(lastEval)) {
		return nil
	}

	start := time.Now()
	queryWindow := time.Duration(*alert.QueryWindowSeconds) * time.Second
	toTime := time.Now()
	fromTime := toTime.Add(-queryWindow)

	opts, err := e.buildQueryOpts(ctx, alert, fromTime, toTime, cache)
	if err != nil {
		return err
	}

	timeout := getAlertTimeout() * 3
	if timeout < 30 {
		timeout = 30
	}

	results, err := e.runAlertQuery(ctx, alert, opts, timeout)
	if err != nil {
		return err
	}

	e.advanceCursor(ctx, alert, toTime)
	return e.processAlertResults(ctx, alert, results, int(time.Since(start).Milliseconds()))
}

// ---------------------------------------------------------------------------
// Result processing
// ---------------------------------------------------------------------------

// processAlertResults handles post-query logic: throttle checking, action
// execution, and audit recording. Only records an execution when results
// are found (or throttled) to prevent table bloat at scale.
func (e *Engine) processAlertResults(ctx context.Context, alert *Alert, results []map[string]interface{}, executionTimeMs int) error {
	if len(results) == 0 {
		return nil
	}

	throttled, throttleKey := e.isThrottled(alert, results)
	if throttled {
		if err := e.updateLastTriggered(ctx, alert.ID); err != nil {
			log.Printf("[Alert Engine] Failed to update last triggered for throttled alert %s: %v", alert.Name, err)
		}
		return e.recordExecution(ctx, alert.ID, alert.FractalID, len(results), true, throttleKey, executionTimeMs, []WebhookResult{}, []FractalResult{}, []EmailResult{})
	}

	resolvedName := ResolveTemplateName(alert.Name, results)

	// Trigger webhook actions
	webhookResults := make([]WebhookResult, 0, len(alert.WebhookActions))
	if len(alert.WebhookActions) > 0 {
		var webhookWg sync.WaitGroup
		webhookResultsCh := make(chan WebhookResult, len(alert.WebhookActions))

		for _, webhook := range alert.WebhookActions {
			if !webhook.Enabled {
				continue
			}
			webhookWg.Add(1)
			go func(wh WebhookAction) {
				defer webhookWg.Done()
				webhookResultsCh <- e.webhookClient.Send(ctx, wh, alert, resolvedName, results)
			}(webhook)
		}

		webhookWg.Wait()
		close(webhookResultsCh)
		for result := range webhookResultsCh {
			webhookResults = append(webhookResults, result)
		}
	}

	// Trigger fractal actions
	fractalResults := make([]FractalResult, 0, len(alert.FractalActions))
	if len(alert.FractalActions) > 0 {
		var fractalWg sync.WaitGroup
		fractalResultsCh := make(chan FractalResult, len(alert.FractalActions))

		for _, fractalAction := range alert.FractalActions {
			if !fractalAction.Enabled {
				continue
			}
			fractalWg.Add(1)
			go func(fa FractalAction) {
				defer fractalWg.Done()
				fractalResultsCh <- e.fractalActionClient.Send(ctx, fa, alert, resolvedName, results)
			}(fractalAction)
		}

		fractalWg.Wait()
		close(fractalResultsCh)
		for result := range fractalResultsCh {
			fractalResults = append(fractalResults, result)
		}
	}

	// Trigger email actions
	emailResults := make([]EmailResult, 0, len(alert.EmailActions))
	if len(alert.EmailActions) > 0 {
		var emailWg sync.WaitGroup
		emailResultsCh := make(chan EmailResult, len(alert.EmailActions))

		for _, emailAction := range alert.EmailActions {
			if !emailAction.Enabled {
				continue
			}
			emailWg.Add(1)
			go func(ea EmailAction) {
				defer emailWg.Done()
				emailResultsCh <- e.emailClient.Send(ctx, ea, alert, resolvedName, results)
			}(emailAction)
		}

		emailWg.Wait()
		close(emailResultsCh)
		for result := range emailResultsCh {
			emailResults = append(emailResults, result)
		}
	}

	// Execute dictionary actions
	if e.dictManager != nil && len(alert.DictionaryActions) > 0 {
		for _, da := range alert.DictionaryActions {
			if !da.Enabled {
				continue
			}
			count, err := e.dictManager.ExecuteDictionaryAction(ctx, da, alert.FractalID, alert.PrismID, results)
			if err != nil {
				log.Printf("[Alert Engine] Dictionary action %s failed for alert %s: %v", da.Name, alert.Name, err)
			} else {
				log.Printf("[Alert Engine] Dictionary action %s upserted %d rows for alert %s", da.Name, count, alert.Name)
			}
		}
	}

	// Send to system alerts fractal
	if alertsFractalID := e.getAlertsFractalID(ctx); alertsFractalID != "" {
		systemAction := FractalAction{
			ID:                "system-alerts",
			Name:              "System Alerts Fractal",
			TargetFractalID:   alertsFractalID,
			PreserveTimestamp: true,
			AddAlertContext:   true,
			FieldMappings:     map[string]string{},
			MaxLogsPerTrigger: 1000,
			Enabled:           true,
		}
		result := e.fractalActionClient.Send(ctx, systemAction, alert, resolvedName, results)
		fractalResults = append(fractalResults, result)
	}

	e.updateThrottle(alert, results)

	if err := e.updateLastTriggered(ctx, alert.ID); err != nil {
		log.Printf("[Alert Engine] Failed to update last triggered for alert %s: %v", alert.Name, err)
	}

	return e.recordExecution(ctx, alert.ID, alert.FractalID, len(results), false, throttleKey, executionTimeMs, webhookResults, fractalResults, emailResults)
}

// ---------------------------------------------------------------------------
// Cache management
// ---------------------------------------------------------------------------

// getEnabledAlerts returns enabled alerts, refreshing the cache if it is
// empty or older than cacheRefreshInterval.
func (e *Engine) getEnabledAlerts(ctx context.Context) ([]*Alert, error) {
	e.mu.RLock()
	cacheValid := len(e.alertsCache) > 0 && time.Since(e.cacheRefreshedAt) < cacheRefreshInterval
	e.mu.RUnlock()

	if cacheValid {
		e.mu.RLock()
		alerts := make([]*Alert, 0, len(e.alertsCache))
		for _, alert := range e.alertsCache {
			if alert.Enabled {
				alerts = append(alerts, alert)
			}
		}
		e.mu.RUnlock()
		return alerts, nil
	}

	return e.refreshAlertsCache(ctx)
}

// refreshAlertsCache loads all alerts from PostgreSQL and updates the cache.
// Queries and cron expressions are parsed once here, not on every tick.
func (e *Engine) refreshAlertsCache(ctx context.Context) ([]*Alert, error) {
	query := `
		SELECT a.id, a.name, a.description, a.query_string,
		       COALESCE(a.alert_type, 'event'), a.enabled,
		       COALESCE(a.throttle_time_seconds, 0), COALESCE(a.throttle_field, ''), a.labels,
		       COALESCE(a.severity, 'medium'), COALESCE(a.fractal_id::text, ''), COALESCE(a.prism_id::text, ''),
		       COALESCE(a.created_by, ''), a.created_at, a.updated_at, a.last_triggered,
		       a.last_evaluated_at, COALESCE(a.disabled_reason, ''), a.window_duration,
		       COALESCE(a.schedule_cron, ''), a.query_window_seconds,
		       COALESCE(a.feed_id::text, ''), COALESCE(a.feed_rule_path, ''), COALESCE(a.feed_rule_hash, ''),
		       COALESCE(json_agg(
		           json_build_object(
		               'id', wa.id,
		               'name', wa.name,
		               'url', wa.url,
		               'method', wa.method,
		               'headers', wa.headers,
		               'auth_type', wa.auth_type,
		               'auth_config', wa.auth_config,
		               'timeout_seconds', wa.timeout_seconds,
		               'retry_count', wa.retry_count,
		               'include_alert_link', wa.include_alert_link,
		               'enabled', wa.enabled
		           ) ORDER BY wa.name
		       ) FILTER (WHERE wa.id IS NOT NULL), '[]'::json) as webhook_actions,
		       COALESCE(json_agg(
		           json_build_object(
		               'id', fa.id,
		               'name', fa.name,
		               'description', fa.description,
		               'target_fractal_id', fa.target_fractal_id,
		               'preserve_timestamp', fa.preserve_timestamp,
		               'add_alert_context', fa.add_alert_context,
		               'field_mappings', fa.field_mappings,
		               'max_logs_per_trigger', fa.max_logs_per_trigger,
		               'enabled', fa.enabled
		           ) ORDER BY fa.name
		       ) FILTER (WHERE fa.id IS NOT NULL), '[]'::json) as fractal_actions,
		       COALESCE(json_agg(
		           json_build_object(
		               'id', ea.id,
		               'name', ea.name,
		               'recipients', ea.recipients,
		               'subject_template', ea.subject_template,
		               'body_template', ea.body_template,
		               'enabled', ea.enabled
		           ) ORDER BY ea.name
		       ) FILTER (WHERE ea.id IS NOT NULL), '[]'::json) as email_actions
		FROM alerts a
		LEFT JOIN alert_webhook_actions awa ON a.id = awa.alert_id
		LEFT JOIN webhook_actions wa ON awa.webhook_id = wa.id AND wa.enabled = true
		LEFT JOIN alert_fractal_actions afa ON a.id = afa.alert_id
		LEFT JOIN fractal_actions fa ON afa.fractal_action_id = fa.id AND fa.enabled = true
		LEFT JOIN alert_email_actions aea ON a.id = aea.alert_id
		LEFT JOIN email_actions ea ON aea.email_action_id = ea.id AND ea.enabled = true
		GROUP BY a.id, a.name, a.description, a.query_string, a.alert_type, a.enabled,
		         a.throttle_time_seconds, a.throttle_field, a.labels, a.severity, a.fractal_id, a.prism_id,
		         a.created_by, a.created_at, a.updated_at, a.last_triggered,
		         a.last_evaluated_at, a.disabled_reason, a.window_duration,
		         a.schedule_cron, a.query_window_seconds,
		         a.feed_id, a.feed_rule_path, a.feed_rule_hash
		ORDER BY a.name
	`

	rows, err := e.pg.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query alerts: %w", err)
	}
	defer rows.Close()

	cronParser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

	alerts := make([]*Alert, 0)
	newCache := make(map[string]*Alert)

	for rows.Next() {
		var alert Alert
		var webhookActionsJSON []byte
		var fractalActionsJSON []byte
		var emailActionsJSON []byte

		err := rows.Scan(
			&alert.ID, &alert.Name, &alert.Description, &alert.QueryString,
			&alert.AlertType, &alert.Enabled, &alert.ThrottleTimeSeconds, &alert.ThrottleField,
			pq.Array(&alert.Labels), &alert.Severity, &alert.FractalID, &alert.PrismID,
			&alert.CreatedBy, &alert.CreatedAt, &alert.UpdatedAt,
			&alert.LastTriggered, &alert.LastEvaluatedAt, &alert.DisabledReason, &alert.WindowDuration,
			&alert.ScheduleCron, &alert.QueryWindowSeconds,
			&alert.FeedID, &alert.FeedRulePath, &alert.FeedRuleHash,
			&webhookActionsJSON, &fractalActionsJSON, &emailActionsJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan alert: %w", err)
		}

		if err := json.Unmarshal(webhookActionsJSON, &alert.WebhookActions); err != nil {
			return nil, fmt.Errorf("failed to parse webhook actions: %w", err)
		}

		if err := json.Unmarshal(fractalActionsJSON, &alert.FractalActions); err != nil {
			return nil, fmt.Errorf("failed to parse fractal actions: %w", err)
		}

		if err := json.Unmarshal(emailActionsJSON, &alert.EmailActions); err != nil {
			return nil, fmt.Errorf("failed to parse email actions: %w", err)
		}

		parsedQuery, err := parser.ParseQuery(alert.QueryString)
		if err != nil {
			log.Printf("[Alert Engine] Failed to parse query for alert %s: %v", alert.Name, err)
			continue
		}
		alert.ParsedQuery = parsedQuery

		// Parse cron schedule once at cache load time.
		if alert.AlertType == "scheduled" && alert.ScheduleCron != nil && *alert.ScheduleCron != "" {
			schedule, cronErr := cronParser.Parse(*alert.ScheduleCron)
			if cronErr != nil {
				log.Printf("[Alert Engine] Failed to parse cron for alert %s: %v", alert.Name, cronErr)
				reason := fmt.Sprintf("Auto-disabled: invalid cron expression: %v", cronErr)
				if disableErr := e.disableAlertWithReason(ctx, alert.ID, reason); disableErr != nil {
					log.Printf("[Alert Engine] Failed to disable alert %s: %v", alert.Name, disableErr)
				}
				continue
			}
			alert.CronSchedule = schedule
		}

		if e.dictManager != nil {
			dictActions, dictErr := e.dictManager.GetDictionaryActionsByAlertID(ctx, alert.ID)
			if dictErr != nil {
				log.Printf("[Alert Engine] Failed to load dictionary actions for alert %s: %v", alert.Name, dictErr)
			} else {
				alert.DictionaryActions = dictActions
			}
		}

		newCache[alert.ID] = &alert
		if alert.Enabled {
			alerts = append(alerts, &alert)
		}
	}

	e.mu.Lock()
	e.alertsCache = newCache
	e.cacheRefreshedAt = time.Now()
	e.mu.Unlock()

	return alerts, nil
}

// RefreshAlerts forces a cache refresh (called when alerts are modified via API).
func (e *Engine) RefreshAlerts(ctx context.Context) error {
	_, err := e.refreshAlertsCache(ctx)
	return err
}

// ---------------------------------------------------------------------------
// Throttle helpers
// ---------------------------------------------------------------------------

func (e *Engine) isThrottled(alert *Alert, results []map[string]interface{}) (bool, string) {
	if alert.ThrottleTimeSeconds <= 0 {
		return false, ""
	}
	key := e.throttleKey(alert, results)
	return e.throttleCache.IsThrottled(key, time.Duration(alert.ThrottleTimeSeconds)*time.Second), key
}

func (e *Engine) updateThrottle(alert *Alert, results []map[string]interface{}) {
	if alert.ThrottleTimeSeconds <= 0 {
		return
	}
	e.throttleCache.Set(
		e.throttleKey(alert, results),
		time.Duration(alert.ThrottleTimeSeconds)*time.Second,
	)
}

func (e *Engine) throttleKey(alert *Alert, results []map[string]interface{}) string {
	if alert.ThrottleField != "" && len(results) > 0 {
		if val, ok := results[0][alert.ThrottleField]; ok {
			return fmt.Sprintf("%s:%s:%v", alert.ID, alert.ThrottleField, val)
		}
	}
	return fmt.Sprintf("%s:global", alert.ID)
}

// ---------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------

func (e *Engine) updateLastTriggered(ctx context.Context, alertID string) error {
	_, err := e.pg.Exec(ctx, `UPDATE alerts SET last_triggered = NOW() WHERE id = $1`, alertID)
	return err
}

func (e *Engine) updateLastEvaluated(ctx context.Context, alertID string, t time.Time) error {
	_, err := e.pg.Exec(ctx, `UPDATE alerts SET last_evaluated_at = $2 WHERE id = $1`, alertID, t)
	return err
}

func (e *Engine) getAlertsFractalID(ctx context.Context) string {
	e.alertsFractalMu.RLock()
	id := e.alertsFractalID
	e.alertsFractalMu.RUnlock()
	if id != "" {
		return id
	}

	e.alertsFractalMu.Lock()
	defer e.alertsFractalMu.Unlock()
	var fractalID string
	err := e.pg.QueryRow(ctx, "SELECT id FROM fractals WHERE name = 'alerts' AND is_system = true LIMIT 1").Scan(&fractalID)
	if err == nil {
		e.alertsFractalID = fractalID
	}
	return e.alertsFractalID
}

func (e *Engine) recordExecution(ctx context.Context, alertID string, fractalID string, logCount int, throttled bool, throttleKey string, executionTimeMs int, webhookResults []WebhookResult, fractalResults []FractalResult, emailResults []EmailResult) error {
	webhookResultsJSON, err := json.Marshal(webhookResults)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook results: %w", err)
	}

	fractalResultsJSON, err := json.Marshal(fractalResults)
	if err != nil {
		return fmt.Errorf("failed to marshal fractal results: %w", err)
	}

	emailResultsJSON, err := json.Marshal(emailResults)
	if err != nil {
		return fmt.Errorf("failed to marshal email results: %w", err)
	}

	_, err = e.pg.Exec(ctx,
		`INSERT INTO alert_executions (alert_id, fractal_id, log_count, throttled, throttle_key, execution_time_ms, webhook_results, fractal_results, email_results)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		alertID, fractalID, logCount, throttled, throttleKey, executionTimeMs, string(webhookResultsJSON), string(fractalResultsJSON), string(emailResultsJSON),
	)
	if err != nil {
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return nil
		}
		return fmt.Errorf("failed to record alert execution: %w", err)
	}
	return nil
}

func (e *Engine) disableAlertWithReason(ctx context.Context, alertID string, reason string) error {
	_, err := e.pg.Exec(ctx,
		`UPDATE alerts SET enabled = false, disabled_reason = $1, updated_at = NOW() WHERE id = $2`,
		reason, alertID,
	)
	if err != nil {
		return fmt.Errorf("failed to disable alert: %w", err)
	}
	e.RefreshAlerts(ctx)
	return nil
}

// ---------------------------------------------------------------------------
// Retention cleanup
// ---------------------------------------------------------------------------

// maybeRunRetention deletes old alert_executions rows once per hour.
func (e *Engine) maybeRunRetention() {
	if time.Since(e.lastCleanupAt) < retentionInterval {
		return
	}
	e.lastCleanupAt = time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := e.pg.Exec(ctx,
		`DELETE FROM alert_executions WHERE triggered_at < NOW() - INTERVAL '1 day' * $1`,
		retentionDays,
	)
	if err != nil {
		log.Printf("[Alert Engine] Retention cleanup failed: %v", err)
		return
	}
	if count, _ := result.RowsAffected(); count > 0 {
		log.Printf("[Alert Engine] Retention cleanup removed %d old executions", count)
	}
}

// ---------------------------------------------------------------------------
// ThrottleCache
// ---------------------------------------------------------------------------

type ThrottleCache struct {
	entries map[string]time.Time
	mu      sync.RWMutex
}

func NewThrottleCache() *ThrottleCache {
	cache := &ThrottleCache{
		entries: make(map[string]time.Time),
	}
	go cache.cleanupLoop()
	return cache
}

func (tc *ThrottleCache) IsThrottled(key string, duration time.Duration) bool {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	if expiry, exists := tc.entries[key]; exists {
		return time.Now().Before(expiry)
	}
	return false
}

func (tc *ThrottleCache) Set(key string, duration time.Duration) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.entries[key] = time.Now().Add(duration)
}

func (tc *ThrottleCache) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		tc.cleanup()
	}
}

func (tc *ThrottleCache) cleanup() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	now := time.Now()
	for key, expiry := range tc.entries {
		if now.After(expiry) {
			delete(tc.entries, key)
		}
	}
}
