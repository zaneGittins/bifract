package dashboards

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"bifract/pkg/bqlvars"
	"bifract/pkg/query"
	"bifract/pkg/sse"
	"bifract/pkg/storage"
)

// errWidgetMismatch guards against executing a widget that does not belong to
// the dashboard named in the request path.
var errWidgetMismatch = errors.New("widget does not belong to dashboard")

// QueryRunner executes a BQL string server-side. *query.QueryHandler satisfies it.
type QueryRunner interface {
	ExecuteBQL(ctx context.Context, queryStr, fractalID, prismID string, start, end time.Time) (*query.ExecuteResult, error)
}

// HealthChecker reports whether the backend is healthy enough to take on
// background query load. *ingest.IngestQueue satisfies it (CPU/disk/failure
// backpressure), so dashboard refreshes yield to ingestion under load.
type HealthChecker interface {
	Healthy() bool
}

// ExecutorConfig tunes the background dashboard executor.
type ExecutorConfig struct {
	// Tick is the base scheduling granularity. Dashboards are checked for
	// due-ness this often; it bounds how precisely intervals are honored.
	Tick time.Duration
	// MinInterval is the floor for any per-dashboard refresh interval. It
	// prevents a user from setting a cadence that would overload ClickHouse.
	MinInterval time.Duration
	// Workers caps concurrent widget executions across all dashboards.
	Workers int
}

// DefaultExecutorConfig returns conservative defaults.
func DefaultExecutorConfig() ExecutorConfig {
	return ExecutorConfig{
		Tick:        5 * time.Second,
		MinInterval: 10 * time.Second,
		Workers:     4,
	}
}

// Executor periodically re-runs the widgets of dashboards that currently have
// live viewers, pushing fresh results over SSE. It is presence-gated (only
// dashboards with a live SSE room are refreshed), backpressure-aware (skips
// cycles while the backend is unhealthy), coalesced (one execution shared across
// all viewers of a dashboard), and bounded by a worker pool.
type Executor struct {
	pg     *storage.PostgresClient
	runner QueryRunner
	hub    *sse.Hub
	health HealthChecker
	cfg    ExecutorConfig

	sem chan struct{} // worker-pool semaphore

	mu       sync.Mutex
	lastRun  map[string]time.Time // dashboardID -> last refresh time
	inFlight map[string]bool      // dashboardID -> a refresh is currently running

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewExecutor constructs a dashboard executor. health may be nil to disable
// backpressure gating (not recommended in production).
func NewExecutor(pg *storage.PostgresClient, runner QueryRunner, hub *sse.Hub, health HealthChecker, cfg ExecutorConfig) *Executor {
	if cfg.Tick <= 0 {
		cfg.Tick = DefaultExecutorConfig().Tick
	}
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = DefaultExecutorConfig().MinInterval
	}
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultExecutorConfig().Workers
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Executor{
		pg:       pg,
		runner:   runner,
		hub:      hub,
		health:   health,
		cfg:      cfg,
		sem:      make(chan struct{}, cfg.Workers),
		lastRun:  make(map[string]time.Time),
		inFlight: make(map[string]bool),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start launches the background scheduling loop.
func (e *Executor) Start() {
	if e.hub == nil || e.pg == nil || e.runner == nil {
		log.Println("[DashboardExecutor] Disabled (missing dependency)")
		return
	}
	e.wg.Add(1)
	go e.loop()
	log.Printf("[DashboardExecutor] Started (tick=%s, min_interval=%s, workers=%d)", e.cfg.Tick, e.cfg.MinInterval, e.cfg.Workers)
}

// Stop signals the loop to exit and waits for in-flight executions to finish.
func (e *Executor) Stop() {
	e.cancel()
	e.wg.Wait()
}

func (e *Executor) loop() {
	defer e.wg.Done()
	ticker := time.NewTicker(e.cfg.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.tick()
		}
	}
}

// tick discovers dashboards with live viewers and refreshes any that are due.
func (e *Executor) tick() {
	// Yield to ingestion / interactive load when the backend is under pressure.
	if e.health != nil && !e.health.Healthy() {
		return
	}

	rooms := e.hub.RoomsWithPrefix("dashboard:")
	if len(rooms) == 0 {
		return
	}

	now := time.Now()
	active := make(map[string]bool, len(rooms))
	for _, room := range rooms {
		dashboardID := strings.TrimPrefix(room, "dashboard:")
		if dashboardID == "" {
			continue
		}
		active[dashboardID] = true

		d, err := e.pg.GetDashboard(e.ctx, dashboardID)
		if err != nil {
			continue
		}
		interval := e.effectiveInterval(d)
		if interval <= 0 {
			continue // auto-refresh off for this dashboard
		}

		// Skip if not yet due, or if a previous refresh of this dashboard is still
		// running (a slow dashboard must not pile up overlapping refreshes).
		e.mu.Lock()
		due := now.Sub(e.lastRun[dashboardID]) >= interval && !e.inFlight[dashboardID]
		if due {
			e.lastRun[dashboardID] = now
			e.inFlight[dashboardID] = true
		}
		e.mu.Unlock()
		if !due {
			continue
		}

		// Dispatch on its own goroutine so a saturated worker pool never stalls
		// the scan of the remaining dashboards.
		e.wg.Add(1)
		go func(dash *storage.Dashboard) {
			defer e.wg.Done()
			defer func() {
				e.mu.Lock()
				delete(e.inFlight, dash.ID)
				e.mu.Unlock()
			}()
			e.refreshDashboard(dash)
		}(d)
	}

	// Drop bookkeeping for dashboards no longer being viewed.
	e.mu.Lock()
	for id := range e.lastRun {
		if !active[id] {
			delete(e.lastRun, id)
		}
	}
	e.mu.Unlock()
}

// refreshDashboard executes every widget of a dashboard through the worker pool,
// blocking until all of them finish so the caller's in-flight guard stays
// accurate. Concurrency is bounded globally by the executor's semaphore.
func (e *Executor) refreshDashboard(d *storage.Dashboard) {
	widgets, err := e.pg.GetDashboardWidgets(e.ctx, d.ID)
	if err != nil {
		log.Printf("[DashboardExecutor] Failed to load widgets for dashboard %s: %v", d.ID, err)
		return
	}
	var wg sync.WaitGroup
	for i := range widgets {
		w := widgets[i]
		if strings.TrimSpace(w.QueryContent) == "" {
			continue
		}
		select {
		case e.sem <- struct{}{}:
		case <-e.ctx.Done():
			wg.Wait()
			return
		}
		wg.Add(1)
		go func(widget storage.DashboardWidget) {
			defer wg.Done()
			defer func() { <-e.sem }()
			if _, _, err := e.executeWidget(e.ctx, d, &widget, ""); err != nil {
				log.Printf("[DashboardExecutor] Widget %s execution failed: %v", widget.ID, err)
			}
		}(w)
	}
	wg.Wait()
}

// executeWidget runs a single widget's query, persists the results as the
// authoritative cache, and broadcasts them to viewers over SSE. excludeClientID,
// when non-empty, omits the originating client from the broadcast (it renders
// from the direct response instead). It returns the serialized result payload
// and the resolved chart type so on-demand callers can return them directly.
func (e *Executor) executeWidget(ctx context.Context, d *storage.Dashboard, w *storage.DashboardWidget, excludeClientID string) ([]byte, string, error) {
	start, end := computeTimeRange(d)
	queryStr := bqlvars.Substitute(w.QueryContent, d.Variables)

	res, err := e.runner.ExecuteBQL(ctx, queryStr, d.FractalID, d.PrismID, start, end)
	if err != nil {
		return nil, "", err
	}

	resultJSON, err := json.Marshal(res)
	if err != nil {
		return nil, "", err
	}

	chartType := res.ChartType
	if chartType == "" {
		chartType = "table"
	}

	if err := e.pg.UpdateDashboardWidgetResults(ctx, w.ID, string(resultJSON), &chartType); err != nil {
		return nil, "", err
	}

	if e.hub != nil {
		e.hub.Broadcast("dashboard:"+d.ID, sse.Event{
			Type: sse.WidgetResultsUpdated,
			Data: map[string]interface{}{
				"id":           w.ID,
				"last_results": string(resultJSON),
				"chart_type":   chartType,
			},
		}, excludeClientID)
	}

	return resultJSON, chartType, nil
}

// ExecuteWidgetByID runs one widget on demand (e.g. the play button). It loads
// the dashboard and widget, executes, stores, and broadcasts, returning the
// result payload for the immediate HTTP response.
func (e *Executor) ExecuteWidgetByID(ctx context.Context, dashboardID, widgetID, excludeClientID string) ([]byte, string, error) {
	d, err := e.pg.GetDashboard(ctx, dashboardID)
	if err != nil {
		return nil, "", err
	}
	w, err := e.pg.GetDashboardWidget(ctx, widgetID)
	if err != nil {
		return nil, "", err
	}
	if w.DashboardID != dashboardID {
		return nil, "", errWidgetMismatch
	}
	return e.executeWidget(ctx, d, w, excludeClientID)
}

// executeWidgetPreview runs a widget with transient overrides and returns the
// result payload WITHOUT persisting it as the canonical cache or broadcasting it
// over SSE. It backs per-user pivot drilldowns: a drilldown is a private view, so
// it must not overwrite what collaborators see. overrideVars (when non-nil)
// replaces the dashboard's stored variable set for substitution.
func (e *Executor) executeWidgetPreview(ctx context.Context, d *storage.Dashboard, w *storage.DashboardWidget, overrideVars json.RawMessage, start, end time.Time) ([]byte, string, error) {
	vars := d.Variables
	if overrideVars != nil {
		vars = overrideVars
	}
	queryStr := bqlvars.Substitute(w.QueryContent, vars)

	res, err := e.runner.ExecuteBQL(ctx, queryStr, d.FractalID, d.PrismID, start, end)
	if err != nil {
		return nil, "", err
	}
	resultJSON, err := json.Marshal(res)
	if err != nil {
		return nil, "", err
	}
	chartType := res.ChartType
	if chartType == "" {
		chartType = "table"
	}
	return resultJSON, chartType, nil
}

// ExecuteWidgetPreviewByID loads the dashboard+widget and runs a transient
// preview (see executeWidgetPreview). When start and end are both non-zero they
// override the computed dashboard window; otherwise the dashboard's own range is
// used.
func (e *Executor) ExecuteWidgetPreviewByID(ctx context.Context, dashboardID, widgetID string, overrideVars json.RawMessage, start, end time.Time) ([]byte, string, error) {
	d, err := e.pg.GetDashboard(ctx, dashboardID)
	if err != nil {
		return nil, "", err
	}
	w, err := e.pg.GetDashboardWidget(ctx, widgetID)
	if err != nil {
		return nil, "", err
	}
	if w.DashboardID != dashboardID {
		return nil, "", errWidgetMismatch
	}
	ds, de := computeTimeRange(d)
	if !start.IsZero() && !end.IsZero() {
		ds, de = start, end
	}
	return e.executeWidgetPreview(ctx, d, w, overrideVars, ds, de)
}

// ExecuteDashboardNow runs all widgets of a dashboard on demand, returning when
// every widget has finished (or errored). Used by the "refresh all" endpoint.
func (e *Executor) ExecuteDashboardNow(ctx context.Context, dashboardID, excludeClientID string) error {
	d, err := e.pg.GetDashboard(ctx, dashboardID)
	if err != nil {
		return err
	}
	widgets, err := e.pg.GetDashboardWidgets(ctx, dashboardID)
	if err != nil {
		return err
	}
	// Mark as just-refreshed so the background loop doesn't immediately re-run it.
	e.mu.Lock()
	e.lastRun[dashboardID] = time.Now()
	e.mu.Unlock()

	var wg sync.WaitGroup
	for i := range widgets {
		w := widgets[i]
		if strings.TrimSpace(w.QueryContent) == "" {
			continue
		}
		select {
		case e.sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		wg.Add(1)
		go func(widget storage.DashboardWidget) {
			defer wg.Done()
			defer func() { <-e.sem }()
			if _, _, err := e.executeWidget(ctx, d, &widget, excludeClientID); err != nil {
				log.Printf("[DashboardExecutor] Widget %s execution failed: %v", widget.ID, err)
			}
		}(w)
	}
	wg.Wait()
	return nil
}

// effectiveInterval resolves a dashboard's configured refresh cadence into a
// concrete duration: 0 = off, <0 = auto (derived from time range), >0 = fixed
// (clamped up to the configured minimum floor).
func (e *Executor) effectiveInterval(d *storage.Dashboard) time.Duration {
	switch {
	case d.RefreshInterval == 0:
		return 0
	case d.RefreshInterval < 0:
		return e.clampInterval(autoIntervalSeconds(d.TimeRangeType))
	default:
		return e.clampInterval(d.RefreshInterval)
	}
}

func (e *Executor) clampInterval(seconds int) time.Duration {
	min := int(e.cfg.MinInterval / time.Second)
	if seconds < min {
		seconds = min
	}
	return time.Duration(seconds) * time.Second
}

// autoIntervalSeconds maps a dashboard time range to a sensible auto cadence:
// shorter windows refresh more often; long historical windows barely change.
func autoIntervalSeconds(timeRangeType string) int {
	switch timeRangeType {
	case "last1h":
		return 30
	case "last24h":
		return 300
	case "last7d":
		return 1800
	case "last30d", "all":
		return 3600
	default:
		return 300
	}
}

// computeTimeRange mirrors the frontend getDashboardTimeRange so server-executed
// results match what an interactive run would produce.
func computeTimeRange(d *storage.Dashboard) (time.Time, time.Time) {
	now := time.Now()
	switch d.TimeRangeType {
	case "last1h":
		return now.Add(-time.Hour), now
	case "last24h":
		return now.Add(-24 * time.Hour), now
	case "last7d":
		return now.Add(-7 * 24 * time.Hour), now
	case "last30d":
		return now.Add(-30 * 24 * time.Hour), now
	case "all":
		return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), now
	case "custom":
		if d.TimeRangeStart != nil && d.TimeRangeEnd != nil {
			return *d.TimeRangeStart, *d.TimeRangeEnd
		}
		return now.Add(-24 * time.Hour), now
	default:
		return now.Add(-24 * time.Hour), now
	}
}

// Variable substitution is centralized in pkg/bqlvars (quote- and
// boundary-aware, prefix-safe) and shared by search, dashboards and notebooks.
