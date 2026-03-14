// Package metrics provides an optional Prometheus metrics exporter for Bifract.
//
// Enabled via BIFRACT_METRICS_ENABLED=true. Listens on a separate address
// (default :9090) so it can be firewalled independently from the main app.
// Exports read-only counters and gauges; no secrets or log data are exposed.
package metrics

import (
	"context"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// IngestSource provides read-only access to ingest queue metrics.
type IngestSource interface {
	// Counters (monotonically increasing).
	AcceptedTotal() int64
	InsertedTotal() int64
	InsertErrorsTotal() int64
	QueueDropsTotal() int64
	RetriesTotal() int64

	// Gauges (point-in-time).
	Depth() int
	Healthy() bool
	CPUPressure() bool
	ConsecutiveFailures() int64
}

// AlertSource provides read-only access to alert engine metrics.
type AlertSource interface {
	CachedAlertCount() int
	IsRunning() bool
}

// Collector registers Prometheus metrics and periodically collects them
// from the running Bifract subsystems.
type Collector struct {
	reg *prometheus.Registry

	// Ingest counters.
	ingestAccepted     prometheus.Counter
	ingestInserted     prometheus.Counter
	ingestErrors       prometheus.Counter
	ingestDrops        prometheus.Counter
	ingestRetries      prometheus.Counter

	// Ingest gauges.
	ingestQueueDepth   prometheus.Gauge
	ingestHealthy      prometheus.Gauge
	ingestCPUPressure  prometheus.Gauge
	ingestConsecFails  prometheus.Gauge

	// Alert gauges.
	alertsCached       prometheus.Gauge
	alertsRunning      prometheus.Gauge

	// Build info.
	buildInfo          *prometheus.GaugeVec

	// Sources (set via Attach methods).
	ingestSource atomic.Pointer[IngestSource]
	alertSource  atomic.Pointer[AlertSource]

	// Snapshot of previous counter values for computing deltas.
	prevAccepted     atomic.Int64
	prevInserted     atomic.Int64
	prevErrors       atomic.Int64
	prevDrops        atomic.Int64
	prevRetries      atomic.Int64
}

// New creates a Collector with a dedicated Prometheus registry (no default
// Go runtime metrics or process metrics are exposed).
func New(version string) *Collector {
	reg := prometheus.NewRegistry()

	c := &Collector{
		reg: reg,

		ingestAccepted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bifract",
			Subsystem: "ingest",
			Name:      "accepted_total",
			Help:      "Total logs accepted into the ingestion queue.",
		}),
		ingestInserted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bifract",
			Subsystem: "ingest",
			Name:      "inserted_total",
			Help:      "Total logs successfully inserted into ClickHouse.",
		}),
		ingestErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bifract",
			Subsystem: "ingest",
			Name:      "insert_errors_total",
			Help:      "Total logs that failed to insert after all retries.",
		}),
		ingestDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bifract",
			Subsystem: "ingest",
			Name:      "drops_total",
			Help:      "Total logs dropped due to backpressure.",
		}),
		ingestRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "bifract",
			Subsystem: "ingest",
			Name:      "retries_total",
			Help:      "Total retry attempts for failed batch inserts.",
		}),

		ingestQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bifract",
			Subsystem: "ingest",
			Name:      "queue_depth",
			Help:      "Current number of pending batches in the ingestion queue.",
		}),
		ingestHealthy: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bifract",
			Subsystem: "ingest",
			Name:      "healthy",
			Help:      "1 if the ingestion queue is healthy, 0 if under backpressure.",
		}),
		ingestCPUPressure: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bifract",
			Subsystem: "ingest",
			Name:      "cpu_pressure",
			Help:      "1 if ClickHouse CPU backpressure is active, 0 otherwise.",
		}),
		ingestConsecFails: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bifract",
			Subsystem: "ingest",
			Name:      "consecutive_failures",
			Help:      "Number of consecutive ClickHouse insert failures.",
		}),

		alertsCached: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bifract",
			Subsystem: "alerts",
			Name:      "cached_count",
			Help:      "Number of alerts currently cached by the evaluation engine.",
		}),
		alertsRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "bifract",
			Subsystem: "alerts",
			Name:      "evaluating",
			Help:      "1 if an alert evaluation cycle is currently running.",
		}),

		buildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "bifract",
			Name:      "build_info",
			Help:      "Build information. Value is always 1; labels carry metadata.",
		}, []string{"version"}),
	}

	// Register all metrics with the dedicated registry.
	reg.MustRegister(
		c.ingestAccepted,
		c.ingestInserted,
		c.ingestErrors,
		c.ingestDrops,
		c.ingestRetries,
		c.ingestQueueDepth,
		c.ingestHealthy,
		c.ingestCPUPressure,
		c.ingestConsecFails,
		c.alertsCached,
		c.alertsRunning,
		c.buildInfo,
	)

	c.buildInfo.WithLabelValues(version).Set(1)

	return c
}

// AttachIngest registers the ingest queue as a metrics source.
func (c *Collector) AttachIngest(src IngestSource) {
	c.ingestSource.Store(&src)
}

// AttachAlerts registers the alert engine as a metrics source.
func (c *Collector) AttachAlerts(src AlertSource) {
	c.alertSource.Store(&src)
}

// collect reads current values from all attached sources and updates
// the Prometheus metrics. Called periodically by the background loop.
func (c *Collector) collect() {
	if src := c.ingestSource.Load(); src != nil {
		s := *src

		// Counters: compute delta from previous snapshot and add.
		cur := s.AcceptedTotal()
		if delta := cur - c.prevAccepted.Swap(cur); delta > 0 {
			c.ingestAccepted.Add(float64(delta))
		}
		cur = s.InsertedTotal()
		if delta := cur - c.prevInserted.Swap(cur); delta > 0 {
			c.ingestInserted.Add(float64(delta))
		}
		cur = s.InsertErrorsTotal()
		if delta := cur - c.prevErrors.Swap(cur); delta > 0 {
			c.ingestErrors.Add(float64(delta))
		}
		cur = s.QueueDropsTotal()
		if delta := cur - c.prevDrops.Swap(cur); delta > 0 {
			c.ingestDrops.Add(float64(delta))
		}
		cur = s.RetriesTotal()
		if delta := cur - c.prevRetries.Swap(cur); delta > 0 {
			c.ingestRetries.Add(float64(delta))
		}

		// Gauges: set directly.
		c.ingestQueueDepth.Set(float64(s.Depth()))
		if s.Healthy() {
			c.ingestHealthy.Set(1)
		} else {
			c.ingestHealthy.Set(0)
		}
		if s.CPUPressure() {
			c.ingestCPUPressure.Set(1)
		} else {
			c.ingestCPUPressure.Set(0)
		}
		c.ingestConsecFails.Set(float64(s.ConsecutiveFailures()))
	}

	if src := c.alertSource.Load(); src != nil {
		s := *src
		c.alertsCached.Set(float64(s.CachedAlertCount()))
		if s.IsRunning() {
			c.alertsRunning.Set(1)
		} else {
			c.alertsRunning.Set(0)
		}
	}
}

// Handler returns an http.Handler that serves Prometheus metrics.
// Only the dedicated registry is used (no default Go/process collectors).
func (c *Collector) Handler() http.Handler {
	return promhttp.HandlerFor(c.reg, promhttp.HandlerOpts{})
}

// Server manages the dedicated metrics HTTP listener.
type Server struct {
	collector *Collector
	httpSrv   *http.Server
	stopCh    chan struct{}
}

// NewServer creates a metrics server listening on addr (e.g. ":9090").
func NewServer(addr string, collector *Collector) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", collector.Handler())

	return &Server{
		collector: collector,
		httpSrv: &http.Server{
			Addr:         addr,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  30 * time.Second,
		},
		stopCh: make(chan struct{}),
	}
}

// Start begins serving metrics and collecting from sources every 5 seconds.
func (s *Server) Start() {
	// Background collection loop.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				s.collector.collect()
			}
		}
	}()

	go func() {
		log.Printf("Prometheus metrics server listening on %s/metrics", s.httpSrv.Addr)
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Metrics server error: %v", err)
		}
	}()
}

// Shutdown gracefully stops the metrics server.
func (s *Server) Shutdown() {
	close(s.stopCh)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.httpSrv.Shutdown(ctx); err != nil {
		log.Printf("Metrics server shutdown error: %v", err)
	}
}
