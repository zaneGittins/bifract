package storage

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// DistributionQueueStats is the last-sampled state of ClickHouse's async
// distribution queue for logs_distributed. Zero/true values indicate healthy.
type DistributionQueueStats struct {
	DataFiles       int64
	BrokenDataFiles int64
	ErrorCount      int64
	Healthy         bool
}

// DistributionMonitor polls system.distribution_queue every 60 seconds and
// fires onEvent when the queue transitions between healthy and degraded states.
// Only active in cluster mode — Start is a no-op for single-node deployments.
type DistributionMonitor struct {
	ch      *ClickHouseClient
	onEvent func(event string, fields map[string]string)
	state   atomic.Value // stores DistributionQueueStats
	stop    chan struct{}
}

// NewDistributionMonitor creates a monitor. onEvent is called with system-fractal
// event names (ch.distribution.*) on health state transitions.
func NewDistributionMonitor(ch *ClickHouseClient, onEvent func(string, map[string]string)) *DistributionMonitor {
	m := &DistributionMonitor{
		ch:      ch,
		onEvent: onEvent,
		stop:    make(chan struct{}),
	}
	m.state.Store(DistributionQueueStats{Healthy: true})
	return m
}

// Stats returns the most recently sampled distribution queue state.
func (m *DistributionMonitor) Stats() DistributionQueueStats {
	if v := m.state.Load(); v != nil {
		return v.(DistributionQueueStats)
	}
	return DistributionQueueStats{Healthy: true}
}

// Start begins background polling. No-op for single-node deployments.
func (m *DistributionMonitor) Start() {
	if !m.ch.IsCluster() {
		return
	}
	go m.run()
}

// Stop shuts down the background poll loop.
func (m *DistributionMonitor) Stop() {
	select {
	case <-m.stop:
	default:
		close(m.stop)
	}
}

func (m *DistributionMonitor) run() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	var prevErrorCount int64 = -1
	wasDegraded := false

	m.poll(&prevErrorCount, &wasDegraded)

	for {
		select {
		case <-ticker.C:
			m.poll(&prevErrorCount, &wasDegraded)
		case <-m.stop:
			return
		}
	}
}

func (m *DistributionMonitor) poll(prevErrorCount *int64, wasDegraded *bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rows, err := m.ch.Query(ctx, `
		SELECT
			COALESCE(sum(data_files), 0)        AS data_files,
			COALESCE(sum(broken_data_files), 0) AS broken_data_files,
			COALESCE(max(error_count), 0)       AS error_count
		FROM system.distribution_queue
		WHERE table = 'logs_distributed'`)
	if err != nil {
		log.Printf("[DistributionMonitor] query failed: %v", err)
		return
	}

	var dataFiles, broken, errorCount int64
	if len(rows) > 0 {
		dataFiles = distMonInt64(rows[0]["data_files"])
		broken = distMonInt64(rows[0]["broken_data_files"])
		errorCount = distMonInt64(rows[0]["error_count"])
	}

	hasNewErrors := *prevErrorCount >= 0 && errorCount > *prevErrorCount
	*prevErrorCount = errorCount
	healthy := broken == 0 && !hasNewErrors

	if broken > 0 {
		m.onEvent("ch.distribution.broken_data", map[string]string{
			"broken_data_files": fmt.Sprintf("%d", broken),
			"data_files":        fmt.Sprintf("%d", dataFiles),
		})
	}
	if hasNewErrors && !*wasDegraded {
		m.onEvent("ch.distribution.degraded", map[string]string{
			"error_count": fmt.Sprintf("%d", errorCount),
			"data_files":  fmt.Sprintf("%d", dataFiles),
		})
		*wasDegraded = true
	} else if *wasDegraded && healthy && dataFiles == 0 {
		m.onEvent("ch.distribution.healthy", map[string]string{})
		*wasDegraded = false
	}

	m.state.Store(DistributionQueueStats{
		DataFiles:       dataFiles,
		BrokenDataFiles: broken,
		ErrorCount:      errorCount,
		Healthy:         healthy,
	})
}

func distMonInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case int32:
		return int64(n)
	case uint32:
		return int64(n)
	case float64:
		return int64(n)
	case *int64:
		if n != nil {
			return *n
		}
	case *uint64:
		if n != nil {
			return int64(*n)
		}
	}
	return 0
}
