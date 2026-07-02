package storage

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync/atomic"
	"time"
)

const (
	ddlQueueMetric        = "ddl_queue_pending"
	ddlQueueWindow        = 2 * time.Hour
	ddlQueueBucketSec     = 60
	ddlQueueWarnThreshold = 5
	ddlQueueCritThreshold = 50
	ddlQueueConsecForWarn = 3
)

// DDLQueueSample is a single time-series data point for the DDL task queue.
type DDLQueueSample struct {
	Time    int64 `json:"time"`
	Pending int64 `json:"pending"`
}

// DDLMonitor polls system.distributed_ddl_queue every 60 seconds and fires
// onEvent when the pending task count crosses warning or critical thresholds.
// Only active in cluster mode — Start is a no-op for single-node deployments.
type DDLMonitor struct {
	ch          *ClickHouseClient
	pg          *PostgresClient
	onEvent     func(event string, fields map[string]string)
	lastPending atomic.Int64
	stop        chan struct{}
}

func NewDDLMonitor(ch *ClickHouseClient, pg *PostgresClient, onEvent func(string, map[string]string)) *DDLMonitor {
	return &DDLMonitor{
		ch:      ch,
		pg:      pg,
		onEvent: onEvent,
		stop:    make(chan struct{}),
	}
}

// History returns DDL queue samples from Postgres (oldest first) over the
// given lookback window and bucket width. Returns nil when persistence is
// unavailable or in single-node mode (monitor never writes samples).
func (m *DDLMonitor) History(ctx context.Context, since time.Duration, bucketSec int) []DDLQueueSample {
	if m.pg == nil {
		return nil
	}
	if since <= 0 {
		since = ddlQueueWindow
	}
	if bucketSec <= 0 {
		bucketSec = ddlQueueBucketSec
	}
	pts, err := m.pg.QueryMetricSeries(ctx, ddlQueueMetric, since, bucketSec)
	if err != nil {
		log.Printf("[DDLMonitor] history query failed: %v", err)
		return nil
	}
	out := make([]DDLQueueSample, 0, len(pts))
	for _, p := range pts {
		out = append(out, DDLQueueSample{Time: p.Bucket.Unix(), Pending: int64(math.Round(p.Value))})
	}
	return out
}

// Start begins background polling. No-op for single-node deployments.
func (m *DDLMonitor) Start() {
	if !m.ch.IsCluster() {
		return
	}
	go m.run()
}

// Stop shuts down the background poll loop.
func (m *DDLMonitor) Stop() {
	select {
	case <-m.stop:
	default:
		close(m.stop)
	}
}

func (m *DDLMonitor) run() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	var consecAboveWarn int

	m.poll(&consecAboveWarn)

	for {
		select {
		case <-ticker.C:
			m.poll(&consecAboveWarn)
		case <-m.stop:
			return
		}
	}
}

func (m *DDLMonitor) poll(consecAboveWarn *int) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rows, err := m.ch.Query(ctx,
		"SELECT countIf(status NOT IN ('Finished','Removing')) AS pending FROM system.distributed_ddl_queue")
	if err != nil {
		log.Printf("[DDLMonitor] query failed: %v", err)
		return
	}

	var pending int64
	if len(rows) > 0 {
		pending = distMonInt64(rows[0]["pending"])
	}
	m.lastPending.Store(pending)

	if pending >= ddlQueueCritThreshold {
		m.onEvent("ch.ddl_queue.critical", map[string]string{
			"pending": fmt.Sprintf("%d", pending),
		})
		*consecAboveWarn = 0
	} else if pending > ddlQueueWarnThreshold {
		*consecAboveWarn++
		if *consecAboveWarn >= ddlQueueConsecForWarn {
			m.onEvent("ch.ddl_queue.warning", map[string]string{
				"pending": fmt.Sprintf("%d", pending),
			})
			*consecAboveWarn = 0
		}
	} else {
		*consecAboveWarn = 0
	}

	if m.pg != nil {
		if err := m.pg.InsertSystemMetrics(ctx, time.Now(), []SystemMetricSample{
			{Metric: ddlQueueMetric, Value: float64(pending)},
		}); err != nil {
			log.Printf("[DDLMonitor] failed to persist: %v", err)
		}
	}
}
