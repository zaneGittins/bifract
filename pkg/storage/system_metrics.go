package storage

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// SystemMetricSample is one persisted health metric reading. fractal_id is
// optional and left empty for node/global metrics.
type SystemMetricSample struct {
	Metric    string
	Node      string
	FractalID string
	Value     float64
}

// MetricPoint is a single bucketed time-series point.
type MetricPoint struct {
	Bucket time.Time
	Value  float64
}

// InsertSystemMetrics writes a batch of samples that share a single timestamp.
// Batching keeps Postgres write load to one statement per collection tick.
func (c *PostgresClient) InsertSystemMetrics(ctx context.Context, ts time.Time, samples []SystemMetricSample) error {
	if len(samples) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO system_metrics (ts, metric, node, fractal_id, value) VALUES ")
	args := make([]interface{}, 0, len(samples)*5)
	for i, s := range samples {
		if i > 0 {
			b.WriteByte(',')
		}
		n := i * 5
		fmt.Fprintf(&b, "($%d,$%d,$%d,$%d,$%d)", n+1, n+2, n+3, n+4, n+5)
		args = append(args, ts, s.Metric, s.Node, s.FractalID, s.Value)
	}
	_, err := c.Exec(ctx, b.String(), args...)
	return err
}

// QueryMetricSeries returns a single time-bucketed series for a metric, averaged
// across all nodes, ordered by time ascending. bucketSec controls the bucket
// width; since bounds the lookback window.
func (c *PostgresClient) QueryMetricSeries(ctx context.Context, metric string, since time.Duration, bucketSec int) ([]MetricPoint, error) {
	if bucketSec <= 0 {
		bucketSec = 60
	}
	rows, err := c.Query(ctx,
		`SELECT date_bin(make_interval(secs => $1), ts, TIMESTAMPTZ 'epoch') AS bucket,
		        avg(value) AS v
		   FROM system_metrics
		  WHERE metric = $2 AND ts >= now() - make_interval(secs => $3)
		  GROUP BY bucket
		  ORDER BY bucket`,
		bucketSec, metric, int(since.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var points []MetricPoint
	for rows.Next() {
		var p MetricPoint
		if err := rows.Scan(&p.Bucket, &p.Value); err != nil {
			return nil, err
		}
		points = append(points, p)
	}
	return points, rows.Err()
}

// QueryMetricSeriesByNode returns a time-bucketed series per node, excluding the
// empty-node aggregate. Used for per-node CPU in cluster mode; returns nil when
// no named nodes are present (single-node installs).
func (c *PostgresClient) QueryMetricSeriesByNode(ctx context.Context, metric string, since time.Duration, bucketSec int) (map[string][]MetricPoint, error) {
	if bucketSec <= 0 {
		bucketSec = 60
	}
	rows, err := c.Query(ctx,
		`SELECT node,
		        date_bin(make_interval(secs => $1), ts, TIMESTAMPTZ 'epoch') AS bucket,
		        avg(value) AS v
		   FROM system_metrics
		  WHERE metric = $2 AND ts >= now() - make_interval(secs => $3) AND node <> ''
		  GROUP BY node, bucket
		  ORDER BY bucket`,
		bucketSec, metric, int(since.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string][]MetricPoint{}
	for rows.Next() {
		var node string
		var p MetricPoint
		if err := rows.Scan(&node, &p.Bucket, &p.Value); err != nil {
			return nil, err
		}
		result[node] = append(result[node], p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// PruneSystemMetrics deletes samples older than retentionDays and returns the
// number of rows removed.
func (c *PostgresClient) PruneSystemMetrics(ctx context.Context, retentionDays int) (int64, error) {
	res, err := c.Exec(ctx,
		`DELETE FROM system_metrics WHERE ts < now() - make_interval(days => $1)`,
		retentionDays)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
