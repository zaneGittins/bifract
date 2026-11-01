package watermark

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/lib/pq"
)

const (
	// PublishInterval is how often each writing process reports its W. A late
	// report is safe: W only trails, it never overstates what is visible.
	PublishInterval = 5 * time.Second
	// StaleAfter is the heartbeat age at which a node is reported as holding W.
	StaleAfter = 3 * PublishInterval
	// NodeTTL drops a node that stopped reporting, so a crashed pod cannot hold
	// every reader forever. Rows it had pending are lost with it.
	NodeTTL = 5 * time.Minute
	// BacklogLag is the lag past which the write tier counts as backed up: inserts
	// are retrying or queued well past a quiet partition's flush interval.
	BacklogLag = 2 * time.Minute
)

// NodeID names this process's row: its role and host. A restarted container
// reuses its row rather than leaving a stale one behind for NodeTTL.
func NodeID(role string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return role + "@" + host
}

// Publisher reports a tracker's W to Postgres on an interval.
type Publisher struct {
	db   *sql.DB
	t    *Tracker
	node string
	stop chan struct{}
	done chan struct{}
}

func NewPublisher(db *sql.DB, t *Tracker, node string) *Publisher {
	return &Publisher{db: db, t: t, node: node}
}

// Publish writes the current W. heartbeat_at is Postgres time, so staleness is
// judged on one clock whatever the pods' skew.
func (p *Publisher) Publish(ctx context.Context) error {
	_, err := p.db.ExecContext(ctx,
		`INSERT INTO ingest_watermarks (node_id, watermark, heartbeat_at) VALUES ($1, $2, now())
		 ON CONFLICT (node_id) DO UPDATE SET watermark = EXCLUDED.watermark, heartbeat_at = now()`,
		p.node, p.t.Watermark())
	return err
}

// Start publishes on PublishInterval until Stop.
func (p *Publisher) Start() {
	p.stop = make(chan struct{})
	p.done = make(chan struct{})
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(PublishInterval)
		defer ticker.Stop()
		failing := false
		for {
			select {
			case <-p.stop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), PublishInterval)
				err := p.Publish(ctx)
				cancel()
				if err != nil && !failing {
					log.Printf("[Watermark] publish %s failed (readers hold at the last value): %v", p.node, err)
				} else if err == nil && failing {
					log.Printf("[Watermark] publish %s recovered", p.node)
				}
				failing = err != nil
			}
		}
	}()
}

// Stop ends publishing and deletes the node's row. Call it after the process has
// drained its writes, so nothing it stamped is still pending.
func (p *Publisher) Stop(ctx context.Context) {
	if p.stop != nil {
		close(p.stop)
		<-p.done
		p.stop = nil
	}
	if _, err := p.db.ExecContext(ctx, `DELETE FROM ingest_watermarks WHERE node_id = $1`, p.node); err != nil {
		log.Printf("[Watermark] remove %s on shutdown: %v (expires after %v)", p.node, err, NodeTTL)
	}
}

// NodeState is one node's last report, with its heartbeat age on Postgres's clock.
type NodeState struct {
	Node string
	W    time.Time
	Age  time.Duration
}

// Snapshot is the combined watermark across every writing process.
type Snapshot struct {
	W   time.Time
	Lag time.Duration
	// Held names nodes whose heartbeat is stale; W stays at their last value.
	Held []string
	// Expired lists nodes dropped after NodeTTL.
	Expired []string
}

// Combine takes the oldest W among live nodes, capped at now.
func Combine(nodes []NodeState, now time.Time) Snapshot {
	s := Snapshot{W: now}
	for _, n := range nodes {
		if n.Age > NodeTTL {
			s.Expired = append(s.Expired, n.Node)
			continue
		}
		if n.Age > StaleAfter {
			s.Held = append(s.Held, fmt.Sprintf("no heartbeat for %s from %s", n.Age.Round(time.Second), n.Node))
		}
		if n.W.Before(s.W) {
			s.W = n.W
		}
	}
	sort.Strings(s.Held)
	s.Lag = now.Sub(s.W)
	return s
}

// Read returns the combined watermark. An error means it is unknown, and a reader
// must not advance: treating it as now is the bug this package exists to prevent.
func Read(ctx context.Context, db *sql.DB, now time.Time) (Snapshot, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT node_id, watermark, EXTRACT(EPOCH FROM now() - heartbeat_at)::float8 FROM ingest_watermarks`)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read ingest watermark: %w", err)
	}
	var nodes []NodeState
	for rows.Next() {
		var n NodeState
		var age float64
		if err := rows.Scan(&n.Node, &n.W, &age); err != nil {
			rows.Close()
			return Snapshot{}, fmt.Errorf("read ingest watermark: %w", err)
		}
		n.Age = time.Duration(age * float64(time.Second))
		nodes = append(nodes, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("read ingest watermark: %w", err)
	}

	s := Combine(nodes, now)
	if len(s.Expired) > 0 {
		log.Printf("[Watermark] dropping %v: no heartbeat for over %v, rows they had pending are presumed lost", s.Expired, NodeTTL)
		if _, err := db.ExecContext(ctx,
			`DELETE FROM ingest_watermarks WHERE node_id = ANY($1) AND heartbeat_at < now() - make_interval(secs => $2)`,
			pq.Array(s.Expired), NodeTTL.Seconds()); err != nil {
			log.Printf("[Watermark] prune expired nodes: %v", err)
		}
	}
	return s, nil
}
