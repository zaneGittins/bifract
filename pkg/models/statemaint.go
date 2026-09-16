package models

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"bifract/pkg/storage"
)

// Model state is maintained by a scheduled reader over logs.ingest_timestamp
// rather than by a materialized view at insert.
//
// An MV is an insert trigger, so anything in its SELECT runs while the log is
// being written. That rules out a dictionary lookup in a model's source: a
// dictionary that fails to load would break ingestion rather than one model, and
// the state would be frozen against the list as it stood when each log arrived,
// so editing the list would never reach data already ingested.
//
// The cost is bounded by the partition key, (fractal_id, toDate(ingest_timestamp)),
// plus the minmax index on ingest_timestamp: a cycle reads only the parts holding
// the window it asks for.
const (
	// stateMaintDefaultInterval is how often a cycle runs. Every model's state
	// lags by at most this much, so it is well under the alert ticker's default.
	stateMaintDefaultInterval = 15 * time.Second

	// stateMaintLag holds the cutoff back from now so a cycle never reads a
	// window that inserts are still landing in. ingest_timestamp is stamped by
	// the writer, so a row can be committed with a timestamp slightly in the
	// past; reading right up to now would step over it and the watermark would
	// advance past a row that was never counted.
	stateMaintLag = 30 * time.Second

	// stateMaintMaxWindow caps one cycle's read. After downtime the watermark can
	// be far behind, and a single unbounded read would scan every partition since;
	// instead each cycle advances by at most this much and the loop catches up.
	stateMaintMaxWindow = 6 * time.Hour
)

// StateMaintainer keeps every model's state table current.
type StateMaintainer struct {
	pg  *storage.PostgresClient
	ch  *storage.ClickHouseClient
	mgr *Manager

	stopCh chan struct{}
	// ingestActive defers a cycle while ingestion is under pressure, so state
	// maintenance never competes with the write path it was moved off.
	ingestActive func() bool
}

func NewStateMaintainer(pg *storage.PostgresClient, ch *storage.ClickHouseClient, mgr *Manager) *StateMaintainer {
	return &StateMaintainer{pg: pg, ch: ch, mgr: mgr}
}

// SetIngestPressureFunc registers a callback checked before each cycle.
func (s *StateMaintainer) SetIngestPressureFunc(f func() bool) { s.ingestActive = f }

// StateMaintInterval is the configured cycle period.
func StateMaintInterval() time.Duration {
	if v := os.Getenv("BIFRACT_MODEL_STATE_INTERVAL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
		log.Printf("[Model State] ignoring BIFRACT_MODEL_STATE_INTERVAL=%q: want whole seconds", v)
	}
	return stateMaintDefaultInterval
}

func (s *StateMaintainer) Start(interval time.Duration) {
	s.stopCh = make(chan struct{})
	go s.loop(interval)
	log.Printf("[Model State] Started (interval: %v, lag: %v)", interval, stateMaintLag)
}

func (s *StateMaintainer) Stop() {
	if s.stopCh != nil {
		close(s.stopCh)
		log.Println("[Model State] Stopped")
	}
}

func (s *StateMaintainer) loop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if s.ingestActive != nil && s.ingestActive() {
				continue
			}
			s.cycle(context.Background())
		}
	}
}

// maintainRow is the per-model state one cycle needs.
type maintainRow struct {
	id        string
	name      string
	fractalID string
	modelType ModelType
	def       ModelDefinition
	table     string
	watermark time.Time
}

// insertSQL is the model's state aggregation over one window. A network model
// keeps its own rolling-state shape, and writes to its state table: ch_table_name
// holds its results table, which the scorer owns.
//
// target is resolved by the caller: on a cluster it is the Distributed companion,
// so a cycle's rows shard the way the backfill's do instead of landing entirely on
// whichever node happened to run it.
func (r maintainRow) insertSQL(target, sourceTable, where string) (string, error) {
	if r.modelType.IsNetwork() {
		return BuildNetStateInsert(r.def, "`"+target+"`", sourceTable, where, r.fractalID)
	}
	return BuildBackfillInsert(r.def, r.modelType, "`"+target+"`", sourceTable, where, r.fractalID)
}

// stateTarget is the table a cycle writes, distributed when the deployment is.
func stateTarget(r maintainRow, clustered bool) string {
	if r.modelType.IsNetwork() {
		if clustered {
			return chModelStateDistName(r.id)
		}
		return chModelStateName(r.id)
	}
	if clustered {
		return chModelDistName(r.id)
	}
	return r.table
}

func (s *StateMaintainer) cycle(ctx context.Context) {
	rows, err := s.dueModels(ctx)
	if err != nil {
		log.Printf("[Model State] list models: %v", err)
		return
	}
	for _, r := range rows {
		if err := s.maintain(ctx, r); err != nil {
			log.Printf("[Model State] %s (%s): %v", r.name, r.id, err)
		}
	}
}

// dueModelsQuery selects the models a cycle maintains. state_watermark IS NOT
// NULL is the handover: it is set when a model's insert-time view is dropped, or
// at creation for one that never had a view. A NULL watermark means the handover
// has not happened, and defaulting to created_at would re-read the model's whole
// history into state the view already counted, doubling every aggregate.
const dueModelsQuery = `SELECT id, name, COALESCE(fractal_id::text, ''), model_type, definition,
	       COALESCE(ch_table_name, ''), state_watermark
	FROM analytics_models
	WHERE status = 'active' AND COALESCE(ch_table_name, '') <> '' AND state_watermark IS NOT NULL`

func (s *StateMaintainer) dueModels(ctx context.Context) ([]maintainRow, error) {
	pgRows, err := s.pg.Query(ctx, dueModelsQuery)
	if err != nil {
		return nil, err
	}
	defer pgRows.Close()

	var out []maintainRow
	for pgRows.Next() {
		var r maintainRow
		var defJSON []byte
		var mt string
		if err := pgRows.Scan(&r.id, &r.name, &r.fractalID, &mt, &defJSON, &r.table, &r.watermark); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(defJSON, &r.def); err != nil {
			log.Printf("[Model State] %s: unreadable definition: %v", r.name, err)
			continue
		}
		r.modelType = ModelType(mt)
		out = append(out, r)
	}
	return out, pgRows.Err()
}

// maintain advances one model's state by a single window.
func (s *StateMaintainer) maintain(ctx context.Context, r maintainRow) error {
	from, to := maintainWindow(r.watermark, time.Now().UTC())
	if !to.After(from) {
		return nil
	}

	// Bounded on both sides and keyed on ingest_timestamp, so the read prunes to
	// the partitions and parts holding this window rather than the model's whole
	// history.
	where := fmt.Sprintf("ingest_timestamp > '%s' AND ingest_timestamp <= '%s'",
		storage.EscCHStr(from.Format(chTimeLayout)), storage.EscCHStr(to.Format(chTimeLayout)))

	insertSQL, err := r.insertSQL(stateTarget(r, s.ch.Topology().DistributedTables), s.ch.ReadTable(), where)
	if err != nil {
		return fmt.Errorf("build: %w", err)
	}
	if err := s.ch.Exec(ctx, insertSQL); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	// Only after the insert commits: a watermark advanced first would skip the
	// window on the next cycle if the insert then failed.
	if _, err := s.pg.Exec(ctx,
		`UPDATE analytics_models SET state_watermark = $1 WHERE id = $2`, to, r.id); err != nil {
		return fmt.Errorf("advance watermark: %w", err)
	}
	return nil
}

// maintainWindow is the half-open range a cycle reads, capped so a watermark far
// behind is caught up over several cycles rather than in one unbounded scan.
func maintainWindow(watermark, now time.Time) (from, to time.Time) {
	from = watermark
	to = now.Add(-stateMaintLag)
	if max := from.Add(stateMaintMaxWindow); to.After(max) {
		to = max
	}
	return from, to
}

const chTimeLayout = "2006-01-02 15:04:05.000"
