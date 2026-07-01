package models

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"bifract/pkg/storage"
)

// ScorerEngine periodically scores scheduled (network) models. It mirrors the alert
// engine: a heartbeat ticker, single-replica execution via a Postgres advisory lock,
// and deferral under ingest pressure. Each model has its own rescore cadence tracked
// by the last_scored_at cursor, so long-window models are not rescored every tick.
//
// The scorer reads only the compact rolling-state table (never raw logs), streams the
// qualifying pairs row-by-row (never materializing the full set), scores them in Go,
// and writes the results table. Cost is bounded by qualifying-pair cardinality, which
// ClickHouse bounds via the read query's HAVING and array cap.
type ScorerEngine struct {
	pg  *storage.PostgresClient
	ch  *storage.ClickHouseClient
	mgr *Manager

	ingestActive func() bool

	stopCh    chan struct{}
	startedAt time.Time
	running   bool
}

// scorerLockID is the Postgres advisory lock ID ensuring only one replica scores at
// a time. Distinct from the alert engine's lock so the two run independently.
const scorerLockID int64 = 0x6269667261637402 // "bifract\x02"

const (
	// scorerPerModelPairCap hard-bounds the pairs scored per model per cycle so a
	// pathological deployment cannot run an unbounded synchronous burst. Truncation
	// past this is logged (never silent).
	scorerPerModelPairCap = 50000

	// scorerResultFlushRows bounds the pending-insert buffer: results flush in
	// batches so the output side stays bounded regardless of pair count.
	scorerResultFlushRows = 1000

	// Rescore cadence bounds. rescoreInterval = clamp(windowDays hours, min, max):
	// a 1d model rescores ~hourly, a 7d/14d model every 6h. Reads the state table,
	// so even long windows are cheap.
	scorerMinRescore = 10 * time.Minute
	scorerMaxRescore = 6 * time.Hour
)

// errPairCapReached stops stream iteration once the per-cycle pair cap is hit.
var errPairCapReached = errors.New("per-model pair cap reached")

// NewScorerEngine constructs a scorer engine. The manager supplies CH object naming
// (state/results/distributed) so the scorer and lifecycle stay consistent.
func NewScorerEngine(pg *storage.PostgresClient, ch *storage.ClickHouseClient, mgr *Manager) *ScorerEngine {
	return &ScorerEngine{pg: pg, ch: ch, mgr: mgr}
}

// SetIngestPressureFunc registers a callback checked before each cycle; when it
// returns true, the cycle is deferred so ClickHouse is free for ingest.
func (e *ScorerEngine) SetIngestPressureFunc(f func() bool) { e.ingestActive = f }

// Start launches the background scoring loop.
func (e *ScorerEngine) Start(interval time.Duration) {
	e.startedAt = time.Now()
	e.stopCh = make(chan struct{})
	go e.loop(interval)
	log.Printf("[Model Scorer] Started (heartbeat: %v)", interval)
}

// Stop halts the loop.
func (e *ScorerEngine) Stop() {
	if e.stopCh != nil {
		close(e.stopCh)
		log.Println("[Model Scorer] Stopped")
	}
}

func (e *ScorerEngine) loop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.scoreCycle()
		}
	}
}

// scheduledModelRow is the lightweight per-model state the scorer needs each cycle.
type scheduledModelRow struct {
	id         string
	fractalID  string
	modelType  ModelType
	def        ModelDefinition
	tableName  string
	lastScored *time.Time
}

// rescoreInterval returns the model's per-model cadence, derived from its window.
func (r scheduledModelRow) rescoreInterval() time.Duration {
	d := time.Duration(r.def.WindowDays()) * time.Hour
	if d < scorerMinRescore {
		d = scorerMinRescore
	}
	if d > scorerMaxRescore {
		d = scorerMaxRescore
	}
	return d
}

// due reports whether the model should be rescored this cycle.
func (r scheduledModelRow) due(now time.Time) bool {
	if r.lastScored == nil {
		return true
	}
	return now.Sub(*r.lastScored) >= r.rescoreInterval()
}

func (e *ScorerEngine) scoreCycle() {
	if e.running {
		return
	}
	e.running = true
	defer func() { e.running = false }()

	// Single-replica execution.
	unlock, acquired := e.pg.TryAdvisoryLock(context.Background(), scorerLockID)
	if !acquired {
		return
	}
	defer unlock()

	// Yield to ingest pressure.
	if e.ingestActive != nil && e.ingestActive() {
		return
	}

	models, err := e.listScheduledModels(context.Background())
	if err != nil {
		log.Printf("[Model Scorer] list scheduled models: %v", err)
		return
	}
	// Clean no-op when idle: no network models -> nothing to do (one cheap SELECT).
	if len(models) == 0 {
		return
	}

	now := time.Now()
	var scored int
	for _, mdl := range models {
		if !mdl.due(now) {
			continue
		}
		// Per-model panic recovery: a malformed model can never crash the engine.
		func(m scheduledModelRow) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[Model Scorer] model %s panic: %v", m.id, r)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			if err := e.scoreModel(ctx, m); err != nil {
				// Defensive: a dropped state table mid-cycle, a CH timeout, or a
				// model still warming up is logged and retried next cadence, never
				// fatal.
				log.Printf("[Model Scorer] model %s: %v", m.id, err)
				return
			}
			scored++
		}(mdl)
	}
	if scored > 0 {
		log.Printf("[Model Scorer] scored %d model(s)", scored)
	}
}

// listScheduledModels returns all active network models with their rescore cursor.
func (e *ScorerEngine) listScheduledModels(ctx context.Context) ([]scheduledModelRow, error) {
	rows, err := e.pg.Query(ctx,
		`SELECT id, COALESCE(fractal_id::text,''), model_type, definition, ch_table_name, last_scored_at
		 FROM analytics_models
		 WHERE status = 'active' AND model_type IN ('beacon','long_connection')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []scheduledModelRow
	for rows.Next() {
		var id, fid, mt, tbl string
		var defRaw []byte
		var last *time.Time
		if err := rows.Scan(&id, &fid, &mt, &defRaw, &tbl, &last); err != nil {
			return nil, err
		}
		var def ModelDefinition
		_ = json.Unmarshal(defRaw, &def)
		out = append(out, scheduledModelRow{
			id: id, fractalID: fid, modelType: ModelType(mt), def: def,
			tableName: tbl, lastScored: last,
		})
	}
	return out, rows.Err()
}

// scoreModel runs one scoring pass for a single model: read compact state, score in
// Go, write the results table, advance the cursor.
func (e *ScorerEngine) scoreModel(ctx context.Context, m scheduledModelRow) error {
	windowDays := m.def.WindowDays()
	windowSecs := int64(windowDays) * 86400
	stateRead := e.mgr.networkStateReadTable(m.id)

	// Prevalence context: distinct-source count per destination, and the total
	// distinct-source count (network size) as the denominator.
	networkSize, err := e.readNetworkSize(ctx, stateRead, m.fractalID, windowDays)
	if err != nil {
		return fmt.Errorf("network size: %w", err)
	}
	if networkSize == 0 {
		return nil // warming up: no state yet
	}
	prevalence, err := e.readPrevalence(ctx, stateRead, m.fractalID, windowDays)
	if err != nil {
		return fmt.Errorf("prevalence: %w", err)
	}

	bp := m.def.Beacon.WithDefaults(windowSecs)
	lc := m.def.LongConn.WithDefaults()
	mp := m.def.Modifiers.WithDefaults()

	now := time.Now()
	cutoff := now.Unix() - windowSecs

	buf := newResultBuffer(e.ch, m.tableName)
	var count int
	truncated := false

	readSQL := BuildNetScoreReadQuery(m.def, m.modelType, stateRead, m.fractalID, windowDays)
	streamErr := e.ch.StreamQuery(ctx, "", readSQL, func(row map[string]interface{}) error {
		if count >= scorerPerModelPairCap {
			truncated = true
			return errPairCapReached
		}
		count++

		p := pairFromRow(row, cutoff)
		prevTotal := prevalence[p.Dst]
		prevRatio := float64(prevTotal) / float64(networkSize)

		var rs RegularityScore
		if m.modelType == ModelTypeLongConnection {
			rs = RegularityScore{Score: ScoreLongConn(p.TotalDuration, lc)}
		} else {
			rs = ScoreBeacon(p, bp, windowSecs)
		}
		final, mods := ApplyModifiers(rs.Score, prevRatio, mp)

		buf.add(resultRow{
			fractalID: m.fractalID, src: p.Src, dst: p.Dst, port: p.Port,
			rs: rs, prevalence: round3(prevRatio), prevalenceTotal: prevTotal,
			mods: mods, final: final, connCount: p.Count, totalDuration: p.TotalDuration,
			firstSeen: unixToTime(p.FirstTs), lastSeen: unixToTime(p.LastTs), scoredAt: now,
		})
		if buf.len() >= scorerResultFlushRows {
			return buf.flush(ctx)
		}
		return nil
	}, nil)

	if streamErr != nil && !errors.Is(streamErr, errPairCapReached) {
		return fmt.Errorf("stream state: %w", streamErr)
	}
	if err := buf.flush(ctx); err != nil {
		return fmt.Errorf("flush results: %w", err)
	}
	if truncated {
		log.Printf("[Model Scorer] model %s: pair cap %d reached; results truncated", m.id, scorerPerModelPairCap)
	}

	// Advance the cursor only after a successful pass.
	if _, err := e.pg.Exec(ctx,
		`UPDATE analytics_models SET last_scored_at = NOW() WHERE id = $1`, m.id); err != nil {
		return fmt.Errorf("update cursor: %w", err)
	}
	return nil
}

func (e *ScorerEngine) readNetworkSize(ctx context.Context, stateRead, fractalID string, windowDays int) (uint64, error) {
	var n uint64
	err := e.ch.QueryRow(ctx, BuildNetNetworkSizeQuery(stateRead, fractalID, windowDays)).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (e *ScorerEngine) readPrevalence(ctx context.Context, stateRead, fractalID string, windowDays int) (map[string]uint64, error) {
	out := make(map[string]uint64)
	err := e.ch.StreamQuery(ctx, "", BuildNetPrevalenceQuery(stateRead, fractalID, windowDays),
		func(row map[string]interface{}) error {
			out[getString(row, "dst")] = getUint64(row, "prev_total")
			return nil
		}, nil)
	return out, err
}

// ---- result buffering / insert ----

type resultRow struct {
	fractalID, src, dst, port string
	rs                        RegularityScore
	prevalence                float64
	prevalenceTotal           uint64
	mods                      Modifiers
	final                     float64
	connCount                 uint64
	totalDuration             float64
	firstSeen, lastSeen       time.Time
	scoredAt                  time.Time
}

type resultBuffer struct {
	ch    *storage.ClickHouseClient
	table string
	rows  []resultRow
}

func newResultBuffer(ch *storage.ClickHouseClient, table string) *resultBuffer {
	return &resultBuffer{ch: ch, table: table}
}

func (b *resultBuffer) add(r resultRow) { b.rows = append(b.rows, r) }
func (b *resultBuffer) len() int        { return len(b.rows) }

// flush inserts the buffered rows via a prepared batch and clears the buffer.
func (b *resultBuffer) flush(ctx context.Context) error {
	if len(b.rows) == 0 {
		return nil
	}
	batch, err := b.ch.Conn().PrepareBatch(ctx, fmt.Sprintf(
		"INSERT INTO `%s` (fractal_id, src_ip, dst_ip, dst_port, "+
			"regularity_score, ts_score, ds_score, dur_score, hist_score, "+
			"prevalence, prevalence_total, prevalence_score, first_seen_score, threat_intel_score, "+
			"final_score, conn_count, total_duration, first_seen, last_seen, scored_at)", b.table))
	if err != nil {
		return err
	}
	for _, r := range b.rows {
		if err := batch.Append(
			r.fractalID, r.src, r.dst, r.port,
			r.rs.Score, r.rs.TsScore, r.rs.DsScore, r.rs.DurScore, r.rs.HistScore,
			r.prevalence, r.prevalenceTotal, r.mods.PrevalenceScore, r.mods.FirstSeenScore, r.mods.ThreatIntelScore,
			r.final, r.connCount, r.totalDuration, r.firstSeen, r.lastSeen, r.scoredAt,
		); err != nil {
			return err
		}
	}
	if err := batch.Send(); err != nil {
		return err
	}
	b.rows = b.rows[:0]
	return nil
}

// ---- row -> PairAgg ----

// pairFromRow builds a PairAgg from a state-read row, sorting timestamps ascending
// and trimming them to the exact window (cutoff = now - windowSecs).
func pairFromRow(row map[string]interface{}, cutoff int64) PairAgg {
	ts := getInt64Slice(row, "ts_list")
	sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
	// Trim to the window; ts is sorted so find the first index >= cutoff.
	idx := sort.Search(len(ts), func(i int) bool { return ts[i] >= cutoff })
	ts = ts[idx:]

	sizes := getFloat64Slice(row, "size_list")
	// size_list is parallel to the untrimmed ts_list; keep the tail matching ts.
	if len(sizes) >= idx {
		sizes = sizes[idx:]
	} else {
		sizes = nil
	}

	p := PairAgg{
		Src:           getString(row, "src"),
		Dst:           getString(row, "dst"),
		Port:          getString(row, "port"),
		Count:         getUint64(row, "cnt"),
		TsList:        ts,
		SizeList:      sizes,
		TotalDuration: getFloat64(row, "total_duration"),
	}
	if len(ts) > 0 {
		p.FirstTs = ts[0]
		p.LastTs = ts[len(ts)-1]
	}
	return p
}

func unixToTime(sec int64) time.Time {
	if sec <= 0 {
		return time.Unix(0, 0).UTC()
	}
	return time.Unix(sec, 0).UTC()
}

// ---- tolerant row accessors (ClickHouse driver scan types vary by column) ----

func getString(row map[string]interface{}, key string) string {
	if v, ok := row[key].(string); ok {
		return v
	}
	if v, ok := row[key].(*string); ok && v != nil {
		return *v
	}
	return ""
}

func getUint64(row map[string]interface{}, key string) uint64 {
	switch n := row[key].(type) {
	case uint64:
		return n
	case uint32:
		return uint64(n)
	case int64:
		if n > 0 {
			return uint64(n)
		}
	case float64:
		if n > 0 {
			return uint64(n)
		}
	}
	return 0
}

func getFloat64(row map[string]interface{}, key string) float64 {
	switch n := row[key].(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case uint64:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}

func getInt64Slice(row map[string]interface{}, key string) []int64 {
	switch v := row[key].(type) {
	case []int64:
		return v
	case []uint32:
		out := make([]int64, len(v))
		for i, x := range v {
			out[i] = int64(x)
		}
		return out
	case []uint64:
		out := make([]int64, len(v))
		for i, x := range v {
			out[i] = int64(x)
		}
		return out
	}
	return nil
}

func getFloat64Slice(row map[string]interface{}, key string) []float64 {
	if v, ok := row[key].([]float64); ok {
		return v
	}
	return nil
}
