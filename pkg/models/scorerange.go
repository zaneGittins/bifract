package models

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"bifract/pkg/storage"
)

// netRawPass is what a raw-log scoring pass learned besides its pairs.
type netRawPass struct {
	networkSize uint64
	warnings    []string
}

// The steps of a raw-log scoring pass, as a Score range job reports them.
const (
	phaseNetwork    = "network"
	phasePrevalence = "prevalence"
	phaseFirstSeen  = "first_seen"
	phasePairs      = "pairs"
)

// netProgress hears which step a raw-log pass is on and, for the steps that
// stream, how many rows ClickHouse has read of its estimate.
type netProgress func(phase string, read, total uint64)

func (p netProgress) at(phase string) func(read, total uint64) {
	if p == nil {
		return nil
	}
	p(phase, 0, 0)
	return func(read, total uint64) { p(phase, read, total) }
}

// scoreNetRaw aggregates raw logs over w once and scores each qualifying pair
// with the scorer's own functions, handing each to visit. Modifiers age from
// asOf, the pass's "now". settings bounds every query. progress may be nil.
// Errors name the read that failed.
func (m *Manager) scoreNetRaw(ctx context.Context, fractalID string, mt ModelType, def ModelDefinition, w NetWindow, asOf time.Time, settings string, progress netProgress, visit func(netScored) error) (netRawPass, error) {
	var pass netRawPass
	ctx = scoringReadContext(ctx, fractalID)
	source := m.ch.ReadTable()
	ti, tiWarnings := m.threatIntelColumn(ctx, fractalID, def)

	// Prevalence context over the same window (all sources, not just qualifying).
	sizeSQL, err := BuildNetPreviewNetworkSize(def, source, fractalID, w)
	if err != nil {
		return pass, err
	}
	progress.at(phaseNetwork)
	if err := m.ch.QueryRow(ctx, sizeSQL+settings).Scan(&pass.networkSize); err != nil {
		return pass, fmt.Errorf("network size: %w", err)
	}
	if pass.networkSize == 0 {
		// Nothing in the window: no pairs to score, but a broken link still says so.
		pass.networkSize, pass.warnings = 1, tiWarnings
		return pass, nil
	}
	prevalence := make(map[string]uint64)
	prevSQL, err := BuildNetPreviewPrevalence(def, source, fractalID, w)
	if err != nil {
		return pass, err
	}
	if err := m.ch.StreamQuery(ctx, "", prevSQL+settings, func(row map[string]interface{}) error {
		prevalence[getString(row, "dst")] = getUint64(row, "prev_total")
		return nil
	}, progress.at(phasePrevalence)); err != nil {
		return pass, fmt.Errorf("prevalence: %w", err)
	}
	pairsSQL, err := BuildNetPreviewPairs(def, mt, source, fractalID, w)
	if err != nil {
		return pass, err
	}
	progress.at(phaseFirstSeen)
	fs, warnings := m.loadFirstSeen(ctx, fractalID, def, pairsSQL, settings, asOf)
	pass.warnings = append(warnings, tiWarnings...)

	aggSQL, err := BuildNetPreviewAgg(def, mt, source, fractalID, w, ti)
	if err != nil {
		return pass, err
	}
	sc := newPairScorer(mt, def, int64(w.Days)*86400, fs, prevalence, pass.networkSize, asOf)
	blockRows := fmt.Sprintf(", max_block_size=%d", netReadBlockRows)
	err = m.ch.StreamQuery(ctx, "", aggSQL+settings+blockRows, func(row map[string]interface{}) error {
		return visit(sc.score(pairFromRow(row)))
	}, progress.at(phasePairs))
	if err != nil && !errors.Is(err, errPairCapReached) {
		return pass, fmt.Errorf("aggregation: %w", err)
	}
	return pass, nil
}

// ScoreRangeRequest is the body for POST /models/{id}/score-range: the
// event-time range a network model is scored over.
type ScoreRangeRequest struct {
	From *time.Time `json:"from"`
	To   *time.Time `json:"to"`
}

// ScoreRangeResult reports a finished range scoring pass. Start and End are the
// whole hours scored; every row written carries End as its scored_at.
type ScoreRangeResult struct {
	Start       time.Time `json:"start"`
	End         time.Time `json:"end"`
	PairsScored int       `json:"pairs_scored"`
	Truncated   bool      `json:"truncated"`
	Warnings    []string  `json:"warnings,omitempty"`
}

// Score range job states.
const (
	scoreRangeNone      = "none"
	scoreRangeQueued    = "queued"
	scoreRangeRunning   = "running"
	scoreRangeCompleted = "completed"
	scoreRangeFailed    = "failed"
	scoreRangeCancelled = "cancelled"
)

// phaseWaiting is a claimed job waiting for the raw-log slot or for ingest
// headroom; phaseWriting is one inserting its scores.
const (
	phaseWaiting = "waiting"
	phaseWriting = "writing"
)

// ScoreRangeJob is a network model's latest Score range run. QueuedAt
// identifies the run. Phase, RowsRead and RowsTotal are its progress while it
// runs: RowsTotal is ClickHouse's estimate for the current phase.
type ScoreRangeJob struct {
	Status    string            `json:"status"`
	From      *time.Time        `json:"from,omitempty"`
	To        *time.Time        `json:"to,omitempty"`
	QueuedAt  *time.Time        `json:"queued_at,omitempty"`
	Phase     string            `json:"phase,omitempty"`
	RowsRead  int64             `json:"rows_read"`
	RowsTotal int64             `json:"rows_total"`
	Result    *ScoreRangeResult `json:"result,omitempty"`
	Error     string            `json:"error,omitempty"`
}

// Active reports whether the job is queued or running.
func (j ScoreRangeJob) Active() bool {
	return j.Status == scoreRangeQueued || j.Status == scoreRangeRunning
}

var (
	// scoreRangePoll is how often every replica looks for a queued job it did not
	// queue itself, such as one whose replica died.
	scoreRangePoll = 10 * time.Second
	// scoreRangeBeat is how often a running job records its progress and checks it
	// is still the model's current run.
	scoreRangeBeat = 2 * time.Second
	// scoreRangeWaitPoll is how often a dataset import checks on the jobs it queued.
	scoreRangeWaitPoll = 2 * time.Second
)

var (
	errScoreRangeActive = errors.New("this model is already scoring a range; cancel it first")
	// errScoreTimeout reports a pass that ran out of time and wrote nothing.
	errScoreTimeout = errors.New("scoring did not finish in time and wrote nothing; score a shorter range")
	// errScoreRangeLost reports a run that is no longer the model's current one:
	// it was cancelled, replaced, or its model was edited or deleted.
	errScoreRangeLost = errors.New("the run was cancelled or replaced")
)

// QueueScoreRange queues a Score range job for a network model and returns it
// at once; a replica runs it in the background (see RunScoreRanges). With
// replace, a queued or running job is replaced; without, it is refused.
//
// The job scores the range the way the model's preview does and writes the
// scores to its results table as of the range's end. It is how a fractal holding
// an imported or replayed dataset gets scores: the scheduled scorer reads only
// the rolling state, which covers recent ingest. The state table and the
// scorer's cursor are left alone. Per pair the newest scored_at wins, so a live
// score outranks any range score. In a dataset's fractal the scores never expire.
func (m *Manager) QueueScoreRange(ctx context.Context, model *Model, req ScoreRangeRequest, replace bool) (*ScoreRangeJob, error) {
	if !model.ModelType.IsScheduled() {
		return nil, fmt.Errorf("only beacon and long_connection models score a range; %s models backfill", model.ModelType)
	}
	if model.Status != "active" {
		return nil, fmt.Errorf("model is not active (status=%s)", model.Status)
	}
	if model.CHTableName == "" {
		return nil, fmt.Errorf("model has no data table yet")
	}
	if req.From == nil || req.To == nil {
		return nil, fmt.Errorf("a score range needs both from and to")
	}
	rng, err := ResolvePreviewRange("", req.From, req.To, time.Now())
	if err != nil {
		return nil, err
	}
	if _, err := m.ResolveSource(ctx, model.Definition, model.FractalID, ""); err != nil {
		return nil, err
	}
	w := netPreviewWindow(rng)
	queued := time.Now().UTC().Truncate(time.Microsecond)
	res, err := m.pg.Exec(ctx,
		`UPDATE analytics_models
		    SET score_range_status = 'queued', score_range_from = $2, score_range_to = $3,
		        score_range_queued_at = $4, score_range_phase = '', score_range_read = 0,
		        score_range_total = 0, score_range_result = NULL, score_range_error = ''
		  WHERE id = $1 AND status = 'active'
		    AND ($5 OR score_range_status NOT IN ('queued', 'running'))`,
		model.ID, w.Start, w.End, queued, replace)
	if err != nil {
		return nil, fmt.Errorf("queue score range: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, errScoreRangeActive
	}
	m.nudgeScoreRanges()
	start, end := w.Start, w.End
	return &ScoreRangeJob{Status: scoreRangeQueued, From: &start, To: &end, QueuedAt: &queued}, nil
}

// CancelScoreRange stops a model's queued or running Score range job, on
// whichever replica runs it, recording why. The run writes nothing more.
func (m *Manager) CancelScoreRange(ctx context.Context, id, why string) error {
	_, err := m.pg.Exec(ctx,
		`UPDATE analytics_models SET score_range_status = 'cancelled', score_range_error = $2
		  WHERE id = $1 AND score_range_status IN ('queued', 'running')`, id, why)
	return err
}

func (m *Manager) nudgeScoreRanges() {
	select {
	case m.srNudge <- struct{}{}:
	default:
	}
}

// RunScoreRanges runs queued Score range jobs until ctx ends. Every replica
// runs it: the one that queues a job starts it at once, and the others look on
// a poll so a job outlives the replica that queued it. A cluster-wide lock runs
// one job at a time.
func (m *Manager) RunScoreRanges(ctx context.Context) {
	t := time.NewTicker(scoreRangePoll)
	defer t.Stop()
	for {
		m.runQueuedScoreRanges(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-m.srNudge:
		}
	}
}

// runQueuedScoreRanges runs every queued job, oldest first, while holding the
// cluster-wide lock. Holding it, a job still marked running is one whose
// replica died, so it runs again.
func (m *Manager) runQueuedScoreRanges(ctx context.Context) {
	var pending bool
	if err := m.pg.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM analytics_models WHERE score_range_status IN ('queued', 'running'))`,
	).Scan(&pending); err != nil || !pending {
		return
	}
	held, release, ok := m.pg.HoldAdvisoryLock(ctx, storage.LockModelScoreRange)
	if !ok {
		return
	}
	defer release()
	for held.Err() == nil {
		var id string
		var job ScoreRangeJob
		err := m.pg.QueryRow(held,
			`UPDATE analytics_models
			    SET score_range_status = 'running', score_range_phase = 'waiting',
			        score_range_read = 0, score_range_total = 0
			  WHERE id = (SELECT id FROM analytics_models
			               WHERE score_range_status IN ('queued', 'running')
			               ORDER BY score_range_queued_at, id LIMIT 1)
			  RETURNING id, score_range_queued_at, score_range_from, score_range_to`,
		).Scan(&id, &job.QueuedAt, &job.From, &job.To)
		if err != nil {
			if err != sql.ErrNoRows {
				log.Printf("models: claim score range: %v", err)
			}
			return
		}
		m.runScoreRange(held, id, job)
	}
}

// scoreRangeRun is one claimed job: its progress, written while the row is
// still this run's.
type scoreRangeRun struct {
	m        *Manager
	id       string
	queuedAt time.Time

	mu          sync.Mutex
	phase       string
	read, total uint64
	writeMu     sync.Mutex
	lost        bool
}

// set records progress; a new phase is written at once, rows on the beat.
func (r *scoreRangeRun) set(phase string, read, total uint64) {
	r.mu.Lock()
	changed := phase != r.phase
	r.phase, r.read, r.total = phase, read, total
	r.mu.Unlock()
	if changed {
		r.write()
	}
}

// enter starts a phase and reports whether the row is still this run's.
func (r *scoreRangeRun) enter(phase string) bool {
	r.mu.Lock()
	r.phase, r.read, r.total = phase, 0, 0
	r.mu.Unlock()
	return r.write()
}

// write records the latest progress and reports whether the row is still this
// run's. Once it is not, it stays lost.
func (r *scoreRangeRun) write() bool {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	if r.lost {
		return false
	}
	r.mu.Lock()
	phase, read, total := r.phase, int64(min(r.read, math.MaxInt64)), int64(min(r.total, math.MaxInt64))
	r.mu.Unlock()
	res, err := r.m.pg.Exec(context.Background(),
		`UPDATE analytics_models SET score_range_phase = $3, score_range_read = $4, score_range_total = $5
		  WHERE id = $1 AND score_range_status = 'running' AND score_range_queued_at = $2`,
		r.id, r.queuedAt, phase, read, total)
	if err != nil {
		log.Printf("model %s: score range progress: %v", r.id, err)
		return true
	}
	if n, _ := res.RowsAffected(); n == 0 {
		r.lost = true
	}
	return !r.lost
}

// runScoreRange runs a claimed job and records how it ended. A run that lost
// its row, or whose replica is stopping or lost the lock, records nothing: the
// row is another run's, or is left running for the next replica to pick up.
func (m *Manager) runScoreRange(ctx context.Context, id string, job ScoreRangeJob) {
	if job.QueuedAt == nil || job.From == nil || job.To == nil {
		return
	}
	run := &scoreRangeRun{m: m, id: id, queuedAt: *job.QueuedAt, phase: phaseWaiting}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	beatDone := make(chan struct{})
	go func() {
		defer close(beatDone)
		t := time.NewTicker(scoreRangeBeat)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				if !run.write() {
					cancel()
					return
				}
			}
		}
	}()
	res, err := m.scoreRangeJob(runCtx, id, *job.From, *job.To, run)
	cancel()
	<-beatDone
	if ctx.Err() != nil || errors.Is(err, errScoreRangeLost) {
		return
	}
	status, msg, result := scoreRangeCompleted, "", []byte(nil)
	if err != nil {
		log.Printf("model %s: score range: %v", id, err)
		status, msg = scoreRangeFailed, err.Error()
	} else {
		result, _ = json.Marshal(res)
	}
	if _, err := m.pg.Exec(context.Background(),
		`UPDATE analytics_models
		    SET score_range_status = $3, score_range_result = $4, score_range_error = $5, score_range_phase = ''
		  WHERE id = $1 AND score_range_status = 'running' AND score_range_queued_at = $2`,
		id, run.queuedAt, status, result, msg); err != nil {
		log.Printf("model %s: record score range %s: %v", id, status, err)
	}
}

// scoreRangeJob scores a model over [from, to), which are whole hours, and
// writes every score in one insert once the read has finished, so a run cut
// short writes nothing. It waits for the raw-log slot it shares with backfills,
// and for ingest headroom, then has scoreRangeTimeoutSec to finish.
func (m *Manager) scoreRangeJob(ctx context.Context, id string, from, to time.Time, run *scoreRangeRun) (*ScoreRangeResult, error) {
	select {
	case m.bfSem <- struct{}{}:
		defer func() { <-m.bfSem }()
	case <-ctx.Done():
		return nil, errScoreRangeLost
	}
	if err := m.waitForHealth(ctx); err != nil {
		return nil, errScoreRangeLost
	}
	model, err := m.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if model.Status != "active" || model.CHTableName == "" {
		return nil, fmt.Errorf("model is not active (status=%s)", model.Status)
	}
	def, err := m.ResolveSource(ctx, model.Definition, model.FractalID, "")
	if err != nil {
		return nil, err
	}
	rng, err := ResolvePreviewRange("", &from, &to, time.Now())
	if err != nil {
		return nil, err
	}
	w := netPreviewWindow(rng)

	budget := time.Duration(m.bfCfg.scoreRangeTimeoutSec) * time.Second
	readCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	res := &ScoreRangeResult{Start: w.Start, End: w.End}
	expires := netResultsWrittenExpiry(model.DatasetEnd != nil, time.Now())
	var rows []resultRow
	pass, err := m.scoreNetRaw(readCtx, model.FractalID, model.ModelType, def, w, rng.End, m.bfCfg.settings(m.bfCfg.scoreRangeTimeoutSec), run.set, func(s netScored) error {
		if len(rows) >= scorerPerModelPairCap {
			res.Truncated = true
			return errPairCapReached
		}
		rows = append(rows, s.row(model.FractalID, w.End, expires))
		return nil
	})
	if ctx.Err() != nil {
		return nil, errScoreRangeLost
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(readCtx.Err(), context.DeadlineExceeded) {
		return nil, errScoreTimeout
	}
	if err != nil {
		return nil, err
	}
	res.PairsScored, res.Warnings = len(rows), pass.warnings

	// A model edited, cancelled or deleted during the read no longer owns the run.
	if !run.enter(phaseWriting) {
		return nil, errScoreRangeLost
	}
	if len(rows) == 0 {
		return res, nil
	}
	buf := newResultBuffer(m.ch, model.CHTableName)
	buf.rows = rows
	if err := buf.flush(ctx); err != nil {
		return nil, fmt.Errorf("write scores: %w", err)
	}
	// The range is a pass as of its end, never later than now. A dataset's is its
	// findings; in a live fractal it never moves the pass back, so a past range
	// lands in history.
	passAt := w.End
	if now := time.Now().UTC().Truncate(time.Millisecond); passAt.After(now) {
		passAt = now
	}
	q := `UPDATE analytics_models SET scored_pass_at = GREATEST(COALESCE(scored_pass_at, $2), $2) WHERE id = $1`
	if model.DatasetEnd != nil {
		q = `UPDATE analytics_models SET scored_pass_at = $2 WHERE id = $1`
	}
	if _, err := m.pg.Exec(ctx, q, model.ID, passAt); err != nil {
		return nil, fmt.Errorf("record pass: %w", err)
	}
	return res, nil
}
