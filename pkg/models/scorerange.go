package models

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// netRawPass is what a raw-log scoring pass learned besides its pairs.
type netRawPass struct {
	networkSize uint64
	warnings    []string
}

// scoreNetRaw aggregates raw logs over w once and scores each qualifying pair
// with the scorer's own functions, handing each to visit. Modifiers age from
// asOf, the pass's "now". settings bounds every query. Errors name the read
// that failed.
func (m *Manager) scoreNetRaw(ctx context.Context, fractalID string, mt ModelType, def ModelDefinition, w NetWindow, asOf time.Time, settings string, visit func(netScored) error) (netRawPass, error) {
	var pass netRawPass
	source := m.ch.ReadTable()
	ti, tiWarnings := m.threatIntelColumn(ctx, fractalID, def)

	// Prevalence context over the same window (all sources, not just qualifying).
	sizeSQL, err := BuildNetPreviewNetworkSize(def, source, fractalID, w)
	if err != nil {
		return pass, err
	}
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
	}, nil); err != nil {
		return pass, fmt.Errorf("prevalence: %w", err)
	}
	pairsSQL, err := BuildNetPreviewPairs(def, mt, source, fractalID, w)
	if err != nil {
		return pass, err
	}
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
	}, nil)
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

// ScoreRangeResult reports a range scoring pass. Start and End are the whole
// hours scored; every row written carries End as its scored_at.
type ScoreRangeResult struct {
	Start       time.Time `json:"start"`
	End         time.Time `json:"end"`
	PairsScored int       `json:"pairs_scored"`
	Truncated   bool      `json:"truncated"`
	Warnings    []string  `json:"warnings,omitempty"`
}

// ScoreRange scores a network model over a past range, the way its preview
// does, and writes the scores to its results table as of the range's end. It is
// how a fractal holding an imported or replayed dataset gets scores: the
// scheduled scorer reads only the rolling state, which covers recent ingest.
// The state table and the scorer's cursor are left alone. Per pair the newest
// scored_at wins, so a live score outranks any range score.
//
// It runs within the caller's deadline and writes every score in one insert
// once the read has finished, so a run cut short writes nothing.
func (m *Manager) ScoreRange(ctx context.Context, model *Model, req ScoreRangeRequest) (*ScoreRangeResult, error) {
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
	def, err := m.ResolveSource(ctx, model.Definition, model.FractalID, "")
	if err != nil {
		return nil, err
	}
	w := netPreviewWindow(rng)

	// One heavy raw-log read at a time, shared with backfills, and only while
	// ingest has headroom. A held slot fails at once: a backfill can hold it
	// for hours.
	select {
	case m.bfSem <- struct{}{}:
		defer func() { <-m.bfSem }()
	default:
		return nil, fmt.Errorf("a backfill or range scoring is running; try again when it finishes")
	}
	if m.bfHealth != nil && !m.bfHealth.Healthy() {
		return nil, fmt.Errorf("ingest is under pressure; try again shortly")
	}
	budget := time.Duration(m.bfCfg.chunkTimeoutSec) * time.Second
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) < budget {
		budget = time.Until(dl)
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	res := &ScoreRangeResult{Start: w.Start, End: w.End}
	var rows []resultRow
	pass, err := m.scoreNetRaw(ctx, model.FractalID, model.ModelType, def, w, rng.End, m.bfCfg.settings(max(1, int(budget/time.Second))), func(s netScored) error {
		if len(rows) >= scorerPerModelPairCap {
			res.Truncated = true
			return errPairCapReached
		}
		rows = append(rows, s.row(model.FractalID, w.End))
		return nil
	})
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return nil, fmt.Errorf("scoring did not finish in time and wrote nothing; score a shorter range")
	}
	if err != nil {
		return nil, err
	}
	res.PairsScored, res.Warnings = len(rows), pass.warnings
	if len(rows) == 0 {
		return res, nil
	}

	// An edit during the run may have rebuilt the table for a new definition.
	if cur, err := m.Get(ctx, model.ID); err != nil || cur.Status != "active" || !cur.UpdatedAt.Equal(model.UpdatedAt) {
		return nil, fmt.Errorf("the model changed while it was scored; nothing was written")
	}
	buf := newResultBuffer(m.ch, model.CHTableName)
	buf.rows = rows
	if err := buf.flush(ctx); err != nil {
		return nil, fmt.Errorf("write scores: %w", err)
	}
	return res, nil
}
