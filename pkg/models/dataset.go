package models

import (
	"context"
	"fmt"
	"log"
	"time"

	"bifract/pkg/sqlquote"
)

// BuildingModels names a fractal's models whose ClickHouse objects are still being
// created. They record only logs ingested after they finish, so an import waits.
func (m *Manager) BuildingModels(ctx context.Context, fractalID string) ([]string, error) {
	list, err := m.List(ctx, fractalID)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, mo := range list {
		if mo.Status == "rebuilding" {
			names = append(names, mo.Name)
		}
	}
	return names, nil
}

// ResetFractal empties every active model in a fractal, for when its logs were
// dropped to be imported again: the new rows would otherwise add to the state the
// old ones left. Drop the logs first, so no state cycle can read them back. The
// watermark stays, since the new logs are stamped after it.
func (m *Manager) ResetFractal(ctx context.Context, fractalID string) error {
	list, err := m.List(ctx, fractalID)
	if err != nil {
		return err
	}
	for _, mo := range list {
		if mo.Status != "active" {
			continue
		}
		m.CancelBackfill(mo.ID)
		// Cancelling stops a Score range read of the dropped logs writing.
		if err := m.CancelScoreRange(ctx, mo.ID, "the fractal's logs were reset"); err != nil {
			return fmt.Errorf("reset model %s: %w", mo.Name, err)
		}
		if err := m.ch.ClearModelData(ctx, mo.ID); err != nil {
			return fmt.Errorf("reset model %s: %w", mo.Name, err)
		}
		if _, err := m.pg.Exec(ctx,
			`UPDATE analytics_models
			    SET backfill_status = 'none', backfill_window = '', backfill_total = 0, backfill_done = 0,
			        backfill_started_at = NULL, backfill_from = NULL, backfill_to = NULL, backfill_error = '',
			        scored_pass_at = NULL, updated_at = NOW()
			  WHERE id = $1`, mo.ID); err != nil {
			return fmt.Errorf("reset model %s: %w", mo.Name, err)
		}
	}
	return nil
}

// ScoreFractal scores a fractal's network models over a dataset's range, which
// their rolling state cannot cover. The other types read the logs as they arrive.
// It queues a Score range job per model, replacing any in progress, and waits for
// them. It returns a line, safe to show, for anything to know about the pass: a
// model left unscored, or one that recorded nothing because it is not active. An
// import is complete without them, and Score range can run them again.
func (m *Manager) ScoreFractal(ctx context.Context, fractalID string, start, end time.Time) []string {
	list, err := m.List(ctx, fractalID)
	if err != nil {
		log.Printf("[Models] Listing the models of fractal %s to score: %v", fractalID, err)
		return []string{"Network models were not scored. Use Score range on each under Analytics."}
	}
	// Whole hours, so the hour holding the last log is inside the range, unless
	// that alone would take it over the limit.
	to := end.UTC().Truncate(time.Hour).Add(time.Hour)
	if limit := start.Add(previewMaxSpan); to.After(limit) && !end.After(limit) {
		to = limit
	}
	var warnings []string
	queued := map[*Model]time.Time{}
	var order []*Model
	for _, mo := range list {
		switch {
		case mo.Status != "active":
			warnings = append(warnings, fmt.Sprintf("%s is not active, so it did not record this import.", mo.Name))
		case !mo.ModelType.IsScheduled():
		case to.Sub(start) > previewMaxSpan:
			warnings = append(warnings, fmt.Sprintf("%s was not scored: the dataset spans more than 90 days. Use Score range on part of it under Analytics.", mo.Name))
		default:
			job, err := m.QueueScoreRange(ctx, mo, ScoreRangeRequest{From: &start, To: &to}, true)
			if err != nil {
				log.Printf("[Models] Queueing %s to score its dataset: %v", mo.Name, err)
				warnings = append(warnings, unscored(mo, "the server log has the details"))
				continue
			}
			queued[mo], order = *job.QueuedAt, append(order, mo)
		}
	}
	for _, mo := range order {
		notes, ok := m.awaitScoreRange(ctx, mo, queued[mo])
		if !ok {
			return nil
		}
		warnings = append(warnings, notes...)
	}
	return warnings
}

func unscored(mo *Model, why string) string {
	return fmt.Sprintf("%s was not scored: %s. Use Score range on it under Analytics.", mo.Name, why)
}

// awaitScoreRange waits for the job queued at queuedAt to end and says what to
// know about it. ok is false once ctx ends.
func (m *Manager) awaitScoreRange(ctx context.Context, mo *Model, queuedAt time.Time) (notes []string, ok bool) {
	for {
		cur, err := m.Get(ctx, mo.ID)
		switch {
		case ctx.Err() != nil:
			return nil, false
		case err != nil:
			return []string{unscored(mo, "it was deleted")}, true
		case cur.ScoreRange.QueuedAt == nil || !cur.ScoreRange.QueuedAt.Equal(queuedAt):
			return []string{unscored(mo, "another Score range replaced it")}, true
		case cur.ScoreRange.Active():
		case cur.ScoreRange.Status == scoreRangeCompleted && cur.ScoreRange.Result != nil:
			res := cur.ScoreRange.Result
			if res.Truncated {
				notes = append(notes, fmt.Sprintf("%s scored its first %d pairs, which is all a pass keeps.", mo.Name, res.PairsScored))
			}
			for _, w := range res.Warnings {
				notes = append(notes, mo.Name+": "+w)
			}
			return notes, true
		case cur.ScoreRange.Status == scoreRangeCancelled:
			return []string{unscored(mo, "it was cancelled")}, true
		case cur.ScoreRange.Error == errScoreTimeout.Error():
			return []string{unscored(mo, "the dataset took too long to score")}, true
		default:
			return []string{unscored(mo, "the server log has the details")}, true
		}
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(scoreRangeWaitPoll):
		}
	}
}

// volumeAsOf is what a volume model scores as of, "" for now. A dataset's logs end
// in the past, so against today every entity's latest bucket would be empty. It
// scores the bucket holding the last log, whole or not: a lab's last day is often
// the one that matters.
func volumeAsOf(mo *Model) string {
	if mo.DatasetEnd == nil {
		return ""
	}
	bucket := 24 * time.Hour
	if mo.Definition.TimeBucket == "hour" {
		bucket = time.Hour
	}
	at := mo.DatasetEnd.UTC().Truncate(bucket).Add(bucket)
	return fmt.Sprintf("toDateTime(%s, 'UTC')", sqlquote.String(at.Format("2006-01-02 15:04:05")))
}
