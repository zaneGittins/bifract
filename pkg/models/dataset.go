package models

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

var (
	// scoreBusyPoll is how often a dataset's scoring pass retries a busy slot, and
	// scoreBusyWait how long the whole pass may wait for one.
	scoreBusyPoll = 5 * time.Second
	scoreBusyWait = 15 * time.Minute
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
		if err := m.ch.ClearModelData(ctx, mo.ID); err != nil {
			return fmt.Errorf("reset model %s: %w", mo.Name, err)
		}
		// updated_at is what stops a Score range read of the dropped logs writing.
		if _, err := m.pg.Exec(ctx,
			`UPDATE analytics_models
			    SET backfill_status = 'none', backfill_window = '', backfill_total = 0, backfill_done = 0,
			        backfill_started_at = NULL, backfill_from = NULL, backfill_to = NULL, backfill_error = '',
			        updated_at = NOW()
			  WHERE id = $1`, mo.ID); err != nil {
			return fmt.Errorf("reset model %s: %w", mo.Name, err)
		}
	}
	return nil
}

// ScoreFractal scores a fractal's network models over a dataset's range, which
// their rolling state cannot cover. The other types read the logs as they arrive.
// It returns a line, safe to show, for anything to know about the pass: a model
// left unscored, or one that recorded nothing because it is not active. An import
// is complete without them, and Score range can run them again.
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
	deadline := time.Now().Add(scoreBusyWait)
	var warnings []string
	for _, mo := range list {
		switch {
		case mo.Status != "active":
			warnings = append(warnings, fmt.Sprintf("%s is not active, so it did not record this import.", mo.Name))
		case mo.ModelType.IsScheduled():
			warnings = append(warnings, m.scoreDatasetModel(ctx, mo, start, to, deadline)...)
		}
	}
	return warnings
}

// scoreDatasetModel scores one model, waiting out a busy scorer until deadline.
func (m *Manager) scoreDatasetModel(ctx context.Context, mo *Model, start, end, deadline time.Time) []string {
	unscored := func(why string) []string {
		return []string{fmt.Sprintf("%s was not scored: %s. Use Score range on it under Analytics.", mo.Name, why)}
	}
	if end.Sub(start) > previewMaxSpan {
		return []string{fmt.Sprintf("%s was not scored: the dataset spans more than 90 days. Use Score range on part of it under Analytics.", mo.Name)}
	}
	for {
		res, err := m.ScoreRange(ctx, mo, ScoreRangeRequest{From: &start, To: &end})
		switch {
		case err == nil:
			var notes []string
			if res.Truncated {
				notes = append(notes, fmt.Sprintf("%s scored its first %d pairs, which is all a pass keeps.", mo.Name, res.PairsScored))
			}
			for _, w := range res.Warnings {
				notes = append(notes, mo.Name+": "+w)
			}
			return notes
		case errors.Is(err, errScoreTimeout):
			return unscored("the dataset took too long to score")
		case !errors.Is(err, errScoreBusy) && !errors.Is(err, errScoreIngest):
			log.Printf("[Models] Scoring %s over its dataset: %v", mo.Name, err)
			return unscored("the server log has the details")
		case time.Now().After(deadline):
			return unscored("the server stayed busy")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(scoreBusyPoll):
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
	return fmt.Sprintf("toDateTime('%s', 'UTC')", at.Format("2006-01-02 15:04:05"))
}
