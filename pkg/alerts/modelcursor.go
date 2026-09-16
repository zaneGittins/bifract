package alerts

import (
	"context"
	"strings"
	"time"

	"bifract/pkg/parser"
)

// Model state is maintained by a scheduled reader rather than at insert, so it
// trails ingestion by up to one maintenance cycle. An alert that consults a model
// must not evaluate past that model's watermark: a log whose entity the model has
// not recorded yet is dropped by model_lookup's default require=true, and the
// alert's cursor would move past it. For a first_seen model the entity may never
// recur, so the detection would be lost rather than delayed.
//
// Holding the cursor at the watermark turns that loss into a delay bounded by the
// maintenance interval.

// modelWatermark caps toTime at the oldest watermark among the models an alert
// consults. A zero return means the alert should not evaluate yet.
func (e *Engine) modelWatermark(ctx context.Context, alert *Alert, toTime time.Time) time.Time {
	names := modelNamesIn(alert.QueryString)
	if len(names) == 0 {
		return toTime
	}
	rows, err := e.pg.Query(ctx,
		`SELECT MIN(COALESCE(state_watermark, created_at)) FROM analytics_models WHERE name = ANY($1)`,
		names)
	if err != nil {
		// Unknown rather than current: evaluating past an unknown watermark is the
		// case this exists to prevent.
		return time.Time{}
	}
	defer rows.Close()
	if !rows.Next() {
		return toTime
	}
	var wm *time.Time
	if err := rows.Scan(&wm); err != nil || wm == nil {
		return toTime
	}
	if wm.Before(toTime) {
		return *wm
	}
	return toTime
}

// modelNamesIn lists the models a query consults. Parsed rather than matched on
// text so a name inside a string literal is not mistaken for a reference.
func modelNamesIn(query string) []string {
	pipeline, err := parser.ParseQuery(query)
	if err != nil {
		return nil
	}
	var names []string
	seen := map[string]bool{}
	for _, cmd := range pipeline.Commands {
		if !strings.EqualFold(cmd.Name, "model_lookup") && !strings.EqualFold(cmd.Name, "modellookup") {
			continue
		}
		b, err := parser.BindCommand(cmd)
		if err != nil {
			continue
		}
		if n := b.Str("model", ""); n != "" && !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	return names
}
