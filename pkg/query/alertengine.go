package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"bifract/pkg/settings"
)

// The System > Alerts view reports on the alert *engine*: is it keeping up, what
// is firing, what has been auto-disabled, and are the actions landing. The rules
// themselves belong to the Alerts tab.
//
// Everything here reads columns that already exist. One shape is deliberately not
// available: alert_executions records a row only when an alert triggers, never on
// a plain evaluation, so per-alert latency history does not exist. The latency
// chart is the fleet-wide alert_exec_ms sample series, and per-alert exec time is
// the most recent evaluation only. Anything claiming otherwise would be inventing
// data.
const (
	alertRowLimit   = 100
	alertStripLimit = 8
	// alertLagWarnSec is when evaluation is far enough behind to say so. The
	// engine ticks on an admin-set interval, so "behind" is relative to it.
	alertLagWarnMultiple = 2
)

// alertMaxWindowMinutes caps the lookback at 24 hours however wide the page's
// range selector is set. alert_executions is unpartitioned and retained for 30
// days, so a window approaching retention is the whole table: the planner drops
// the triggered_at index for a sequential scan and the action JSONB is expanded
// per row, three times over (once per UNION branch). A page whose job is to
// monitor the system must not become the load. Matches activityRange, which caps
// for the same reason, and the effective window is reported so the UI can say so.
const alertMaxWindowMinutes = 1440

// alertRange maps the page's range selector to the effective window in minutes
// and the bucket width that divides it into a readable number of points. Both
// come from here so the two charts always cover the same span.
func alertRange(r string) (windowMin, bucketSec int) {
	switch r {
	case "8h":
		return 480, 1200
	case "24h", "7d", "30d":
		return alertMaxWindowMinutes, 3600
	}
	return 60, 150
}

// alertAction is one action attempt flattened out of the three JSONB columns, so
// a webhook, a fractal write and an email read the same way.
type alertAction struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// actionsCTE flattens the three result columns into one set. Each carries its own
// name field, so they are normalised here rather than at every call site.
const actionsCTE = `
WITH acts AS (
	SELECT ae.alert_id, ae.triggered_at, 'webhook' AS kind,
	       COALESCE(r->>'webhook_name', 'webhook') AS name,
	       COALESCE((r->>'success')::boolean, false) AS ok
	FROM alert_executions ae
	CROSS JOIN LATERAL jsonb_array_elements(COALESCE(ae.webhook_results, '[]'::jsonb)) r
	WHERE ae.triggered_at > NOW() - make_interval(mins => $1)
	UNION ALL
	SELECT ae.alert_id, ae.triggered_at, 'fractal',
	       COALESCE(r->>'fractal_action_name', 'fractal'),
	       COALESCE((r->>'success')::boolean, false)
	FROM alert_executions ae
	CROSS JOIN LATERAL jsonb_array_elements(COALESCE(ae.fractal_results, '[]'::jsonb)) r
	WHERE ae.triggered_at > NOW() - make_interval(mins => $1)
	UNION ALL
	SELECT ae.alert_id, ae.triggered_at, 'email',
	       COALESCE(r->>'email_action_name', 'email'),
	       COALESCE((r->>'success')::boolean, false)
	FROM alert_executions ae
	CROSS JOIN LATERAL jsonb_array_elements(COALESCE(ae.email_results, '[]'::jsonb)) r
	WHERE ae.triggered_at > NOW() - make_interval(mins => $1)
)`

// HandleAlertStats returns alert engine health: the headline tiles, the persisted
// latency series, fires over time by severity, and the auto-disabled list.
func (h *PerformanceHandler) HandleAlertStats(w http.ResponseWriter, r *http.Request) {
	if h.pg == nil {
		respondJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"success": false, "error": "Postgres not available",
		})
		return
	}
	ctx := r.Context()
	mins, bucketSec := alertRange(r.URL.Query().Get("range"))
	interval := settings.Get().AlertEvalIntervalSeconds
	if interval <= 0 {
		interval = 60
	}

	result := map[string]interface{}{"success": true}
	summary := map[string]interface{}{
		"eval_interval_sec": interval,
		"lag_warn_sec":      interval * alertLagWarnMultiple,
		"window_minutes":    mins,
	}

	// Evaluation health. Lag is measured only over alerts that are actually being
	// evaluated: a disabled alert's cursor is frozen and would dominate the p95
	// with a number that means nothing.
	//
	// The subselects are deliberate. An earlier version counted disabled alerts
	// with FILTER inside a query already restricted to enabled = true, so it could
	// only ever find alerts that had been auto-disabled and then re-enabled
	// without their reason being cleared. It reported zero on a fleet with real
	// auto-disabled alerts in it.
	var evaluating, disabled int64
	var lagP95, lagMax float64
	err := h.pg.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM alerts WHERE enabled),
			(SELECT count(*) FROM alerts WHERE NOT enabled AND COALESCE(disabled_reason, '') <> ''),
			COALESCE((SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (
				ORDER BY EXTRACT(EPOCH FROM (NOW() - last_evaluated_at))) FROM alerts WHERE enabled), 0),
			COALESCE((SELECT MAX(EXTRACT(EPOCH FROM (NOW() - last_evaluated_at))) FROM alerts WHERE enabled), 0)
	`).Scan(&evaluating, &disabled, &lagP95, &lagMax)
	if err != nil {
		log.Printf("[AlertEngine] health: %v", err)
	}
	summary["evaluating"] = evaluating
	summary["disabled"] = disabled
	summary["lag_p95_sec"] = int64(math.Round(math.Max(lagP95, 0)))
	summary["lag_max_sec"] = int64(math.Round(math.Max(lagMax, 0)))

	// Name the worst offender: a lag number without a culprit is not actionable.
	var worstName string
	if err := h.pg.QueryRow(ctx,
		`SELECT name FROM alerts WHERE enabled ORDER BY last_evaluated_at ASC NULLS FIRST LIMIT 1`,
	).Scan(&worstName); err != nil && err != sql.ErrNoRows {
		log.Printf("[AlertEngine] worst lag: %v", err)
	}
	summary["lag_max_alert"] = worstName

	// What actually fired in the range.
	var fires, firingAlerts, throttled int64
	var logsMatched sql.NullInt64
	if err := h.pg.QueryRow(ctx, `
		SELECT count(*), count(DISTINCT alert_id), COALESCE(SUM(log_count), 0), count(*) FILTER (WHERE throttled)
		FROM alert_executions
		WHERE triggered_at > NOW() - make_interval(mins => $1)`, mins,
	).Scan(&fires, &firingAlerts, &logsMatched, &throttled); err != nil {
		log.Printf("[AlertEngine] fires: %v", err)
	}
	summary["fires"] = fires
	summary["firing_alerts"] = firingAlerts
	summary["logs_matched"] = logsMatched.Int64
	summary["throttled"] = throttled

	// Action delivery. A webhook that has been failing all day is invisible
	// anywhere else in the product.
	var actionFailures, actionTotal int64
	var worstAction sql.NullString
	if err := h.pg.QueryRow(ctx, actionsCTE+`
		SELECT
			count(*) FILTER (WHERE NOT ok),
			count(*),
			(SELECT name FROM acts WHERE NOT ok GROUP BY name ORDER BY count(*) DESC LIMIT 1)
		FROM acts`, mins,
	).Scan(&actionFailures, &actionTotal, &worstAction); err != nil {
		log.Printf("[AlertEngine] actions: %v", err)
	}
	summary["action_failures"] = actionFailures
	summary["action_total"] = actionTotal
	summary["action_worst"] = worstAction.String
	result["summary"] = summary

	// Latency series: persisted every 30s by the metrics collector, so it survives
	// restarts instead of being rebuilt only while the tab is open. It reads the
	// same effective window as the fires chart beside it.
	since := time.Duration(mins) * time.Minute
	execHist := []map[string]interface{}{}
	if points, err := h.pg.QueryMetricSeries(ctx, "alert_exec_ms", since, bucketSec); err != nil {
		log.Printf("[AlertEngine] exec history: %v", err)
	} else {
		for _, p := range points {
			execHist = append(execHist, map[string]interface{}{
				"time": p.Bucket.Unix(), "avg_ms": int64(math.Round(p.Value)),
			})
		}
	}
	result["exec_history"] = execHist
	result["fires_history"] = h.alertFiresHistory(ctx, mins, bucketSec)
	result["disabled_alerts"] = h.alertDisabledList(ctx)
	respondJSON(w, http.StatusOK, result)
}

// alertFiresHistory buckets triggers by severity so the chart can stack them.
func (h *PerformanceHandler) alertFiresHistory(ctx context.Context, mins, bucketSec int) []map[string]interface{} {
	out := []map[string]interface{}{}
	if bucketSec <= 0 {
		bucketSec = 60
	}
	rows, err := h.pg.Query(ctx, `
		SELECT
			(floor(EXTRACT(EPOCH FROM ae.triggered_at) / $2) * $2)::bigint AS bucket,
			COALESCE(NULLIF(a.severity, ''), 'medium') AS severity,
			count(*) AS n
		FROM alert_executions ae
		JOIN alerts a ON a.id = ae.alert_id
		WHERE ae.triggered_at > NOW() - make_interval(mins => $1)
		GROUP BY 1, 2
		ORDER BY 1`, mins, bucketSec)
	if err != nil {
		log.Printf("[AlertEngine] fires history: %v", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var bucket, n int64
		var severity string
		if err := rows.Scan(&bucket, &severity, &n); err != nil {
			continue
		}
		out = append(out, map[string]interface{}{"time": bucket, "severity": severity, "n": n})
	}
	return out
}

// alertDisabledList names the auto-disabled alerts and why. "Disabled: 3" told an
// operator something was wrong and gave them nothing to act on.
func (h *PerformanceHandler) alertDisabledList(ctx context.Context) []map[string]interface{} {
	out := []map[string]interface{}{}
	rows, err := h.pg.Query(ctx, `
		SELECT id::text, name, COALESCE(NULLIF(severity, ''), 'medium'),
		       COALESCE(fractal_id::text, ''), COALESCE(prism_id::text, ''),
		       COALESCE(disabled_reason, ''), updated_at
		FROM alerts
		WHERE NOT enabled AND COALESCE(disabled_reason, '') <> ''
		ORDER BY updated_at DESC
		LIMIT $1`, alertStripLimit)
	if err != nil {
		log.Printf("[AlertEngine] disabled list: %v", err)
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var id, name, severity, fractalID, prismID, reason string
		var since time.Time
		if err := rows.Scan(&id, &name, &severity, &fractalID, &prismID, &reason, &since); err != nil {
			continue
		}
		out = append(out, map[string]interface{}{
			"id": id, "name": name, "severity": severity,
			"fractal_id": fractalID, "prism_id": prismID,
			"reason": alertShortReason(reason), "since": since.UTC().Format(time.RFC3339),
		})
	}
	return out
}

// alertShortReason trims the engine's "Auto-disabled: " prefix, which is implied
// by the column the value is read from.
func alertShortReason(reason string) string {
	reason = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(reason), "Auto-disabled:"))
	if runes := []rune(reason); len(runes) > 180 {
		reason = string(runes[:180])
	}
	return reason
}

// alertRowFilters builds the shared name/fractal predicate for both table modes.
// Args start at $2 because $1 is always the window.
func alertRowFilters(q, fractal, nameCol, fractalCol string, args *[]interface{}) string {
	var where string
	// The placeholder is len(args) after the append: args is 1-indexed in SQL, so
	// the value just appended is $len. Adding one leaves a gap and Postgres
	// rejects the statement with "could not determine data type of parameter".
	if q = strings.TrimSpace(q); q != "" {
		if len(q) > 120 {
			q = q[:120]
		}
		*args = append(*args, "%"+q+"%")
		where += fmt.Sprintf(" AND %s ILIKE $%d", nameCol, len(*args))
	}
	if fractal = strings.TrimSpace(fractal); fractal != "" {
		*args = append(*args, fractal)
		where += fmt.Sprintf(" AND %s = $%d", fractalCol, len(*args))
	}
	return where
}

// HandleAlertRows returns the table under the charts: one row per alert, or one
// row per recent trigger.
func (h *PerformanceHandler) HandleAlertRows(w http.ResponseWriter, r *http.Request) {
	if h.pg == nil {
		respondJSON(w, http.StatusServiceUnavailable, map[string]interface{}{
			"success": false, "error": "Postgres not available",
		})
		return
	}
	ctx := r.Context()
	query := r.URL.Query()
	mins, _ := alertRange(query.Get("range"))
	if query.Get("mode") == "fires" {
		h.respondAlertFires(ctx, w, mins, query.Get("q"), query.Get("fractal"))
		return
	}
	h.respondAlertList(ctx, w, mins, query.Get("q"), query.Get("fractal"))
}

func (h *PerformanceHandler) respondAlertList(ctx context.Context, w http.ResponseWriter, mins int, q, fractal string) {
	args := []interface{}{mins}
	where := alertRowFilters(q, fractal, "a.name", "a.fractal_id::text", &args)

	// Disabled first, then the furthest behind: the rows that need attention sort
	// to the top rather than the alphabet deciding.
	sqlText := actionsCTE + `,
	fired AS (
		SELECT alert_id, count(*) AS fires, COALESCE(SUM(log_count), 0) AS logs,
		       count(*) FILTER (WHERE throttled) AS throttled
		FROM alert_executions
		WHERE triggered_at > NOW() - make_interval(mins => $1)
		GROUP BY alert_id
	),
	acted AS (
		SELECT alert_id, count(*) AS total, count(*) FILTER (WHERE NOT ok) AS failed,
		       (array_agg(name ORDER BY ok ASC))[1] AS worst
		FROM acts GROUP BY alert_id
	)
	SELECT a.id::text, a.name, COALESCE(NULLIF(a.severity, ''), 'medium'),
	       COALESCE(a.fractal_id::text, ''), COALESCE(a.prism_id::text, ''), a.enabled,
	       COALESCE(a.disabled_reason, ''),
	       GREATEST(COALESCE(EXTRACT(EPOCH FROM (NOW() - a.last_evaluated_at)), 0), 0),
	       COALESCE(a.last_execution_time_ms, 0),
	       COALESCE(f.fires, 0), COALESCE(f.throttled, 0), COALESCE(f.logs, 0),
	       COALESCE(x.total, 0), COALESCE(x.failed, 0), COALESCE(x.worst, '')
	FROM alerts a
	LEFT JOIN fired f ON f.alert_id = a.id
	LEFT JOIN acted x ON x.alert_id = a.id
	WHERE (a.enabled OR COALESCE(a.disabled_reason, '') <> '')` + where + `
	ORDER BY (a.enabled = false) DESC,
	         GREATEST(COALESCE(EXTRACT(EPOCH FROM (NOW() - a.last_evaluated_at)), 0), 0) DESC
	LIMIT ` + fmt.Sprint(alertRowLimit)

	rows, err := h.pg.Query(ctx, sqlText, args...)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": fmt.Sprintf("alert rows query failed: %v", err),
		})
		return
	}
	defer rows.Close()

	out := []map[string]interface{}{}
	for rows.Next() {
		var id, name, severity, fractalID, prismID, reason, worst string
		var enabled bool
		var lag float64
		var execMs, fires, throttled, logs, actTotal, actFailed int64
		if err := rows.Scan(&id, &name, &severity, &fractalID, &prismID, &enabled, &reason,
			&lag, &execMs, &fires, &throttled, &logs, &actTotal, &actFailed, &worst); err != nil {
			continue
		}
		out = append(out, map[string]interface{}{
			"id": id, "name": name, "severity": severity,
			"fractal_id": fractalID, "prism_id": prismID,
			"enabled": enabled, "disabled_reason": alertShortReason(reason),
			"lag_sec": int64(math.Round(lag)), "exec_ms": execMs,
			"fires": fires, "throttled": throttled, "logs": logs,
			"action_total": actTotal, "action_failed": actFailed, "action_worst": worst,
		})
	}
	// Say when the list was cut. A fleet can hold thousands of alerts, and a table
	// that silently shows the first hundred reads as the whole fleet.
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success": true, "mode": "alerts", "rows": out,
		"truncated": len(out) >= alertRowLimit, "limit": alertRowLimit,
	})
}

func (h *PerformanceHandler) respondAlertFires(ctx context.Context, w http.ResponseWriter, mins int, q, fractal string) {
	args := []interface{}{mins}
	where := alertRowFilters(q, fractal, "a.name", "ae.fractal_id::text", &args)

	rows, err := h.pg.Query(ctx, `
		SELECT ae.triggered_at, a.name, COALESCE(NULLIF(a.severity, ''), 'medium'),
		       COALESCE(ae.fractal_id::text, ''), COALESCE(ae.prism_id::text, ''),
		       ae.log_count, COALESCE(ae.throttled, false), COALESCE(ae.throttle_key, ''),
		       COALESCE(ae.execution_time_ms, 0),
		       COALESCE(ae.webhook_results, '[]'::jsonb)::text,
		       COALESCE(ae.fractal_results, '[]'::jsonb)::text,
		       COALESCE(ae.email_results, '[]'::jsonb)::text
		FROM alert_executions ae
		JOIN alerts a ON a.id = ae.alert_id
		WHERE ae.triggered_at > NOW() - make_interval(mins => $1)`+where+`
		ORDER BY ae.triggered_at DESC
		LIMIT `+fmt.Sprint(alertRowLimit), args...)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "error": fmt.Sprintf("alert fires query failed: %v", err),
		})
		return
	}
	defer rows.Close()

	out := []map[string]interface{}{}
	for rows.Next() {
		var at time.Time
		var name, severity, fractalID, prismID, throttleKey string
		var logCount, execMs int64
		var throttled bool
		var webhookJSON, fractalJSON, emailJSON string
		if err := rows.Scan(&at, &name, &severity, &fractalID, &prismID, &logCount,
			&throttled, &throttleKey, &execMs, &webhookJSON, &fractalJSON, &emailJSON); err != nil {
			continue
		}
		// Flattened in Go rather than SQL: the row set is already bounded, and the
		// three result shapes differ only by which key holds the name.
		actions := append(append(
			decodeAlertActions("webhook", webhookJSON, "webhook_name"),
			decodeAlertActions("fractal", fractalJSON, "fractal_action_name")...),
			decodeAlertActions("email", emailJSON, "email_action_name")...)
		out = append(out, map[string]interface{}{
			"time": at.UTC().Format(time.RFC3339), "name": name, "severity": severity,
			"fractal_id": fractalID, "prism_id": prismID,
			"logs": logCount, "throttled": throttled, "throttle_key": throttleKey,
			"exec_ms": execMs, "actions": actions,
		})
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success": true, "mode": "fires", "rows": out,
		"truncated": len(out) >= alertRowLimit, "limit": alertRowLimit,
	})
}

// decodeAlertActions reads one results column into the common action shape.
func decodeAlertActions(kind, raw, nameKey string) []alertAction {
	var items []map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return nil
	}
	out := make([]alertAction, 0, len(items))
	for _, item := range items {
		ok, _ := item["success"].(bool)
		name, _ := item[nameKey].(string)
		if name == "" {
			name = kind
		}
		detail, _ := item["error"].(string)
		if ok {
			detail = ""
		} else if detail == "" {
			if code, isNum := item["status_code"].(float64); isNum && code > 0 {
				detail = fmt.Sprintf("HTTP %d", int(code))
			} else {
				detail = "failed"
			}
		}
		out = append(out, alertAction{Kind: kind, Name: name, OK: ok, Detail: detail})
	}
	return out
}
