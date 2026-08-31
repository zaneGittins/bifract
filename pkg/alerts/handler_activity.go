package alerts

import (
	"errors"
	"net/http"
	"time"

	"bifract/pkg/api"
	"bifract/pkg/rbac"
	"github.com/go-chi/chi/v5"
)

// AlertActivityDay is one gap-filled daily bucket of an alert's trigger history.
type AlertActivityDay struct {
	Date       string `json:"date"`
	Executions int    `json:"executions"`
	Logs       int64  `json:"logs"`
}

// AlertActivity summarizes how often an alert has been firing, for the detail
// panel sparkline. Aggregated in Postgres so the client never pulls raw
// execution rows (with their JSONB result payloads) just to draw bars.
type AlertActivity struct {
	Days            []AlertActivityDay `json:"days"`
	TotalExecutions int                `json:"total_executions"`
	TotalLogs       int64              `json:"total_logs"`
	WindowDays      int                `json:"window_days"`
	RetentionDays   int                `json:"retention_days"`
}

// HandleGetActivity returns daily trigger counts for an alert over a window
// bounded by alert_executions retention.
func (h *Handler) HandleGetActivity(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	alertID := chi.URLParam(r, "id")
	if alertID == "" {
		h.respondError(w, http.StatusBadRequest, "Alert ID is required")
		return
	}

	alert, err := h.manager.GetAlert(ctx, alertID)
	if err != nil {
		if errors.Is(err, ErrAlertNotFound) {
			h.respondError(w, http.StatusNotFound, "Alert not found")
		} else {
			h.respondError(w, http.StatusInternalServerError, "Failed to load alert")
		}
		return
	}
	if alert.FractalID != "" {
		if !h.requireRoleOnFractal(w, r, alert.FractalID, rbac.RoleViewer) {
			return
		}
	} else if alert.PrismID != "" {
		if !h.requireRoleOnPrism(w, r, alert.PrismID, rbac.RoleViewer) {
			return
		}
	}

	days := retentionDays
	if v := r.URL.Query().Get("days"); v != "" {
		if parsed, perr := parseIntParam(v); perr == nil && parsed > 0 {
			days = parsed
		}
	}
	if days > retentionDays {
		days = retentionDays
	}

	// generate_series gap-fills so the client draws a fixed number of bars
	// without reasoning about missing days.
	const q = `
		SELECT d::date, COALESCE(x.execs, 0), COALESCE(x.logs, 0)
		FROM generate_series(CURRENT_DATE - ($2::int - 1), CURRENT_DATE, INTERVAL '1 day') d
		LEFT JOIN (
			SELECT date_trunc('day', triggered_at)::date AS day,
			       COUNT(*) AS execs,
			       COALESCE(SUM(log_count), 0) AS logs
			FROM alert_executions
			WHERE alert_id = $1 AND triggered_at >= CURRENT_DATE - ($2::int - 1)
			GROUP BY 1
		) x ON x.day = d::date
		ORDER BY d`

	rows, err := h.manager.pg.Query(ctx, q, alertID, days)
	if err != nil {
		h.respondError(w, http.StatusInternalServerError, "Failed to load alert activity")
		return
	}
	defer rows.Close()

	activity := AlertActivity{
		Days:          make([]AlertActivityDay, 0, days),
		WindowDays:    days,
		RetentionDays: retentionDays,
	}
	for rows.Next() {
		var day time.Time
		var execs int
		var logs int64
		if err := rows.Scan(&day, &execs, &logs); err != nil {
			h.respondError(w, http.StatusInternalServerError, "Failed to read alert activity")
			return
		}
		activity.Days = append(activity.Days, AlertActivityDay{
			Date:       day.Format("2006-01-02"),
			Executions: execs,
			Logs:       logs,
		})
		activity.TotalExecutions += execs
		activity.TotalLogs += logs
	}
	if err := rows.Err(); err != nil {
		h.respondError(w, http.StatusInternalServerError, "Failed to read alert activity")
		return
	}

	api.WriteSuccess(w, activity)
}
