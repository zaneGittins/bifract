package query

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"bifract/pkg/storage"
)

// quotaClearer is the narrow interface used to notify the quota manager when
// logs are cleared, so it can reset in-memory usage counters.
type quotaClearer interface {
	NotifyCleared(fractalID string)
}

type StatusHandler struct {
	db           *storage.ClickHouseClient
	pg           *storage.PostgresClient
	quotaClearer quotaClearer
}

// SetQuotaClearer attaches a quota manager that is notified when logs are cleared.
func (h *StatusHandler) SetQuotaClearer(qc quotaClearer) {
	h.quotaClearer = qc
}

type StatusResponse struct {
	Success    bool              `json:"success"`
	ClickHouse ClickHouseStatus  `json:"clickhouse"`
	Error      string            `json:"error,omitempty"`
}

type ClickHouseStatus struct {
	Connected    bool   `json:"connected"`
	TotalLogs    uint64 `json:"total_logs"`
	StorageBytes uint64 `json:"storage_bytes"`
	StorageMB    string `json:"storage_mb"`
	OldestLog    string `json:"oldest_log,omitempty"`
	NewestLog    string `json:"newest_log,omitempty"`
	TableSize    string `json:"table_size"`
}

func NewStatusHandler(db *storage.ClickHouseClient, pg *storage.PostgresClient) *StatusHandler {
	return &StatusHandler{
		db: db,
		pg: pg,
	}
}

func (h *StatusHandler) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Admin only: exposes system-level ClickHouse statistics
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		respondJSON(w, http.StatusForbidden, StatusResponse{
			Success: false,
			Error:   "Admin access required",
		})
		return
	}

	status := StatusResponse{
		Success: true,
		ClickHouse: ClickHouseStatus{
			Connected: false,
		},
	}

	// Check connection
	if err := h.db.HealthCheck(r.Context()); err != nil {
		status.Success = false
		status.Error = fmt.Sprintf("ClickHouse health check failed: %v", err)
		respondJSON(w, http.StatusOK, status)
		return
	}

	status.ClickHouse.Connected = true

	// Get total log count
	countQuery := "SELECT count() as count FROM logs"
	results, err := h.db.Query(r.Context(), countQuery)
	if err == nil && len(results) > 0 {
		if count, ok := results[0]["count"].(uint64); ok {
			status.ClickHouse.TotalLogs = count
		}
	}

	// Get storage size
	sizeQuery := `
		SELECT
			formatReadableSize(sum(bytes_on_disk)) as size,
			sum(bytes_on_disk) as bytes
		FROM system.parts
		WHERE database = 'logs' AND table = 'logs' AND active = 1
	`
	results, err = h.db.Query(r.Context(), sizeQuery)
	if err == nil && len(results) > 0 {
		if size, ok := results[0]["size"].(string); ok {
			status.ClickHouse.TableSize = size
		}
		if bytes, ok := results[0]["bytes"].(uint64); ok {
			status.ClickHouse.StorageBytes = bytes
			status.ClickHouse.StorageMB = fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
		}
	}

	// Get oldest and newest log timestamps
	if status.ClickHouse.TotalLogs > 0 {
		oldestQuery := "SELECT min(timestamp) as oldest FROM logs"
		results, err = h.db.Query(r.Context(), oldestQuery)
		if err == nil && len(results) > 0 {
			if oldest, ok := results[0]["oldest"]; ok {
				status.ClickHouse.OldestLog = fmt.Sprintf("%v", oldest)
			}
		}

		newestQuery := "SELECT max(timestamp) as newest FROM logs"
		results, err = h.db.Query(r.Context(), newestQuery)
		if err == nil && len(results) > 0 {
			if newest, ok := results[0]["newest"]; ok {
				status.ClickHouse.NewestLog = fmt.Sprintf("%v", newest)
			}
		}
	}

	respondJSON(w, http.StatusOK, status)
}

func (h *StatusHandler) HandleClearLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Only admins can clear logs
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil || !user.IsAdmin {
		respondJSON(w, http.StatusForbidden, map[string]interface{}{
			"success": false,
			"error":   "Only administrators can clear logs",
		})
		return
	}

	// Check for fractal_id query parameter for per-fractal log clearing
	fractalID := r.URL.Query().Get("fractal_id")

	if fractalID != "" {
		// Per-fractal log clearing
		fmt.Printf("Clearing logs for fractal: %s\n", fractalID)

		// First, delete associated comments for this fractal
		if h.pg != nil {
			err := h.pg.DeleteCommentsByFractalID(r.Context(), fractalID)
			if err != nil {
				respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"success": false,
					"error":   fmt.Sprintf("Failed to delete associated comments for fractal: %v", err),
				})
				return
			}
		}

		// Reset quota counters immediately so the fractal can accept new logs
		// right away rather than waiting for the next stats refresh.
		if h.quotaClearer != nil {
			h.quotaClearer.NotifyCleared(fractalID)
		}

		// Delete logs in the background; large fractals can take minutes and
		// we don't want the HTTP connection to outlive the operation.
		go func(id string) {
			if err := h.db.DeleteLogsByFractalID(context.Background(), id); err != nil {
				log.Printf("background delete failed for fractal %s: %v", id, err)
			}
		}(fractalID)

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": fmt.Sprintf("All logs and associated comments for fractal %s have been cleared", fractalID),
		})
	} else {
		// Global log clearing (existing behavior)
		fmt.Println("Clearing all logs globally")

		// First, delete all associated comments (cascading delete)
		if h.pg != nil {
			err := h.pg.DeleteAllComments(r.Context())
			if err != nil {
				respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"success": false,
					"error":   fmt.Sprintf("Failed to delete associated comments: %v", err),
				})
				return
			}
			fmt.Println("Successfully deleted all comments before clearing logs")
		}

		// Then truncate the logs table
		err := h.db.Exec(r.Context(), "TRUNCATE TABLE logs")
		if err != nil {
			respondJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"error":   fmt.Sprintf("Failed to clear logs: %v", err),
			})
			return
		}

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"success": true,
			"message": "All logs and associated comments have been cleared",
		})
	}
}
