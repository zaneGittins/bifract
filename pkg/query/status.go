package query

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

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

	cacheMu    sync.RWMutex
	cached     *StatusResponse
	cachedAt   time.Time
	cacheTTL   time.Duration
}

// SetQuotaClearer attaches a quota manager that is notified when logs are cleared.
func (h *StatusHandler) SetQuotaClearer(qc quotaClearer) {
	h.quotaClearer = qc
}

type StatusResponse struct {
	Success    bool             `json:"success"`
	ClickHouse ClickHouseStatus `json:"clickhouse"`
	Error      string           `json:"error,omitempty"`
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
		db:       db,
		pg:       pg,
		cacheTTL: 60 * time.Second,
	}
}

// HandleHealthCheck is an ultralight endpoint that only pings ClickHouse.
// Used by the UI status dot; no expensive queries.
func (h *StatusHandler) HandleHealthCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	connected := h.db.HealthCheck(ctx) == nil
	w.Header().Set("Content-Type", "application/json")
	if connected {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success":true,"connected":true}`))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success":true,"connected":false}`))
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

	// Serve from cache if fresh
	h.cacheMu.RLock()
	if h.cached != nil && time.Since(h.cachedAt) < h.cacheTTL {
		resp := *h.cached
		h.cacheMu.RUnlock()
		respondJSON(w, http.StatusOK, resp)
		return
	}
	h.cacheMu.RUnlock()

	status := h.fetchStatus()

	// Store in cache
	h.cacheMu.Lock()
	h.cached = &status
	h.cachedAt = time.Now()
	h.cacheMu.Unlock()

	respondJSON(w, http.StatusOK, status)
}

// fetchStatus queries ClickHouse for the full status. Uses a background
// context so it is not canceled by a departing HTTP client.
func (h *StatusHandler) fetchStatus() StatusResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	status := StatusResponse{
		Success: true,
		ClickHouse: ClickHouseStatus{
			Connected: false,
		},
	}

	if err := h.db.HealthCheck(ctx); err != nil {
		status.Success = false
		status.Error = fmt.Sprintf("ClickHouse health check failed: %v", err)
		return status
	}

	status.ClickHouse.Connected = true

	// Storage size from system.parts metadata (lightweight, no data scan)
	sizeQuery := `
		SELECT
			formatReadableSize(sum(bytes_on_disk)) as size,
			sum(bytes_on_disk) as bytes
		FROM system.parts
		WHERE database = 'logs' AND table = 'logs' AND active = 1
	`
	results, err := h.db.Query(ctx, sizeQuery)
	if err == nil && len(results) > 0 {
		if size, ok := results[0]["size"].(string); ok {
			status.ClickHouse.TableSize = size
		}
		if bytes, ok := results[0]["bytes"].(uint64); ok {
			status.ClickHouse.StorageBytes = bytes
			status.ClickHouse.StorageMB = fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
		}
	}

	// Use system.parts row counts instead of SELECT count() full scan.
	// system.parts.rows is maintained by ClickHouse metadata and is free.
	countQuery := `
		SELECT sum(rows) as count
		FROM system.parts
		WHERE database = 'logs' AND table = 'logs' AND active = 1
	`
	results, err = h.db.Query(ctx, countQuery)
	if err == nil && len(results) > 0 {
		if count, ok := results[0]["count"].(uint64); ok {
			status.ClickHouse.TotalLogs = count
		}
	}

	// min/max timestamp: use system.parts min/max columns to avoid full scans.
	// MergeTree stores per-part min/max for the partition key and ORDER BY columns.
	if status.ClickHouse.TotalLogs > 0 {
		minMaxQuery := `
			SELECT
				min(min_time) as oldest,
				max(max_time) as newest
			FROM system.parts
			WHERE database = 'logs' AND table = 'logs' AND active = 1
		`
		results, err = h.db.Query(ctx, minMaxQuery)
		if err == nil && len(results) > 0 {
			if oldest, ok := results[0]["oldest"]; ok {
				status.ClickHouse.OldestLog = fmt.Sprintf("%v", oldest)
			}
			if newest, ok := results[0]["newest"]; ok {
				status.ClickHouse.NewestLog = fmt.Sprintf("%v", newest)
			}
		}
	}

	return status
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
		truncateSQL := h.db.InjectOnCluster("TRUNCATE TABLE logs")
		err := h.db.Exec(r.Context(), truncateSQL)
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
