package notifications

import (
	"bifract/pkg/api"
	"database/sql"
	"log"
	"net/http"
	"time"

	"bifract/pkg/auth"
	"bifract/pkg/storage"
)

type Handler struct {
	db *sql.DB
}

func NewHandler(pg *storage.PostgresClient) *Handler {
	return &Handler{db: pg.DB()}
}

// apiResponse is the shared API envelope. The alias keeps the package-local
// name while there is one type, and one schema, behind it.
type apiResponse = api.Response[any]

// NotificationFeed carries the feed and the caller's unread count together, so
// the list and the badge come from one request.
type NotificationFeed struct {
	Notifications []notificationItem `json:"notifications"`
	UnreadCount   int                `json:"unread_count"`
}

// UnreadCount is the badge-only response.
type UnreadCount struct {
	UnreadCount int `json:"unread_count"`
}

type notificationItem struct {
	ID               string    `json:"id"`
	NotificationType string    `json:"notification_type"`
	Severity         string    `json:"severity"`
	Title            string    `json:"title"`
	Message          string    `json:"message"`
	CreatedAt        time.Time `json:"created_at"`
	Read             bool      `json:"read"`
}

// HandleList returns the last 24h notifications with per-item read flag and
// an aggregate unread_count. GET /api/v1/notifications
func (h *Handler) HandleList(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}

	rows, err := h.db.QueryContext(r.Context(), `
		SELECT n.id, n.notification_type, n.severity, n.title, n.message, n.created_at,
		       CASE WHEN nr.last_read_at IS NULL OR n.created_at > nr.last_read_at
		            THEN false ELSE true END AS read
		FROM health_notifications n
		LEFT JOIN notification_reads nr ON nr.username = $1
		WHERE n.created_at > NOW() - INTERVAL '24 hours'
		ORDER BY n.created_at DESC`,
		user.Username,
	)
	if err != nil {
		log.Printf("[Notifications] list query: %v", err)
		h.respondError(w, http.StatusInternalServerError, "failed to load notifications")
		return
	}
	defer rows.Close()

	items := []notificationItem{}
	unreadCount := 0
	for rows.Next() {
		var item notificationItem
		if err := rows.Scan(&item.ID, &item.NotificationType, &item.Severity,
			&item.Title, &item.Message, &item.CreatedAt, &item.Read); err != nil {
			log.Printf("[Notifications] scan: %v", err)
			continue
		}
		if !item.Read {
			unreadCount++
		}
		items = append(items, item)
	}

	h.respondSuccess(w, NotificationFeed{Notifications: items, UnreadCount: unreadCount})
}

// HandleCount returns only the unread count (lightweight, for badge polling).
// GET /api/v1/notifications/count
func (h *Handler) HandleCount(w http.ResponseWriter, r *http.Request) {
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}

	var count int
	err := h.db.QueryRowContext(r.Context(), `
		SELECT COUNT(*)
		FROM health_notifications n
		LEFT JOIN notification_reads nr ON nr.username = $1
		WHERE n.created_at > NOW() - INTERVAL '24 hours'
		  AND (nr.last_read_at IS NULL OR n.created_at > nr.last_read_at)`,
		user.Username,
	).Scan(&count)
	if err != nil {
		log.Printf("[Notifications] count query: %v", err)
		h.respondError(w, http.StatusInternalServerError, "failed to count notifications")
		return
	}

	h.respondSuccess(w, UnreadCount{UnreadCount: count})
}

// HandleMarkRead upserts notification_reads for the current user, setting
// last_read_at = NOW(). POST /api/v1/notifications/read
func (h *Handler) HandleMarkRead(w http.ResponseWriter, r *http.Request) {
	if auth.IsAPIKey(r.Context()) {
		h.respondError(w, http.StatusForbidden, "read state is per-user and not available for API key authentication")
		return
	}
	user, ok := r.Context().Value("user").(*storage.User)
	if !ok || user == nil {
		h.respondError(w, http.StatusUnauthorized, "authentication required")
		return
	}

	_, err := h.db.ExecContext(r.Context(), `
		INSERT INTO notification_reads (username, last_read_at)
		VALUES ($1, NOW())
		ON CONFLICT (username) DO UPDATE SET last_read_at = NOW()`,
		user.Username,
	)
	if err != nil {
		log.Printf("[Notifications] mark-read for %s: %v", user.Username, err)
		h.respondError(w, http.StatusInternalServerError, "failed to mark read")
		return
	}

	api.WriteOK(w, "Notifications marked read")
}

func (h *Handler) respondSuccess(w http.ResponseWriter, data interface{}) {
	api.WriteSuccess(w, data)
}

func (h *Handler) respondError(w http.ResponseWriter, statusCode int, message string) {
	api.WriteError(w, statusCode, message)
}
