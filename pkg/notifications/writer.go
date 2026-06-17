package notifications

import (
	"context"
	"database/sql"
	"log"
	"time"

	"bifract/pkg/storage"
)

// WriterIface is the subset used by ingest and alert packages to avoid
// importing this package directly (prevents potential import cycles).
type WriterIface interface {
	Write(notifType, severity, title, message string) error
}

// NotificationWriter inserts health_notifications rows with 4-hour dedup.
type NotificationWriter struct {
	db *sql.DB
}

// New creates a NotificationWriter and starts the background cleanup goroutine
// that deletes notifications older than 24 hours.
func New(pg *storage.PostgresClient) *NotificationWriter {
	w := &NotificationWriter{db: pg.DB()}
	go w.cleanupLoop()
	return w
}

// Write inserts a notification unless the same notification_type already
// exists within the last 4 hours. Concurrent calls are safe — the dedup
// is a single INSERT ... WHERE NOT EXISTS (best-effort; rare duplicates
// across replicas are cosmetically harmless).
func (w *NotificationWriter) Write(notifType, severity, title, message string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := w.db.ExecContext(ctx, `
		INSERT INTO health_notifications (notification_type, severity, title, message)
		SELECT $1, $2, $3, $4
		WHERE NOT EXISTS (
			SELECT 1 FROM health_notifications
			WHERE notification_type = $1
			  AND created_at > NOW() - INTERVAL '4 hours'
		)`, notifType, severity, title, message)
	if err != nil {
		log.Printf("[Notifications] write %s: %v", notifType, err)
	}
	return err
}

func (w *NotificationWriter) cleanupLoop() {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for range t.C {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := w.db.ExecContext(ctx,
			`DELETE FROM health_notifications WHERE created_at < NOW() - INTERVAL '24 hours'`)
		cancel()
		if err != nil {
			log.Printf("[Notifications] cleanup: %v", err)
			continue
		}
		if n, _ := res.RowsAffected(); n > 0 {
			log.Printf("[Notifications] cleaned up %d expired notification(s)", n)
		}
	}
}
