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
	WriteSustained(notifType, severity, title, message string, minInterval time.Duration) error
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
	// Params are explicitly cast to text: an untyped $1 in the SELECT list
	// defaults to text while `notification_type = $1` compares against a varchar
	// column, leaving $1 deduced as both types (Postgres error 42P08). Casting
	// pins $1 to a single type everywhere it appears.
	_, err := w.db.ExecContext(ctx, `
		INSERT INTO health_notifications (notification_type, severity, title, message)
		SELECT $1::text, $2::text, $3::text, $4::text
		WHERE NOT EXISTS (
			SELECT 1 FROM health_notifications
			WHERE notification_type = $1::text
			  AND created_at > NOW() - INTERVAL '4 hours'
		)`, notifType, severity, title, message)
	if err != nil {
		log.Printf("[Notifications] write %s: %v", notifType, err)
	}
	return err
}

// WriteSustained records a condition that is still happening. Write suppresses
// repeats for 4 hours and then lets the row age out of the 24h retention window,
// so an outage lasting longer than a day disappears from the bell while it is
// still dropping data. This instead refreshes the newest notification of the
// same type once it is older than minInterval, and immediately when the severity
// changes (warning -> critical), keeping one always-current row per condition.
// A minInterval of 0 always refreshes.
func (w *NotificationWriter) WriteSustained(notifType, severity, title, message string, minInterval time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Params are cast explicitly for the same reason as in Write.
	_, err := w.db.ExecContext(ctx, `
		WITH latest AS (
			SELECT id, created_at, severity FROM health_notifications
			WHERE notification_type = $1::text
			ORDER BY created_at DESC LIMIT 1
		), refreshed AS (
			UPDATE health_notifications h
			SET created_at = NOW(), severity = $2::text, title = $3::text, message = $4::text
			FROM latest l
			WHERE h.id = l.id
			  AND (l.severity IS DISTINCT FROM $2::text
			       OR l.created_at < NOW() - make_interval(secs => $5::float8))
			RETURNING 1
		)
		INSERT INTO health_notifications (notification_type, severity, title, message)
		SELECT $1::text, $2::text, $3::text, $4::text
		WHERE NOT EXISTS (SELECT 1 FROM latest)`,
		notifType, severity, title, message, minInterval.Seconds())
	if err != nil {
		log.Printf("[Notifications] write sustained %s: %v", notifType, err)
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
