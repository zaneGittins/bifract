package sse

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

const (
	// notifyChannel is the Postgres LISTEN/NOTIFY channel carrying event ids.
	notifyChannel = "bifract_sse"

	// retention bounds how long a relayed event stays readable. Replicas consume
	// within milliseconds; this only covers a receiver that was briefly behind.
	relayRetention = 2 * time.Minute

	// maxRelayPayload guards against a single oversized event (a very large
	// result set) being written to the relay table on every refresh.
	maxRelayPayload = 4 << 20 // 4MB
)

// Relay fans SSE events out across app replicas.
//
// The hub is per-process, so with more than one replica behind a load balancer
// two people editing the same notebook or dashboard frequently land on
// different pods and see none of each other's changes. Every broadcast is
// written to the sse_events table and announced with pg_notify; each replica
// listens, reads the row, and delivers to its own clients.
//
// Delivery is best effort, matching SSE itself: events published while a
// replica is disconnected from Postgres are not replayed, and the client
// re-fetches state on reconnect.
type Relay struct {
	db       *sql.DB
	hub      *Hub
	origin   string
	listener *pq.Listener
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewRelay starts the listener and the janitor. Returns an error if the initial
// LISTEN fails, so a misconfigured relay is visible at startup rather than as
// silently missing events later.
func NewRelay(db *sql.DB, connStr string, hub *Hub) (*Relay, error) {
	r := &Relay{
		db:     db,
		hub:    hub,
		origin: uuid.New().String(),
		done:   make(chan struct{}),
	}

	// A failed listener reconnect is reported here; pq.Listener retries on its
	// own, so this is informational rather than fatal.
	r.listener = pq.NewListener(connStr, 1*time.Second, 30*time.Second, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("[SSE] Relay listener event %d: %v", ev, err)
		}
	})
	if err := r.listener.Listen(notifyChannel); err != nil {
		r.listener.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go r.run(ctx)

	hub.SetRelay(r)
	log.Printf("[SSE] Cross-replica relay active (origin %s)", r.origin)
	return r, nil
}

// Publish writes an event for other replicas. Local delivery has already
// happened by the time this is called.
func (r *Relay) Publish(room string, payload []byte, excludeClientID string) {
	if len(payload) > maxRelayPayload {
		log.Printf("[SSE] Relay skipping oversized event for room %s (%d bytes)", room, len(payload))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Insert and notify in one round trip. The notification carries the id and
	// the origin so a replica can discard its own events without reading the row.
	_, err := r.db.ExecContext(ctx,
		`WITH ins AS (
		     INSERT INTO sse_events (origin, room, exclude_client_id, payload)
		     VALUES ($1, $2, $3, $4)
		     RETURNING id
		 )
		 SELECT pg_notify($5, id || ':' || $1) FROM ins`,
		r.origin, room, excludeClientID, payload, notifyChannel,
	)
	if err != nil {
		log.Printf("[SSE] Relay publish failed for room %s: %v", room, err)
	}
}

func (r *Relay) run(ctx context.Context) {
	defer close(r.done)

	janitor := time.NewTicker(60 * time.Second)
	defer janitor.Stop()

	// Tickers, not time.After: the latter allocates a fresh timer on every loop
	// iteration and a busy relay iterates on every notification.
	keepalive := time.NewTicker(90 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case n := <-r.listener.Notify:
			// A nil notification means the listener reconnected. Events missed
			// while it was down are not replayed.
			if n == nil {
				continue
			}
			r.handleNotification(ctx, n.Extra)

		case <-janitor.C:
			r.cleanup(ctx)

		case <-keepalive.C:
			// Verify the listener connection is still alive.
			go r.listener.Ping()
		}
	}
}

func (r *Relay) handleNotification(ctx context.Context, extra string) {
	id, origin, ok := strings.Cut(extra, ":")
	if !ok {
		return
	}
	// Our own event: it was already delivered locally before publishing.
	if origin == r.origin {
		return
	}

	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var room, excludeClientID string
	var payload []byte
	err := r.db.QueryRowContext(qctx,
		`SELECT room, exclude_client_id, payload FROM sse_events WHERE id = $1`, id,
	).Scan(&room, &excludeClientID, &payload)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("[SSE] Relay failed to read event %s: %v", id, err)
		}
		return
	}

	// deliverLocal, not Broadcast: re-publishing would loop.
	r.hub.deliverLocal(room, payload, excludeClientID)
}

func (r *Relay) cleanup(ctx context.Context) {
	qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := r.db.ExecContext(qctx,
		`DELETE FROM sse_events WHERE created_at < NOW() - make_interval(secs => $1)`,
		relayRetention.Seconds(),
	); err != nil {
		log.Printf("[SSE] Relay cleanup failed: %v", err)
	}
}

// Close stops the relay.
func (r *Relay) Close() {
	if r.cancel != nil {
		r.cancel()
		<-r.done
	}
	if r.listener != nil {
		r.listener.Close()
	}
}
