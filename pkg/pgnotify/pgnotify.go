// Package pgnotify tells every app and ingest process when shared state changes, so their caches drop it at once.
package pgnotify

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"github.com/lib/pq"
)

// channel is the Postgres LISTEN/NOTIFY channel; the payload is a topic.
const channel = "bifract_invalidate"

// Topics published by Bifract.
const (
	TopicIngestTokens = "ingest_tokens"
	TopicSettings     = "settings"
)

// Bus publishes and receives change notices; after a reconnect every subscriber runs, since notices may have been lost.
type Bus struct {
	db       *sql.DB
	mu       sync.Mutex
	subs     map[string][]func()
	listener *pq.Listener
}

// New returns a bus that can publish over db. Call Listen to also receive.
func New(db *sql.DB) *Bus {
	return &Bus{db: db, subs: map[string][]func(){}}
}

// Subscribe runs fn whenever topic changes in any process, this one included.
func (b *Bus) Subscribe(topic string, fn func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[topic] = append(b.subs[topic], fn)
}

// Publish announces that topic changed; failures are only logged, as the change itself is already saved.
func (b *Bus) Publish(topic string) {
	if b == nil || b.db == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := b.db.ExecContext(ctx, `SELECT pg_notify($1, $2)`, channel, topic); err != nil {
		log.Printf("[pgnotify] Failed to announce %s change: %v", topic, err)
	}
}

// Listen receives notices without waiting to connect; once connected every subscriber runs to catch up.
func (b *Bus) Listen(connStr string) {
	b.listener = pq.NewListener(connStr, time.Second, 30*time.Second, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("[pgnotify] Listener event %d: %v", ev, err)
		}
	})
	go b.run()
	go func() {
		if err := b.listener.Listen(channel); err != nil {
			log.Printf("[pgnotify] LISTEN failed; changes from other processes apply only when caches expire or on restart: %v", err)
			return
		}
		b.dispatch(nil)
	}()
}

// Close stops listening.
func (b *Bus) Close() {
	if b.listener != nil {
		b.listener.Close()
	}
}

func (b *Bus) run() {
	keepalive := time.NewTicker(90 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case n, ok := <-b.listener.Notify:
			if !ok {
				return
			}
			b.dispatch(n)
		case <-keepalive.C:
			b.listener.Ping()
		}
	}
}

// dispatch runs the subscribers of a notice. nil is pq's signal that the
// connection was re-established and notices may have been missed.
func (b *Bus) dispatch(n *pq.Notification) {
	if n != nil {
		b.Deliver(n.Extra)
		return
	}
	b.mu.Lock()
	var fns []func()
	for _, s := range b.subs {
		fns = append(fns, s...)
	}
	b.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

// Deliver runs topic's subscribers in this process only, as a received notice does.
func (b *Bus) Deliver(topic string) {
	b.mu.Lock()
	fns := append([]func(){}, b.subs[topic]...)
	b.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}
