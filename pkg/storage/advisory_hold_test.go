package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A held lock's context ends when the connection holding the lock stops
// answering, since Postgres has then released the lock to whoever asks next.
func TestWatchLockCancelsWhenTheConnectionDies(t *testing.T) {
	fail := make(chan struct{})
	ping := func(context.Context) error {
		select {
		case <-fail:
			return errors.New("connection reset")
		default:
			return nil
		}
	}
	ctx, stop := watchLock(context.Background(), ping, time.Millisecond)
	defer stop()

	select {
	case <-ctx.Done():
		t.Fatal("cancelled while the connection was healthy")
	case <-time.After(20 * time.Millisecond):
	}
	close(fail)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("not cancelled after the connection died")
	}
}

// Releasing the lock stops the watcher and ends the context.
func TestWatchLockStops(t *testing.T) {
	ctx, stop := watchLock(context.Background(), func(context.Context) error { return nil }, time.Millisecond)
	stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context still live after stop")
	}
}

// The work ending mid-ping must not cancel the ping: lib/pq marks a connection
// bad when a query's context is cancelled, and the unlock that follows then fails.
func TestWatchLockPingOutlivesTheWork(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	pingErr := make(chan error, 1)
	ping := func(ctx context.Context) error {
		close(started)
		<-release
		pingErr <- ctx.Err()
		return ctx.Err()
	}
	parent, cancelWork := context.WithCancel(context.Background())
	ctx, stop := watchLock(parent, ping, time.Millisecond)
	<-started
	cancelWork()
	<-ctx.Done()

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("stop returned while a ping was still using the connection")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-stopped
	if err := <-pingErr; err != nil {
		t.Errorf("ping context = %v, want it left alive until the ping finishes", err)
	}
}
