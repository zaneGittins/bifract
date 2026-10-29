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
