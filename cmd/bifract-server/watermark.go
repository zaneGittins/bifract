package main

import (
	"context"
	"log"
	"time"

	"bifract/pkg/ingest"
	"bifract/pkg/storage"
	"bifract/pkg/watermark"
)

// ingestWatermarkSource reads the oldest watermark across every writing process.
func ingestWatermarkSource(pg *storage.PostgresClient) func(context.Context) (time.Time, error) {
	return func(ctx context.Context) (time.Time, error) {
		s, err := watermark.Read(ctx, pg.DB(), time.Now())
		return s.W, err
	}
}

// ingestBacklogged reports ingest pressure: this process's queue is deep, or the
// write tier (usually another process) is holding the watermark back, so reads
// would compete with its inserts.
func ingestBacklogged(pg *storage.PostgresClient, q *ingest.IngestQueue, depthThreshold int) func() bool {
	return func() bool {
		if q.Depth() > depthThreshold {
			return true
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s, err := watermark.Read(ctx, pg.DB(), time.Now())
		return err == nil && s.Lag > watermark.BacklogLag
	}
}

// startWatermarkPublisher reports this process's watermark until stopped. It
// publishes once before returning, retrying until wait elapses, so readers see the
// node before it stamps anything. Past wait it fails, since a node readers cannot
// see could have its rows read past.
func startWatermarkPublisher(pg *storage.PostgresClient, role string, wait time.Duration) *watermark.Publisher {
	p := watermark.NewPublisher(pg.DB(), watermark.Local, watermark.NodeID(role))
	deadline := time.Now().Add(wait)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := p.Publish(ctx)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			log.Fatalf("Could not publish the ingest watermark: %v", err)
		}
		log.Printf("Waiting to publish the ingest watermark (retrying): %v", err)
		time.Sleep(3 * time.Second)
	}
	p.Start()
	return p
}
