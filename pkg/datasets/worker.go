package datasets

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"bifract/pkg/datasetfile"
	"bifract/pkg/exchange"
	"bifract/pkg/ingest"
	"bifract/pkg/sse"
	"bifract/pkg/storage"
)

// Room is the SSE room import progress is broadcast to.
const Room = "datasets"

// EventProgress carries an Import each time one changes.
const EventProgress = "dataset_import"

const (
	pollInterval = 5 * time.Second
	// staleAfter must stay well above reportInterval, and above the time a
	// stopping server takes to drain its ingest queue, so a claim is only taken
	// over once nothing can still be writing for it.
	staleAfter = 3 * time.Minute
	batchRows  = 5000
	batchBytes = 8 << 20
)

var (
	// reportInterval is how often a running import records progress, which also
	// keeps its claim.
	reportInterval = 2 * time.Second
	// retryEnqueue is the wait while the ingest queue is busy or refuses a batch.
	retryEnqueue = 500 * time.Millisecond
	// settlePoll is how often stored logs are counted once all are queued, and
	// settleTimeout how long that count may stand still before the import fails.
	settlePoll    = 2 * time.Second
	settleTimeout = 5 * time.Minute
)

// Queue accepts logs for insert, refusing them under backpressure.
type Queue interface {
	Enqueue([]storage.LogEntry) bool
	// Depth is how many batches are waiting to be inserted.
	Depth() int
}

// LogStore holds a fractal's logs in ClickHouse.
type LogStore interface {
	DeleteLogsByFractalID(ctx context.Context, fractalID string, includeUnscoped bool) error
	CountFractalLogs(ctx context.Context, fractalID string) (uint64, error)
}

// Broadcaster sends an event to an SSE room.
type Broadcaster interface {
	Broadcast(room string, event sse.Event, excludeClientID string)
}

// Worker runs dataset imports, one at a time per server.
type Worker struct {
	store  *Store
	client *http.Client
	queue  Queue
	logs   LogStore
	hub    Broadcaster
	// maxDepth is the queue depth an import waits below, leaving the rest of the
	// queue to live ingest.
	maxDepth int
	// dir holds a download while it is verified and read.
	dir string
}

func NewWorker(store *Store, client *http.Client, queue Queue, logs LogStore, hub Broadcaster, maxDepth int, dir string) *Worker {
	return &Worker{store: store, client: client, queue: queue, logs: logs, hub: hub, maxDepth: max(maxDepth, 1), dir: dir}
}

// Start polls for imports until the returned stop is called. Stopping leaves a
// running import as it is; it is claimed again, from the start, once stale.
func (w *Worker) Start() (stop func()) {
	datasetfile.RemoveStale(w.dir, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(pollInterval)
		defer t.Stop()
		for {
			for w.RunOnce(ctx) {
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return func() { cancel(); wg.Wait() }
}

// RunOnce claims one import and runs it, reporting whether there was one.
func (w *Worker) RunOnce(ctx context.Context) bool {
	imp, err := w.store.Claim(ctx, staleAfter)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("[Datasets] %v", err)
		}
		return false
	}
	if imp == nil {
		return false
	}
	w.broadcast(imp)
	log.Printf("[Datasets] Importing %s into fractal %s", imp.DatasetID, imp.FractalName)
	w.run(ctx, imp)
	return ctx.Err() == nil
}

// run imports one claimed dataset from the start: the fractal is emptied first,
// since a claim may be a retry or a takeover of an import that got part way.
// A failure empties it again, so a failed import leaves no partial dataset to
// search.
func (w *Worker) run(parent context.Context, imp *Import) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var bytesDone, logsDone atomic.Int64
	var status atomic.Value
	status.Store(StatusDownloading)
	snapshot := func() *Import {
		cp := *imp
		cp.Status, cp.BytesDone, cp.LogsDone = status.Load().(string), bytesDone.Load(), logsDone.Load()
		cp.UpdatedAt = time.Now().UTC()
		return &cp
	}

	// Progress also holds the claim. Losing it (the fractal was deleted, or a
	// stale claim was taken over) stops this run without recording an outcome.
	var lost atomic.Bool
	reported := make(chan struct{})
	go func() {
		defer close(reported)
		t := time.NewTicker(reportInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			cur := snapshot()
			if err := w.store.Progress(ctx, cur); err != nil {
				if errors.Is(err, ErrNotFound) {
					lost.Store(true)
					cancel()
					return
				}
				if ctx.Err() == nil {
					log.Printf("[Datasets] Recording progress: %v", err)
				}
				continue
			}
			w.broadcast(cur)
		}
	}()

	err := w.load(ctx, imp, &status, &bytesDone, &logsDone)
	if err != nil && ctx.Err() == nil {
		log.Printf("[Datasets] Import of %s failed: %v", imp.DatasetID, err)
		// Let queued batches land first, or they would arrive after the drop.
		if logsDone.Load() > 0 {
			w.settle(ctx, imp.FractalID, logsDone.Load())
		}
		if derr := w.logs.DeleteLogsByFractalID(ctx, imp.FractalID, false); derr != nil {
			log.Printf("[Datasets] Emptying fractal %s after a failed import: %v", imp.FractalName, derr)
		}
	}
	cancel()
	<-reported

	// A stopping server leaves the claim to go stale and be run again.
	if err != nil && (parent.Err() != nil || lost.Load()) {
		if lost.Load() {
			log.Printf("[Datasets] Import %d of %s is no longer held here; stopped", imp.ID, imp.DatasetID)
		}
		return
	}
	final := snapshot()
	if ferr := w.store.Finish(context.Background(), final, publicError(err)); ferr != nil {
		log.Printf("[Datasets] Recording the end of import %d: %v", imp.ID, ferr)
		return
	}
	if err == nil {
		log.Printf("[Datasets] Imported %d logs of %s into %s", final.LogsDone, imp.DatasetID, imp.FractalName)
	}
	w.broadcast(final)
}

func (w *Worker) load(ctx context.Context, imp *Import, status *atomic.Value, bytesDone, logsDone *atomic.Int64) error {
	if err := w.logs.DeleteLogsByFractalID(ctx, imp.FractalID, false); err != nil {
		return fmt.Errorf("empty the fractal before importing: %w", err)
	}
	d := &exchange.Dataset{URL: imp.URL, SHA256: imp.SHA256, Bytes: imp.BytesTotal, Logs: imp.LogsTotal}
	path, err := datasetfile.DownloadFile(ctx, w.client, d, w.dir, bytesDone.Store)
	if err != nil {
		return err
	}
	defer os.Remove(path)

	// Count before importing, so a file that does not hold what the Exchange lists
	// fails before any of it is ingested.
	n, err := w.read(ctx, path, datasetfile.CountLogs)
	if err != nil {
		return err
	}
	if n != imp.LogsTotal {
		return failf("the file holds %d logs, but the Exchange lists %d", n, imp.LogsTotal)
	}

	status.Store(StatusImporting)
	var batch []storage.LogEntry
	size := 0
	flush := func() error {
		if err := w.enqueue(ctx, batch); err != nil {
			return err
		}
		logsDone.Add(int64(len(batch)))
		batch, size = nil, 0
		return nil
	}
	_, err = w.read(ctx, path, func(r io.Reader) (int64, error) {
		return datasetfile.ReadLogs(r, func(l datasetfile.Log) error {
			entry, err := ingest.BuildLogEntry(l.Fields, nil, nil)
			if err != nil {
				return err
			}
			entry.Timestamp = l.Time
			entry.LogID = storage.GenerateLogID(l.Time, entry.RawLog)
			entry.FractalID = imp.FractalID
			batch = append(batch, entry)
			if size += l.Size; len(batch) < batchRows && size < batchBytes {
				return nil
			}
			return flush()
		})
	})
	if err == nil && len(batch) > 0 {
		err = flush()
	}
	if err != nil {
		return err
	}
	if stored, ok := w.settle(ctx, imp.FractalID, imp.LogsTotal); !ok {
		if err := ctx.Err(); err != nil {
			return err
		}
		return failf("only %d of %d logs were stored", stored, imp.LogsTotal)
	}
	return nil
}

// settle waits until the fractal stores at least want logs. It gives up once the
// count stands still for settleTimeout, returning the last count.
func (w *Worker) settle(ctx context.Context, fractalID string, want int64) (int64, bool) {
	last, since := int64(-1), time.Now()
	for {
		n, err := w.logs.CountFractalLogs(ctx, fractalID)
		switch {
		case err != nil:
			if ctx.Err() == nil {
				log.Printf("[Datasets] Counting stored logs: %v", err)
			}
		case int64(n) >= want:
			return int64(n), true
		case int64(n) != last:
			last, since = int64(n), time.Now()
		}
		if time.Since(since) > settleTimeout {
			return max(last, 0), false
		}
		select {
		case <-ctx.Done():
			return max(last, 0), false
		case <-time.After(settlePoll):
		}
	}
}

// read opens a dataset file and hands fn its decompressed contents, which stop
// with ctx's error once ctx ends.
func (w *Worker) read(ctx context.Context, path string, fn func(io.Reader) (int64, error)) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r, err := datasetfile.NewReader(f)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	return fn(ctxReader{ctx, r})
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// enqueue hands a batch to the ingest queue once it is shallow enough, waiting
// out backpressure as an ingest client does after a 429.
func (w *Worker) enqueue(ctx context.Context, batch []storage.LogEntry) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if w.queue.Depth() < w.maxDepth && w.queue.Enqueue(batch) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryEnqueue):
		}
	}
}

func (w *Worker) broadcast(imp *Import) {
	if w.hub != nil {
		w.hub.Broadcast(Room, sse.Event{Type: EventProgress, Data: imp}, "")
	}
}

// failure is an import error whose text is safe to show.
type failure struct{ msg string }

func (f failure) Error() string { return f.msg }

func failf(format string, a ...any) error { return failure{fmt.Sprintf(format, a...)} }

// publicError is what an import is recorded as failing with: the cause when it
// is about the dataset, else a pointer to the server log, which has the rest.
func publicError(err error) error {
	var f failure
	switch {
	case err == nil:
		return nil
	case errors.As(err, &f), errors.Is(err, datasetfile.ErrDownload),
		errors.Is(err, datasetfile.ErrChecksum), errors.Is(err, datasetfile.ErrFormat):
		return err
	}
	return errors.New("the import failed; the server log has the details")
}
