package archive

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icetable "github.com/apache/iceberg-go/table"

	"bifract/pkg/objstore"
)

// ScanEstimate is what a Recall over a window would have to open, derived
// entirely from Iceberg manifests: no object data is read.
//
// Bytes and Rows are an upper bound. ClickHouse still prunes row groups with
// Parquet min/max and bloom statistics and reads only the columns the query
// touches, so a selective search reads a fraction of this. What the number is
// actually for is the other direction: a window with 40k files behind it is
// going to be slow no matter how selective the predicate, and that is worth
// knowing before waiting several minutes to find out.
type ScanEstimate struct {
	Files      int   `json:"files"`
	Rows       int64 `json:"rows"`
	Bytes      int64 `json:"bytes"`
	Partitions int   `json:"partitions"`
	// Archived is false when the fractal has no archive table at all, which is
	// different from a window that simply holds no data.
	Archived bool `json:"archived"`
}

const (
	// estimateTTL is how long a computed estimate is reused. Estimates are keyed
	// by ingest DAY (the partition axis), so a relative window like "last 30d"
	// produces the same key all day and the manifests behind it only change when
	// the archiver commits.
	estimateTTL = 60 * time.Second
	// estimateScanConcurrency bounds the manifest reads behind one estimate.
	estimateScanConcurrency = 4
)

// Estimator answers pre-flight scan questions for the Recall UI. It holds a
// lazily-built catalog (so an install with archiving off never opens object
// storage) plus a short-lived result cache, and collapses concurrent duplicate
// requests so a slow manifest read is paid once.
type Estimator struct {
	cfg Config

	mu       sync.Mutex
	cat      *Catalog
	cache    map[string]estimateEntry
	inflight map[string]chan struct{}
}

type estimateEntry struct {
	est ScanEstimate
	at  time.Time
}

// NewEstimator builds an estimator over the archive config.
func NewEstimator(cfg Config) *Estimator {
	return &Estimator{
		cfg:      cfg,
		cache:    make(map[string]estimateEntry),
		inflight: make(map[string]chan struct{}),
	}
}

// Estimate reports what an ingest-time window covers in a fractal's archive.
// The window is widened to whole UTC ingest days because that is the partition
// granularity ClickHouse prunes on: a sub-day window still opens the whole day's
// files.
func (e *Estimator) Estimate(ctx context.Context, fractalID string, from, to time.Time) (ScanEstimate, error) {
	if e.cfg.Obj.Backend == objstore.BackendDisk || e.cfg.Obj.Backend == "" {
		return ScanEstimate{}, fmt.Errorf("archive estimate requires an object-storage backend")
	}
	fromDay := from.UTC().Truncate(24 * time.Hour)
	toDay := to.UTC().Truncate(24 * time.Hour)
	key := fmt.Sprintf("%s|%d|%d", fractalID, fromDay.Unix(), toDay.Unix())

	for {
		e.mu.Lock()
		if ent, ok := e.cache[key]; ok && time.Since(ent.at) < estimateTTL {
			e.mu.Unlock()
			return ent.est, nil
		}
		wait, running := e.inflight[key]
		if !running {
			done := make(chan struct{})
			e.inflight[key] = done
			e.mu.Unlock()

			est, err := e.compute(ctx, fractalID, fromDay, toDay)

			e.mu.Lock()
			if err == nil {
				e.cache[key] = estimateEntry{est: est, at: time.Now()}
			}
			delete(e.inflight, key)
			e.mu.Unlock()
			close(done)
			return est, err
		}
		e.mu.Unlock()

		// Another caller is computing this exact estimate; wait for it and re-read
		// the cache rather than issuing a duplicate manifest scan.
		select {
		case <-wait:
		case <-ctx.Done():
			return ScanEstimate{}, ctx.Err()
		}
		e.mu.Lock()
		ent, ok := e.cache[key]
		e.mu.Unlock()
		if ok {
			return ent.est, nil
		}
		// The other caller failed and cached nothing. Take the work ourselves.
	}
}

func (e *Estimator) compute(ctx context.Context, fractalID string, fromDay, toDay time.Time) (ScanEstimate, error) {
	cat, err := e.catalog(ctx)
	if err != nil {
		return ScanEstimate{}, err
	}
	ident := catalog.ToIdentifier(Namespace, tableName(fractalID))
	tbl, err := cat.cat.LoadTable(ctx, ident)
	if err != nil {
		// No table: this fractal has never been archived. Not an error to report
		// to the user, just an empty archive.
		return ScanEstimate{}, nil
	}
	est := ScanEstimate{Archived: true}
	if tbl.CurrentSnapshot() == nil {
		return est, nil
	}

	filter := iceberg.NewAnd(
		iceberg.GreaterThanEqual(iceberg.Reference(partitionFieldName), iceberg.Date(epochDay(fromDay))),
		iceberg.LessThanEqual(iceberg.Reference(partitionFieldName), iceberg.Date(epochDay(toDay))),
	)
	tasks, err := tbl.Scan(
		icetable.WithRowFilter(filter),
		icetable.WitMaxConcurrency(estimateScanConcurrency),
	).PlanFiles(ctx)
	if err != nil {
		return ScanEstimate{}, fmt.Errorf("archive: plan files: %w", err)
	}

	days := make(map[any]struct{}, 32)
	for _, t := range tasks {
		est.Files++
		est.Rows += t.File.Count()
		est.Bytes += t.File.FileSizeBytes()
		if v, ok := t.File.Partition()[partitionFieldID]; ok {
			days[v] = struct{}{}
		}
	}
	est.Partitions = len(days)
	return est, nil
}

// catalog lazily opens the Iceberg catalog on first use.
func (e *Estimator) catalog(ctx context.Context) (*Catalog, error) {
	e.mu.Lock()
	cat := e.cat
	e.mu.Unlock()
	if cat != nil {
		return cat, nil
	}
	ApplyBackendEnv(e.cfg.Obj)
	built, err := NewCatalog(ctx, Namespace, e.cfg.PGDSN, e.cfg.Obj)
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	e.mu.Lock()
	// Another caller may have won the race; keep whichever landed first so the
	// process only ever uses one catalog handle.
	if e.cat == nil {
		e.cat = built
	}
	cat = e.cat
	e.mu.Unlock()
	return cat, nil
}
