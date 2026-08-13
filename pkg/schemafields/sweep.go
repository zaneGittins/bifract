package schemafields

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"bifract/pkg/settings"
	"bifract/pkg/storage"
)

// The schema sweep is the single background owner of every measurement the
// schema tab shows. It runs on one elected replica, writes its findings to
// Postgres, and the tab reads nothing else.
//
// It exists because the measurements are expensive and the page is not: field
// distributions move over hours, so recomputing them per page load bought
// nothing and cost a scan whose size grew with retention. Everything here is
// bounded by an explicit sample size, a memory ceiling, and an execution ceiling.

// sweepLockID elects a single replica. The queries are read-only, so concurrent
// runs would be correct, just wasteful.
const sweepLockID int64 = 0x6269667261637402

// sweepTimeout bounds one complete pass so a slow cluster cannot leave the
// advisory lock held and starve every later sweep.
const sweepTimeout = 10 * time.Minute

// sweepStartupDelay keeps the first pass clear of startup work (migrations,
// schema reconcile, materialized view setup).
const sweepStartupDelay = 60 * time.Second

// sweepMaxMemoryBytes bounds every query the sweep issues to a small, explicit
// budget (see storage.QueryLowPriorityBounded) so it can only ever fail itself
// cleanly and never threatens merges, mutations, or interactive search.
func sweepMaxMemoryBytes() int64 {
	if v := os.Getenv("BIFRACT_SCHEMA_SWEEP_MAX_MEMORY_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return 1 << 30 // 1GB
}

// sweepQueryBudgetSeconds is the server-side execution ceiling per query. A Go
// deadline only stops the client reading; this stops ClickHouse executing.
const sweepQueryBudgetSeconds = 120

// sweptAtKey records when a pass last completed. It is kept separately from the
// measurement rows because a deployment with no data yet produces no rows, and
// "the sweep ran and found nothing" must not read as "the sweep never ran".
const sweptAtKey = "schema_swept_at"

// Sweeper measures the schema in the background: what each field costs and how
// much of the column budget is used (part metadata), how the fields are
// distributed (a bounded sample per fractal), how often each is queried (saved
// BQL), and which have spilled out of their own column.
type Sweeper struct {
	ch      *storage.ClickHouseClient
	pg      *storage.PostgresClient
	manager *Manager
	notify  notifier
	// trigger coalesces manual refreshes: a request while a sweep is running
	// queues exactly one more pass rather than starting a second.
	trigger chan struct{}
}

func NewSweeper(ch *storage.ClickHouseClient, pg *storage.PostgresClient, manager *Manager, n notifier) *Sweeper {
	return &Sweeper{ch: ch, pg: pg, manager: manager, notify: n, trigger: make(chan struct{}, 1)}
}

// Start runs the sweep on an interval until ctx is cancelled.
func (s *Sweeper) Start(ctx context.Context) {
	if s.ch == nil || s.pg == nil {
		return
	}
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(sweepStartupDelay):
		}
		for {
			s.runOnce(ctx)
			select {
			case <-ctx.Done():
				return
			case <-s.trigger:
			case <-time.After(sweepInterval()):
			}
		}
	}()
}

// Refresh requests an out-of-band sweep. It returns immediately: the caller is a
// UI action, and the result arrives in Postgres for the next page load rather
// than being waited on.
func (s *Sweeper) Refresh() {
	select {
	case s.trigger <- struct{}{}:
	default: // one already queued; a second would measure the same thing twice
	}
}

// sweepInterval is how often the measurements refresh. It is an admin setting
// rather than an env var because it is the knob that trades ClickHouse load
// against how quickly a newly ingested field appears on the tab.
func sweepInterval() time.Duration {
	if m := settings.Get().SchemaSweepIntervalMinutes; m > 0 {
		return time.Duration(m) * time.Minute
	}
	return 15 * time.Minute
}

// runOnce performs one complete pass, guarded by the advisory lock.
func (s *Sweeper) runOnce(parent context.Context) {
	unlock, ok := s.pg.TryAdvisoryLock(parent, sweepLockID)
	if !ok {
		return // another replica owns this sweep
	}
	defer unlock()

	ctx, cancel := context.WithTimeout(parent, sweepTimeout)
	defer cancel()
	ctx = storage.QueryBudgetContext(ctx, sweepQueryBudgetSeconds)

	s.sweepStats(ctx)
	s.sweepUsage(ctx)
	s.sweepOverflow(ctx)

	if err := s.pg.SetSetting(ctx, sweptAtKey, time.Now().UTC().Format(time.RFC3339)); err != nil {
		log.Printf("[SchemaSweep] record completion: %v", err)
	}
}

// sweepStats measures storage and distribution: Tier A from part metadata, then
// one bounded sample per fractal.
func (s *Sweeper) sweepStats(ctx context.Context) {
	bytes, fractals, err := s.readInventory(ctx)
	if err != nil {
		log.Printf("[SchemaSweep] inventory: %v", err)
		return
	}
	if len(fractals) == 0 {
		return // no data yet; leave whatever was measured before
	}

	results := make([]fractalResult, 0, len(fractals))
	for id, meta := range fractals {
		if ctx.Err() != nil {
			log.Printf("[SchemaSweep] stopping early: %v", ctx.Err())
			return
		}
		fields, sampled, maxPaths, err := s.sampleFractal(ctx, meta)
		if err != nil {
			// One fractal's failed sample must not discard every other fractal's
			// measurement, nor its own storage figures, which came from metadata
			// and are still good. Only its distribution is missing this pass.
			log.Printf("[SchemaSweep] sample fractal %s: %v", id, err)
			fields, sampled, maxPaths = nil, 0, 0
		}
		results = append(results, fractalResult{
			Meta:        meta,
			SampledRows: sampled,
			MaxPaths:    maxPaths,
			Fields:      fields,
			Bytes:       bytes[id],
		})
	}
	if len(results) == 0 {
		return
	}
	if err := saveStats(ctx, s.pg, results); err != nil {
		log.Printf("[SchemaSweep] save stats: %v", err)
	}
}

// sweepUsage recomputes which saved BQL references which fields.
func (s *Sweeper) sweepUsage(ctx context.Context) {
	usage := queryUsage(ctx, s.pg)
	if err := saveUsage(ctx, s.pg, usage); err != nil {
		log.Printf("[SchemaSweep] save usage: %v", err)
	}
}
