package archive

import (
	"context"
	"fmt"
	"sync"

	"bifract/pkg/objstore"
	"bifract/pkg/storage"
)

// sharedDeps is the Iceberg catalog + ClickHouse client shared by every recall
// and restore worker in a process. Sharing (rather than one per worker) bounds
// the Postgres connection footprint: the iceberg-go SQL catalog opens its own
// unbounded pool, so one-per-worker would multiply it by the worker-pool size
// (RecallWorkerPool). Both the catalog (DB-backed, per-op transactions) and the
// ClickHouse client (its own connection pool) are safe for concurrent use; only
// the one-time construction needs guarding. Built lazily on the first claimed
// job, so a disabled, job-free archive never opens object storage.
type sharedDeps struct {
	cfg Config
	mu  sync.Mutex
	cat *Catalog
	ch  *storage.ClickHouseClient
}

func newSharedDeps(cfg Config) *sharedDeps { return &sharedDeps{cfg: cfg} }

// ensure builds the catalog + ClickHouse client on first use and returns the
// shared instances. A disk backend is rejected up front: it is pod-local and
// unreadable by ClickHouse.
func (d *sharedDeps) ensure(ctx context.Context) (*Catalog, *storage.ClickHouseClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cat != nil && d.ch != nil {
		return d.cat, d.ch, nil
	}
	if d.cfg.Obj.Backend == objstore.BackendDisk {
		return nil, nil, fmt.Errorf("archive search requires an object-storage backend (s3, minio, or azure); the disk backend is pod-local and cannot be read by ClickHouse")
	}
	ApplyBackendEnv(d.cfg.Obj)
	if d.cat == nil {
		cat, err := NewCatalog(ctx, Namespace, d.cfg.PGDSN, d.cfg.Obj)
		if err != nil {
			return nil, nil, fmt.Errorf("open catalog: %w", err)
		}
		d.cat = cat
	}
	if d.ch == nil {
		ch, err := NewCHClient(d.cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("connect clickhouse: %w", err)
		}
		d.ch = ch
	}
	return d.cat, d.ch, nil
}
