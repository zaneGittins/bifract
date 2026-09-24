package archive

import (
	"fmt"
	"sync"

	"bifract/pkg/objstore"
	"bifract/pkg/storage"
)

// sharedDeps is the Iceberg catalog + ClickHouse client shared by every recall
// and restore worker and the estimator in a process, so the catalog's Postgres
// pool exists once rather than once per worker. Both are safe for concurrent
// use; only construction is guarded. Built lazily, so a disabled, job-free
// archive never opens object storage.
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
func (d *sharedDeps) ensure() (*Catalog, *storage.ClickHouseClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cat != nil && d.ch != nil {
		return d.cat, d.ch, nil
	}
	if d.cfg.Obj.Backend == objstore.BackendDisk {
		return nil, nil, fmt.Errorf("archive search requires an object-storage backend (s3, minio, or azure); the disk backend is pod-local and cannot be read by ClickHouse")
	}
	if _, err := d.catalogLocked(); err != nil {
		return nil, nil, err
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

// catalog returns the shared catalog, building it on first use.
func (d *sharedDeps) catalog() (*Catalog, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.catalogLocked()
}

func (d *sharedDeps) catalogLocked() (*Catalog, error) {
	if d.cat != nil {
		return d.cat, nil
	}
	ApplyBackendEnv(d.cfg.Obj)
	cat, err := NewCatalog(d.cfg.PGDSN, d.cfg.Obj)
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	d.cat = cat
	return cat, nil
}
