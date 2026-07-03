package ingest

import "bifract/pkg/spool"

// SetSpool attaches the durable archive spool and its capacity watermark. Called
// from main.go only when the archive feature is provisioned (dormant-but-present).
// With a spool attached but archiving disabled, the tee stays a no-op; enabling
// is a separate runtime toggle via SetArchiveEnabled.
func (q *IngestQueue) SetSpool(w *spool.Writer, maxBytes int64) {
	q.spool = w
	q.spoolMaxBytes = maxBytes
}

// SetArchiveEnabled toggles the ingest tee at runtime. When false (the default),
// Enqueue does not touch the spool, so a provisioned-but-disabled archive adds
// zero ingest overhead. Backed by the persisted archive_enabled setting, and
// refreshable without a restart.
func (q *IngestQueue) SetArchiveEnabled(enabled bool) {
	q.archiveEnabled.Store(enabled)
}

// ArchiveEnabled reports whether the ingest tee is currently active.
func (q *IngestQueue) ArchiveEnabled() bool {
	return q.archiveEnabled.Load()
}

// SpoolProvisioned reports whether the archive spool machinery is present (the
// shared volume is mounted and the writer attached). Enabling archiving requires
// this; a fresh install without the archiver sidecar reports false.
func (q *IngestQueue) SpoolProvisioned() bool {
	return q.spool != nil
}

// SpoolUsageBytes returns current spool disk usage, or 0 when not provisioned.
func (q *IngestQueue) SpoolUsageBytes() int64 {
	if q.spool == nil {
		return 0
	}
	n, err := q.spool.DiskUsage()
	if err != nil {
		return 0
	}
	return n
}

// SpoolMaxBytes returns the spool capacity watermark (0 when not provisioned).
func (q *IngestQueue) SpoolMaxBytes() int64 {
	return q.spoolMaxBytes
}
