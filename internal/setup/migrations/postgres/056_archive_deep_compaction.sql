-- Claim stamp for the periodic whole-table compaction pass. Routine maintain
-- passes plan only recent ingest_date partitions; this marks when the last
-- unbounded pass ran so exactly one maintainer per interval takes it.
ALTER TABLE archive_maintain_status ADD COLUMN IF NOT EXISTS last_deep_compaction_at TIMESTAMPTZ;
