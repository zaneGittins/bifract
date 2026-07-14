-- Extends archive_maintain_status with attempt-level outcome tracking, not
-- just successful-run stats, so a crashed or lock-skipped maintain pass is
-- visible to the admin UI instead of looking identical to "ran, nothing to
-- do". last_run_at (existing) keeps meaning "last successful pass finished";
-- last_attempt_at covers every invocation regardless of outcome, which is
-- what staleness/liveness checks should key off.
ALTER TABLE archive_maintain_status
    ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_outcome    TEXT NOT NULL DEFAULT 'never',
    ADD COLUMN IF NOT EXISTS last_error      TEXT;

-- Per-run history (bounded to the most recent rows by the writer, see
-- appendMaintainHistory) so the admin UI can show a trend across multiple
-- passes -- e.g. backlog shrinking, growing, or runs being skipped -- rather
-- than only ever showing the single latest data point.
CREATE TABLE IF NOT EXISTS archive_maintain_history (
    id              BIGSERIAL PRIMARY KEY,
    ran_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    outcome         TEXT    NOT NULL,
    duration_ms     BIGINT  NOT NULL DEFAULT 0,
    tables_seen     INTEGER NOT NULL DEFAULT 0,
    compacted       INTEGER NOT NULL DEFAULT 0,
    groups_failed   INTEGER NOT NULL DEFAULT 0,
    expired         INTEGER NOT NULL DEFAULT 0,
    candidate_bytes BIGINT  NOT NULL DEFAULT 0,
    compacted_bytes BIGINT  NOT NULL DEFAULT 0,
    error           TEXT
);
CREATE INDEX IF NOT EXISTS archive_maintain_history_ran_at_idx ON archive_maintain_history (ran_at DESC);
