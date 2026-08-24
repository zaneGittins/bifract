-- DEAD: stamped but never executed since the ingest-partition reset. Kept for numbering (next: 016) and TestUpgradeFromV002.
-- Align logs_hot with the base columns BQL can reference. Migration 006 added norm_log and
-- normalizer to logs but not to logs_hot, leaving the two schemas divergent.
-- Alert queries route to logs_hot whenever the cursor is under 110 min old (the normal
-- case). Any alert whose query has a pipeline command projects norm_log, and any alert
-- referencing normalizer selects that -- either way a missing column fails with code 47
-- (unknown identifier), which the engine treats as unrecoverable and auto-disables the alert.
-- No MATERIALIZE COLUMN needed: the DEFAULTs compute on read, and the 4h TTL ages out
-- pre-migration parts on its own.
ALTER TABLE logs_hot ADD COLUMN IF NOT EXISTS norm_log String DEFAULT toString(fields) CODEC(ZSTD(3));
ALTER TABLE logs_hot ADD COLUMN IF NOT EXISTS normalizer LowCardinality(String) DEFAULT '';

-- Point the feeding MV at the new columns. MODIFY QUERY edits the view in place; DROP and
-- re-CREATE would drop every insert landing in the gap, which for a table that alerts read
-- means silently missed alerts.
ALTER TABLE logs_hot_mv MODIFY QUERY
SELECT
    timestamp,
    raw_log,
    log_id,
    fields,
    fractal_id,
    ingest_timestamp,
    norm_log,
    normalizer
FROM logs;
