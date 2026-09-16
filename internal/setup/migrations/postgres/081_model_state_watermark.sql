-- How far into logs.ingest_timestamp a model's state has been maintained.
--
-- Model state moves from an insert-time materialized view to a scheduled reader over
-- (state_watermark, cutoff]. A dictionary lookup cannot run in the ingest path: a dictionary that
-- fails to load would break ingestion rather than one model, and the state would be frozen against
-- the list as it stood when each log arrived, so editing the list would never reach data already
-- ingested.
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS state_watermark TIMESTAMPTZ;

-- Seed every existing model at the upgrade instant. Their materialized views have been maintaining
-- state until now, so the scheduled reader must start here and not at created_at: starting earlier
-- would re-read history the MV already counted and double every aggregate in the state table.
-- New models are created with a NULL watermark and correctly start from their own created_at,
-- because nothing has counted for them yet.
UPDATE analytics_models SET state_watermark = NOW() WHERE state_watermark IS NULL;
