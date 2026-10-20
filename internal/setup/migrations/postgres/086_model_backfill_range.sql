-- Backfills store the event-time range they cover, so one can target any [from, to).
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS backfill_from TIMESTAMP;
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS backfill_to   TIMESTAMP;
UPDATE analytics_models
   SET backfill_to = backfill_started_at,
       backfill_from = backfill_started_at - CASE backfill_window
           WHEN '24h' THEN INTERVAL '1 day' WHEN '7d' THEN INTERVAL '7 days'
           WHEN '30d' THEN INTERVAL '30 days' WHEN '90d' THEN INTERVAL '90 days' END
 WHERE backfill_from IS NULL AND backfill_started_at IS NOT NULL
   AND backfill_window IN ('24h', '7d', '30d', '90d');
