-- Persist @name -> value query variable bindings with each saved query, so
-- reopening it restores the values it was last run with (empty defaults to "*").
ALTER TABLE saved_queries ADD COLUMN IF NOT EXISTS variables JSONB DEFAULT '[]';
