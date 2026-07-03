-- Normalizer versioning. Each normalizer gets an integer version bumped on every
-- content edit (Manager.Update). Ingested logs are stamped with "name@version" in the
-- ClickHouse logs.normalizer column so normalization output is traceable to the exact
-- config that produced it. Additive and non-destructive.
ALTER TABLE normalizers ADD COLUMN IF NOT EXISTS version INT NOT NULL DEFAULT 1;
