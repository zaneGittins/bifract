-- A network model's latest completed scoring pass. Its findings and modelLookup
-- read only the pairs scored since, so a pair that went quiet drops out. Startup
-- seeds it for existing models from their newest scores.
ALTER TABLE analytics_models ADD COLUMN IF NOT EXISTS scored_pass_at TIMESTAMPTZ;
