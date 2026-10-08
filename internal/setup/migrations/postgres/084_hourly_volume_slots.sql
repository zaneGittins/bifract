-- Hourly volume models now compare an hour only with the same hour on the same kind of day, so min
-- history counts past samples of that hour (one a day) rather than hours. Keep each model's intent.
UPDATE analytics_models
SET definition = jsonb_set(definition, '{min_sample}',
                           to_jsonb(LEAST(8, GREATEST(1, CEIL((definition->>'min_sample')::numeric / 24)))::int))
WHERE model_type = 'volume_baseline'
  AND definition->>'time_bucket' = 'hour'
  AND COALESCE((definition->>'min_sample')::int, 0) > 0;
