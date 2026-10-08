-- Rarity models report Good-Turing coverage, which was mislabelled confidence.
UPDATE analytics_models
SET definition = jsonb_set(definition #- '{alert,confidence_threshold}', '{alert,coverage_threshold}',
                           definition->'alert'->'confidence_threshold')
WHERE definition->'alert' ? 'confidence_threshold';

UPDATE alerts a
SET query_string = regexp_replace(a.query_string, '^\| confidence >', '| coverage >', 'gn')
FROM analytics_models m
WHERE m.linked_alert_id = a.id AND m.model_type = 'rarity';
