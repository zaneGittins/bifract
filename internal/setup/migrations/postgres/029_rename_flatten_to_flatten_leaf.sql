-- Rename "flatten" transform to "flatten_leaf" in all normalizers.
UPDATE normalizers
SET transforms = (
    SELECT jsonb_agg(
        CASE WHEN elem #>> '{}' = 'flatten' THEN '"flatten_leaf"'::jsonb ELSE elem END
    )
    FROM jsonb_array_elements(transforms) AS elem
)
WHERE transforms @> '["flatten"]';
