-- Add timestamp_fields column to normalizers table
ALTER TABLE normalizers ADD COLUMN IF NOT EXISTS timestamp_fields JSONB NOT NULL DEFAULT '[]';

-- Populate the default normalizer with standard timestamp fields
UPDATE normalizers
SET timestamp_fields = '[
    {"field": "system_time", "format": "2006-01-02T15:04:05.999999999Z07:00"},
    {"field": "timestamp", "format": "2006-01-02T15:04:05.999999999Z07:00"},
    {"field": "@timestamp", "format": "2006-01-02T15:04:05.999999999Z07:00"},
    {"field": "time", "format": "2006-01-02T15:04:05.999999999Z07:00"}
]'::jsonb
WHERE is_default = true AND (timestamp_fields IS NULL OR timestamp_fields = '[]'::jsonb);
