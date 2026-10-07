-- Layout units of a dashboard's widgets: 1 = 12 columns x 130px rows, 2 = 24 columns x 26px row pitch.
-- Version 1 layouts are scaled on read and converted on their first layout write.
ALTER TABLE dashboards ADD COLUMN IF NOT EXISTS grid_version SMALLINT NOT NULL DEFAULT 1;
ALTER TABLE dashboards ALTER COLUMN grid_version SET DEFAULT 2;
