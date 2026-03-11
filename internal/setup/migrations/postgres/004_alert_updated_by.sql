-- Track who last modified an alert
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS updated_by VARCHAR(50) REFERENCES users(username) ON DELETE SET NULL;
