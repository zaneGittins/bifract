-- Add an enabled flag so admins can disable/enable user accounts without
-- deleting them. Disabled users cannot log in and are locked out of any
-- active session on their next request. Defaults to TRUE so existing users
-- remain enabled after upgrade.
ALTER TABLE users ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE;
