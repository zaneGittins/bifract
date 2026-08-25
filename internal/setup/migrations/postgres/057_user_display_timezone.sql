-- Per-user display timezone. The UI renders every absolute timestamp in this
-- IANA zone; stored data, query generation, and alerting all stay UTC.
--
-- On the users table rather than a preferences table: it is a single scalar
-- read on every request that already loads the user, so a join would buy
-- nothing. A preferences table earns its keep once there is a second setting
-- with a different lifetime.
ALTER TABLE users ADD COLUMN IF NOT EXISTS display_timezone VARCHAR(64) NOT NULL DEFAULT 'UTC';
