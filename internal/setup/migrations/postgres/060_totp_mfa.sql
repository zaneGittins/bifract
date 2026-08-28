-- TOTP multi-factor authentication.
--
-- The secret is stored encrypted with a key derived from BIFRACT_PASSWORD_PEPPER,
-- so a database dump alone does not defeat the second factor. totp_last_counter
-- records the highest time step already accepted, which is what prevents a code
-- being replayed inside its 30 second window.
ALTER TABLE users ADD COLUMN IF NOT EXISTS totp_secret TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS totp_enrolled_at TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS totp_last_counter BIGINT NOT NULL DEFAULT 0;

-- A session that passed the password but not yet the second factor. Held in
-- Postgres so the state survives a restart and is shared across replicas.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS mfa_pending BOOLEAN NOT NULL DEFAULT FALSE;

-- Single-use recovery codes. High entropy, so SHA-256 matches how invite tokens
-- are stored and bcrypt would buy nothing.
CREATE TABLE IF NOT EXISTS user_recovery_codes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    code_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    used_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_user_recovery_codes_username ON user_recovery_codes(username);
