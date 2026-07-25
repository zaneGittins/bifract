-- Cross-replica relay for SSE. Publishers insert here and pg_notify the row id;
-- every replica listens and delivers to its own connected clients. The payload
-- lives in the row because NOTIFY is capped at 8000 bytes and widget/section
-- result events routinely exceed that.
CREATE TABLE IF NOT EXISTS sse_events (
    id BIGSERIAL PRIMARY KEY,
    origin VARCHAR(64) NOT NULL,
    room TEXT NOT NULL,
    exclude_client_id VARCHAR(64) NOT NULL DEFAULT '',
    payload BYTEA NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sse_events_created_at ON sse_events(created_at);
