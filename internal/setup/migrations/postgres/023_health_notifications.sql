CREATE TABLE IF NOT EXISTS health_notifications (
    id                UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_type VARCHAR(50)  NOT NULL,
    severity          VARCHAR(10)  NOT NULL DEFAULT 'info'
                          CHECK (severity IN ('info', 'warning', 'critical')),
    title             VARCHAR(255) NOT NULL,
    message           TEXT         NOT NULL DEFAULT '',
    created_at        TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_health_notif_created_at
    ON health_notifications (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_health_notif_type_created
    ON health_notifications (notification_type, created_at DESC);

CREATE TABLE IF NOT EXISTS notification_reads (
    username     VARCHAR(50) PRIMARY KEY REFERENCES users(username) ON DELETE CASCADE,
    last_read_at TIMESTAMP   NOT NULL DEFAULT NOW()
);
