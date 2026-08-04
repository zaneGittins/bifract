-- Every rule a feed's repository offers, whether or not it became an alert.
-- The alerts table only records what was imported, so it cannot answer "what
-- exists for this ATT&CK technique that we are not running, and why not" --
-- which is the question the coverage map's gap list needs.
CREATE TABLE IF NOT EXISTS feed_rule_catalog (
    feed_id      UUID NOT NULL REFERENCES alert_feeds(id) ON DELETE CASCADE,
    path         TEXT NOT NULL,
    title        TEXT NOT NULL DEFAULT '',
    level        VARCHAR(20) NOT NULL DEFAULT '',
    status       VARCHAR(20) NOT NULL DEFAULT '',
    tags         TEXT[] NOT NULL DEFAULT '{}',
    rule_hash    TEXT NOT NULL DEFAULT '',
    imported     BOOLEAN NOT NULL DEFAULT false,
    -- '' when imported, otherwise min_level, min_status, translate_error or parse_error.
    skip_reason  TEXT NOT NULL DEFAULT '',
    skip_detail  TEXT NOT NULL DEFAULT '',
    last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (feed_id, path)
);

CREATE INDEX IF NOT EXISTS idx_feed_rule_catalog_tags ON feed_rule_catalog USING GIN(tags);
CREATE INDEX IF NOT EXISTS idx_feed_rule_catalog_pending ON feed_rule_catalog(feed_id) WHERE imported = false;
