-- Observed pgr() severity distribution, accumulated from real queries.
-- Severity cutoffs are derived from this rather than hard-coded, so the share of edges an
-- analyst sees as high stays where the admin set it across scoring-model changes.
CREATE TABLE IF NOT EXISTS pgr_score_histogram (
    bucket     SMALLINT PRIMARY KEY,
    edge_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);
