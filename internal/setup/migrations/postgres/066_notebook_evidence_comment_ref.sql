-- Evidence sections reference a comment instead of embedding a copy of one.
--
-- comment_context.content was a JSON snapshot taken when the section was
-- created, so editing the comment afterwards left the notebook showing stale
-- text, and "pin this log" wrote a snapshot with no comment row at all: those
-- events were invisible to the comments tab, to the row accent, and to
-- comments(). The edge now lives in one place and content is projected from
-- the comment on read.
ALTER TABLE notebook_sections ADD COLUMN IF NOT EXISTS comment_id UUID REFERENCES comments(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_notebook_sections_comment_id ON notebook_sections(comment_id);

-- One evidence section per comment per notebook.
CREATE UNIQUE INDEX IF NOT EXISTS idx_notebook_sections_comment_unique
    ON notebook_sections(notebook_id, comment_id) WHERE comment_id IS NOT NULL;

-- The notebook the rail captures into, per user and per scope. Previously
-- localStorage only, so it did not survive a browser change and no server-side
-- caller could know where to file evidence.
CREATE TABLE IF NOT EXISTS user_active_notebooks (
    username VARCHAR(50) NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    scope_key VARCHAR(64) NOT NULL,
    notebook_id UUID NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (username, scope_key)
);

CREATE INDEX IF NOT EXISTS idx_user_active_notebooks_notebook ON user_active_notebooks(notebook_id);

-- Backfill: link every existing snapshot to a comment, creating one for the
-- pins that never had a row. Rows whose author no longer exists cannot be
-- recreated under the comments.author foreign key; they keep comment_id NULL
-- and continue to render from the stored snapshot.
DO $$
DECLARE
    sec RECORD;
    data JSONB;
    found_id UUID;
    log_ts TIMESTAMP;
BEGIN
    FOR sec IN
        SELECT s.id, s.content, s.event_time, n.fractal_id, n.prism_id
        FROM notebook_sections s
        JOIN notebooks n ON n.id = s.notebook_id
        WHERE s.section_type = 'comment_context' AND s.comment_id IS NULL
    LOOP
        BEGIN
            data := sec.content::jsonb;
        EXCEPTION WHEN others THEN
            CONTINUE;
        END;

        IF data IS NULL OR jsonb_typeof(data) <> 'object' OR COALESCE(data->>'log_id', '') = '' THEN
            CONTINUE;
        END IF;

        log_ts := COALESCE(
            NULLIF(data->>'log_timestamp', '')::timestamptz AT TIME ZONE 'UTC',
            sec.event_time,
            NULLIF(data->>'commented_at', '')::timestamptz AT TIME ZONE 'UTC',
            NOW()
        );

        SELECT c.id INTO found_id
        FROM comments c
        WHERE c.log_id = data->>'log_id'
          AND c.author = COALESCE(data->>'author', '')
          AND c.text = COALESCE(data->>'comment_text', '')
          AND c.fractal_id IS NOT DISTINCT FROM sec.fractal_id
          AND c.prism_id IS NOT DISTINCT FROM sec.prism_id
        ORDER BY c.created_at
        LIMIT 1;

        IF found_id IS NULL THEN
            BEGIN
                INSERT INTO comments (log_id, log_timestamp, text, author, query, fractal_id, prism_id, created_at)
                VALUES (
                    data->>'log_id',
                    log_ts,
                    COALESCE(data->>'comment_text', ''),
                    data->>'author',
                    COALESCE(data->>'query', ''),
                    sec.fractal_id,
                    sec.prism_id,
                    COALESCE(NULLIF(data->>'commented_at', '')::timestamptz AT TIME ZONE 'UTC', NOW())
                )
                RETURNING id INTO found_id;
            EXCEPTION WHEN others THEN
                CONTINUE;
            END;
        END IF;

        BEGIN
            UPDATE notebook_sections SET comment_id = found_id WHERE id = sec.id;
        EXCEPTION WHEN unique_violation THEN
            CONTINUE;
        END;
    END LOOP;
END $$;
