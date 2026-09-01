-- Date existing evidence from the log it references.
--
-- event_time arrived in 064 with no backfill, so evidence filed before it reads
-- as undated in a chronological view even though the comment it points at
-- carries a log_timestamp. Every filing path has set event_time from the log
-- since; this catches up the rows that predate them.

UPDATE notebook_sections s
SET event_time = c.log_timestamp
FROM comments c
WHERE s.comment_id = c.id
  AND s.section_type = 'comment_context'
  AND s.event_time IS NULL;

-- Sections 066 could not link to a comment (their author no longer exists)
-- still render from their JSON snapshot, which carries the same timestamp.
-- Cast per row: one unparseable value must not abort the migration.
DO $$
DECLARE
    sec RECORD;
    log_ts TIMESTAMP;
BEGIN
    FOR sec IN
        SELECT id, content
        FROM notebook_sections
        WHERE section_type = 'comment_context'
          AND comment_id IS NULL
          AND event_time IS NULL
          AND content IS JSON OBJECT
    LOOP
        BEGIN
            log_ts := NULLIF(sec.content::jsonb->>'log_timestamp', '')::timestamptz AT TIME ZONE 'UTC';
        EXCEPTION WHEN others THEN
            CONTINUE;
        END;

        IF log_ts IS NOT NULL THEN
            UPDATE notebook_sections SET event_time = log_ts WHERE id = sec.id;
        END IF;
    END LOOP;
END $$;
