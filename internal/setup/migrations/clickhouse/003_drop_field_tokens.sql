-- Retire the deprecated field_tokens column and its text index. Equality now resolves against
-- the JSON sub-column directly (see pkg/parser), so field_tokens is no longer written or queried.
-- The compound field:value lookup it was built for could not work: hasToken rejects the colon
-- separator and hasAllTokens matched nothing, while adding a wide-column read that was slower
-- than the raw_log index alone.
--
-- DROP INDEX must precede DROP COLUMN. DROP COLUMN schedules a background mutation on existing
-- parts to reclaim disk. IF EXISTS keeps both statements idempotent.
ALTER TABLE logs DROP INDEX IF EXISTS field_tokens_text;
ALTER TABLE logs DROP COLUMN IF EXISTS field_tokens;
