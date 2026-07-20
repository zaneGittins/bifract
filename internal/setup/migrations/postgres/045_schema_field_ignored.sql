-- Schema suggestion dismissals.
--
-- The schema tab now proposes fields to type-hint, derived from what is actually
-- in the logs and what users actually query. Any such list needs a way to say
-- "not this one" or it becomes noise and gets ignored wholesale.
--
-- This is a durable per-field decision rather than a per-user, time-boxed snooze.
-- A snooze expires and re-nags, is invisible to other admins, and would need
-- per-user UI state that does not exist today (notification_reads is keyed on
-- username alone). A shared, inspectable, reversible record is strictly better:
-- it survives restarts, explains itself to the next admin, and can be undone.
--
-- Ignoring also suppresses the field from the capacity warning, so an admin who
-- has decided that some high-cardinality junk field is not worth a column stops
-- being told about it.
CREATE TABLE IF NOT EXISTS schema_field_ignored (
    field_name VARCHAR(255) PRIMARY KEY,
    ignored_by VARCHAR(255) NOT NULL DEFAULT '',
    ignored_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
