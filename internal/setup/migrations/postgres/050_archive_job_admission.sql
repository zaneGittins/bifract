-- Recall/restore job admission caps concurrent archive scans by counting rows in
-- 'running' at claim time, under a global advisory lock. Without these partial
-- indexes that count is a sequential scan on the critical path of that lock.
CREATE INDEX IF NOT EXISTS idx_archive_search_jobs_running
    ON archive_search_jobs (id) WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_archive_restore_jobs_running
    ON archive_restore_jobs (id) WHERE status = 'running';
