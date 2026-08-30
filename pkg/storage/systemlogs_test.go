package storage

import "testing"

// TestOrphanedSystemLogRe pins what the sweep will and will not drop. The pattern
// gates a name that gets interpolated into DDL, and a live system log table
// matching it by accident would be dropped out from under the server.
func TestOrphanedSystemLogRe(t *testing.T) {
	orphans := []string{
		"text_log_0", "trace_log_7", "query_log_12", "part_log_0",
		"metric_log_3", "asynchronous_metric_log_0", "query_metric_log_1",
		"background_schedule_pool_log_0", "processors_profile_log_0",
	}
	for _, name := range orphans {
		if !orphanedSystemLogRe.MatchString(name) {
			t.Errorf("%q should be treated as a stranded table", name)
		}
	}

	keep := []string{
		// Live tables ClickHouse actively writes.
		"text_log", "trace_log", "query_log", "part_log", "metric_log",
		"asynchronous_metric_log", "crash_log", "error_log",
		// Bifract's own tables must never be reachable by this sweep.
		"logs", "logs_raw", "logs_histogram", "proc_lineage",
		// Shapes that must not slip past an unanchored pattern.
		"text_log_0_backup", "my_log_0x", "text_log_", "TEXT_LOG_0",
	}
	for _, name := range keep {
		if orphanedSystemLogRe.MatchString(name) {
			t.Errorf("%q must not be treated as a stranded table", name)
		}
	}
}
