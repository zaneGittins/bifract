package storage

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// A copy retried after an interruption must land again, not be discarded as a
// duplicate block. Since ClickHouse 26.2, insert_deduplicate is ignored unless
// deduplicate_insert is backward_compatible_choice, so each copy's settings are run
// against a table that keeps a deduplication log, with a stable SELECT that would
// otherwise be deduplicated.
//
// It needs a clickhouse binary (clickhouse local); set BIFRACT_TEST_CLICKHOUSE_BIN to
// one not on PATH. Without one it skips, unless BIFRACT_REQUIRE_CLICKHOUSE is set.
func TestCopySettingsDisableInsertSelectDedup(t *testing.T) {
	bin := os.Getenv("BIFRACT_TEST_CLICKHOUSE_BIN")
	if bin == "" {
		bin = "clickhouse"
	}
	if _, err := exec.LookPath(bin); err != nil {
		if os.Getenv("BIFRACT_REQUIRE_CLICKHOUSE") != "" {
			t.Fatalf("no clickhouse binary %q, and BIFRACT_REQUIRE_CLICKHOUSE is set", bin)
		}
		t.Skipf("no clickhouse binary %q; set BIFRACT_TEST_CLICKHOUSE_BIN", bin)
	}
	copies := map[string]string{
		"repartition copy":  repartitionCopySettings,
		"repartition probe": " LIMIT 1" + repartitionProbeSettings,
		"lineage backfill":  lineageBackfillSettings,
	}
	for name, settings := range copies {
		t.Run(name, func(t *testing.T) {
			insert := "INSERT INTO dst SELECT a FROM src ORDER BY ALL" + settings + ";"
			script := "CREATE TABLE src (a UInt32) ENGINE = MergeTree ORDER BY a;" +
				"INSERT INTO src VALUES (1);" +
				"CREATE TABLE dst (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS non_replicated_deduplication_window = 100;" +
				insert + insert +
				"SELECT count() FROM dst;"
			out, err := exec.Command(bin, "local", "--multiquery", "--query", script).CombinedOutput()
			if err != nil {
				t.Fatalf("clickhouse local: %v\n%s", err, out)
			}
			if got := strings.TrimSpace(string(out)); got != "2" {
				t.Errorf("rows after the same copy twice = %s, want 2 (the retry was deduplicated)", got)
			}
		})
	}
}
