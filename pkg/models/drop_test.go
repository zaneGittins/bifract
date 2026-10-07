package models

import (
	"strings"
	"testing"
)

// Editing a model drops and re-creates its replicated tables under the same name
// and Keeper path. A drop without SYNC leaves the replica registered until the
// Atomic drop delay passes, and the re-create fails with REPLICA_ALREADY_EXISTS.
func TestModelTableDropIsSync(t *testing.T) {
	got := dropModelTableSQL("model_abc")
	for _, want := range []string{"DROP TABLE IF EXISTS `model_abc`", " SYNC", "max_table_size_to_drop = 0"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
}
