package schemafields

import (
	"context"
	"testing"

	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// The parts reads fan out with cluster() on a cluster, so they ask for the privileged
// connection explicitly; on the reader identity they would fail.
func TestPartsReadsAreAdmin(t *testing.T) {
	ch, chf := storagetest.ClickHouse()
	pg, _ := storagetest.Postgres()
	s := NewSweeper(ch, pg, nil, nil)
	var marks []bool
	chf.CtxArgs = func(ctx context.Context) []any {
		marks = append(marks, storage.IsAdminRead(ctx))
		return nil
	}
	ctx := context.Background()
	_, _, _ = s.readInventory(ctx)
	_, _ = s.readFractalMeta(ctx)
	_, _ = s.rollupFractals(ctx)
	if len(marks) < 3 {
		t.Fatalf("%d reads: %q", len(marks), chf.Queried())
	}
	for i, admin := range marks {
		if !admin {
			t.Errorf("read not admin: %.120s", chf.Queried()[i])
		}
	}
}
