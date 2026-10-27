package prisms

import (
	"context"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/storage/storagetest"
)

// Deleting a prism cascades its lookups' rows, so the delete hook runs once the
// prism is gone, and not when there was nothing to delete.
func TestDeletePrismRunsHook(t *testing.T) {
	for _, affected := range []int64{1, 0} {
		pgc, pg := storagetest.Postgres()
		pg.On("DELETE FROM prisms", fakedb.PGResult{Affected: affected})
		m := NewManager(pgc)
		var calls []string
		m.SetOnDeleteHook(func(_ context.Context, id string) { calls = append(calls, id) })

		err := m.DeletePrism(context.Background(), "p1")
		if (err == nil) != (affected == 1) {
			t.Fatalf("affected=%d: err %v", affected, err)
		}
		if want := int(affected); len(calls) != want {
			t.Errorf("affected=%d: hook calls %v", affected, calls)
		}
	}
}
