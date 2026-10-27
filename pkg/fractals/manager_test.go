package fractals

import (
	"context"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage/storagetest"
)

const fractalID = "11111111-1111-1111-1111-111111111111"

func fractalRow(isSystem bool) fakedb.PGResult {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return fakedb.Row(fractalID, "web", "", false, isSystem, "admin", now, now,
		nil, nil, nil, "reject", int64(0), int64(0), nil, nil)
}

func newDeleteManager(t *testing.T, row fakedb.PGResult, deleted int64) (*Manager, *[]string) {
	t.Helper()
	pgc, pg := storagetest.Postgres()
	chc, _ := storagetest.ClickHouse()
	pg.On("FROM fractals\n\t\tWHERE id = $1", row)
	pg.On("SELECT is_default, is_system FROM fractals", fakedb.Row(false, false))
	pg.On("DELETE FROM fractals", fakedb.PGResult{Affected: deleted})
	m := NewManager(pgc, chc)
	var calls []string
	m.SetOnDeleteHook(func(_ context.Context, id string) { calls = append(calls, id) })
	return m, &calls
}

// Deleting a fractal cascades rows that own ClickHouse objects (lookups), so the
// delete hook runs once the fractal is gone, and never when the delete fails.
func TestDeleteFractalRunsHook(t *testing.T) {
	m, calls := newDeleteManager(t, fractalRow(false), 1)
	if err := m.DeleteFractal(context.Background(), fractalID); err != nil {
		t.Fatal(err)
	}
	if len(*calls) != 1 || (*calls)[0] != fractalID {
		t.Errorf("hook calls %v", *calls)
	}

	m, calls = newDeleteManager(t, fractalRow(true), 1)
	if err := m.DeleteFractal(context.Background(), fractalID); err == nil {
		t.Error("a system fractal was deleted")
	}
	m2, calls2 := newDeleteManager(t, fractalRow(false), 0)
	if err := m2.DeleteFractal(context.Background(), fractalID); err == nil {
		t.Error("a missing fractal reported deleted")
	}
	if len(*calls) != 0 || len(*calls2) != 0 {
		t.Errorf("hook ran on a failed delete: %v %v", *calls, *calls2)
	}
}
