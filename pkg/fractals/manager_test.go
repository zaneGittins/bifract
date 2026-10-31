package fractals

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"

	"bifract/internal/fakedb"
	"bifract/pkg/storage/storagetest"
)

const fractalID = "11111111-1111-1111-1111-111111111111"

func fractalRow(isSystem bool) fakedb.PGResult {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return fakedb.Row(fractalID, "web", "", false, isSystem, "admin", now, now,
		nil, nil, nil, "reject", int64(0), int64(0), nil, nil, nil, nil)
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

// A dataset fractal keeps its logs (no retention), opens on the dataset's range,
// and is created like any other: same name rules, no clobbering, the create hook.
func TestCreateFractalForDataset(t *testing.T) {
	start := time.Date(2026, 10, 27, 8, 37, 53, 0, time.UTC)
	end := time.Date(2026, 10, 30, 13, 49, 59, 0, time.UTC)
	pgc, pg := storagetest.Postgres()
	chc, _ := storagetest.ClickHouse()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	pg.On("INSERT INTO fractals", fakedb.Row(fractalID, "bifract-labs-001", "Lab", false, false, "admin", now, now,
		nil, nil, nil, "reject", int64(0), int64(0), nil, nil, start, end))
	m := NewManager(pgc, chc)
	var hooked []string
	m.SetOnCreateHook(func(_ context.Context, f *Fractal) { hooked = append(hooked, f.ID) })

	f, err := m.CreateFractalForDataset(context.Background(), "bifract-labs-001", "Lab", "admin", start, end)
	if err != nil {
		t.Fatal(err)
	}
	if f.DefaultRangeStart == nil || !f.DefaultRangeStart.Equal(start) || f.DefaultRangeEnd == nil || !f.DefaultRangeEnd.Equal(end) {
		t.Errorf("default range = %v - %v", f.DefaultRangeStart, f.DefaultRangeEnd)
	}
	ins := pg.Matching("INSERT INTO fractals")
	if len(ins) != 1 || !strings.Contains(ins[0].Query, "retention_days") || ins[0].Args[3] != start || ins[0].Args[4] != end {
		t.Errorf("insert = %+v", ins)
	}
	if len(hooked) != 1 {
		t.Errorf("create hook calls %v", hooked)
	}

	if _, err := m.CreateFractalForDataset(context.Background(), "system", "", "admin", start, end); err == nil {
		t.Error("a reserved name was accepted")
	}
	// Two adds racing past the existence check meet the unique name constraint.
	pg.ClearRules()
	pg.On("INSERT INTO fractals", fakedb.PGResult{Err: &pq.Error{Code: "23505"}})
	if _, err := m.CreateFractalForDataset(context.Background(), "web", "", "admin", start, end); !errors.Is(err, ErrNameTaken) {
		t.Errorf("a concurrent add was not refused as taken: %v", err)
	}
	pg.On("WHERE name = $1", fractalRow(false))
	if _, err := m.CreateFractalForDataset(context.Background(), "web", "", "admin", start, end); !errors.Is(err, ErrNameTaken) {
		t.Errorf("an existing fractal was not refused: %v", err)
	}
}
