package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/parser"
)

// A slice's watermark is written only once every statement of it has run, so a slice cut short
// is redone, and a failed slice leaves the watermark where it was.
func TestReflectProcAncestryAdvancesOnlyAfterABuild(t *testing.T) {
	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for _, fail := range []bool{false, true} {
		c, f := newFakeClient()
		f.On("SELECT argMax(at, set_at) FROM proc_ancestry_watermark", fakedb.One("argMax(at, set_at)", "DateTime64(3, 'UTC')", from))
		f.On("SELECT count() FROM proc_lineage", fakedb.Count(10))
		f.On(lateDescendantsMatch, fakedb.Count(1))
		if fail {
			f.FailExec("depth + x.depth", errors.New("boom"))
		}
		slice := time.Hour
		err := c.reflectProcAncestry(context.Background(), from.Add(30*time.Minute), &slice)
		execs := f.Executed()
		mark := indexOf(execs, "INSERT INTO proc_ancestry_watermark")
		if fail {
			if err == nil || mark >= 0 {
				t.Errorf("failed build: err %v, watermark written at %d", err, mark)
			}
			continue
		}
		last := indexOf(execs, "WHERE parent_key = '' AND parent_image = ''")
		if err != nil || last < 0 || mark < last {
			t.Errorf("err %v; watermark at %d must follow the last build statement at %d:\n%s", err, mark, last, strings.Join(execs, "\n"))
		}
	}
}

// The builder records ancestors exactly as deep as the filters may ask for.
func TestProcAncestryDepthMatchesFilters(t *testing.T) {
	if procAncestryDepth != parser.MaxPgrDepth {
		t.Errorf("ancestry is built to depth %d, filters allow %d", procAncestryDepth, parser.MaxPgrDepth)
	}
}

// A slice's cost is mostly fixed: one that leaves room doubles, and a slow one keeps its size,
// since shrinking it would only add fixed costs.
func TestNextSlice(t *testing.T) {
	half := procAncestryCycleBudget / 2
	for _, tc := range []struct {
		took time.Duration
		want time.Duration
	}{
		{0, 2 * time.Hour},
		{half / 2, 2 * time.Hour},
		{half, time.Hour},
		{procAncestryCycleBudget, time.Hour},
	} {
		if got := nextSlice(time.Hour, tc.took); got != tc.want {
			t.Errorf("a 1h slice in %s: next %s, want %s", tc.took, got, tc.want)
		}
	}
	if got := nextSlice(20*time.Hour, 0); got != 24*time.Hour {
		t.Errorf("growth is capped at a day, got %s", got)
	}
}

// A cycle ends when its budget does. Running out partway through a later slice is that end,
// not a failure: no error, and the slice keeps its size. Only a slice that could not finish
// with the whole budget to itself is too large.
func TestReflectProcAncestryBudgetEnd(t *testing.T) {
	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for _, failAt := range []int{1, 2} {
		c, f := newFakeClient()
		f.On("SELECT argMax(at, set_at) FROM proc_ancestry_watermark", fakedb.One("argMax(at, set_at)", "DateTime64(3, 'UTC')", from))
		f.On("SELECT count() FROM proc_lineage", fakedb.Count(10))
		f.On(lateDescendantsMatch, fakedb.Count(0))
		builds := 0
		f.FailExecFunc(func(stmt string) error {
			if strings.Contains(stmt, "WHERE parent_key != '' OR parent_image != ''") {
				if builds++; builds == failAt {
					return context.DeadlineExceeded
				}
			}
			return nil
		})
		slice := time.Hour
		err := c.reflectProcAncestry(context.Background(), from.Add(24*time.Hour), &slice)
		if failAt == 1 && (err == nil || slice != 30*time.Minute) {
			t.Errorf("first slice cut off: err %v, slice %s; want an error and a halved slice", err, slice)
		}
		if failAt == 2 && (err != nil || slice < time.Hour) {
			t.Errorf("later slice cut off: err %v, slice %s; want no error and no shrink", err, slice)
		}
	}
}

// lateDescendantsMatch picks out the check for processes waiting on one in the slice.
const lateDescendantsMatch = "proc_key NOT IN"

// Chains are extended through a slice's processes only when an earlier process is waiting on
// one of them; otherwise the depth statements have already built every chain the slice touches.
func TestProcAncestryExtendsChainsOnlyForLateDescendants(t *testing.T) {
	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for _, late := range []uint64{0, 1} {
		c, f := newFakeClient()
		f.On(lateDescendantsMatch, fakedb.Count(late))
		if err := c.buildProcAncestry(context.Background(), from, from.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		passes := len(f.ExecutedMatching("d.depth + x.depth"))
		status := f.ExecutedMatching("SELECT fractal_id, proc_key, 0, process_guid, image, '', '', if(")
		if want := map[uint64]int{0: 0, 1: procAncestryDepth}[late]; passes != want {
			t.Errorf("late descendants %d: %d extension passes, want %d", late, passes, want)
		}
		if len(status) != 1 {
			t.Fatalf("late descendants %d: %d status statements", late, len(status))
		}
		if got := strings.Count(status[0], "UNION ALL"); late == 0 && got != 0 {
			t.Errorf("without late descendants the status rows cover the slice alone, got %d unions", got)
		}
	}
}
