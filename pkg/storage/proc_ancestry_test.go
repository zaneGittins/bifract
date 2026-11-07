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
