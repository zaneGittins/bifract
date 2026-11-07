package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// The backfill appends its partition window to the view's WHERE, so the SELECT must end there.
func TestLineageViewSelectEndsInWhere(t *testing.T) {
	sel, err := initViewSelect("proc_lineage_mv")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(sel, "SELECT") || !strings.HasSuffix(sel, "fields.process_guid != ''") {
		t.Errorf("select = %q", sel)
	}
}

// A partition copies only what arrived after the last copy, moves the ancestry back to it, and is
// recorded only after both.
func TestBackfillLineageOnShard(t *testing.T) {
	sel, _ := initViewSelect("proc_lineage_mv")
	f := fakedb.NewCH()
	f.On(sel+" LIMIT 0", fakedb.CHResult{Cols: []fakedb.Col{{Column: "fractal_id", Type: "String"}, {Column: "process_guid", Type: "String"}}})
	f.On("SELECT DISTINCT partition_id FROM system.parts", fakedb.Strs("partition_id", "p1", "p2"))
	done, first, last := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 2, 1, 0, 0, 0, time.UTC), time.Date(2026, 1, 2, 2, 0, 0, 0, time.UTC)
	cols := []fakedb.Col{{Column: "done", Type: "DateTime64(3, 'UTC')"}, {Column: "first", Type: "DateTime64(3, 'UTC')"}, {Column: "last", Type: "DateTime64(3, 'UTC')"}}
	f.OnArg("SELECT (SELECT max(upto)", "p1", fakedb.CHResult{Cols: cols, Rows: [][]any{{done, first, last}}})
	f.OnArg("SELECT (SELECT max(upto)", "p2", fakedb.CHResult{Cols: cols, Rows: [][]any{{last, last, last}}})
	var rewound []time.Time
	err := backfillLineageOnShard(context.Background(), f, sel, func(at time.Time) error {
		rewound = append(rewound, at)
		f.Exec(context.Background(), "-- rewind")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	execs := f.Executed()
	copyAt := indexOf(execs, "INSERT INTO proc_lineage (`fractal_id`, `process_guid`) "+sel+" AND _partition_id = 'p1' AND ingest_timestamp > "+chTime(done)+" AND ingest_timestamp <= "+chTime(last))
	rewindAt, doneAt := indexOf(execs, "-- rewind"), indexOf(execs, "INSERT INTO "+lineageBackfillDone+" VALUES (?, ?)")
	if copyAt < 0 || rewindAt < copyAt || doneAt < rewindAt {
		t.Errorf("copy %d, rewind %d, done %d:\n%s", copyAt, rewindAt, doneAt, strings.Join(execs, "\n"))
	}
	if len(rewound) != 1 || !rewound[0].Equal(first) {
		t.Errorf("rewound %v, want once to %s", rewound, first)
	}
	if indexOf(execs, "_partition_id = 'p2'") >= 0 {
		t.Error("a partition with nothing new was copied again")
	}
}
