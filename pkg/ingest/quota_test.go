package ingest

import (
	"database/sql/driver"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage/storagetest"
)

func TestCheckQuota(t *testing.T) {
	qm := &QuotaManager{state: map[string]*fractalQuotaState{
		"none":     {quotaBytes: 0, action: "reject", baseBytes: 1 << 40},
		"rollover": {quotaBytes: 100, action: "rollover", baseBytes: 1 << 40},
		"reject":   {quotaBytes: 100, action: "reject", baseBytes: 60},
	}}
	tests := []struct {
		fractal string
		batch   int64
		want    bool
	}{
		{"unknown", 1 << 40, true},
		{"none", 1 << 40, true},
		{"rollover", 1 << 40, true}, // rollover trims old data instead of refusing new
		{"reject", 40, true},        // exactly at the quota is allowed
		{"reject", 41, false},
	}
	for _, tc := range tests {
		if got := qm.CheckQuota(tc.fractal, tc.batch); got != tc.want {
			t.Errorf("CheckQuota(%s, %d) = %v, want %v", tc.fractal, tc.batch, got, tc.want)
		}
	}

	// Inserts since the last refresh count toward the estimate.
	qm.RecordInsert("reject", 30, 3)
	if qm.CheckQuota("reject", 11) {
		t.Error("in-memory deltas ignored: 60+30+11 > 100 was accepted")
	}
	qm.RecordInsert("unknown", 30, 3) // no state: ignored, no panic
	qm.NotifyCleared("reject")
	if !qm.CheckQuota("reject", 40) {
		t.Error("NotifyCleared did not reset the deltas")
	}
	qm.NotifyCleared("unknown")
}

func fractalRow(id string, quota int64, action string, size, count int64) []driver.Value {
	return []driver.Value{id, quota, action, size, count}
}

// TestQuotaLoadAndRefresh: a refresh re-seats the base from Postgres and drops
// the in-memory deltas, which the new size_bytes already includes.
func TestQuotaLoadAndRefresh(t *testing.T) {
	pgc, pg := storagetest.Postgres()
	pg.OnOnce("FROM fractals", fakedb.RowsOf(fractalRow("f1", 100, "reject", 50, 5)))
	qm := NewQuotaManager(pgc, nil)
	defer qm.Stop()

	if qm.CheckQuota("f1", 51) {
		t.Fatal("initial load not applied")
	}
	qm.RecordInsert("f1", 40, 4)

	pg.OnOnce("FROM fractals", fakedb.RowsOf(
		fractalRow("f1", 200, "rollover", 90, 9),
		fractalRow("f2", 10, "reject", 0, 0),
	))
	if err := qm.loadFromPostgres(); err != nil {
		t.Fatal(err)
	}
	st := qm.state["f1"]
	if st.quotaBytes != 200 || st.action != "rollover" || st.baseBytes != 90 || st.baseLogCount != 9 ||
		st.deltaBytes.Load() != 0 || st.deltaCount.Load() != 0 {
		t.Errorf("f1 after refresh = quota %d action %s base %d/%d delta %d/%d",
			st.quotaBytes, st.action, st.baseBytes, st.baseLogCount, st.deltaBytes.Load(), st.deltaCount.Load())
	}
	if qm.CheckQuota("f2", 11) {
		t.Error("new fractal's quota not loaded")
	}

	// A failed refresh keeps the last known state rather than lifting quotas.
	pg.OnOnce("FROM fractals", fakedb.PGResult{Err: errors.New("down")})
	if err := qm.loadFromPostgres(); err == nil {
		t.Fatal("query failure not reported")
	}
	if qm.CheckQuota("f2", 11) {
		t.Error("quota lifted after a failed refresh")
	}
	pg.OnOnce("FROM fractals", fakedb.RowsOf([]driver.Value{"f3", "not a number", "reject", int64(0), int64(0)}))
	if err := qm.loadFromPostgres(); err == nil {
		t.Fatal("scan failure not reported")
	}
}

// TestSweepRollovers: over-quota rollover fractals lose their oldest-ingested
// partitions until they are back under 80% of quota, the newest partition is
// never dropped, a failed drop moves on to the next, and nothing runs without
// the cross-replica lock.
func TestSweepRollovers(t *testing.T) {
	day := func(d int) time.Time { return time.Date(2026, 7, d, 0, 0, 0, 0, time.UTC) }
	part := func(f string, d int) string { return "('" + f + "','" + day(d).Format("2006-01-02") + "')" }
	partsResult := fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "partition", Type: "String"}, {Column: "bytes", Type: "UInt64"}, {Column: "mn", Type: "DateTime"}},
		Rows: [][]any{
			// f1: 1200 bytes over a 1000 quota; target 800 frees the oldest only.
			{part("f1", 3), uint64(400), day(3)},
			{part("f1", 1), uint64(400), day(1)},
			{part("f1", 2), uint64(400), day(2)},
			// f2 is reject-action: never trimmed however large.
			{part("f2", 1), uint64(1 << 40), day(1)},
			// f3: a single partition over quota is kept.
			{part("f3", 1), uint64(500), day(1)},
			// f4: the oldest drop fails, so the next one is taken instead.
			{part("f4", 1), uint64(600), day(1)},
			{part("f4", 2), uint64(600), day(2)},
			{part("f4", 3), uint64(600), day(3)},
			// f5 is under quota.
			{part("f5", 1), uint64(10), day(1)},
			// Not a logs partition key: ignored.
			{"tuple()", uint64(1 << 40), day(1)},
		},
	}

	setup := func(lock bool) (*QuotaManager, *fakedb.CH) {
		pgc, pg := storagetest.Postgres()
		pg.On("pg_try_advisory_lock", fakedb.Row(lock))
		pg.On("pg_advisory_unlock", fakedb.Row(true))
		admin, ch := storagetest.ClickHouse()
		ch.On("sum(bytes_on_disk)", partsResult)
		ch.FailExec(part("f4", 1), errors.New("drop failed"))
		qm := &QuotaManager{pg: pgc, admin: admin, state: map[string]*fractalQuotaState{
			"f1": {quotaBytes: 1000, action: "rollover"},
			"f2": {quotaBytes: 1, action: "reject"},
			"f3": {quotaBytes: 100, action: "rollover"},
			"f4": {quotaBytes: 1000, action: "rollover"},
			"f5": {quotaBytes: 1000, action: "rollover"},
			"f6": {quotaBytes: 0, action: "rollover"},
		}}
		return qm, ch
	}

	qm, ch := setup(true)
	qm.sweepRollovers()
	var dropped []string
	for _, stmt := range ch.ExecutedMatching("ALTER TABLE logs DROP PARTITION") {
		dropped = append(dropped, strings.TrimSpace(strings.SplitN(strings.TrimPrefix(stmt, "ALTER TABLE logs DROP PARTITION "), " SETTINGS", 2)[0]))
	}
	sort.Strings(dropped)
	want := []string{part("f1", 1), part("f4", 1), part("f4", 2)} // f4 day 1 attempted and failed
	if strings.Join(dropped, " ") != strings.Join(want, " ") {
		t.Fatalf("drop statements for %v, want %v", dropped, want)
	}

	qm, ch = setup(false)
	qm.sweepRollovers()
	if len(ch.Queried()) != 0 || len(ch.Executed()) != 0 {
		t.Fatal("sweep ran without holding the advisory lock")
	}

	// No rollover quotas: nothing is even queried.
	qm, ch = setup(true)
	qm.state = map[string]*fractalQuotaState{"f2": {quotaBytes: 1, action: "reject"}}
	qm.sweepRollovers()
	if len(ch.Queried()) != 0 {
		t.Fatal("sweep queried partitions with no rollover quotas")
	}
}

func TestStartRolloverSweepNeedsAdmin(t *testing.T) {
	qm := &QuotaManager{stop: make(chan struct{}), state: map[string]*fractalQuotaState{}}
	qm.StartRolloverSweep(nil)
	if qm.admin != nil {
		t.Fatal("sweep configured without an admin client")
	}
	admin, _ := storagetest.ClickHouse()
	qm.StartRolloverSweep(admin)
	qm.Stop() // the loop must exit on Stop
}
