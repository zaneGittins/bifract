package ruleeval

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/dictionaries"
	"bifract/pkg/normalizers"
	"bifract/pkg/parser"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
	"bifract/pkg/tlshresolve"
)

func logsKeysResult(sorting, partition string) fakedb.CHResult {
	return fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "sorting_key", Type: "String"}, {Column: "partition_key", Type: "String"}},
		Rows: [][]any{{sorting, partition}},
	}
}

func newScratch(t *testing.T) (*Scratch, *fakedb.CH) {
	t.Helper()
	ch, fake := storagetest.ClickHouse()
	fake.On("FROM system.tables", logsKeysResult("fractal_id, timestamp, log_id", "(fractal_id, toDate(ingest_timestamp))"))
	s, err := NewScratch(context.Background(), ch)
	if err != nil {
		t.Fatal(err)
	}
	return s, fake
}

var scratchNameRe = regexp.MustCompile(`^ruletest_[0-9a-f]{16}$`)

// The clone states its engine and copies the real table's keys, so it neither
// registers as a replica of logs nor inherits a TTL, yet reads rows in the same
// shape production does.
func TestNewScratchClonesLogsKeysWithPlainEngine(t *testing.T) {
	s, fake := newScratch(t)

	if !scratchNameRe.MatchString(s.Table()) {
		t.Fatalf("scratch table name %q is not ruletest_<16 hex>", s.Table())
	}
	if s.Qualified() != "logs."+s.Table() {
		t.Fatalf("Qualified() = %q", s.Qualified())
	}
	if s.Client() == nil {
		t.Fatal("Client() must expose the connection")
	}

	q := fake.QueriedMatching("system.tables")
	if len(q) != 1 || !strings.Contains(q[0], "database = 'logs' AND name = 'logs'") {
		t.Fatalf("keys query = %v", q)
	}
	execs := fake.Executed()
	if len(execs) != 1 {
		t.Fatalf("expected one CREATE, got %v", execs)
	}
	want := "CREATE TABLE " + s.Qualified() + " AS logs.logs ENGINE = MergeTree" +
		" PARTITION BY (fractal_id, toDate(ingest_timestamp))" +
		" ORDER BY (fractal_id, timestamp, log_id)"
	if execs[0] != want {
		t.Fatalf("CREATE =\n %s\nwant\n %s", execs[0], want)
	}
	for _, banned := range []string{"Replicated", "TTL", "ON CLUSTER"} {
		if strings.Contains(execs[0], banned) {
			t.Errorf("CREATE must not carry %s: %s", banned, execs[0])
		}
	}
}

func TestNewScratchNamesAreUnique(t *testing.T) {
	a, _ := newScratch(t)
	b, _ := newScratch(t)
	if a.Table() == b.Table() {
		t.Fatalf("two scratch tables share a name: %s", a.Table())
	}
}

func TestNewScratchWithoutKeysOrdersByTuple(t *testing.T) {
	// An empty database name falls back to "logs".
	fake := fakedb.NewCH()
	ch := storage.NewClickHouseClientFromConn(fake, "", storage.Topology{Kind: storage.DeploymentSingleNode})
	fake.On("FROM system.tables", logsKeysResult("", ""))
	s, err := NewScratch(context.Background(), ch)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(s.Qualified(), "logs.ruletest_") {
		t.Fatalf("Qualified() = %q, want the default logs database", s.Qualified())
	}
	got := fake.Executed()[0]
	if !strings.HasSuffix(got, "AS logs.logs ENGINE = MergeTree ORDER BY tuple()") || strings.Contains(got, "PARTITION BY") {
		t.Fatalf("CREATE = %s", got)
	}
}

func TestNewScratchFailures(t *testing.T) {
	if _, err := NewScratch(context.Background(), nil); err == nil {
		t.Fatal("a nil client must be rejected")
	}

	ch, fake := storagetest.ClickHouse() // no system.tables row
	if _, err := NewScratch(context.Background(), ch); err == nil || !strings.Contains(err.Error(), "no logs table in database logs") {
		t.Fatalf("missing logs table: %v", err)
	}
	if len(fake.Executed()) != 0 {
		t.Fatal("must not create anything when the source table is missing")
	}

	ch, fake = storagetest.ClickHouse()
	fake.On("FROM system.tables", fakedb.CHResult{Err: errors.New("auth failed")})
	if _, err := NewScratch(context.Background(), ch); err == nil || !strings.Contains(err.Error(), "reading logs table keys") {
		t.Fatalf("keys query failure: %v", err)
	}

	ch, fake = storagetest.ClickHouse()
	fake.On("FROM system.tables", logsKeysResult("timestamp", ""))
	fake.FailExec("CREATE TABLE", errors.New("readonly"))
	if _, err := NewScratch(context.Background(), ch); err == nil || !strings.Contains(err.Error(), "creating scratch table") {
		t.Fatalf("create failure: %v", err)
	}
}

func TestDropIsIdempotentAndNilSafe(t *testing.T) {
	s, fake := newScratch(t)
	qualified := s.Qualified()
	if err := s.Drop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Drop(context.Background()); err != nil {
		t.Fatal(err)
	}
	drops := fake.ExecutedMatching("DROP TABLE")
	if len(drops) != 1 || drops[0] != "DROP TABLE IF EXISTS "+qualified {
		t.Fatalf("drops = %v, want exactly one DROP of %s", drops, qualified)
	}

	var nilScratch *Scratch
	if err := nilScratch.Drop(context.Background()); err != nil {
		t.Fatalf("nil scratch: %v", err)
	}
	if err := (&Scratch{}).Drop(context.Background()); err != nil {
		t.Fatalf("zero scratch: %v", err)
	}
}

// A failed drop keeps the table name so the caller can retry it.
func TestDropFailureKeepsTableForRetry(t *testing.T) {
	s, fake := newScratch(t)
	fake.FailExec("DROP TABLE", errors.New("timeout"))
	if err := s.Drop(context.Background()); err == nil || !strings.Contains(err.Error(), s.Table()) {
		t.Fatalf("drop error must name the table: %v", err)
	}
	if s.Table() == "" {
		t.Fatal("a failed drop must not forget the table")
	}
}

func TestNewWindowIsUTCAndCentred(t *testing.T) {
	loc := time.FixedZone("UTC+5", 5*3600)
	now := time.Date(2026, 6, 1, 17, 0, 0, 0, loc)
	w := NewWindow(now)
	if w.Start.Location() != time.UTC || w.End.Location() != time.UTC {
		t.Fatal("window bounds must be UTC: ClickHouse reads naive literals in server time")
	}
	if !w.Start.Equal(now.Add(-WindowSpan)) || !w.End.Equal(now.Add(WindowSpan)) {
		t.Fatalf("window = %v .. %v", w.Start, w.End)
	}
	if w.Start.Hour() != 11 {
		t.Fatalf("start hour = %d, want 11 (12:00 UTC minus one hour)", w.Start.Hour())
	}
}

func TestNewUnitIsolatesEachUnitInItsOwnFractal(t *testing.T) {
	events := []map[string]interface{}{
		{"Image": `C:\Windows\cmd.exe`, "EventID": float64(1)},
		{"Image": `C:\Windows\explorer.exe`, "EventID": float64(1)},
	}
	a, err := NewUnit("case a", events, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewUnit("case b", events[:1], nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.FractalID == "" || a.FractalID == b.FractalID {
		t.Fatalf("units must get distinct synthetic fractals: %q vs %q", a.FractalID, b.FractalID)
	}
	if a.Label != "case a" || len(a.Entries) != 2 || len(b.Entries) != 1 {
		t.Fatalf("unit a = %+v", a)
	}
	for _, e := range a.Entries {
		if e.FractalID != a.FractalID {
			t.Fatalf("entry fractal %q, want the unit's %q", e.FractalID, a.FractalID)
		}
		if e.Normalizer != "" {
			t.Fatalf("no normalizer means no stamp, got %q", e.Normalizer)
		}
	}
	// A nil normalizer takes the event as already-normalized fields.
	if got := a.Entries[0].Fields["Image"]; got != `C:\Windows\cmd.exe` {
		t.Fatalf("Image = %q", got)
	}
	if got := a.Entries[0].Fields["EventID"]; got != "1" {
		t.Fatalf("EventID = %q", got)
	}
}

// With a normalizer, events are shaped exactly as ingestion would shape them, so
// a rule written against normalized names sees normalized names.
func TestNewUnitAppliesNormalizer(t *testing.T) {
	norm := (&normalizers.Normalizer{
		Name: "edr", Version: 4,
		Transforms:    []normalizers.Transform{normalizers.TransformFlattenLeaf, normalizers.TransformSnakeCase},
		FieldMappings: []normalizers.FieldMapping{{Sources: []string{"image"}, Target: "process_path"}},
	}).Compile()
	u, err := NewUnit("n", []map[string]interface{}{
		{"Event": map[string]interface{}{"Image": "a.exe", "ParentImage": "b.exe"}},
	}, norm)
	if err != nil {
		t.Fatal(err)
	}
	f := u.Entries[0].Fields
	if f["process_path"] != "a.exe" || f["parent_image"] != "b.exe" {
		t.Fatalf("fields = %v", f)
	}
	if u.Entries[0].Normalizer != "edr@4" {
		t.Fatalf("stamp = %q", u.Entries[0].Normalizer)
	}
}

func TestNewUnitReportsWhichEventFailed(t *testing.T) {
	_, err := NewUnit("bad", []map[string]interface{}{
		{"ok": "x"},
		{"unencodable": make(chan int)},
	}, nil)
	if err == nil || !strings.HasPrefix(err.Error(), "event 2:") {
		t.Fatalf("got %v, want an error naming event 2", err)
	}
}

func TestInsertBatchesEveryUnitIntoScratch(t *testing.T) {
	s, fake := newScratch(t)
	a, _ := NewUnit("a", []map[string]interface{}{{"x": "1"}, {"x": "2"}}, nil)
	b, _ := NewUnit("b", []map[string]interface{}{{"x": "3"}}, nil)

	if err := s.Insert(context.Background(), []Unit{a, b}); err != nil {
		t.Fatal(err)
	}
	batches := fake.Batches()
	if len(batches) != 1 {
		t.Fatalf("expected one batch for all units, got %d", len(batches))
	}
	if !strings.HasPrefix(batches[0].Query, "INSERT INTO "+s.Table()+" ") {
		t.Fatalf("insert must target the scratch table, not logs: %s", batches[0].Query)
	}
	if !batches[0].Sent || len(batches[0].Appended) != 3 {
		t.Fatalf("batch sent=%v rows=%d", batches[0].Sent, len(batches[0].Appended))
	}
	// fractal_id is the fourth column of the insert.
	fractals := []any{batches[0].Appended[0][3], batches[0].Appended[1][3], batches[0].Appended[2][3]}
	if fractals[0] != a.FractalID || fractals[1] != a.FractalID || fractals[2] != b.FractalID {
		t.Fatalf("rows carry fractals %v", fractals)
	}
}

func TestInsertEmptyAndFailure(t *testing.T) {
	s, fake := newScratch(t)
	if err := s.Insert(context.Background(), []Unit{{FractalID: "f"}}); err != nil {
		t.Fatal(err)
	}
	if len(fake.Batches()) != 0 {
		t.Fatal("no entries must not prepare a batch")
	}

	fake.PrepareErr = errors.New("too many parts")
	u, _ := NewUnit("a", []map[string]interface{}{{"x": "1"}}, nil)
	if err := s.Insert(context.Background(), []Unit{u}); err == nil || !strings.Contains(err.Error(), "inserting test events") {
		t.Fatalf("got %v", err)
	}
}

func countsResult(rows ...[]any) fakedb.CHResult {
	return fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "fractal_id", Type: "String"}, {Column: "c", Type: "UInt64"}},
		Rows: rows,
	}
}

func TestWaitVisibleReturnsOnceAllRowsAreQueryable(t *testing.T) {
	s, fake := newScratch(t)
	w := NewWindow(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	units := []Unit{
		{FractalID: "f-a", Label: "a", Entries: make([]storage.LogEntry, 2)},
		{FractalID: "f-b", Label: "b", Entries: make([]storage.LogEntry, 1)},
	}
	// First poll: one row of f-a still in flight. Second: everything visible.
	fake.OnOnce("GROUP BY fractal_id", countsResult([]any{"f-a", uint64(1)}, []any{"f-b", uint64(1)}))
	fake.On("GROUP BY fractal_id", countsResult([]any{"f-a", uint64(2)}, []any{"f-b", uint64(1)}, []any{"other", uint64(9)}))

	if err := s.WaitVisible(context.Background(), units, w); err != nil {
		t.Fatal(err)
	}
	q := fake.QueriedMatching("GROUP BY fractal_id")
	if len(q) != 2 {
		t.Fatalf("expected a retry after the partial read, got %d polls", len(q))
	}
	want := "FROM " + s.Qualified() + " WHERE ingest_timestamp >= '2026-06-01 11:00:00' AND ingest_timestamp <= '2026-06-01 13:00:00'"
	if !strings.Contains(q[0], want) {
		t.Fatalf("visibility query must read the scratch table within the window:\n %s\nwant fragment\n %s", q[0], want)
	}
}

// An invisible row is a hard error, never a pass: otherwise every no-match case
// would pass for the wrong reason.
func TestWaitVisibleNeverReportsSuccessWithMissingRows(t *testing.T) {
	s, fake := newScratch(t)
	fake.On("GROUP BY fractal_id", countsResult([]any{"f-a", uint64(1)}))
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	err := s.WaitVisible(ctx, []Unit{{FractalID: "f-a", Entries: make([]storage.LogEntry, 3)}}, NewWindow(time.Now()))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the context to end the wait", err)
	}
}

func TestWaitVisibleEdgeCases(t *testing.T) {
	s, fake := newScratch(t)
	if err := s.WaitVisible(context.Background(), nil, NewWindow(time.Now())); err != nil {
		t.Fatal(err)
	}
	if len(fake.QueriedMatching("GROUP BY")) != 0 {
		t.Fatal("no units must not query")
	}

	fake.On("GROUP BY fractal_id", fakedb.CHResult{Err: errors.New("down")})
	err := s.WaitVisible(context.Background(), []Unit{{FractalID: "f", Entries: make([]storage.LogEntry, 1)}}, NewWindow(time.Now()))
	if err == nil || !strings.Contains(err.Error(), "verifying test events are queryable") {
		t.Fatalf("got %v", err)
	}
}

func TestToUint64(t *testing.T) {
	cases := []struct {
		in   interface{}
		want uint64
	}{
		{uint64(7), 7}, {int64(8), 8}, {int(9), 9}, {uint32(10), 10}, {float64(11), 11},
		{"12", 0}, {nil, 0},
	}
	for _, c := range cases {
		if got := toUint64(c.in); got != c.want {
			t.Errorf("toUint64(%#v) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestQueryOptionsScopeToUnitAndScratch(t *testing.T) {
	s, _ := newScratch(t)
	scope := dictionaries.Scope{
		Mappings:        map[string]map[string]string{"bad_hosts": {"host": "dict_x"}},
		CaseInsensitive: map[string]bool{"bad_hosts": true},
		Network:         map[string]bool{"dict_net": true},
		Pattern:         map[string]bool{"dict_re": true},
	}
	withDicts := s.WithDictionaries(scope)
	w := NewWindow(time.Now())
	opts := withDicts.QueryOptions(Unit{FractalID: "f-unit"}, w)

	if opts.TableName != s.Table() || opts.FractalID != "f-unit" || !opts.UseIngestTimestamp {
		t.Fatalf("opts = %+v", opts)
	}
	if !opts.StartTime.Equal(w.Start) || !opts.EndTime.Equal(w.End) || opts.MaxRows != 10000 {
		t.Fatalf("window/limit = %v %v %d", opts.StartTime, opts.EndTime, opts.MaxRows)
	}
	if opts.Dictionaries["bad_hosts"]["host"] != "dict_x" || !opts.CaseInsensitiveDicts["bad_hosts"] ||
		!opts.NetworkDicts["dict_net"] || !opts.PatternDicts["dict_re"] || opts.DictionaryDatabase != "logs" {
		t.Fatalf("dictionary options not carried: %+v", opts)
	}

	// The view is a copy: the original scratch is untouched.
	if s.QueryOptions(Unit{}, w).Dictionaries != nil {
		t.Fatal("WithDictionaries mutated the shared scratch")
	}
	if withDicts.Table() != s.Table() {
		t.Fatal("the view must share the table")
	}
}

func TestWithTLSHIsACopy(t *testing.T) {
	s, _ := newScratch(t)
	r := &tlshresolve.Resolver{}
	v := s.WithTLSH(r, "f-rule", "p-rule")
	if s.tlsh != nil || s.tlshFractalID != "" {
		t.Fatal("WithTLSH mutated the shared scratch")
	}
	if v.tlsh != r || v.tlshFractalID != "f-rule" || v.tlshPrismID != "p-rule" || v.Table() != s.Table() {
		t.Fatalf("view = %+v", v)
	}
}

func TestEvaluateRunsTranslatedRuleAgainstUnit(t *testing.T) {
	s, fake := newScratch(t)
	fake.On("multiSearchAnyCaseInsensitive", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "log_id", Type: "String"}},
		Rows: [][]any{{"l1"}, {"l2"}},
	})
	pipeline, err := parser.ParseQuery(`CommandLine=~"-enc"`)
	if err != nil {
		t.Fatal(err)
	}
	u := Unit{FractalID: "f-unit"}
	rows, sql, err := s.Evaluate(context.Background(), pipeline, u, NewWindow(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("rows = %d, want 2", rows)
	}
	for _, frag := range []string{"FROM " + s.Table(), "'f-unit'", "ingest_timestamp"} {
		if !strings.Contains(sql, frag) {
			t.Errorf("SQL must contain %q:\n%s", frag, sql)
		}
	}
	if q := fake.Queried(); q[len(q)-1] != sql {
		t.Fatal("the returned SQL must be the SQL that ran")
	}
}

func TestEvaluateErrors(t *testing.T) {
	s, fake := newScratch(t)
	fake.On("SELECT", fakedb.CHResult{Err: errors.New("code 47: unknown identifier")})
	pipeline, _ := parser.ParseQuery(`Image="cmd.exe"`)
	_, sql, err := s.Evaluate(context.Background(), pipeline, Unit{FractalID: "f"}, NewWindow(time.Now()))
	if err == nil || !strings.Contains(err.Error(), "query failed") {
		t.Fatalf("got %v", err)
	}
	if sql == "" {
		t.Fatal("a failed query must still return its SQL for explain output")
	}

	// tlsh() without a resolver fails before any SQL is produced.
	pipeline, _ = parser.ParseQuery(`* | tlsh(field=tlsh, dict="known_bad")`)
	_, sql, err = s.Evaluate(context.Background(), pipeline, Unit{FractalID: "f"}, NewWindow(time.Now()))
	if err == nil || sql != "" {
		t.Fatalf("got sql=%q err=%v", sql, err)
	}

	// A rule that parses but cannot be translated says so, and nothing runs.
	before := len(fake.Queried())
	pipeline, err = parser.ParseQuery(`bytes > abc`)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.Evaluate(context.Background(), pipeline, Unit{FractalID: "f"}, NewWindow(time.Now()))
	if err == nil || !strings.Contains(err.Error(), "translating BQL to SQL") {
		t.Fatalf("got %v", err)
	}
	if len(fake.Queried()) != before {
		t.Fatal("an untranslatable rule must not run a query")
	}
}

func TestEvaluateTLSHDigestReadFailure(t *testing.T) {
	s, fake := newScratch(t)
	fake.On("SELECT DISTINCT", fakedb.CHResult{Err: errors.New("table dropped")})
	v := s.WithTLSH(tlshresolve.NewResolver(nil, nil, s.Client()), "f-rule", "")
	pipeline, _ := parser.ParseQuery(`* | tlsh(field=tlsh, hash="` + digestA + `")`)
	_, sql, err := v.Evaluate(context.Background(), pipeline, Unit{FractalID: "f-unit"}, NewWindow(time.Now()))
	if err == nil || !strings.Contains(err.Error(), "table dropped") || sql != "" {
		t.Fatalf("got sql=%q err=%v", sql, err)
	}
}

const (
	digestA  = "8aa32957b3e520f9e1b28a3884954a49d775f8361b219fef03b442961f237e48d3ab31"
	digestA2 = "8aa32957b3e520f9e1b28a3884954a49d775f8361b219fef03b442961f237e48d3ab30"
	digestB  = "c1716de2793a163c2cb95403ff9c33dae415c8c44f192262786270eb9136a0c8b1d549"
)

// tlsh() in a rule test reads candidate digests from the scratch rows of the unit
// under test, and the query then filters on the digests that matched.
func TestEvaluateResolvesTLSHFromScratchRows(t *testing.T) {
	s, fake := newScratch(t)
	fake.On("SELECT DISTINCT", fakedb.CHResult{
		Cols: []fakedb.Col{{Column: "digest", Type: "String"}},
		Rows: [][]any{{digestA2}, {digestB}},
	})
	v := s.WithTLSH(tlshresolve.NewResolver(nil, nil, s.Client()), "f-rule", "")
	pipeline, err := parser.ParseQuery(`* | tlsh(field=tlsh, hash="` + digestA + `", threshold=30)`)
	if err != nil {
		t.Fatal(err)
	}
	_, sql, err := v.Evaluate(context.Background(), pipeline, Unit{FractalID: "f-unit"}, NewWindow(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	probe := fake.QueriedMatching("SELECT DISTINCT")
	if len(probe) != 1 || !strings.Contains(probe[0], "FROM "+s.Table()+" WHERE fractal_id = 'f-unit'") {
		t.Fatalf("digests must be read from the unit's scratch rows: %v", probe)
	}
	if !strings.Contains(sql, digestA2) {
		t.Errorf("the near digest must be in the final filter:\n%s", sql)
	}
	if strings.Contains(sql, digestB) {
		t.Errorf("a distant digest must not be in the final filter:\n%s", sql)
	}
}
