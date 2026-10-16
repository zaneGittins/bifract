package dictionaries

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// feed serves a source file the test can change between refreshes.
type feed struct {
	mu     sync.Mutex
	body   string
	status int
	hits   int
	url    string
}

func newFeed(t *testing.T, body string) *feed {
	f := &feed{body: body, status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.hits++
		w.WriteHeader(f.status)
		_, _ = w.Write([]byte(f.body))
	}))
	t.Cleanup(srv.Close)
	f.url = srv.URL + "/list.csv"
	return f
}

func (f *feed) set(status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status, f.body = status, body
}

func (f *feed) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits
}

// sourcedWorld is a world whose egress policy admits the loopback test server.
func sourcedWorld(t *testing.T, cluster bool) *world {
	w := newWorld(t)
	if cluster {
		w = newClusterWorld(t)
	}
	w.settings[allowPrivateSetting] = "true"
	return w
}

// age moves a list's last check back past the manual cooldown and its interval.
func (w *world) age(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	d := w.dicts[id]
	var st SourceStatus
	_ = json.Unmarshal(d.status, &st)
	old := time.Now().Add(-48 * time.Hour)
	st.CheckedAt = &old
	d.status, _ = json.Marshal(st)
}

func (w *world) execsMatching(sub string) int { return len(w.ch.ExecutedMatching(sub)) }

func createSourced(t *testing.T, w *world, f *feed, key string) *Dictionary {
	t.Helper()
	d, err := w.m.CreateDictionary(ctx, CreateOptions{FractalID: fractalA, Name: "feed",
		Source: &Source{URL: f.url, KeyField: key}})
	if err != nil {
		t.Fatalf("create with a source: %v", err)
	}
	return d
}

// A list created with a source is filled before the call returns: its columns
// come from the file, its rows are numbered in file order, and the status
// records the load. The lock is released afterwards.
func TestCreateWithSourceLoadsTheFile(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		w := sourcedWorld(t, cluster)
		f := newFeed(t, "host,owner\nweb1,ops\ndb1,dba\n")
		d := createSourced(t, w, f, "host")

		if d.KeyColumn != "host" || !slices.Equal(colNames(d.Columns), []string{"host", "owner"}) {
			t.Fatalf("cluster=%v: key %q columns %v", cluster, d.KeyColumn, colNames(d.Columns))
		}
		live := w.liveRows(d.ID)
		if got := keysOf(live, "host"); !slices.Equal(got, []string{"web1", "db1"}) || live[1].vals["owner"] != "dba" {
			t.Errorf("cluster=%v rows %v", cluster, got)
		}
		st := w.statusOf(d.ID)
		if st.CheckedAt == nil || st.ChangedAt == nil || st.Hash == "" || st.Error != "" || st.Failures != 0 {
			t.Errorf("status %+v", st)
		}
		if d.SourceStatus == nil || d.SourceStatus.Hash != st.Hash {
			t.Errorf("returned list does not carry the stored status: %+v", d.SourceStatus)
		}
		if rec, _ := w.dict(d.ID); rec.rowCount != 2 {
			t.Errorf("row count %d", rec.rowCount)
		}
		if !slices.Contains(w.reloads, d.CHDictName) {
			t.Error("dictionary not reloaded")
		}
		if len(w.locks) != 0 {
			t.Errorf("lock held after the load: %v", w.locks)
		}
	}
}

// A list whose first load fails is removed with its objects: it has no columns,
// so nothing could use it. Private space is read from Postgres per fetch, so
// without the setting the loopback source is refused.
func TestFirstLoadFailureRemovesTheList(t *testing.T) {
	for name, setup := range map[string]func(*world, *feed){
		"egress refused": func(w *world, _ *feed) { delete(w.settings, allowPrivateSetting) },
		"http error":     func(_ *world, f *feed) { f.set(http.StatusInternalServerError, "") },
		"no usable rows": func(_ *world, f *feed) { f.set(http.StatusOK, "host,owner\n,ops\n") },
		"missing column": func(_ *world, f *feed) { f.set(http.StatusOK, "name,owner\na,b\n") },
	} {
		w := sourcedWorld(t, false)
		f := newFeed(t, "host,owner\nweb1,ops\n")
		setup(w, f)
		_, err := w.m.CreateDictionary(ctx, CreateOptions{FractalID: fractalA, Name: "feed",
			Source: &Source{URL: f.url, KeyField: "host"}})
		if !IsInvalidInput(err) {
			t.Errorf("%s: err = %v, want a caller-facing reason", name, err)
		}
		if len(w.dicts) != 0 || len(w.tables) != 0 {
			t.Errorf("%s: left %d lists and %d tables", name, len(w.dicts), len(w.tables))
		}
	}
}

// A refresh loads the new file above every ordinal the table holds, then deletes
// what was below: the list is never empty, the editor shows file order, and a
// key that left the file leaves the list.
func TestRefreshReplacesRows(t *testing.T) {
	w := sourcedWorld(t, true)
	f := newFeed(t, "host,owner\nweb1,ops\ndb1,dba\n")
	d := createSourced(t, w, f, "host")
	firstHash := w.statusOf(d.ID).Hash

	f.set(http.StatusOK, "host,owner\ncache,ops\nweb1,web-team\n")
	w.age(d.ID)
	d, err := w.m.SyncSource(ctx, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	live := w.liveRows(d.ID)
	if got := keysOf(live, "host"); !slices.Equal(got, []string{"cache", "web1"}) {
		t.Errorf("rows %v", got)
	}
	if got := seqsOf(live); !slices.Equal(got, []uint64{3, 4}) {
		t.Errorf("ordinals %v, want both above the old maximum", got)
	}
	if len(live) == 2 && live[1].vals["owner"] != "web-team" {
		t.Errorf("updated value lost: %v", live[1].vals)
	}
	deletes := w.ch.ExecutedMatching("DELETE WHERE `_bf_seq` < 3")
	if len(deletes) != 1 || !strings.Contains(deletes[0], "ALTER TABLE `"+d.CHTableName+"`") {
		t.Errorf("swap delete %v", deletes)
	}
	if st := w.statusOf(d.ID); st.Hash == firstHash || st.Hash == "" {
		t.Error("hash not updated")
	}
}

// An unchanged file costs a download and nothing else, unless the table lost
// its rows, in which case it is loaded again.
func TestUnchangedFileIsSkippedOnlyWithRows(t *testing.T) {
	w := sourcedWorld(t, false)
	f := newFeed(t, "host\nweb1\n")
	d := createSourced(t, w, f, "host")
	inserts := w.execsMatching("INSERT INTO")

	w.age(d.ID)
	if _, err := w.m.SyncSource(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if w.execsMatching("INSERT INTO") != inserts || f.count() != 2 {
		t.Errorf("unchanged file: %d inserts, %d fetches", w.execsMatching("INSERT INTO")-inserts, f.count())
	}

	w.mu.Lock()
	w.tables[d.CHTableName].rows = nil
	w.mu.Unlock()
	w.age(d.ID)
	if _, err := w.m.SyncSource(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if got := keysOf(w.liveRows(d.ID), "host"); !slices.Equal(got, []string{"web1"}) {
		t.Errorf("emptied table not refilled: %v", got)
	}
}

// A refresh that fails leaves every old row in place and records why, with the
// failure count that spaces out retries. The reason shown is the caller-facing
// one; a server-side failure says only that the log has details.
func TestFailedRefreshKeepsOldRows(t *testing.T) {
	w := sourcedWorld(t, false)
	f := newFeed(t, "host\nweb1\ndb1\n")
	d := createSourced(t, w, f, "host")
	hash := w.statusOf(d.ID).Hash

	f.set(http.StatusNotFound, "")
	w.age(d.ID)
	if _, err := w.m.SyncSource(ctx, d.ID); !IsInvalidInput(err) {
		t.Fatalf("err = %v", err)
	}
	if got := keysOf(w.liveRows(d.ID), "host"); !slices.Equal(got, []string{"web1", "db1"}) {
		t.Errorf("rows after a failed fetch: %v", got)
	}
	st := w.statusOf(d.ID)
	if !strings.Contains(st.Error, "404") || st.Failures != 1 || st.Hash != hash {
		t.Errorf("status %+v", st)
	}

	// An insert failure part way deletes nothing.
	f.set(http.StatusOK, "host\nnew1\n")
	w.ch.FailExec("INSERT INTO", errors.New("too many parts"))
	w.age(d.ID)
	if _, err := w.m.SyncSource(ctx, d.ID); err == nil || IsInvalidInput(err) {
		t.Fatalf("err = %v", err)
	}
	if got := keysOf(w.liveRows(d.ID), "host"); !slices.Equal(got, []string{"web1", "db1"}) {
		t.Errorf("rows after a failed insert: %v", got)
	}
	st = w.statusOf(d.ID)
	if st.Failures != 2 || st.Error != sourceErrorText(errors.New("x")) {
		t.Errorf("status after a server-side failure %+v", st)
	}
}

// When the new rows land but the old ones cannot be deleted, both stay: the
// list has more rows than the file, never fewer. The hash is not recorded, so
// the next refresh loads again and clears both.
func TestRefreshDeleteFailureKeepsBoth(t *testing.T) {
	w := sourcedWorld(t, false)
	f := newFeed(t, "host\nold\n")
	d := createSourced(t, w, f, "host")
	hash := w.statusOf(d.ID).Hash

	f.set(http.StatusOK, "host\nnew\n")
	failing := true
	w.ch.FailExecFunc(func(stmt string) error {
		if failing && strings.Contains(stmt, "DELETE WHERE") {
			return errors.New("mutation timeout")
		}
		return nil
	})
	w.age(d.ID)
	if _, err := w.m.SyncSource(ctx, d.ID); err == nil || !strings.Contains(err.Error(), "could not be removed") {
		t.Fatalf("err = %v", err)
	}
	if got := keysOf(w.liveRows(d.ID), "host"); !slices.Equal(got, []string{"old", "new"}) {
		t.Errorf("rows %v", got)
	}
	if w.statusOf(d.ID).Hash != hash {
		t.Error("a half-applied load recorded the new hash")
	}

	failing = false
	w.age(d.ID)
	if _, err := w.m.SyncSource(ctx, d.ID); err != nil {
		t.Fatal(err)
	}
	if got := keysOf(w.liveRows(d.ID), "host"); !slices.Equal(got, []string{"new"}) {
		t.Errorf("rows after the retry %v", got)
	}
	if st := w.statusOf(d.ID); st.Failures != 0 || st.Error != "" {
		t.Errorf("success did not clear the failure: %+v", st)
	}
}

// A manual refresh right after a check is refused, one under another replica's
// lock reports busy, and a list without a source says so.
func TestSyncSourceRefusals(t *testing.T) {
	w := sourcedWorld(t, false)
	f := newFeed(t, "host\nweb1\n")
	d := createSourced(t, w, f, "host")

	if _, err := w.m.SyncSource(ctx, d.ID); !errors.Is(err, errSourceCooldown) {
		t.Errorf("right after a check: %v", err)
	}
	w.age(d.ID)
	w.busy[sourceLockID(d.ID)] = true
	if _, err := w.m.SyncSource(ctx, d.ID); !errors.Is(err, errSourceBusy) {
		t.Errorf("under a held lock: %v", err)
	}
	if _, err := w.m.SetSource(ctx, d.ID, Source{URL: f.url, KeyField: "host"}); !errors.Is(err, errSourceBusy) {
		t.Errorf("set source under a held lock: %v", err)
	}
	if _, err := w.m.DetachSource(ctx, d.ID); !errors.Is(err, errSourceBusy) {
		t.Errorf("detach under a held lock: %v", err)
	}
	delete(w.busy, sourceLockID(d.ID))

	plain := mustCreate(t, w, CreateOptions{Name: "plain", Columns: cols("k")})
	if _, err := w.m.SyncSource(ctx, plain.ID); !IsInvalidInput(err) {
		t.Errorf("list with no source: %v", err)
	}
	if _, err := w.m.SyncSource(ctx, "missing"); err == nil {
		t.Error("missing list synced")
	}
}

// The file decides the columns: a new column is added, a dropped one removed,
// both in place. A new key rebuilds the list, and a promoted column that is
// still in the file keeps its lookup key, or match(column=...) on it would stop
// resolving.
func TestSourceSchemaFollowsTheFile(t *testing.T) {
	w := sourcedWorld(t, true)
	f := newFeed(t, "host,owner,site\nweb1,ops,eu\n")
	d := createSourced(t, w, f, "host")
	w.mu.Lock()
	w.dicts[d.ID].cols = []byte(`[{"name":"host","type":"string"},{"name":"owner","type":"string","is_key":true},{"name":"site","type":"string"}]`)
	w.mu.Unlock()

	f.set(http.StatusOK, "host,owner,rack\nweb1,ops,r1\n")
	w.age(d.ID)
	d, err := w.m.SyncSource(ctx, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(colNames(d.Columns), []string{"host", "owner", "rack"}) {
		t.Errorf("columns %v", colNames(d.Columns))
	}
	if live := w.liveRows(d.ID); len(live) != 1 || live[0].vals["rack"] != "r1" {
		t.Errorf("rows %v", live)
	}

	// The key moves to owner; host stays as a plain column.
	w.mu.Lock()
	w.dicts[d.ID].cols = []byte(`[{"name":"host","type":"string"},{"name":"owner","type":"string"},{"name":"rack","type":"string","is_key":true}]`)
	w.mu.Unlock()
	w.age(d.ID)
	d, err = w.m.SetSource(ctx, d.ID, Source{URL: f.url, KeyField: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	if d.KeyColumn != "owner" || !slices.Equal(colNames(d.Columns), []string{"owner", "host", "rack"}) {
		t.Errorf("after a key change: key %q columns %v", d.KeyColumn, colNames(d.Columns))
	}
	if !d.Columns[2].IsKey || !w.hasDictObject(chColDictName(d.ID, "rack")) {
		t.Errorf("rack lost its lookup key: %+v", d.Columns)
	}
	if live := w.liveRows(d.ID); len(live) != 1 || live[0].vals["owner"] != "ops" {
		t.Errorf("rows after the rebuild %v", live)
	}
}

// Attaching a source to a hand-made list replaces its rows with the file's and
// makes it read-only; detaching leaves the rows and makes it editable again.
func TestAttachAndDetachSource(t *testing.T) {
	w := sourcedWorld(t, false)
	d := mustCreate(t, w, CreateOptions{Name: "hosts", Columns: cols("host", "note")})
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("host", "manual", "note", "x")}}); err != nil {
		t.Fatal(err)
	}
	f := newFeed(t, "host,owner\nweb1,ops\n")

	d, err := w.m.SetSource(ctx, d.ID, Source{URL: f.url, KeyField: "host", RefreshMinutes: 30})
	if err != nil {
		t.Fatal(err)
	}
	if d.Source == nil || d.Source.RefreshMinutes != 30 {
		t.Errorf("source %+v", d.Source)
	}
	if !slices.Equal(colNames(d.Columns), []string{"host", "owner"}) {
		t.Errorf("columns %v", colNames(d.Columns))
	}
	if got := keysOf(w.liveRows(d.ID), "host"); !slices.Equal(got, []string{"web1"}) {
		t.Errorf("rows %v", got)
	}
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("host", "x")}}); !IsInvalidInput(err) {
		t.Errorf("hand edit of an attached list: %v", err)
	}

	d, err = w.m.DetachSource(ctx, d.ID)
	if err != nil || d.Source != nil || d.SourceStatus != nil {
		t.Fatalf("detach: %+v %v", d, err)
	}
	if got := keysOf(w.liveRows(d.ID), "host"); !slices.Equal(got, []string{"web1"}) {
		t.Errorf("detach changed the rows: %v", got)
	}
	if err := w.m.UpsertRows(ctx, d.ID, []DictionaryRow{{Fields: fields("host", "x")}}); err != nil {
		t.Errorf("hand edit after detaching: %v", err)
	}

	if _, err := w.m.SetSource(ctx, d.ID, Source{URL: "ftp://x", KeyField: "host"}); !IsInvalidInput(err) {
		t.Errorf("invalid source: %v", err)
	}
}

// A source that fails to load is not kept: the list goes back to the source it
// had, with the hash cleared so the next refresh reloads in full, or to none.
func TestSetSourceFailureRestoresThePrevious(t *testing.T) {
	w := sourcedWorld(t, false)
	good := newFeed(t, "host\nweb1\n")
	d := createSourced(t, w, good, "host")
	bad := newFeed(t, "")
	bad.set(http.StatusNotFound, "")

	w.age(d.ID)
	if _, err := w.m.SetSource(ctx, d.ID, Source{URL: bad.url, KeyField: "host"}); !IsInvalidInput(err) {
		t.Fatalf("err = %v", err)
	}
	rec, _ := w.dict(d.ID)
	var src Source
	_ = json.Unmarshal(rec.source, &src)
	if src.URL != good.url {
		t.Errorf("source after a failed change: %+v", src)
	}
	if st := w.statusOf(d.ID); st.Hash != "" {
		t.Error("restored source kept its hash")
	}
	if got := keysOf(w.liveRows(d.ID), "host"); !slices.Equal(got, []string{"web1"}) {
		t.Errorf("rows %v", got)
	}

	plain := mustCreate(t, w, CreateOptions{Name: "plain", Columns: cols("host")})
	if _, err := w.m.SetSource(ctx, plain.ID, Source{URL: bad.url, KeyField: "host"}); err == nil {
		t.Fatal("a failing first source succeeded")
	}
	if rec, _ := w.dict(plain.ID); rec.source != nil || rec.status != nil {
		t.Errorf("a plain list kept a failed source: %s %s", rec.source, rec.status)
	}
}

// The scheduler refreshes only the lists that are due, and skips a row whose
// source it cannot read.
func TestRefreshDueSources(t *testing.T) {
	w := sourcedWorld(t, false)
	dueFeed := newFeed(t, "host\na\n")
	freshFeed := newFeed(t, "host\nb\n")
	due := createSourced(t, w, dueFeed, "host")
	createSourced(t, w, freshFeed, "host")
	w.seed(dictRec{name: "broken", fractalID: fractalA, source: []byte(`{not json`)})
	w.age(due.ID)
	dueFeed.set(http.StatusOK, "host\na\nc\n")

	w.m.refreshDueSources(ctx)
	if dueFeed.count() != 2 || freshFeed.count() != 1 {
		t.Errorf("fetches: due %d fresh %d", dueFeed.count(), freshFeed.count())
	}
	if got := keysOf(w.liveRows(due.ID), "host"); !slices.Equal(got, []string{"a", "c"}) {
		t.Errorf("due list rows %v", got)
	}
}
