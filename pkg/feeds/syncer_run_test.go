package feeds

import (
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
)

// statusWrites returns the (status, rule count, feed id) of each sync status
// update, in order. SetSyncStatus writes have a zero count and are marked.
type statusWrite struct {
	Status string
	Count  driver.Value
	FeedID string
	Final  bool // UpdateSyncStatus (true) vs SetSyncStatus (false)
}

func statusWrites(fake *fakedb.PG) []statusWrite {
	var out []statusWrite
	for _, c := range fake.Matching("UPDATE alert_feeds SET") {
		if strings.Contains(c.Query, "last_synced_at") {
			out = append(out, statusWrite{Status: str(c.Args[0]), Count: c.Args[1], FeedID: str(c.Args[2]), Final: true})
		} else {
			out = append(out, statusWrite{Status: str(c.Args[0]), FeedID: str(c.Args[1])})
		}
	}
	return out
}

func TestStartManualSync(t *testing.T) {
	isolateTempDir(t)
	repo := newGitRepo(t, map[string]string{
		"a.yml": sigmaRuleWith("A", "high", "stable"),
		"b.yml": nativeRule,
	})

	t.Run("publishes syncing, then the outcome", func(t *testing.T) {
		env := newSyncEnv(t)
		env.seed("b.yml", nativeRule)
		if !env.syncer.StartManualSync(testFeed(repo.URL)) {
			t.Fatal("StartManualSync refused an idle feed")
		}
		env.syncer.wg.Wait()

		w := statusWrites(env.pg)
		if len(w) != 2 || w[0] != (statusWrite{Status: "syncing", FeedID: "feed-1"}) {
			t.Fatalf("status writes = %+v", w)
		}
		// The rule count covers rules added, updated and left unchanged.
		if w[1] != (statusWrite{Status: "success", Count: int64(2), FeedID: "feed-1", Final: true}) {
			t.Errorf("final status = %+v", w[1])
		}
		if !env.syncer.acquire("feed-1") {
			t.Error("the feed stayed locked after the sync finished")
		}
	})

	t.Run("a failed sync records the error and releases the feed", func(t *testing.T) {
		env := newSyncEnv(t)
		feed := testFeed(repo.URL)
		feed.Branch = "does-not-exist"
		if !env.syncer.StartManualSync(feed) {
			t.Fatal("refused")
		}
		env.syncer.wg.Wait()
		w := statusWrites(env.pg)
		if len(w) != 2 || !w[1].Final || !strings.HasPrefix(w[1].Status, "error: clone:") || w[1].Count != int64(0) {
			t.Fatalf("status writes = %+v", w)
		}
		if !env.syncer.acquire("feed-1") {
			t.Error("a failed sync left the feed locked")
		}
	})

	t.Run("a second sync of a running feed is refused", func(t *testing.T) {
		env := newSyncEnv(t)
		env.syncer.acquire("feed-1") // a scheduled sync holds it
		if env.syncer.StartManualSync(testFeed(repo.URL)) {
			t.Fatal("two syncs of one feed ran at once")
		}
		if w := statusWrites(env.pg); len(w) != 0 {
			t.Errorf("a refused sync wrote status %+v", w)
		}
	})
}

func TestCheckAndSync(t *testing.T) {
	isolateTempDir(t)
	repo := newGitRepo(t, map[string]string{"a.yml": nativeRule})

	row := func(id string, lastSynced any) []driver.Value {
		r := feedRow(id, "f1", "", "")
		r[3] = repo.URL
		r[4] = "main"
		r[5] = ""
		r[8] = ScheduleHourly
		r[9] = ""
		r[14] = lastSynced
		return r
	}

	env := newSyncEnv(t)
	env.pg.On("FROM alert_feeds f", fakedb.RowsOf(
		row("due-never-synced", nil),
		row("due-stale", time.Now().Add(-2*time.Hour)),
		row("not-due", time.Now().Add(-10*time.Minute)),
		row("busy", nil),
	))
	env.syncer.acquire("busy")

	env.syncer.checkAndSync()

	got := map[string]statusWrite{}
	for _, w := range statusWrites(env.pg) {
		got[w.FeedID] = w
	}
	for _, id := range []string{"due-never-synced", "due-stale"} {
		if w, ok := got[id]; !ok || w.Status != "success" || !w.Final {
			t.Errorf("%s: status %+v, want a successful sync", id, w)
		}
	}
	if _, ok := got["not-due"]; ok {
		t.Error("a feed synced before its interval elapsed")
	}
	if _, ok := got["busy"]; ok {
		t.Error("a feed already syncing was synced again")
	}
	// Each scheduled sync releases its feed; the busy one stays with its holder.
	if !env.syncer.acquire("due-stale") || env.syncer.acquire("busy") {
		t.Error("scheduled sync locking is unbalanced")
	}

	// A broken repo is recorded per feed and does not stop the others.
	env2 := newSyncEnv(t)
	bad := row("bad", nil)
	bad[4] = "no-such-branch"
	env2.pg.On("FROM alert_feeds f", fakedb.RowsOf(bad, row("good", nil)))
	env2.syncer.checkAndSync()
	got = map[string]statusWrite{}
	for _, w := range statusWrites(env2.pg) {
		got[w.FeedID] = w
	}
	if !strings.HasPrefix(got["bad"].Status, "error:") || got["good"].Status != "success" {
		t.Errorf("statuses = %+v", got)
	}

	// Listing failure: nothing is synced, nothing is written.
	env3 := newSyncEnv(t)
	env3.pg.On("FROM alert_feeds f", fakedb.PGResult{Err: errString("db down")})
	env3.syncer.checkAndSync()
	if w := statusWrites(env3.pg); len(w) != 0 {
		t.Errorf("status writes after a listing failure: %+v", w)
	}
}

type errString string

func (e errString) Error() string { return string(e) }
