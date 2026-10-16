package query

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

func clusterClient(t *testing.T) (*storage.ClickHouseClient, *fakedb.CH) {
	t.Helper()
	topo, err := storage.DeriveTopology(storage.TopologySpec{Cluster: "bf"})
	if err != nil {
		t.Fatal(err)
	}
	return storagetest.ClickHouseWith(topo)
}

func TestHandleHealthCheck(t *testing.T) {
	decode := func(rec *httptest.ResponseRecorder) map[string]interface{} {
		var out map[string]interface{}
		json.Unmarshal(rec.Body.Bytes(), &out)
		return out
	}
	t.Run("single node never reports shards", func(t *testing.T) {
		db, ch := storagetest.ClickHouse()
		rec := httptest.NewRecorder()
		NewStatusHandler(db, nil).HandleHealthCheck(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		out := decode(rec)
		if out["connected"] != true || out["degraded"] != nil || out["shards_total"] != nil || len(ch.Queried()) != 0 {
			t.Errorf("got %v after %v", out, ch.Queried())
		}
	})
	t.Run("cluster with a shard down is degraded", func(t *testing.T) {
		db, ch := clusterClient(t)
		ch.On("FROM system.clusters", chResultOf([]map[string]interface{}{{"total": uint64(3), "healthy": uint64(2)}}))
		rec := httptest.NewRecorder()
		NewStatusHandler(db, nil).HandleHealthCheck(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		out := decode(rec)
		if out["degraded"] != true || out["shards_total"] != 3.0 || out["shards_healthy"] != 2.0 {
			t.Errorf("got %v", out)
		}
	})
	t.Run("cluster all healthy", func(t *testing.T) {
		db, ch := clusterClient(t)
		ch.On("FROM system.clusters", chResultOf([]map[string]interface{}{{"total": uint64(2), "healthy": uint64(2)}}))
		rec := httptest.NewRecorder()
		NewStatusHandler(db, nil).HandleHealthCheck(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if out := decode(rec); out["degraded"] != nil || out["shards_total"] != 2.0 {
			t.Errorf("got %v", out)
		}
	})
}

func TestHandleStatusFetchesAndCaches(t *testing.T) {
	db, ch := storagetest.ClickHouse()
	pgc, pg := storagetest.Postgres()
	ch.On("formatReadableSize(sum(bytes_on_disk))", chResultOf([]map[string]interface{}{{"size": "1.00 GiB", "bytes": uint64(1 << 30)}})).
		On("sum(rows) as count", chResultOf([]map[string]interface{}{{"count": uint64(1234)}})).
		On("min(min_time)", chResultOf([]map[string]interface{}{{"oldest": "2026-09-01 00:00:00", "newest": "2026-10-01 00:00:00"}})).
		On("uptime()", chResultOf([]map[string]interface{}{{"uptime": "3 days"}})).
		On("FROM system.disks", chResultOf([]map[string]interface{}{{"total": "100 GiB", "free": "40 GiB", "used_pct": 60.0}}))
	pg.On("FROM fractals", fakedb.Row(int64(4))).On("FROM users", fakedb.Row(int64(9)))

	h := NewStatusHandler(db, pgc)
	get := func() StatusResponse {
		rec := httptest.NewRecorder()
		h.HandleStatus(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		var resp StatusResponse
		json.Unmarshal(rec.Body.Bytes(), &resp)
		return resp
	}
	resp := get()
	c := resp.ClickHouse
	if !resp.Success || !c.Connected || c.TableSize != "1.00 GiB" || c.StorageBytes != 1<<30 || c.StorageMB != "1024.00 MB" ||
		c.TotalLogs != 1234 || c.OldestLog != "2026-09-01 00:00:00" || c.NewestLog != "2026-10-01 00:00:00" ||
		c.UptimeStr != "3 days" || c.DiskTotal != "100 GiB" || c.DiskFree != "40 GiB" || c.DiskUsedPct != 60 {
		t.Errorf("clickhouse = %+v", c)
	}
	if resp.System.FractalCount != 4 || resp.System.UserCount != 9 {
		t.Errorf("system = %+v", resp.System)
	}
	n := len(ch.Queried())
	get()
	if len(ch.Queried()) != n {
		t.Error("a fresh cached status was refetched")
	}
}

// An empty store has no bounds to read: the min/max query is skipped.
func TestFetchStatusEmptyStore(t *testing.T) {
	db, ch := storagetest.ClickHouse()
	st := NewStatusHandler(db, nil).fetchStatus()
	if !st.Success || st.ClickHouse.TotalLogs != 0 || st.ClickHouse.OldestLog != "" {
		t.Errorf("status = %+v", st)
	}
	if q := ch.QueriedMatching("min(min_time)"); len(q) != 0 {
		t.Errorf("read bounds of an empty store: %v", q)
	}
}

type recordingQuota struct {
	mu  sync.Mutex
	ids []string
}

func (q *recordingQuota) NotifyCleared(id string) {
	q.mu.Lock()
	q.ids = append(q.ids, id)
	q.mu.Unlock()
}

// clearEnv is a status handler over scripted drivers, with the fractal's logs
// stored under one partition per table so the drop has something to drop.
type clearEnv struct {
	h     *StatusHandler
	ch    *fakedb.CH
	pg    *fakedb.PG
	quota *recordingQuota

	mu       sync.Mutex
	dropArgs [][]string // fractal id sets each partition listing was asked about
}

func newClearEnv(t *testing.T) *clearEnv {
	db, ch := storagetest.ClickHouse()
	pgc, pg := storagetest.Postgres()
	e := &clearEnv{h: NewStatusHandler(db, pgc), ch: ch, pg: pg, quota: &recordingQuota{}}
	e.h.SetQuotaClearer(e.quota)
	ch.Respond(func(sql string, args []any) (fakedb.CHResult, bool) {
		if strings.Contains(sql, "SELECT DISTINCT partition_id FROM system.parts") {
			e.mu.Lock()
			e.dropArgs = append(e.dropArgs, args[1].([]string))
			e.mu.Unlock()
			return fakedb.Strs("partition_id", "p-20261001"), true
		}
		return fakedb.CHResult{}, false
	})
	return e
}

func (e *clearEnv) clear(query string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	e.h.HandleClearLogs(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/logs"+query, nil))
	return rec
}

func TestHandleClearFractalLogs(t *testing.T) {
	for _, isDefault := range []bool{false, true} {
		name := "fractal"
		if isDefault {
			name = "default fractal also drops its unscoped rows"
		}
		t.Run(name, func(t *testing.T) {
			e := newClearEnv(t)
			e.pg.On("SELECT is_default FROM fractals", fakedb.Row(isDefault))
			rec := e.clear("?fractal_id=f1")
			if rec.Code != http.StatusOK {
				t.Fatalf("got %d %s", rec.Code, rec.Body.String())
			}
			want := []string{"f1"}
			if isDefault {
				want = []string{"f1", ""}
			}
			if len(e.dropArgs) == 0 || !reflect.DeepEqual(e.dropArgs[0], want) {
				t.Errorf("partition listing for %v, want %v", e.dropArgs, want)
			}
			if drops := e.ch.ExecutedMatching("DROP PARTITION ID 'p-20261001'"); len(drops) == 0 {
				t.Errorf("nothing dropped: %v", e.ch.Executed())
			}
			if !reflect.DeepEqual(e.quota.ids, []string{"f1"}) {
				t.Errorf("quota notified for %v", e.quota.ids)
			}
			if c := e.pg.Matching("UPDATE fractals SET log_count = 0"); len(c) != 1 || c[0].Args[0] != "f1" {
				t.Errorf("stats reset = %+v", c)
			}
			if c := e.pg.Matching("DELETE FROM comments WHERE fractal_id"); len(c) != 1 || c[0].Args[0] != "f1" {
				t.Errorf("comment delete = %+v", c)
			}
		})
	}

	t.Run("unknown fractal", func(t *testing.T) {
		e := newClearEnv(t)
		if rec := e.clear("?fractal_id=nope"); rec.Code != http.StatusNotFound {
			t.Errorf("got %d", rec.Code)
		}
		if len(e.ch.Executed()) != 0 {
			t.Errorf("dropped data for an unknown fractal: %v", e.ch.Executed())
		}
	})

	// Comments go only once their logs are gone, so a failed drop keeps both.
	t.Run("failed drop keeps the comments", func(t *testing.T) {
		e := newClearEnv(t)
		e.pg.On("SELECT is_default FROM fractals", fakedb.Row(false))
		e.ch.FailExec("DROP PARTITION", errors.New("disk"))
		if rec := e.clear("?fractal_id=f1"); rec.Code != http.StatusInternalServerError {
			t.Errorf("got %d", rec.Code)
		}
		if len(e.pg.Matching("DELETE FROM comments")) != 0 || len(e.quota.ids) != 0 {
			t.Error("comments or quota cleared after the logs failed to drop")
		}
	})

	t.Run("comments fail to delete", func(t *testing.T) {
		e := newClearEnv(t)
		e.pg.On("SELECT is_default FROM fractals", fakedb.Row(false)).
			On("DELETE FROM comments", fakedb.PGResult{Err: errors.New("pg")})
		rec := e.clear("?fractal_id=f1")
		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "comments could not be deleted") {
			t.Errorf("got %d %s", rec.Code, rec.Body.String())
		}
	})
}

func TestHandleClearAllLogs(t *testing.T) {
	t.Run("clears every table, then the Postgres state, then quotas", func(t *testing.T) {
		e := newClearEnv(t)
		e.pg.On(storage.ModelIDsQuery, fakedb.Row("m1")).
			On("SELECT id FROM fractals", fakedb.RowsOf([]driver.Value{"f1"}, []driver.Value{"f2"}))
		rec := e.clear("")
		if rec.Code != http.StatusOK {
			t.Fatalf("got %d %s", rec.Code, rec.Body.String())
		}
		truncs := e.ch.ExecutedMatching("TRUNCATE TABLE IF EXISTS")
		if len(truncs) == 0 || !strings.Contains(strings.Join(truncs, "\n"), "m1") {
			t.Errorf("truncates = %v; the model's tables must be cleared too", truncs)
		}
		if c := e.pg.Matching("DELETE FROM comments"); len(c) != 1 || !c[0].InTx {
			t.Errorf("comment clear = %+v, want one inside the state transaction", c)
		}
		if _, _, commits, _ := e.pg.Stats(); commits != 1 {
			t.Errorf("%d commits", commits)
		}
		if !reflect.DeepEqual(e.quota.ids, []string{"f1", "f2"}) {
			t.Errorf("quota notified for %v", e.quota.ids)
		}
	})

	t.Run("model listing fails", func(t *testing.T) {
		e := newClearEnv(t)
		e.pg.On(storage.ModelIDsQuery, fakedb.PGResult{Err: errors.New("pg")})
		if rec := e.clear(""); rec.Code != http.StatusInternalServerError || len(e.ch.Executed()) != 0 {
			t.Errorf("got %d after %v", rec.Code, e.ch.Executed())
		}
	})

	t.Run("truncate fails", func(t *testing.T) {
		e := newClearEnv(t)
		e.ch.FailExec("TRUNCATE", errors.New("disk"))
		if rec := e.clear(""); rec.Code != http.StatusInternalServerError {
			t.Errorf("got %d", rec.Code)
		}
		if len(e.pg.Matching("DELETE FROM comments")) != 0 || len(e.quota.ids) != 0 {
			t.Error("Postgres state reset although the logs are still there")
		}
	})

	t.Run("state reset fails", func(t *testing.T) {
		e := newClearEnv(t)
		e.pg.FailCommit = errors.New("pg")
		rec := e.clear("")
		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), "could not be reset") {
			t.Errorf("got %d %s", rec.Code, rec.Body.String())
		}
	})
}
