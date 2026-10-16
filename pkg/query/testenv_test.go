package query

import (
	"bufio"
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/dictionaries"
	"bifract/pkg/fractals"
	"bifract/pkg/models"
	"bifract/pkg/prisms"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"
)

// testEnv is a QueryHandler wired to the scripted drivers in internal/fakedb, with
// every Postgres-backed manager it consults (fractals, prisms, dictionaries,
// models, comments) answering from the scope fixture below. ClickHouse answers come
// from the test, through ch.Respond or ch's rules.
type testEnv struct {
	h   *QueryHandler
	db  *storage.ClickHouseClient
	ch  *fakedb.CH
	pgc *storage.PostgresClient
	pg  *fakedb.PG

	mu sync.Mutex
	// defaultFractal is the id GetDefaultFractal answers; "" means no default.
	defaultFractal string
	// auditFractal is the id GetFractalByName("audit") answers; "" means none.
	auditFractal string
	members      map[string][]string // prism id -> member fractal ids
	// dictRows answer the dictionary mapping read: id, name, key column.
	dictRows [][]driver.Value
	// modelRows answer the analytics model reads: id, name, type, definition JSON, owner.
	modelRows [][]driver.Value
	// commentIDs answer the comment() pre-fetch.
	commentIDs []string
	commentErr error
}

var fixtureTime = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

func newTestEnv(t *testing.T, maxRows int) *testEnv {
	t.Helper()
	db, ch := storagetest.ClickHouse()
	return newTestEnvOn(t, maxRows, db, ch)
}

func newTestEnvOn(t *testing.T, maxRows int, db *storage.ClickHouseClient, ch *fakedb.CH) *testEnv {
	t.Helper()
	pgc, pg := storagetest.Postgres()
	e := &testEnv{db: db, ch: ch, pgc: pgc, pg: pg, defaultFractal: "fdef", members: map[string][]string{}}
	pg.Respond(e.answerPG)
	e.h = NewQueryHandlerFull(db, maxRows, fractals.NewManager(pgc, db), dictionaries.NewManager(pgc, db), prisms.NewManager(pgc))
	e.h.SetModelManager(models.NewManager(pgc, db))
	e.h.SetPostgresClient(pgc)
	return e
}

func fractalRow(id, name string, isDefault bool) []driver.Value {
	return []driver.Value{id, name, "", isDefault, false, "", fixtureTime, fixtureTime, nil, nil, nil, "reject", int64(0), int64(0), nil, nil}
}

func (e *testEnv) answerPG(q string, args []driver.Value) (fakedb.PGResult, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case strings.Contains(q, "FROM fractals") && strings.Contains(q, "is_default = true"):
		if e.defaultFractal == "" {
			return fakedb.PGResult{}, true
		}
		return fakedb.Row(fractalRow(e.defaultFractal, "default", true)...), true
	case strings.Contains(q, "FROM fractals") && strings.Contains(q, "WHERE name = $1"):
		if e.auditFractal == "" || len(args) == 0 || args[0] != "audit" {
			return fakedb.PGResult{}, true
		}
		return fakedb.Row(fractalRow(e.auditFractal, "audit", false)...), true
	case strings.Contains(q, "FROM prism_members"):
		var rows [][]driver.Value
		for _, id := range e.members[args[0].(string)] {
			rows = append(rows, []driver.Value{id})
		}
		return fakedb.RowsOf(rows...), true
	case strings.Contains(q, "FROM dictionaries"):
		var rows [][]driver.Value
		for _, r := range e.dictRows {
			// id, name, key_column, columns, case_insensitive_keys, kind
			rows = append(rows, []driver.Value{r[0], r[1], r[2], []byte("[]"), false, "value"})
		}
		return fakedb.RowsOf(rows...), true
	case strings.Contains(q, "FROM analytics_models"):
		withOwner := strings.Contains(q, "fractal_id = ANY")
		var rows [][]driver.Value
		for _, r := range e.modelRows {
			if withOwner {
				rows = append(rows, r)
			} else {
				rows = append(rows, r[:4])
			}
		}
		return fakedb.RowsOf(rows...), true
	case strings.Contains(q, "FROM comments"):
		if e.commentErr != nil {
			return fakedb.PGResult{Err: e.commentErr}, true
		}
		var rows [][]driver.Value
		for _, id := range e.commentIDs {
			rows = append(rows, []driver.Value{id})
		}
		return fakedb.RowsOf(rows...), true
	}
	return fakedb.PGResult{}, false
}

// set mutates the scope fixture under its lock.
func (e *testEnv) set(fn func(e *testEnv)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	fn(e)
}

// respond answers ClickHouse queries with row maps (see chResultOf). A nil answer
// with a nil error defers to ch's rules.
func (e *testEnv) respond(fn func(sql string) ([]map[string]interface{}, error)) {
	e.ch.Respond(func(sql string, _ []any) (fakedb.CHResult, bool) {
		rows, err := fn(sql)
		if err != nil {
			return fakedb.CHResult{Err: err}, true
		}
		if rows == nil {
			return fakedb.CHResult{}, false
		}
		return chResultOf(rows), true
	})
}

// logRow is a search result row as the translator projects it.
func logRow(ts, logID, fractal string) map[string]interface{} {
	return map[string]interface{}{
		"timestamp":         ts,
		"log_id":            logID,
		"fractal_id":        fractal,
		"norm_log":          `{"level":"error"}`,
		"_ingest_timestamp": ts,
	}
}

// logRows builds n rows, newest first, one second apart, ending at end.
func logRows(n int, end time.Time, fractal string) []map[string]interface{} {
	out := make([]map[string]interface{}, n)
	for i := range out {
		ts := end.Add(-time.Duration(i) * time.Second).UTC().Format("2006-01-02 15:04:05.000")
		out[i] = logRow(ts, "lid-"+ts, fractal)
	}
	return out
}

func isHistogramSQL(sql string) bool { return strings.Contains(sql, "AS bucket") }

// frame is one decoded NDJSON frame of the streaming search protocol.
type frame map[string]interface{}

func (f frame) kind() string { k, _ := f["type"].(string); return k }

func decodeFrames(t *testing.T, rec *httptest.ResponseRecorder) []frame {
	t.Helper()
	var out []frame
	sc := bufio.NewScanner(bytes.NewReader(rec.Body.Bytes()))
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var f frame
		if err := json.Unmarshal(sc.Bytes(), &f); err != nil {
			t.Fatalf("frame %q: %v", sc.Text(), err)
		}
		out = append(out, f)
	}
	return out
}

func frameKinds(frames []frame) []string {
	out := make([]string, len(frames))
	for i, f := range frames {
		out[i] = f.kind()
	}
	return out
}

func framesOf(frames []frame, kind string) []frame {
	var out []frame
	for _, f := range frames {
		if f.kind() == kind {
			out = append(out, f)
		}
	}
	return out
}

// waitFor polls cond until it holds or a generous deadline passes, for work the
// handlers detach into goroutines (audit writes, server-side kills).
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
