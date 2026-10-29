package feeds

import (
	"crypto/sha256"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/lib/pq"

	"bifract/internal/fakedb"
	"bifract/pkg/alerts"
	"bifract/pkg/normalizers"
	"bifract/pkg/settings"
	"bifract/pkg/sigma"
	"bifract/pkg/storage/storagetest"
)

// syncEnv is a Syncer over a scripted Postgres that behaves like the alerts and
// feeds tables for the statements a sync issues. existing seeds the feed's
// current alerts (rule path -> stored hash).
type syncEnv struct {
	t      *testing.T
	pg     *fakedb.PG
	mgr    *Manager
	alerts *alerts.Manager
	syncer *Syncer

	mu        sync.Mutex
	token     string            // stored auth_token column
	existing  map[string]string // path -> feed_rule_hash
	ids       map[string]string // path -> alert id
	deleted   int64             // RowsAffected for the delete pass
	failPaths map[string]error  // GetFeedAlertByPath errors per path
	onLookup  func(path string) // called on every existing-alert lookup
	insertErr map[string]error  // CreateFeedAlert INSERT errors per path
	nextID    int
}

func newSyncEnv(t *testing.T) *syncEnv {
	t.Helper()
	pg, fake := storagetest.Postgres()
	t.Cleanup(func() { pg.Close() })
	e := &syncEnv{
		t:         t,
		pg:        fake,
		existing:  map[string]string{},
		ids:       map[string]string{},
		failPaths: map[string]error{},
		insertErr: map[string]error{},
	}
	e.mgr = &Manager{pg: pg}
	e.alerts = alerts.NewManager(pg, alerts.NewEngineWithDicts(pg, nil, nil, "http://localhost"), nil)
	e.syncer = NewSyncer(e.mgr, e.alerts, nil)
	fake.Respond(e.respond)
	return e
}

// withNormalizers wires a normalizer manager over the same database.
func (e *syncEnv) withNormalizers() {
	e.syncer.normalizerManager = normalizers.NewManager(e.mgr.pg)
}

func (e *syncEnv) respond(q string, args []driver.Value) (fakedb.PGResult, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	// Repositories are served from loopback, so the instance allows private egress.
	case strings.Contains(q, "SELECT value FROM settings WHERE key") && args[0] == settings.OutboundAllowPrivateKey:
		return fakedb.Row("true"), true
	case strings.Contains(q, "SELECT COALESCE(auth_token, '') FROM alert_feeds"):
		return fakedb.Row(e.token), true
	case strings.Contains(q, "SELECT id, COALESCE(feed_rule_hash, '') FROM alerts WHERE feed_id = $1 AND feed_rule_path = $2"):
		path := args[1].(string)
		if e.onLookup != nil {
			e.mu.Unlock()
			e.onLookup(path)
			e.mu.Lock()
		}
		if err := e.failPaths[path]; err != nil {
			return fakedb.PGResult{Err: err}, true
		}
		if hash, ok := e.existing[path]; ok {
			return fakedb.Row(e.ids[path], hash), true
		}
		return fakedb.PGResult{}, true // sql.ErrNoRows
	case strings.Contains(q, "INSERT INTO alerts ("):
		if err := e.insertErr[args[11].(string)]; err != nil {
			return fakedb.PGResult{Err: err}, true
		}
		e.nextID++
		return fakedb.Row(fmt.Sprintf("new-%d", e.nextID)), true
	case strings.Contains(q, "FROM alerts WHERE id = $1"):
		// loadRevisionContentTx: the live definition of an alert being updated.
		return fakedb.Row("old name", "", "old query", "event", "medium", int64(0), int64(0), "",
			[]byte("{}"), []byte("{}"), nil, nil, nil), true
	case strings.Contains(q, "SET name = $2, description = $3"):
		return fakedb.Affected(1), true
	case strings.Contains(q, "DELETE FROM alerts WHERE feed_id"):
		return fakedb.Affected(e.deleted), true
	}
	return fakedb.PGResult{}, false
}

// seed records an alert the feed already owns for path, with the hash a sync of
// content would compute, so the rule counts as unchanged.
func (e *syncEnv) seed(path, content string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.existing[path] = ruleHash(content)
	e.ids[path] = "alert-" + path
}

// seedStale records an alert for path whose stored hash no longer matches.
func (e *syncEnv) seedStale(path string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.existing[path] = "stale-hash"
	e.ids[path] = "alert-" + path
}

func ruleHash(content string) string {
	h := sha256.New()
	h.Write([]byte(content))
	h.Write([]byte(sigma.TranslatorVersion))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// createdAlert is one INSERT INTO alerts a sync issued.
type createdAlert struct {
	Name, Description, Query, Type, Severity string
	Labels, References                       []string
	CreatedBy, FractalID, PrismID            driver.Value
	FeedID, Path, Hash                       string
}

func (e *syncEnv) created() map[string]createdAlert {
	out := map[string]createdAlert{}
	for _, c := range e.pg.Matching("INSERT INTO alerts (") {
		a := createdAlert{
			Name: str(c.Args[0]), Description: str(c.Args[1]), Query: str(c.Args[2]),
			Type: str(c.Args[3]), Severity: str(c.Args[4]),
			Labels: pgArray(e.t, c.Args[5]), References: pgArray(e.t, c.Args[6]),
			CreatedBy: c.Args[7], FractalID: c.Args[8], PrismID: c.Args[9],
			FeedID: str(c.Args[10]), Path: str(c.Args[11]), Hash: str(c.Args[12]),
		}
		out[a.Path] = a
	}
	return out
}

// updatedAlert is one feed alert UPDATE a sync issued.
type updatedAlert struct {
	ID, Name, Query, Hash         string
	UpdatedBy, FractalID, PrismID driver.Value
	SQL                           string
}

func (e *syncEnv) updated() map[string]updatedAlert {
	out := map[string]updatedAlert{}
	for _, c := range e.pg.Matching("SET name = $2, description = $3") {
		u := updatedAlert{
			ID: str(c.Args[0]), Name: str(c.Args[1]), Query: str(c.Args[3]), Hash: str(c.Args[8]),
			UpdatedBy: c.Args[9], FractalID: c.Args[10], PrismID: c.Args[11], SQL: c.Query,
		}
		out[u.ID] = u
	}
	return out
}

// keepPaths returns the keep-list of the delete pass, and whether it ran.
func (e *syncEnv) keepPaths() ([]string, bool) {
	calls := e.pg.Matching("DELETE FROM alerts WHERE feed_id")
	if len(calls) == 0 {
		return nil, false
	}
	if len(calls) > 1 {
		e.t.Fatalf("delete pass ran %d times", len(calls))
	}
	c := calls[0]
	if len(c.Args) < 2 {
		return []string{}, true // the "delete every alert" form
	}
	return pgArray(e.t, c.Args[1]), true
}

// catalog returns the catalog rows the sync upserted, keyed by path.
func (e *syncEnv) catalog() map[string]CatalogEntry {
	out := map[string]CatalogEntry{}
	for _, c := range e.pg.Matching("INSERT INTO feed_rule_catalog") {
		for i := 1; i+9 <= len(c.Args); i += 9 {
			a := c.Args[i : i+9]
			out[str(a[0])] = CatalogEntry{
				Path: str(a[0]), Title: str(a[1]), Level: str(a[2]), Status: str(a[3]),
				Tags: pgArray(e.t, a[4]), RuleHash: str(a[5]), Imported: a[6].(bool),
				SkipReason: str(a[7]), SkipDetail: str(a[8]),
			}
		}
	}
	return out
}

// catalogPrunePaths returns the paths the catalog prune keeps, and whether it ran.
func (e *syncEnv) catalogPrunePaths() ([]string, bool) {
	calls := e.pg.Matching("DELETE FROM feed_rule_catalog")
	if len(calls) == 0 {
		return nil, false
	}
	return pgArray(e.t, calls[0].Args[1]), true
}

func str(v driver.Value) string {
	if v == nil {
		return ""
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprint(v)
}

func pgArray(t *testing.T, v driver.Value) []string {
	t.Helper()
	var a pq.StringArray
	if err := a.Scan(v); err != nil {
		t.Fatalf("decode array %v: %v", v, err)
	}
	return []string(a)
}
