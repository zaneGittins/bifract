package feeds

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage/storagetest"
)

func newTestManager(t *testing.T) (*Manager, *fakedb.PG) {
	t.Helper()
	pg, fake := storagetest.Postgres()
	t.Cleanup(func() { pg.Close() })
	return NewManager(pg), fake
}

// feedRow answers the shared feed SELECT. token is the stored auth_token column.
func feedRow(id, fractalID, prismID, token string) []driver.Value {
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return []driver.Value{
		id, "Feed " + id, "desc", "https://github.com/x/y", "main", "rules",
		token, nil, ScheduleDaily, "high", "", true, fractalID, prismID,
		nil, "success", int64(12), "alice", created, created, int64(3),
	}
}

func TestManagerValidation(t *testing.T) {
	m, fake := newTestManager(t)
	ctx := context.Background()
	valid := CreateRequest{Name: "Sigma", RepoURL: "https://github.com/SigmaHQ/sigma", SyncSchedule: ScheduleDaily}

	tests := []struct {
		name    string
		mutate  func(*CreateRequest)
		fractal string
		prism   string
		wantErr string
	}{
		{"blank name", func(r *CreateRequest) { r.Name = "  " }, "f1", "", "name is required"},
		{"blank URL", func(r *CreateRequest) { r.RepoURL = "" }, "f1", "", "URL is required"},
		{"ssh URL", func(r *CreateRequest) { r.RepoURL = "git@github.com:x/y.git" }, "f1", "", "invalid URL"},
		{"file URL", func(r *CreateRequest) { r.RepoURL = "file:///var/lib/bifract" }, "f1", "", "unsupported URL scheme"},
		{"unknown schedule", func(r *CreateRequest) { r.SyncSchedule = "every-minute" }, "f1", "", "invalid sync schedule"},
		{"empty schedule", func(r *CreateRequest) { r.SyncSchedule = "" }, "f1", "", "invalid sync schedule"},
		{"no scope", func(*CreateRequest) {}, "", "", "scoped to a fractal or prism"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := valid
			tt.mutate(&req)
			if _, err := m.Create(ctx, req, tt.fractal, tt.prism, "alice"); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Create: %v, want error containing %q", err, tt.wantErr)
			}
			if tt.fractal == "" && tt.prism == "" {
				return // Update has no scope argument
			}
			upd := UpdateRequest{Name: req.Name, RepoURL: req.RepoURL, SyncSchedule: req.SyncSchedule}
			if _, err := m.Update(ctx, "feed-1", upd); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Update: %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
	if calls := fake.Calls(); len(calls) != 0 {
		t.Errorf("invalid requests reached the database: %q", calls[0].Query)
	}
}

func TestManagerCreate(t *testing.T) {
	setEncryptionKey(t, hex.EncodeToString(testKey))
	m, fake := newTestManager(t)
	fake.On("INSERT INTO alert_feeds", fakedb.Row("feed-1"))
	fake.On("WHERE f.id = $1", fakedb.Row(feedRow("feed-1", "f1", "", "enc:stored")...))

	feed, err := m.Create(context.Background(), CreateRequest{
		Name: "Sigma", RepoURL: "https://github.com/SigmaHQ/sigma", SyncSchedule: ScheduleDaily,
		AuthToken: "ghp_plaintext", MinLevel: "high", Enabled: true,
	}, "f1", "", "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	ins := fake.Matching("INSERT INTO alert_feeds")[0].Args
	if ins[3] != "main" {
		t.Errorf("branch = %v, want main by default", ins[3])
	}
	stored := ins[5].(string)
	if !strings.HasPrefix(stored, "enc:") || strings.Contains(stored, "ghp_plaintext") {
		t.Errorf("token stored as %q, want ciphertext", stored)
	}
	if got, _ := DecryptToken(stored); got != "ghp_plaintext" {
		t.Errorf("stored token decrypts to %q", got)
	}
	if ins[6] != nil {
		t.Errorf("normalizer_id = %v, want NULL when unset", ins[6])
	}
	if ins[11] != "f1" || ins[12] != nil {
		t.Errorf("scope = fractal %v prism %v", ins[11], ins[12])
	}
	// No attributable user (e.g. a key without a creator) is NULL, never "".
	if ins[13] != nil {
		t.Errorf("created_by = %v, want NULL", ins[13])
	}

	// The returned feed never carries the token, only whether one is set.
	if feed.AuthToken != "" || !feed.HasAuthToken {
		t.Errorf("feed token fields = %q / %v", feed.AuthToken, feed.HasAuthToken)
	}
	if feed.ID != "feed-1" || feed.FractalID != "f1" || feed.AlertCount != 3 || feed.LastSyncedAt != nil || feed.CreatedBy != "alice" {
		t.Errorf("feed = %+v", feed)
	}
}

func TestManagerCreatePrismScopedAndDuplicate(t *testing.T) {
	m, fake := newTestManager(t)
	fake.OnOnce("INSERT INTO alert_feeds", fakedb.Row("feed-2"))
	fake.On("INSERT INTO alert_feeds", fakedb.PGResult{Err: errors.New(`pq: duplicate key value violates unique constraint "alert_feeds_name_key"`)})
	fake.On("WHERE f.id = $1", fakedb.Row(feedRow("feed-2", "", "p1", "")...))

	req := CreateRequest{Name: "S", RepoURL: "https://h/x", SyncSchedule: ScheduleNever, Branch: "dev", NormalizerID: "n1"}
	feed, err := m.Create(context.Background(), req, "", "p1", "bob")
	if err != nil {
		t.Fatal(err)
	}
	ins := fake.Matching("INSERT INTO alert_feeds")[0].Args
	if ins[3] != "dev" || ins[5] != "" || ins[6] != "n1" || ins[11] != nil || ins[12] != "p1" || ins[13] != "bob" {
		t.Errorf("insert args = %v", ins)
	}
	if feed.PrismID != "p1" || feed.HasAuthToken {
		t.Errorf("feed = %+v", feed)
	}

	_, err = m.Create(context.Background(), req, "", "p1", "bob")
	if err == nil || err.Error() != "a feed with this name already exists in this scope" {
		t.Errorf("duplicate create = %v", err)
	}
}

func TestManagerUpdateToken(t *testing.T) {
	setEncryptionKey(t, hex.EncodeToString(testKey))
	base := UpdateRequest{Name: "S", RepoURL: "https://h/x", SyncSchedule: ScheduleHourly}

	tests := []struct {
		name      string
		req       func(UpdateRequest) UpdateRequest
		wantToken string // substring of the SET clause for auth_token; empty = untouched
		idArg     int
	}{
		{"token untouched", func(r UpdateRequest) UpdateRequest { return r }, "", 10},
		{"token replaced", func(r UpdateRequest) UpdateRequest { r.AuthToken = "new-secret"; return r }, "auth_token = $11", 11},
		{"token cleared", func(r UpdateRequest) UpdateRequest { r.ClearToken = true; return r }, "auth_token = ''", 10},
		{"clear wins over a new token", func(r UpdateRequest) UpdateRequest { r.ClearToken = true; r.AuthToken = "x"; return r }, "auth_token = ''", 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, fake := newTestManager(t)
			fake.On("UPDATE alert_feeds", fakedb.Affected(1))
			fake.On("WHERE f.id = $1", fakedb.Row(feedRow("feed-1", "f1", "", "")...))

			if _, err := m.Update(context.Background(), "feed-1", tt.req(base)); err != nil {
				t.Fatal(err)
			}
			c := fake.Matching("UPDATE alert_feeds")[0]
			if tt.wantToken == "" && strings.Contains(c.Query, "auth_token") {
				t.Errorf("update without a token touched auth_token:\n%s", c.Query)
			}
			if tt.wantToken != "" && !strings.Contains(c.Query, tt.wantToken) {
				t.Errorf("query missing %q:\n%s", tt.wantToken, c.Query)
			}
			if c.Args[tt.idArg] != "feed-1" || len(c.Args) != tt.idArg+1 {
				t.Errorf("id bound at %d of %v", tt.idArg, c.Args)
			}
			if c.Args[3] != "main" {
				t.Errorf("branch = %v, want main default", c.Args[3])
			}
			if tt.idArg == 11 {
				stored := c.Args[10].(string)
				if got, _ := DecryptToken(stored); !strings.HasPrefix(stored, "enc:") || got != "new-secret" {
					t.Errorf("new token stored as %q", stored)
				}
			}
		})
	}
}

func TestManagerUpdateAndDeleteNotFound(t *testing.T) {
	m, fake := newTestManager(t)
	fake.On("UPDATE alert_feeds", fakedb.Affected(0))
	fake.On("DELETE FROM alert_feeds", fakedb.Affected(0))

	req := UpdateRequest{Name: "S", RepoURL: "https://h/x", SyncSchedule: ScheduleHourly}
	if _, err := m.Update(context.Background(), "missing", req); err == nil || err.Error() != "feed not found" {
		t.Errorf("Update(missing) = %v", err)
	}
	if err := m.Delete(context.Background(), "missing"); err == nil || err.Error() != "feed not found" {
		t.Errorf("Delete(missing) = %v", err)
	}

	fake.ClearRules()
	fake.On("UPDATE alert_feeds", fakedb.PGResult{Err: errors.New("unique violation")})
	fake.On("DELETE FROM alert_feeds", fakedb.Affected(1))
	if _, err := m.Update(context.Background(), "f", req); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("Update duplicate = %v", err)
	}
	if err := m.Delete(context.Background(), "f"); err != nil {
		t.Errorf("Delete = %v", err)
	}
}

func TestManagerListScopes(t *testing.T) {
	tests := []struct {
		name, fractal, prism, wantWhere, wantArg string
	}{
		{"fractal", "f1", "", "f.fractal_id = $1", "f1"},
		// A prism scope must list by prism even if a fractal is also known.
		{"prism wins", "f1", "p1", "f.prism_id = $1", "p1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, fake := newTestManager(t)
			fake.On("FROM alert_feeds f", fakedb.RowsOf(feedRow("a", "f1", "", "tok"), feedRow("b", "f1", "", "")))
			feeds, err := m.List(context.Background(), tt.fractal, tt.prism)
			if err != nil {
				t.Fatal(err)
			}
			c := fake.Matching("FROM alert_feeds f")[0]
			if !strings.Contains(c.Query, tt.wantWhere) || c.Args[0] != tt.wantArg {
				t.Errorf("query bound %v:\n%s", c.Args, c.Query)
			}
			if len(feeds) != 2 || !feeds[0].HasAuthToken || feeds[1].HasAuthToken || feeds[0].AuthToken != "" {
				t.Errorf("feeds = %+v / %+v", feeds[0], feeds[1])
			}
		})
	}
}

func TestManagerListAllEnabled(t *testing.T) {
	m, fake := newTestManager(t)
	synced := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	row := feedRow("a", "f1", "", "")
	row[7] = "norm-1"
	row[14] = synced
	fake.On("FROM alert_feeds f", fakedb.RowsOf(row))

	feeds, err := m.ListAllEnabled(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	q := fake.Matching("FROM alert_feeds f")[0].Query
	if !strings.Contains(q, "f.enabled = true") || !strings.Contains(q, "f.sync_schedule != 'never'") {
		t.Errorf("scheduler query does not exclude disabled/never feeds:\n%s", q)
	}
	if len(feeds) != 1 || feeds[0].NormalizerID != "norm-1" || feeds[0].LastSyncedAt == nil || !feeds[0].LastSyncedAt.Equal(synced) {
		t.Errorf("feeds = %+v", feeds)
	}

	fake.ClearRules()
	fake.On("FROM alert_feeds f", fakedb.PGResult{Err: errors.New("down")})
	if _, err := m.ListAllEnabled(context.Background()); err == nil {
		t.Error("ListAllEnabled swallowed a query error")
	}
	if _, err := m.List(context.Background(), "f1", ""); err == nil {
		t.Error("List swallowed a query error")
	}
}

func TestManagerGetDecryptedToken(t *testing.T) {
	setEncryptionKey(t, hex.EncodeToString(testKey))
	enc, _ := EncryptToken("the-token")
	m, fake := newTestManager(t)
	fake.On("SELECT COALESCE(auth_token, '')", fakedb.Row(enc))

	got, err := m.GetDecryptedToken(context.Background(), "feed-1")
	if err != nil || got != "the-token" {
		t.Errorf("GetDecryptedToken = %q, %v", got, err)
	}

	m2, _ := newTestManager(t) // no row
	if _, err := m2.GetDecryptedToken(context.Background(), "gone"); err == nil {
		t.Error("GetDecryptedToken of a missing feed succeeded")
	}
}

func TestManagerSyncStatus(t *testing.T) {
	m, fake := newTestManager(t)
	ctx := context.Background()
	if err := m.SetSyncStatus(ctx, "feed-1", "syncing"); err != nil {
		t.Fatal(err)
	}
	if err := m.UpdateSyncStatus(ctx, "feed-1", "success", 42); err != nil {
		t.Fatal(err)
	}
	calls := fake.Matching("UPDATE alert_feeds SET")
	if len(calls) != 2 {
		t.Fatalf("calls = %d", len(calls))
	}
	// In-progress state must not move last_synced_at: the scheduler reads it.
	if strings.Contains(calls[0].Query, "last_synced_at") || calls[0].Args[0] != "syncing" {
		t.Errorf("SetSyncStatus = %q %v", calls[0].Query, calls[0].Args)
	}
	if !strings.Contains(calls[1].Query, "last_synced_at = NOW()") || calls[1].Args[1] != int64(42) || calls[1].Args[2] != "feed-1" {
		t.Errorf("UpdateSyncStatus = %q %v", calls[1].Query, calls[1].Args)
	}
}

func TestUpsertCatalogBatches(t *testing.T) {
	m, fake := newTestManager(t)
	entries := make([]CatalogEntry, 1201)
	for i := range entries {
		entries[i] = CatalogEntry{Path: fmt.Sprintf("r%04d.yml", i), Tags: []string{"attack.t1"}}
	}
	entries[7].SkipDetail = strings.Repeat("x", 2000)

	if err := m.UpsertCatalog(context.Background(), "feed-1", entries); err != nil {
		t.Fatal(err)
	}
	calls := fake.Matching("INSERT INTO feed_rule_catalog")
	if len(calls) != 3 {
		t.Fatalf("statements = %d, want 3 batches of <=500", len(calls))
	}
	total := 0
	for _, c := range calls {
		if c.Args[0] != "feed-1" || (len(c.Args)-1)%9 != 0 {
			t.Fatalf("batch args malformed: %d args", len(c.Args))
		}
		n := (len(c.Args) - 1) / 9
		total += n
		// Every row's placeholders exist: the highest one matches the arg count.
		if !strings.Contains(c.Query, fmt.Sprintf("$%d, NOW())", len(c.Args))) {
			t.Errorf("last placeholder $%d missing from batch", len(c.Args))
		}
	}
	if total != 1201 {
		t.Errorf("rows written = %d", total)
	}
	if detail := calls[0].Args[1+7*9+8].(string); len(detail) != 500 {
		t.Errorf("skip detail stored at %d bytes, want truncated to 500", len(detail))
	}

	fake.ClearRules()
	fake.On("INSERT INTO feed_rule_catalog", fakedb.PGResult{Err: errors.New("boom")})
	if err := m.UpsertCatalog(context.Background(), "feed-1", entries[:1]); err == nil {
		t.Error("UpsertCatalog swallowed an error")
	}
	before := len(fake.Calls())
	if err := m.UpsertCatalog(context.Background(), "feed-1", nil); err != nil || len(fake.Calls()) != before {
		t.Error("an empty catalog issued a statement")
	}
}

func TestDeleteCatalogNotIn(t *testing.T) {
	m, fake := newTestManager(t)
	fake.On("DELETE FROM feed_rule_catalog", fakedb.Affected(4))
	n, err := m.DeleteCatalogNotIn(context.Background(), "feed-1", []string{"a.yml", "b.yml"})
	if err != nil || n != 4 {
		t.Fatalf("DeleteCatalogNotIn = %d, %v", n, err)
	}
	c := fake.Matching("DELETE FROM feed_rule_catalog")[0]
	if c.Args[0] != "feed-1" || c.Args[1] != `{"a.yml","b.yml"}` {
		t.Errorf("args = %v", c.Args)
	}

	fake.ClearRules()
	fake.On("DELETE FROM feed_rule_catalog", fakedb.PGResult{Err: errors.New("boom")})
	if _, err := m.DeleteCatalogNotIn(context.Background(), "feed-1", nil); err == nil {
		t.Error("error swallowed")
	}
}
