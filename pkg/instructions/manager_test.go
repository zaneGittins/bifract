package instructions

import (
	"context"
	"database/sql/driver"
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

// libraryRow is one instruction_libraries row as scanLibrary reads it.
func libraryRow(id, repoURL, token string) []driver.Value {
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return []driver.Value{
		id, "Runbooks", "", false, "f1", "",
		SourceRepo, repoURL, "main", "", token,
		ScheduleNever, nil, "", int64(0),
		"alice", created, created, int64(0),
	}
}

const libraryByID = "WHERE l.id = $1"

func TestCreateLibraryValidatesRepoURL(t *testing.T) {
	cases := map[string]string{
		"credentials": "https://u:ghp_x@github.com/x/y",
		"ssh":         "git@github.com:x/y.git",
		"file":        "file:///var/lib/bifract",
	}
	for name, u := range cases {
		t.Run(name, func(t *testing.T) {
			m, fake := newTestManager(t)
			req := CreateLibraryRequest{Name: "n", Source: SourceRepo, RepoURL: u}
			if _, err := m.CreateLibrary(context.Background(), req, "f1", "", "alice"); err == nil {
				t.Fatalf("CreateLibrary accepted %q", u)
			}
			if len(fake.Matching("INSERT INTO instruction_libraries")) != 0 {
				t.Error("a refused URL was stored")
			}
		})
	}
}

func TestCreateLibraryTrimsRepoURL(t *testing.T) {
	m, fake := newTestManager(t)
	fake.On("INSERT INTO instruction_libraries", fakedb.Row("lib-1"))
	fake.On(libraryByID, fakedb.Row(libraryRow("lib-1", "https://h/x", "")...))
	req := CreateLibraryRequest{Name: "n", Source: SourceRepo, RepoURL: " https://h/x "}
	if _, err := m.CreateLibrary(context.Background(), req, "f1", "", "alice"); err != nil {
		t.Fatal(err)
	}
	if got := fake.Matching("INSERT INTO instruction_libraries")[0].Args[6]; got != "https://h/x" {
		t.Errorf("repo_url stored as %q", got)
	}
}

func TestUpdateLibraryRepoURLAndToken(t *testing.T) {
	tests := []struct {
		name      string
		req       UpdateLibraryRequest
		wantURL   string
		wantToken string // the auth_token assignment; empty means left untouched
		wantErr   string
	}{
		{name: "same origin keeps the token", req: UpdateLibraryRequest{RepoURL: "https://H/other"}, wantURL: "https://H/other"},
		// A rename sends no URL; it must not blank the stored one.
		{name: "no URL keeps the stored one", req: UpdateLibraryRequest{Name: "renamed"}, wantURL: "https://h/x"},
		// The stored token was given for the old host.
		{name: "new host clears the token", req: UpdateLibraryRequest{RepoURL: "https://evil.example/x"}, wantURL: "https://evil.example/x", wantToken: "''"},
		{name: "new port clears the token", req: UpdateLibraryRequest{RepoURL: "https://h:8443/x"}, wantURL: "https://h:8443/x", wantToken: "''"},
		{name: "new host with a new token stores it", req: UpdateLibraryRequest{RepoURL: "https://evil.example/x", AuthToken: "t2"}, wantURL: "https://evil.example/x", wantToken: "t2"},
		{name: "credentials refused", req: UpdateLibraryRequest{RepoURL: "https://u:p@h/x"}, wantErr: "username or password"},
		{name: "other schemes refused", req: UpdateLibraryRequest{RepoURL: "ssh://h/x"}, wantErr: "only http and https"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, fake := newTestManager(t)
			fake.On(libraryByID, fakedb.Row(libraryRow("lib-1", "https://h/x", "stored-token")...))
			fake.On("UPDATE instruction_libraries", fakedb.Affected(1))

			_, err := m.UpdateLibrary(context.Background(), "lib-1", tt.req)
			updates := fake.Matching("UPDATE instruction_libraries")
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("err = %v, want %q", err, tt.wantErr)
				}
				if len(updates) != 0 {
					t.Error("a refused update was written")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			c := updates[0]
			if c.Args[4] != tt.wantURL {
				t.Errorf("repo_url = %v, want %q", c.Args[4], tt.wantURL)
			}
			setsToken := strings.Contains(c.Query, "auth_token")
			switch {
			case tt.wantToken == "" && setsToken:
				t.Errorf("token touched:\n%s", c.Query)
			case tt.wantToken == "''" && (!setsToken || c.Args[9] != ""):
				t.Errorf("token not cleared: %v", c.Args)
			case tt.wantToken != "" && tt.wantToken != "''" && (!setsToken || c.Args[9] != tt.wantToken):
				t.Errorf("token = %v, want %q", c.Args, tt.wantToken)
			}
		})
	}
}

// A URL saved before credentials were refused must not be shown with them.
func TestGetLibraryStripsLegacyURLCredentials(t *testing.T) {
	m, fake := newTestManager(t)
	fake.On(libraryByID, fakedb.Row(libraryRow("lib-1", "https://user:ghp_secret@github.com/x/y", "")...))
	lib, err := m.GetLibrary(context.Background(), "lib-1")
	if err != nil {
		t.Fatal(err)
	}
	if lib.RepoURL != "https://github.com/x/y" {
		t.Errorf("RepoURL = %q", lib.RepoURL)
	}
}
