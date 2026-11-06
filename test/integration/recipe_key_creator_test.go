//go:build integration

// A scoped key acts for the person who issued it, so it stops the moment they lose
// admin on its scope, through a direct grant or a group, and resumes if it comes back.
// This runs the real SQL: the unit fakes only see its text. These recipes are serial
// because they create users, and TestContractPagination compares the user total.
//
//	go test -tags integration ./test/integration/ -run TestKeyCreator -v

package integration

import (
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"testing"
	"time"
)

const creatorPassword = "api-suite-creator-password-1"

// newSessionUser invites a non-tenant user and returns their name and a signed-in client.
func newSessionUser(t *testing.T, c *Client, name string) (string, *Client) {
	t.Helper()

	username := fmt.Sprintf("api-suite-%s-%d", name, time.Now().UnixNano())
	var invite struct {
		Token string `json:"invite_token"`
	}
	c.Do(t, "POST", "/auth/register", map[string]any{"username": username, "role": "user"}, &invite)
	t.Cleanup(func() { c.Status(t, "DELETE", "/users?username="+username, nil) })
	c.Do(t, "POST", "/auth/invite/accept", map[string]any{"token": invite.Token, "password": creatorPassword}, nil)

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	session := *c
	session.Key = ""
	session.http = &http.Client{Jar: jar, Timeout: 60 * time.Second}
	session.Do(t, "POST", "/auth/login", map[string]any{"username": username, "password": creatorPassword}, nil)
	return username, &session
}

// mintKey creates a key through session on the scope path and returns a client using it.
func mintKey(t *testing.T, c, session *Client, path, scope string) *Client {
	t.Helper()

	var key struct {
		Key string `json:"key"`
	}
	scoped := *session
	scoped.Scope = scope
	scoped.Do(t, "POST", path, map[string]any{"name": "creator-recipe", "role": "viewer"}, &key)
	next := c.WithKey(key.Key)
	next.Scope = ""
	return next
}

func expectStatus(t *testing.T, c *Client, path, when string, want int) {
	t.Helper()
	if got := c.Status(t, "GET", path, nil); got != want {
		t.Fatalf("%s: GET %s = %d, want %d", when, path, got, want)
	}
}

func TestKeyCreatorDirectGrant(t *testing.T) {
	c := New(t)
	id, _ := newFractal(t, c, "creator-direct")
	username, session := newSessionUser(t, c, "creator-direct")
	path := "/fractals/" + id

	var perm struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", path+"/permissions", map[string]any{"username": username, "role": "admin"}, &perm)
	key := mintKey(t, c, session, path+"/api-keys", "fractal:"+id)
	expectStatus(t, key, path, "creator is admin", http.StatusOK)

	c.Do(t, "PUT", path+"/permissions/"+perm.ID, map[string]any{"role": "analyst"}, nil)
	expectStatus(t, key, path, "creator demoted", http.StatusUnauthorized)

	c.Do(t, "PUT", path+"/permissions/"+perm.ID, map[string]any{"role": "admin"}, nil)
	expectStatus(t, key, path, "creator restored", http.StatusOK)

	c.Do(t, "DELETE", path+"/permissions/"+perm.ID, nil, nil)
	expectStatus(t, key, path, "creator revoked", http.StatusUnauthorized)
}

func TestKeyCreatorGroupGrant(t *testing.T) {
	c := New(t)
	id, _ := newFractal(t, c, "creator-group")
	username, session := newSessionUser(t, c, "creator-group")
	path := "/fractals/" + id

	var group struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/groups", map[string]any{"name": fmt.Sprintf("api-suite-%d", time.Now().UnixNano())}, &group)
	t.Cleanup(func() { c.Status(t, "DELETE", "/groups/"+group.ID, nil) })
	c.Do(t, "POST", "/groups/"+group.ID+"/members", map[string]any{"username": username}, nil)
	c.Do(t, "POST", path+"/permissions", map[string]any{"group_id": group.ID, "role": "admin"}, nil)

	key := mintKey(t, c, session, path+"/api-keys", "fractal:"+id)
	expectStatus(t, key, path, "creator holds admin through a group", http.StatusOK)

	c.Do(t, "DELETE", "/groups/"+group.ID+"/members/"+username, nil, nil)
	expectStatus(t, key, path, "creator left the group", http.StatusUnauthorized)
}

func TestKeyCreatorPrismGrant(t *testing.T) {
	c := New(t)
	username, session := newSessionUser(t, c, "creator-prism")

	var prism struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/prisms", map[string]any{"name": fmt.Sprintf("api-suite-%d", time.Now().UnixNano())}, &prism)
	t.Cleanup(func() { c.Status(t, "DELETE", "/prisms/"+prism.ID, nil) })
	path := "/prisms/" + prism.ID

	var perm struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", path+"/permissions", map[string]any{"username": username, "role": "admin"}, &perm)
	key := mintKey(t, c, session, path+"/api-keys", "prism:"+prism.ID)
	expectStatus(t, key, path, "creator is prism admin", http.StatusOK)

	c.Do(t, "PUT", path+"/permissions/"+perm.ID, map[string]any{"role": "viewer"}, nil)
	expectStatus(t, key, path, "creator demoted", http.StatusUnauthorized)
}

// Only a tenant admin grants admin, so only a tenant admin takes it away, including
// by granting a lower role over the top of it.
func TestScopeAdminCannotDemoteAdminGrants(t *testing.T) {
	c := New(t)
	id, _ := newFractal(t, c, "peer-admin")
	path := "/fractals/" + id
	admin, session := newSessionUser(t, c, "peer-a")
	peer, _ := newSessionUser(t, c, "peer-b")
	viewer, _ := newSessionUser(t, c, "peer-c")
	scoped := *session
	scoped.Scope = "fractal:" + id

	var mine, theirs struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", path+"/permissions", map[string]any{"username": admin, "role": "admin"}, &mine)
	c.Do(t, "POST", path+"/permissions", map[string]any{"username": peer, "role": "admin"}, &theirs)

	var group struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", "/groups", map[string]any{"name": fmt.Sprintf("api-suite-%d", time.Now().UnixNano())}, &group)
	t.Cleanup(func() { c.Status(t, "DELETE", "/groups/"+group.ID, nil) })
	c.Do(t, "POST", path+"/permissions", map[string]any{"group_id": group.ID, "role": "admin"}, nil)

	for name, call := range map[string]struct {
		method, path string
		body         map[string]any
	}{
		"update a peer":      {"PUT", path + "/permissions/" + theirs.ID, map[string]any{"role": "viewer"}},
		"revoke a peer":      {"DELETE", path + "/permissions/" + theirs.ID, nil},
		"grant over a peer":  {"POST", path + "/permissions", map[string]any{"username": peer, "role": "viewer"}},
		"grant over a group": {"POST", path + "/permissions", map[string]any{"group_id": group.ID, "role": "viewer"}},
		"update yourself":    {"PUT", path + "/permissions/" + mine.ID, map[string]any{"role": "viewer"}},
	} {
		if got := scoped.Status(t, call.method, call.path, call.body); got != http.StatusForbidden {
			t.Errorf("%s: status = %d, want 403", name, got)
		}
	}
	scoped.Do(t, "POST", path+"/permissions", map[string]any{"username": viewer, "role": "viewer"}, nil)
}

// A key stops with its creator, so only they can raise it. Anyone else on the scope
// can still shut it off.
func TestScopeAdminRaisesOnlyTheirOwnKeys(t *testing.T) {
	c := New(t)
	id, scopedTenant := newFractal(t, c, "own-keys")
	path := "/fractals/" + id
	admin, session := newSessionUser(t, c, "own-keys")
	c.Do(t, "POST", path+"/permissions", map[string]any{"username": admin, "role": "admin"}, nil)
	scoped := *session
	scoped.Scope = "fractal:" + id

	var theirs struct {
		APIKey struct {
			ID string `json:"id"`
		} `json:"api_key"`
	}
	scopedTenant.Do(t, "POST", path+"/api-keys", map[string]any{
		"name": "tenant-made", "role": "viewer", "expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	}, &theirs)
	key := path + "/api-keys/" + theirs.APIKey.ID

	if got := scoped.Status(t, "PUT", key, map[string]any{"role": "admin"}); got != http.StatusForbidden {
		t.Errorf("raising another's key: status = %d, want 403", got)
	}
	scoped.Do(t, "PUT", key, map[string]any{"is_active": false}, nil)
	if got := scoped.Status(t, "POST", key+"/toggle", nil); got != http.StatusForbidden {
		t.Errorf("reactivating another's key: status = %d, want 403", got)
	}
}

// System fractals hold audit and cross-fractal data, so a granted admin there can
// read it but only a tenant admin manages it.
func TestSystemFractalStaysTenantManaged(t *testing.T) {
	c := New(t)
	var listing struct {
		Fractals []struct {
			ID       string `json:"id"`
			IsSystem bool   `json:"is_system"`
		} `json:"fractals"`
	}
	c.Do(t, "GET", "/fractals", nil, &listing)
	id := ""
	for _, f := range listing.Fractals {
		if f.IsSystem {
			id = f.ID
		}
	}
	if id == "" {
		t.Fatal("no system fractal to test against")
	}
	path := "/fractals/" + id

	admin, session := newSessionUser(t, c, "system-admin")
	var perm struct {
		ID string `json:"id"`
	}
	c.Do(t, "POST", path+"/permissions", map[string]any{"username": admin, "role": "admin"}, &perm)
	t.Cleanup(func() { c.Status(t, "DELETE", path+"/permissions/"+perm.ID, nil) })
	scoped := *session
	scoped.Scope = "fractal:" + id

	expectStatus(t, &scoped, path, "read as a granted admin", http.StatusOK)
	for name, got := range map[string]int{
		"retention": scoped.Status(t, "PUT", path+"/retention", map[string]any{"retention_days": 7}),
		"clear":     scoped.Status(t, "DELETE", path+"/logs", nil),
		"tokens":    scoped.Status(t, "GET", path+"/ingest-tokens", nil),
		"keys":      scoped.Status(t, "GET", path+"/api-keys", nil),
		"grants":    scoped.Status(t, "GET", path+"/permissions", nil),
	} {
		if got != http.StatusForbidden {
			t.Errorf("%s: status = %d, want 403", name, got)
		}
	}
}
