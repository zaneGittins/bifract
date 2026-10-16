package feeds

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	git "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/backend"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage"
)

// gitRepo is a real repository in a temp dir, served over smart HTTP by go-git's
// own backend on a loopback httptest server. CloneRepo only accepts http(s), so
// this is the narrowest way to exercise the real clone path without a network.
type gitRepo struct {
	t    *testing.T
	dir  string
	repo *git.Repository
	srv  *httptest.Server

	// URL clones the repository.
	URL string

	mu   sync.Mutex
	auth []string // Authorization headers the server received
	// requireAuth, when set, rejects requests without exactly this basic auth.
	requireAuth *[2]string
}

// repoLoader serves the one repository regardless of the URL path.
type repoLoader struct{ st storage.Storer }

func (l repoLoader) Load(*url.URL) (storage.Storer, error) { return l.st, nil }

// newGitRepo initializes a repository on branch main with the given files committed.
func newGitRepo(t *testing.T, files map[string]string) *gitRepo {
	t.Helper()
	dir := t.TempDir()
	repo, err := git.PlainInit(dir, false, git.WithDefaultBranch(plumbing.NewBranchReferenceName("main")))
	if err != nil {
		t.Fatalf("init repo: %v", err)
	}
	r := &gitRepo{t: t, dir: dir, repo: repo}
	r.write(files)
	r.commit("initial")

	be := backend.New(repoLoader{st: repo.Storer})
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.auth = append(r.auth, req.Header.Get("Authorization"))
		need := r.requireAuth
		r.mu.Unlock()
		if need != nil {
			u, p, ok := req.BasicAuth()
			if !ok || u != need[0] || p != need[1] {
				w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		be.ServeHTTP(w, req)
	}))
	t.Cleanup(r.srv.Close)
	r.URL = r.srv.URL + "/rules.git"
	return r
}

// write creates or overwrites files in the working tree. An empty content with a
// trailing "/" in the name creates a directory instead.
func (r *gitRepo) write(files map[string]string) {
	r.t.Helper()
	for name, content := range files {
		full := filepath.Join(r.dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			r.t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			r.t.Fatal(err)
		}
	}
}

func (r *gitRepo) remove(names ...string) {
	r.t.Helper()
	for _, name := range names {
		if err := os.Remove(filepath.Join(r.dir, filepath.FromSlash(name))); err != nil {
			r.t.Fatal(err)
		}
	}
}

// commit stages every change in the working tree, deletions included, and commits it.
func (r *gitRepo) commit(msg string) {
	r.t.Helper()
	wt, err := r.repo.Worktree()
	if err != nil {
		r.t.Fatal(err)
	}
	if err := wt.AddWithOptions(&git.AddOptions{All: true}); err != nil {
		r.t.Fatalf("stage: %v", err)
	}
	sig := &object.Signature{Name: "tester", Email: "tester@example.com", When: time.Now()}
	if _, err := wt.Commit(msg, &git.CommitOptions{Author: sig, AllowEmptyCommits: true}); err != nil {
		r.t.Fatalf("commit: %v", err)
	}
}

// branch creates a branch at HEAD and commits files onto it, then returns to main.
func (r *gitRepo) branch(name string, files map[string]string) {
	r.t.Helper()
	wt, err := r.repo.Worktree()
	if err != nil {
		r.t.Fatal(err)
	}
	ref := plumbing.NewBranchReferenceName(name)
	if err := wt.Checkout(&git.CheckoutOptions{Branch: ref, Create: true}); err != nil {
		r.t.Fatalf("checkout %s: %v", name, err)
	}
	r.write(files)
	r.commit("on " + name)
	if err := wt.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName("main")}); err != nil {
		r.t.Fatalf("checkout main: %v", err)
	}
}

func (r *gitRepo) authHeaders() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.auth...)
}

func (r *gitRepo) requireBasicAuth(user, pass string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requireAuth = &[2]string{user, pass}
}
