package feeds

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestValidateRepoURL(t *testing.T) {
	tests := []struct {
		url     string
		wantErr string // substring; empty means accepted
	}{
		{"https://github.com/SigmaHQ/sigma.git", ""},
		{"http://git.internal:8080/rules", ""},
		{"HTTPS://github.com/x/y", ""},
		// Every other transport is refused: ssh and git would bypass the
		// http(s)-only auth model, file would read the server's own disk.
		{"ssh://git@github.com/x/y.git", "unsupported URL scheme"},
		{"git://github.com/x/y.git", "unsupported URL scheme"},
		{"file:///etc", "unsupported URL scheme"},
		{"ftp://example.com/repo", "unsupported URL scheme"},
		{"git@github.com:x/y.git", "invalid URL"},
		{"/srv/repos/rules", "unsupported URL scheme"},
		{"https:///no-host", "hostname"},
		{"https://:443/x", "hostname"},
		{"", "unsupported URL scheme"},
	}
	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			err := validateRepoURL(tt.url)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateRepoURL(%q) = %v, want accepted", tt.url, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateRepoURL(%q) = %v, want error containing %q", tt.url, err, tt.wantErr)
			}
		})
	}
}

// writeTree creates files (content keyed by slash path) under dir.
func writeTree(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		full := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func symlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
}

func sorted(s []string) []string {
	out := append([]string(nil), s...)
	slices.Sort(out)
	return out
}

func TestListYAMLFiles(t *testing.T) {
	repo := t.TempDir()
	writeTree(t, repo, map[string]string{
		"README.md":                          "docs",
		"top.yml":                            "a",
		"rules/windows/proc.yml":             "a",
		"rules/windows/deep/nested/x.yaml":   "a",
		"rules/linux/UPPER.YML":              "a",
		"rules/linux/notes.txt":              "a",
		"rules/linux/rule.yml.bak":           "a",
		".git/config.yml":                    "a",
		".github/workflows/ci.yml":           "a",
		"rules/.hidden/skip.yml":             "a",
		"rules/windows/.dotfile.yml":         "a",
		"deprecated/old.yml":                 "a",
		".rules-hidden-root/hidden-root.yml": "a",
	})

	tests := []struct {
		name    string
		subPath string
		want    []string
		wantErr string
	}{
		{
			// Only YAML, at any depth, case-insensitively; hidden directories
			// (.git, .github, nested ones) are never descended.
			name: "whole repo",
			want: []string{
				"deprecated/old.yml",
				"rules/linux/UPPER.YML",
				"rules/windows/.dotfile.yml",
				"rules/windows/deep/nested/x.yaml",
				"rules/windows/proc.yml",
				"top.yml",
			},
		},
		{
			// The path filter narrows the walk but keeps paths repo-relative,
			// since they key the feed's alerts.
			name:    "sub path",
			subPath: "rules/windows",
			want:    []string{"rules/windows/.dotfile.yml", "rules/windows/deep/nested/x.yaml", "rules/windows/proc.yml"},
		},
		{name: "sub path with trailing slash", subPath: "rules/linux/", want: []string{"rules/linux/UPPER.YML"}},
		{name: "dot sub path", subPath: ".", want: nil}, // checked separately below
		{
			// A feed explicitly pointed at a dot-directory gets its contents;
			// only hidden directories below the configured root are skipped.
			name:    "hidden root chosen explicitly",
			subPath: ".rules-hidden-root",
			want:    []string{".rules-hidden-root/hidden-root.yml"},
		},
		{name: "missing sub path", subPath: "nope", wantErr: "does not exist"},
		{name: "sub path is a file", subPath: "top.yml", wantErr: "does not exist"},
		{name: "dot-dot traversal", subPath: "../", wantErr: "escapes the repository"},
		{name: "deep traversal", subPath: "rules/../../..", wantErr: "escapes the repository"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ListYAMLFiles(repo, tt.subPath)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ListYAMLFiles(%q) = %v, %v; want error containing %q", tt.subPath, got, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ListYAMLFiles(%q): %v", tt.subPath, err)
			}
			if tt.subPath == "." {
				if len(got) != 6 {
					t.Errorf("ListYAMLFiles(\".\") = %v, want the same 6 files as the whole repo", got)
				}
				return
			}
			if !reflect.DeepEqual(sorted(got), tt.want) {
				t.Errorf("ListYAMLFiles(%q) =\n  %v\nwant\n  %v", tt.subPath, sorted(got), tt.want)
			}
		})
	}
}

// A third-party repo controls its symlinks. None of them may let a sync read
// outside the clone, either through the feed path or through a listed file.
func TestListYAMLFilesSymlinks(t *testing.T) {
	outside := t.TempDir()
	writeTree(t, outside, map[string]string{"secret.yml": "password: hunter2", "dir/also.yml": "x"})

	repo := t.TempDir()
	writeTree(t, repo, map[string]string{"sigma/rules/real.yml": "a"})
	symlink(t, outside, filepath.Join(repo, "escape"))
	symlink(t, filepath.Join(outside, "dir"), filepath.Join(repo, "sigma", "rules", "linked-dir"))
	symlink(t, filepath.Join(outside, "secret.yml"), filepath.Join(repo, "sigma", "rules", "leak.yml"))
	symlink(t, "sigma/rules", filepath.Join(repo, "rules"))

	t.Run("feed path through an escaping symlink is refused", func(t *testing.T) {
		if got, err := ListYAMLFiles(repo, "escape"); err == nil || !strings.Contains(err.Error(), "escapes") {
			t.Fatalf("ListYAMLFiles(escape) = %v, %v; want escape error", got, err)
		}
	})

	t.Run("symlinked directories inside the tree are not followed", func(t *testing.T) {
		got, err := ListYAMLFiles(repo, "sigma")
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range got {
			if strings.Contains(f, "linked-dir") {
				t.Errorf("walk followed a symlinked directory out of the repo: %v", got)
			}
		}
	})

	t.Run("feed path naming an in-repo symlinked dir lists its rules", func(t *testing.T) {
		// Walk does not descend a symlinked root, which used to list nothing
		// and let the delete pass wipe every alert in the feed.
		got, err := ListYAMLFiles(repo, "rules")
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(got, "rules/real.yml") {
			t.Errorf("ListYAMLFiles(rules) = %v, want rules/real.yml (reported under the configured path)", got)
		}
	})

	t.Run("a listed symlinked file escaping the repo cannot be read", func(t *testing.T) {
		got, err := ListYAMLFiles(repo, "sigma")
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(got, "sigma/rules/leak.yml") {
			t.Fatalf("expected the symlinked file to be listed (it is refused on read): %v", got)
		}
		data, err := ReadFile(repo, "sigma/rules/leak.yml")
		if err == nil || !strings.Contains(err.Error(), "escapes") {
			t.Fatalf("ReadFile(leak.yml) = %q, %v; want escape error", data, err)
		}
	})
}

// The clone lands in os.TempDir, which on macOS sits behind a symlink
// (/var -> /private/var). Containment checks must resolve both sides, or every
// file "escapes" and the feed never imports anything.
func TestRepoBehindSymlinkedTempDir(t *testing.T) {
	real := t.TempDir()
	writeTree(t, real, map[string]string{"rules/a.yml": "title: a"})
	link := filepath.Join(t.TempDir(), "tmp-link")
	symlink(t, real, link)

	got, err := ListYAMLFiles(link, "rules")
	if err != nil {
		t.Fatalf("ListYAMLFiles through symlinked repo dir: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"rules/a.yml"}) {
		t.Fatalf("ListYAMLFiles = %v", got)
	}
	data, err := ReadFile(link, "rules/a.yml")
	if err != nil || string(data) != "title: a" {
		t.Fatalf("ReadFile = %q, %v", data, err)
	}
}

func TestReadFile(t *testing.T) {
	outside := t.TempDir()
	writeTree(t, outside, map[string]string{"secret.yml": "s"})
	repo := t.TempDir()
	writeTree(t, repo, map[string]string{"rules/a.yml": "rule-a", "shared/base.yml": "base"})
	symlink(t, filepath.Join(repo, "shared", "base.yml"), filepath.Join(repo, "rules", "alias.yml"))

	tests := []struct {
		name    string
		path    string
		want    string
		wantErr string
	}{
		{name: "plain file", path: "rules/a.yml", want: "rule-a"},
		{name: "in-repo symlink", path: "rules/alias.yml", want: "base"},
		{name: "dot-dot traversal", path: "../" + filepath.Base(outside) + "/secret.yml", wantErr: "escapes"},
		{name: "absolute-looking path stays under repo", path: filepath.Join(outside, "secret.yml"), wantErr: "read file"},
		{name: "missing", path: "rules/missing.yml", wantErr: "read file"},
		{name: "repo root", path: ".", wantErr: "escapes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadFile(repo, tt.path)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ReadFile(%q) = %q, %v; want error containing %q", tt.path, got, err, tt.wantErr)
				}
				return
			}
			if err != nil || string(got) != tt.want {
				t.Fatalf("ReadFile(%q) = %q, %v; want %q", tt.path, got, err, tt.want)
			}
		})
	}
}

// isolateTempDir points os.TempDir at a fresh directory so a test can see what
// CloneRepo leaves behind.
func isolateTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	return dir
}

func dirEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestCloneRepo(t *testing.T) {
	src := newGitRepo(t, map[string]string{"rules/a.yml": "title: on-main"})
	src.branch("detections", map[string]string{"rules/a.yml": "title: on-branch", "rules/b.yml": "title: b"})

	t.Run("clones the requested branch", func(t *testing.T) {
		tmp := isolateTempDir(t)
		dir, err := CloneRepo(context.Background(), src.URL, "detections", "")
		if err != nil {
			t.Fatalf("CloneRepo: %v", err)
		}
		if !strings.HasPrefix(dir, tmp) {
			t.Errorf("clone dir %q is not under the temp dir %q", dir, tmp)
		}
		got, err := ReadFile(dir, "rules/a.yml")
		if err != nil || string(got) != "title: on-branch" {
			t.Errorf("rules/a.yml = %q, %v; want the branch's content", got, err)
		}
		files, _ := ListYAMLFiles(dir, "")
		if !reflect.DeepEqual(sorted(files), []string{"rules/a.yml", "rules/b.yml"}) {
			t.Errorf("files = %v", files)
		}

		CleanupRepo(dir)
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Errorf("CleanupRepo left %q behind: %v", dir, err)
		}
	})

	t.Run("a missing branch fails and leaves no temp dir", func(t *testing.T) {
		tmp := isolateTempDir(t)
		if dir, err := CloneRepo(context.Background(), src.URL, "no-such-branch", ""); err == nil {
			CleanupRepo(dir)
			t.Fatal("CloneRepo of a missing branch succeeded")
		}
		if left := dirEntries(t, tmp); len(left) != 0 {
			t.Errorf("failed clone left %v in the temp dir", left)
		}
	})

	t.Run("a refused URL never touches disk or network", func(t *testing.T) {
		tmp := isolateTempDir(t)
		_, err := CloneRepo(context.Background(), "file://"+src.dir, "main", "")
		if err == nil || !strings.Contains(err.Error(), "unsupported URL scheme") {
			t.Fatalf("CloneRepo(file://) = %v, want scheme refusal", err)
		}
		if left := dirEntries(t, tmp); len(left) != 0 {
			t.Errorf("refused clone created %v", left)
		}
	})

	t.Run("a cancelled context aborts the clone", func(t *testing.T) {
		isolateTempDir(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if dir, err := CloneRepo(ctx, src.URL, "main", ""); err == nil {
			CleanupRepo(dir)
			t.Fatal("CloneRepo with a cancelled context succeeded")
		}
	})
}

func TestCloneRepoAuth(t *testing.T) {
	const token = "ghp_SuperSecretToken123"
	src := newGitRepo(t, map[string]string{"a.yml": "x"})
	src.requireBasicAuth("x-token-auth", token)

	t.Run("token is sent as basic auth", func(t *testing.T) {
		isolateTempDir(t)
		dir, err := CloneRepo(context.Background(), src.URL, "main", token)
		if err != nil {
			t.Fatalf("CloneRepo with the right token: %v", err)
		}
		CleanupRepo(dir)
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte("x-token-auth:"+token))
		if !slices.Contains(src.authHeaders(), want) {
			t.Errorf("server never saw the token as basic auth; headers: %v", src.authHeaders())
		}
	})

	t.Run("no token sends no credentials", func(t *testing.T) {
		isolateTempDir(t)
		open := newGitRepo(t, map[string]string{"a.yml": "x"})
		dir, err := CloneRepo(context.Background(), open.URL, "main", "")
		if err != nil {
			t.Fatal(err)
		}
		CleanupRepo(dir)
		for _, h := range open.authHeaders() {
			if h != "" {
				t.Errorf("anonymous clone sent Authorization %q", h)
			}
		}
	})

	t.Run("a rejected token does not appear in the error", func(t *testing.T) {
		isolateTempDir(t)
		const wrong = "ghp_WrongButStillSecret456"
		_, err := CloneRepo(context.Background(), src.URL, "main", wrong)
		if err == nil {
			t.Fatal("clone with a wrong token succeeded")
		}
		// The error becomes the feed's last_sync_status, which every viewer of
		// the scope can read.
		if strings.Contains(err.Error(), wrong) {
			t.Errorf("clone error leaks the token: %v", err)
		}
	})
}

// Credentials pasted into the repo URL must not come back in an error: clone and
// validation errors are stored as the feed's sync status, readable by every
// viewer of the scope, and logged.
func TestRepoURLCredentialsNotEchoed(t *testing.T) {
	const secret = "s3cretPassw0rd"
	src := newGitRepo(t, map[string]string{"a.yml": "x"})
	src.requireBasicAuth("someone", "different")
	withCreds := strings.Replace(src.URL, "http://", "http://user:"+secret+"@", 1)

	cases := map[string]string{
		"unparseable URL": "http://user:" + secret + "@[::1/x.git",
		"rejected auth":   withCreds,
	}
	for name, u := range cases {
		t.Run(name, func(t *testing.T) {
			isolateTempDir(t)
			_, err := CloneRepo(context.Background(), u, "main", "")
			if err == nil {
				t.Fatal("clone succeeded")
			}
			if strings.Contains(err.Error(), secret) {
				t.Errorf("error leaks the URL password: %v", err)
			}
		})
	}
}
