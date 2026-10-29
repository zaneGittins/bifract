package feeds

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	git "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/client"
	"github.com/go-git/go-git/v6/plumbing/transport"
	githttp "github.com/go-git/go-git/v6/plumbing/transport/http"

	"bifract/pkg/egress"
)

// CloneRepo performs a shallow clone (depth=1) of the given repo into a temp directory.
// Uses go-git (pure Go, no external git binary required). Every connection goes
// through pkg/egress; allowPrivate admits addresses inside the network.
// Returns the path to the cloned directory. Caller must call CleanupRepo when done.
func CloneRepo(ctx context.Context, repoURL, branch, authToken string, allowPrivate bool) (string, error) {
	if err := egress.ValidateURL(repoURL); err != nil {
		return "", fmt.Errorf("repo URL validation failed: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "bifract-feed-*")
	if err != nil {
		return "", fmt.Errorf("create temp dir: %w", err)
	}

	opts := &git.CloneOptions{
		URL:           repoURL,
		ReferenceName: plumbing.NewBranchReferenceName(branch),
		SingleBranch:  true,
		Depth:         1,
		Tags:          plumbing.NoTags,
		ClientOptions: []client.Option{
			client.WithHTTPClient(egress.NewClient(egress.Options{AllowPrivate: allowPrivate})),
		},
	}

	if authToken != "" {
		opts.ClientOptions = append(opts.ClientOptions, client.WithHTTPAuth(&githttp.BasicAuth{
			Username: "x-token-auth",
			Password: authToken,
		}))
	}

	_, err = git.PlainCloneContext(ctx, tmpDir, opts)
	if err != nil {
		os.RemoveAll(tmpDir)
		log.Printf("[Feeds] clone of %s failed: %v", repoURL, err)
		return "", fmt.Errorf("clone repo: %s", describeCloneError(err))
	}

	return tmpDir, nil
}

// describeCloneError reduces a clone failure to a category. The result is
// stored as a sync status that every viewer of the scope can read, so it never
// carries text from the far end or the address a name resolved to.
func describeCloneError(err error) string {
	var status interface{ StatusCode() int }
	switch {
	case errors.Is(err, context.Canceled):
		return "the sync was cancelled"
	case errors.Is(err, transport.ErrAuthenticationRequired), errors.Is(err, transport.ErrAuthorizationFailed):
		return "the repository refused authentication"
	case errors.Is(err, transport.ErrRepositoryNotFound):
		return "the repository was not found"
	// A single-branch clone of a missing branch reports an empty repository.
	case errors.Is(err, transport.ErrEmptyRemoteRepository), errors.Is(err, git.ErrBranchNotFound), errors.Is(err, git.ErrRemoteRefNotFound):
		return "the branch was not found or the repository is empty"
	case errors.As(err, &status) && status.StatusCode() > 0:
		return fmt.Sprintf("the server answered HTTP %d", status.StatusCode())
	}
	return egress.Describe(err)
}

// ListYAMLFiles recursively finds all .yml and .yaml files under the given
// subPath within repoDir. Returns paths relative to repoDir.
//
// The returned paths key each feed alert, and a path missing from them deletes
// that alert, so a listing must never come back silently short.
func ListYAMLFiles(repoDir, subPath string) ([]string, error) {
	searchDir := repoDir
	if subPath != "" {
		searchDir = filepath.Join(repoDir, subPath)
	}

	// Prevent path traversal via symlinks or ".." components. Both sides are
	// resolved: comparing a resolved path against an unresolved root rejects
	// every path when the temp dir itself sits behind a symlink (macOS /var).
	repoReal, err := resolvePath(repoDir)
	if err != nil {
		return nil, fmt.Errorf("resolve repo dir: %w", err)
	}
	searchReal, err := resolvePath(searchDir)
	if err != nil {
		return nil, fmt.Errorf("path %q does not exist in the repository", subPath)
	}
	if !strings.HasPrefix(searchReal, repoReal+string(filepath.Separator)) && searchReal != repoReal {
		return nil, fmt.Errorf("path %q escapes the repository", subPath)
	}

	if info, err := os.Stat(searchReal); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("path %q does not exist in the repository", subPath)
	}

	// Walk the resolved directory: Walk does not follow a symlinked root, so a
	// feed path naming a symlinked directory would otherwise list nothing. Paths
	// are reported under searchDir as configured, keeping alert keys stable.
	var files []string
	err = filepath.Walk(searchReal, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			// Hidden directories (.git, .github) are skipped, but never the
			// configured root itself: a feed path of ".rules" is a choice.
			if path != searchReal && strings.HasPrefix(info.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}

		ext := strings.ToLower(filepath.Ext(info.Name()))
		if ext == ".yml" || ext == ".yaml" {
			underSearch, relErr := filepath.Rel(searchReal, path)
			if relErr != nil {
				return nil
			}
			rel, relErr := filepath.Rel(repoDir, filepath.Join(searchDir, underSearch))
			if relErr != nil {
				return nil
			}
			files = append(files, rel)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk directory: %w", err)
	}

	return files, nil
}

// resolvePath returns the absolute path with every symlink resolved.
func resolvePath(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

// ReadFile reads the content of a file within the cloned repo.
func ReadFile(repoDir, filePath string) ([]byte, error) {
	fullPath := filepath.Join(repoDir, filePath)
	// Resolve symlinks and verify the path stays within the repo boundary.
	repoReal, err := resolvePath(repoDir)
	if err != nil {
		return nil, fmt.Errorf("resolve repo dir: %w", err)
	}
	realPath, err := resolvePath(fullPath)
	if err != nil {
		return nil, fmt.Errorf("read file %s: %w", filePath, err)
	}
	if !strings.HasPrefix(realPath, repoReal+string(filepath.Separator)) {
		return nil, fmt.Errorf("path %q escapes the repository", filePath)
	}
	data, err := os.ReadFile(realPath)
	if err != nil {
		return nil, fmt.Errorf("read file %s: %w", filePath, err)
	}
	return data, nil
}

// CleanupRepo removes the temporary cloned repo directory.
func CleanupRepo(repoDir string) {
	os.RemoveAll(repoDir)
}
