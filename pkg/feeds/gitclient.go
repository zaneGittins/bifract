package feeds

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	git "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	githttp "github.com/go-git/go-git/v6/plumbing/transport/http"
)

// validateRepoURL checks that the URL uses an allowed scheme and does not
// resolve to a private/internal IP address (SSRF protection).
func validateRepoURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("unsupported URL scheme %q: only http and https are allowed", scheme)
	}
	hostname := u.Hostname()
	if hostname == "" {
		return fmt.Errorf("URL must include a hostname")
	}
	ips, err := net.LookupHost(hostname)
	if err != nil {
		return fmt.Errorf("failed to resolve hostname %q: %w", hostname, err)
	}
	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			continue
		}
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
			return fmt.Errorf("URL resolves to a blocked IP address")
		}
	}
	return nil
}

// CloneRepo performs a shallow clone (depth=1) of the given repo into a temp directory.
// Uses go-git (pure Go, no external git binary required).
// Returns the path to the cloned directory. Caller must call CleanupRepo when done.
func CloneRepo(ctx context.Context, repoURL, branch, authToken string) (string, error) {
	if err := validateRepoURL(repoURL); err != nil {
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
	}

	if authToken != "" {
		opts.Auth = &githttp.BasicAuth{
			Username: "x-token-auth",
			Password: authToken,
		}
	}

	_, err = git.PlainCloneContext(ctx, tmpDir, opts)
	if err != nil {
		os.RemoveAll(tmpDir)
		return "", fmt.Errorf("clone repo: %w", err)
	}

	return tmpDir, nil
}

// ListYAMLFiles recursively finds all .yml and .yaml files under the given
// subPath within repoDir. Returns paths relative to repoDir.
func ListYAMLFiles(repoDir, subPath string) ([]string, error) {
	searchDir := repoDir
	if subPath != "" {
		searchDir = filepath.Join(repoDir, subPath)
	}

	if info, err := os.Stat(searchDir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("path %q does not exist in the repository", subPath)
	}

	var files []string
	err := filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			if strings.HasPrefix(info.Name(), ".") && info.Name() != "." {
				return filepath.SkipDir
			}
			return nil
		}

		ext := strings.ToLower(filepath.Ext(info.Name()))
		if ext == ".yml" || ext == ".yaml" {
			rel, relErr := filepath.Rel(repoDir, path)
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

// ReadFile reads the content of a file within the cloned repo.
func ReadFile(repoDir, filePath string) ([]byte, error) {
	fullPath := filepath.Join(repoDir, filePath)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("read file %s: %w", filePath, err)
	}
	return data, nil
}

// CleanupRepo removes the temporary cloned repo directory.
func CleanupRepo(repoDir string) {
	os.RemoveAll(repoDir)
}
