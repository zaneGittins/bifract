package instructions

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"bifract/pkg/feeds"

	"gopkg.in/yaml.v3"
)

// Syncer periodically syncs repo-source instruction libraries.
type Syncer struct {
	manager *Manager
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewSyncer creates a new instruction library syncer.
func NewSyncer(manager *Manager) *Syncer {
	return &Syncer{
		manager: manager,
		stopCh:  make(chan struct{}),
	}
}

// Start begins the background sync loop.
func (s *Syncer) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.checkAndSync()
			case <-s.stopCh:
				return
			}
		}
	}()
	log.Println("[Instructions] Syncer started")
}

// Stop gracefully shuts down the syncer.
func (s *Syncer) Stop() {
	close(s.stopCh)
	s.wg.Wait()
	log.Println("[Instructions] Syncer stopped")
}

func (s *Syncer) checkAndSync() {
	ctx := context.Background()
	libs, err := s.manager.ListAllRepoLibraries(ctx)
	if err != nil {
		log.Printf("[Instructions] Failed to list repo libraries for sync: %v", err)
		return
	}

	for _, lib := range libs {
		interval := ScheduleInterval(lib.SyncSchedule)
		if interval == 0 {
			continue
		}
		if lib.LastSyncedAt != nil && time.Since(*lib.LastSyncedAt) < interval {
			continue
		}
		if _, err := s.SyncLibrary(ctx, lib); err != nil {
			log.Printf("[Instructions] Failed to sync library %s (%s): %v", lib.Name, lib.ID, err)
		}
	}
}

// SyncLibrary performs a full sync of a repo-source library.
func (s *Syncer) SyncLibrary(ctx context.Context, lib *Library) (*SyncResult, error) {
	if lib.Source != SourceRepo {
		return nil, fmt.Errorf("library %q is not a repo source", lib.Name)
	}
	if lib.RepoURL == "" {
		return nil, fmt.Errorf("library %q has no repo URL", lib.Name)
	}

	result := &SyncResult{}

	// Decrypt auth token
	token, err := s.manager.GetDecryptedToken(ctx, lib.ID)
	if err != nil {
		return nil, fmt.Errorf("decrypt token: %w", err)
	}

	branch := lib.Branch
	if branch == "" {
		branch = "main"
	}

	// Clone repo (shallow, single-branch) using feeds git client
	repoDir, err := feeds.CloneRepo(ctx, lib.RepoURL, branch, token)
	if err != nil {
		status := fmt.Sprintf("clone failed: %s", err.Error())
		s.manager.UpdateSyncStatus(ctx, lib.ID, status, 0)
		return nil, fmt.Errorf("clone repo: %w", err)
	}
	defer feeds.CleanupRepo(repoDir)

	// List markdown files
	mdFiles, err := listMarkdownFiles(repoDir, lib.Path)
	if err != nil {
		status := fmt.Sprintf("list files failed: %s", err.Error())
		s.manager.UpdateSyncStatus(ctx, lib.ID, status, 0)
		return nil, fmt.Errorf("list markdown files: %w", err)
	}

	// Load existing pages for this library (for change detection)
	existingPages, err := s.manager.ListPages(ctx, lib.ID)
	if err != nil {
		return nil, fmt.Errorf("list existing pages: %w", err)
	}

	existingByPath := make(map[string]*Page)
	for _, p := range existingPages {
		if p.SourcePath != "" {
			existingByPath[p.SourcePath] = p
		}
	}

	seenPaths := make(map[string]bool)

	for _, filePath := range mdFiles {
		content, err := feeds.ReadFile(repoDir, filePath)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("read %s: %v", filePath, err))
			continue
		}

		hash := fmt.Sprintf("%x", sha256.Sum256(content))
		seenPaths[filePath] = true

		// Parse front matter and content
		meta, body := parseFrontMatter(content)
		pageName := meta.Name
		if pageName == "" {
			// Default name from filename without extension
			base := filepath.Base(filePath)
			pageName = strings.TrimSuffix(base, filepath.Ext(base))
		}

		existing, found := existingByPath[filePath]
		if found {
			// Check if changed
			if existing.SourceHash == hash {
				result.Skipped++
				continue
			}
			// Update existing page
			_, err = s.manager.UpdateSyncedPage(ctx, existing.ID, pageName, meta.Description, body, meta.AlwaysInclude, meta.SortOrder, filePath, hash)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("update %s: %v", filePath, err))
				continue
			}
			result.Updated++
		} else {
			// Create new page
			_, err = s.manager.CreateSyncedPage(ctx, lib.ID, pageName, meta.Description, body, meta.AlwaysInclude, meta.SortOrder, filePath, hash)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("create %s: %v", filePath, err))
				continue
			}
			result.Added++
		}
	}

	// Delete pages for files no longer in repo
	for path, page := range existingByPath {
		if !seenPaths[path] {
			if err := s.manager.DeletePage(ctx, page.ID); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("delete %s: %v", path, err))
				continue
			}
			result.Deleted++
		}
	}

	totalPages := result.Added + result.Updated + result.Skipped
	status := fmt.Sprintf("ok: %d added, %d updated, %d deleted, %d unchanged", result.Added, result.Updated, result.Deleted, result.Skipped)
	if len(result.Errors) > 0 {
		status += fmt.Sprintf(", %d errors", len(result.Errors))
	}
	s.manager.UpdateSyncStatus(ctx, lib.ID, status, totalPages)

	log.Printf("[Instructions] Synced library %q: %s", lib.Name, status)
	return result, nil
}

// pageFrontMatter holds metadata parsed from markdown front matter.
type pageFrontMatter struct {
	Name          string `yaml:"name"`
	Description   string `yaml:"description"`
	AlwaysInclude bool   `yaml:"always_include"`
	SortOrder     int    `yaml:"sort_order"`
}

// parseFrontMatter splits a markdown file into YAML front matter and body content.
func parseFrontMatter(data []byte) (pageFrontMatter, string) {
	var meta pageFrontMatter

	scanner := bufio.NewScanner(bytes.NewReader(data))
	if !scanner.Scan() {
		return meta, string(data)
	}

	firstLine := strings.TrimSpace(scanner.Text())
	if firstLine != "---" {
		return meta, string(data)
	}

	var fmLines []string
	foundEnd := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "---" {
			foundEnd = true
			break
		}
		fmLines = append(fmLines, line)
	}

	if !foundEnd {
		return meta, string(data)
	}

	// Parse YAML front matter
	yaml.Unmarshal([]byte(strings.Join(fmLines, "\n")), &meta)

	// Collect remaining body
	var bodyLines []string
	for scanner.Scan() {
		bodyLines = append(bodyLines, scanner.Text())
	}

	body := strings.Join(bodyLines, "\n")
	body = strings.TrimSpace(body)
	return meta, body
}

// listMarkdownFiles lists .md files in a directory within the cloned repo.
// Follows the same security pattern as feeds.ListYAMLFiles.
func listMarkdownFiles(repoDir, subPath string) ([]string, error) {
	searchDir := repoDir
	if subPath != "" {
		searchDir = filepath.Join(repoDir, subPath)
	}

	repoAbs, err := filepath.Abs(repoDir)
	if err != nil {
		return nil, fmt.Errorf("resolve repo dir: %w", err)
	}
	searchAbs, err := filepath.EvalSymlinks(searchDir)
	if err != nil {
		return nil, fmt.Errorf("path %q does not exist in the repository", subPath)
	}
	if !strings.HasPrefix(searchAbs, repoAbs+string(filepath.Separator)) && searchAbs != repoAbs {
		return nil, fmt.Errorf("path %q escapes the repository", subPath)
	}

	if info, err := os.Stat(searchDir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("path %q does not exist in the repository", subPath)
	}

	var files []string
	err = filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
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
		if ext == ".md" {
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
