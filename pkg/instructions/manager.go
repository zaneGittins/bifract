package instructions

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"bifract/pkg/egress"
	"bifract/pkg/feeds"
	"bifract/pkg/storage"

	"github.com/lib/pq"
)

// wikilinkRe matches [[Page Name]] references in page content.
var wikilinkRe = regexp.MustCompile(`\[\[([^\[\]\n]+)\]\]`)

// normalizeLink lowercases and trims a page name/link for case-insensitive matching.
func normalizeLink(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// extractWikilinks returns the normalized [[targets]] referenced in content.
func extractWikilinks(content string) []string {
	matches := wikilinkRe.FindAllStringSubmatch(content, -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, normalizeLink(m[1]))
	}
	return out
}

// Manager handles CRUD operations for instruction libraries and pages.
type Manager struct {
	pg *storage.PostgresClient
}

// NewManager creates a new instruction library manager.
func NewManager(pg *storage.PostgresClient) *Manager {
	return &Manager{pg: pg}
}

// --- Library CRUD ---

// ListLibraries returns all libraries for a given fractal or prism, with page counts.
func (m *Manager) ListLibraries(ctx context.Context, fractalID, prismID string) ([]*Library, error) {
	var whereClause string
	var arg interface{}
	if prismID != "" {
		whereClause = "l.prism_id = $1"
		arg = prismID
	} else {
		whereClause = "l.fractal_id = $1"
		arg = fractalID
	}

	query := fmt.Sprintf(`
		SELECT l.id, l.name, l.description, l.is_default,
		       COALESCE(l.fractal_id::text, ''), COALESCE(l.prism_id::text, ''),
		       l.source, l.repo_url, l.branch, l.path, l.auth_token,
		       l.sync_schedule, l.last_synced_at, l.last_sync_status, l.last_sync_page_count,
		       COALESCE(l.created_by, ''), l.created_at, l.updated_at,
		       (SELECT COUNT(*) FROM instruction_pages p WHERE p.library_id = l.id) as page_count
		FROM instruction_libraries l
		WHERE %s
		ORDER BY l.is_default DESC, l.name ASC
	`, whereClause)

	rows, err := m.pg.Query(ctx, query, arg)
	if err != nil {
		return nil, fmt.Errorf("list libraries: %w", err)
	}
	defer rows.Close()

	var libs []*Library
	for rows.Next() {
		lib, err := scanLibrary(rows)
		if err != nil {
			return nil, err
		}
		libs = append(libs, lib)
	}
	return libs, nil
}

// GetLibrary returns a single library by ID.
func (m *Manager) GetLibrary(ctx context.Context, id string) (*Library, error) {
	query := `
		SELECT l.id, l.name, l.description, l.is_default,
		       COALESCE(l.fractal_id::text, ''), COALESCE(l.prism_id::text, ''),
		       l.source, l.repo_url, l.branch, l.path, l.auth_token,
		       l.sync_schedule, l.last_synced_at, l.last_sync_status, l.last_sync_page_count,
		       COALESCE(l.created_by, ''), l.created_at, l.updated_at,
		       (SELECT COUNT(*) FROM instruction_pages p WHERE p.library_id = l.id) as page_count
		FROM instruction_libraries l
		WHERE l.id = $1
	`
	row := m.pg.QueryRow(ctx, query, id)
	return scanLibraryRow(row)
}

// GetDefaultLibrary returns the default library for a fractal, or nil.
func (m *Manager) GetDefaultLibrary(ctx context.Context, fractalID string) (*Library, error) {
	query := `
		SELECT l.id, l.name, l.description, l.is_default,
		       COALESCE(l.fractal_id::text, ''), COALESCE(l.prism_id::text, ''),
		       l.source, l.repo_url, l.branch, l.path, l.auth_token,
		       l.sync_schedule, l.last_synced_at, l.last_sync_status, l.last_sync_page_count,
		       COALESCE(l.created_by, ''), l.created_at, l.updated_at,
		       (SELECT COUNT(*) FROM instruction_pages p WHERE p.library_id = l.id) as page_count
		FROM instruction_libraries l
		WHERE l.fractal_id = $1 AND l.is_default = true
	`
	row := m.pg.QueryRow(ctx, query, fractalID)
	lib, err := scanLibraryRow(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return lib, err
}

// GetDefaultPrismLibrary returns the default library for a prism, or nil.
func (m *Manager) GetDefaultPrismLibrary(ctx context.Context, prismID string) (*Library, error) {
	query := `
		SELECT l.id, l.name, l.description, l.is_default,
		       COALESCE(l.fractal_id::text, ''), COALESCE(l.prism_id::text, ''),
		       l.source, l.repo_url, l.branch, l.path, l.auth_token,
		       l.sync_schedule, l.last_synced_at, l.last_sync_status, l.last_sync_page_count,
		       COALESCE(l.created_by, ''), l.created_at, l.updated_at,
		       (SELECT COUNT(*) FROM instruction_pages p WHERE p.library_id = l.id) as page_count
		FROM instruction_libraries l
		WHERE l.prism_id = $1 AND l.is_default = true
	`
	row := m.pg.QueryRow(ctx, query, prismID)
	lib, err := scanLibraryRow(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return lib, err
}

// EnsureDefaultLibrary returns the scope's single default library, creating or
// promoting one as needed. The product model is exactly one library per
// fractal/prism, created by default. canCreate gates brand-new creation
// (analyst+); a viewer with no existing library gets nil.
func (m *Manager) EnsureDefaultLibrary(ctx context.Context, fractalID, prismID, createdBy string, canCreate bool) (*Library, error) {
	var def *Library
	var err error
	if prismID != "" {
		def, err = m.GetDefaultPrismLibrary(ctx, prismID)
	} else {
		def, err = m.GetDefaultLibrary(ctx, fractalID)
	}
	if err != nil {
		return nil, err
	}
	if def != nil {
		return def, nil
	}

	// No default yet: promote an existing library if one exists (migration-safe,
	// so pre-existing instruction pages are never hidden).
	libs, err := m.ListLibraries(ctx, fractalID, prismID)
	if err != nil {
		return nil, err
	}
	if len(libs) > 0 {
		if _, err := m.pg.Exec(ctx, `UPDATE instruction_libraries SET is_default = true, updated_at = NOW() WHERE id = $1`, libs[0].ID); err != nil {
			return nil, fmt.Errorf("promote default library: %w", err)
		}
		libs[0].IsDefault = true
		return libs[0], nil
	}

	if !canCreate {
		return nil, nil
	}
	req := CreateLibraryRequest{
		Name:         "Library",
		Source:       "manual",
		IsDefault:    true,
		Branch:       "main",
		SyncSchedule: "never",
	}
	return m.CreateLibrary(ctx, req, fractalID, prismID, createdBy)
}

// CreateLibrary creates a new library scoped to either a fractal or a prism.
func (m *Manager) CreateLibrary(ctx context.Context, req CreateLibraryRequest, fractalID, prismID, createdBy string) (*Library, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if fractalID == "" && prismID == "" {
		return nil, fmt.Errorf("fractal or prism scope is required")
	}

	source := req.Source
	if source == "" {
		source = SourceManual
	}
	if !ValidSources[source] {
		return nil, fmt.Errorf("invalid source: %s", source)
	}
	repoURL, err := checkRepoURL(source, req.RepoURL)
	if err != nil {
		return nil, err
	}

	schedule := req.SyncSchedule
	if schedule == "" {
		schedule = ScheduleNever
	}
	if !ValidSchedules[schedule] {
		return nil, fmt.Errorf("invalid sync_schedule: %s", schedule)
	}

	branch := req.Branch
	if branch == "" {
		branch = "main"
	}

	// Clear existing default if this is being set as default
	if req.IsDefault {
		if fractalID != "" {
			m.pg.Exec(ctx, `UPDATE instruction_libraries SET is_default = false WHERE fractal_id = $1 AND is_default = true`, fractalID)
		} else {
			m.pg.Exec(ctx, `UPDATE instruction_libraries SET is_default = false WHERE prism_id = $1 AND is_default = true`, prismID)
		}
	}

	authToken, err := encryptToken(req.AuthToken)
	if err != nil {
		return nil, fmt.Errorf("encrypt token: %w", err)
	}

	var fID, pID interface{}
	if fractalID != "" {
		fID = fractalID
	}
	if prismID != "" {
		pID = prismID
	}

	var id string
	err = m.pg.QueryRow(ctx, `
		INSERT INTO instruction_libraries (name, description, is_default, fractal_id, prism_id,
		       source, repo_url, branch, path, auth_token, sync_schedule, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		RETURNING id
	`, name, req.Description, req.IsDefault, fID, pID,
		source, repoURL, branch, req.Path, authToken, schedule, storage.NullableUser(createdBy)).Scan(&id)
	if err != nil {
		if strings.Contains(err.Error(), "idx_il_name_scope") {
			return nil, fmt.Errorf("a library named %q already exists in this scope", name)
		}
		return nil, fmt.Errorf("create library: %w", err)
	}
	return m.GetLibrary(ctx, id)
}

// checkRepoURL trims a library's repository URL and refuses one a clone would
// refuse, so it fails on save rather than on every sync.
func checkRepoURL(source, raw string) (string, error) {
	u := strings.TrimSpace(raw)
	if u == "" {
		if source == SourceRepo {
			return "", fmt.Errorf("repo_url is required for repo source")
		}
		return "", nil
	}
	if err := egress.ValidateURL(u); err != nil {
		return "", fmt.Errorf("invalid repo_url: %w", err)
	}
	return u, nil
}

// UpdateLibrary updates an existing library.
func (m *Manager) UpdateLibrary(ctx context.Context, id string, req UpdateLibraryRequest) (*Library, error) {
	existing, err := m.GetLibrary(ctx, id)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("library not found")
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = existing.Name
	}

	source := req.Source
	if source == "" {
		source = existing.Source
	}

	// An update without a URL, such as a rename, keeps the stored one.
	repoURL := strings.TrimSpace(req.RepoURL)
	if repoURL == "" {
		repoURL = existing.RepoURL
	}
	if repoURL, err = checkRepoURL(source, repoURL); err != nil {
		return nil, err
	}
	// The stored token was given for the stored host; it does not follow the
	// URL to another one unless a new token comes with it.
	clearToken := req.ClearToken ||
		(req.AuthToken == "" && feeds.RepoOrigin(existing.RepoURL) != feeds.RepoOrigin(repoURL))

	schedule := req.SyncSchedule
	if schedule == "" {
		schedule = existing.SyncSchedule
	}
	if !ValidSchedules[schedule] {
		return nil, fmt.Errorf("invalid sync_schedule: %s", schedule)
	}

	branch := req.Branch
	if branch == "" {
		branch = existing.Branch
	}

	// Handle default switching
	if req.IsDefault && !existing.IsDefault {
		if existing.FractalID != "" {
			m.pg.Exec(ctx, `UPDATE instruction_libraries SET is_default = false WHERE fractal_id = $1 AND is_default = true`, existing.FractalID)
		} else if existing.PrismID != "" {
			m.pg.Exec(ctx, `UPDATE instruction_libraries SET is_default = false WHERE prism_id = $1 AND is_default = true`, existing.PrismID)
		}
	}

	if clearToken || req.AuthToken != "" {
		token := ""
		if !req.ClearToken {
			if token, err = encryptToken(req.AuthToken); err != nil {
				return nil, fmt.Errorf("encrypt token: %w", err)
			}
		}
		_, err = m.pg.Exec(ctx, `
			UPDATE instruction_libraries
			SET name = $1, description = $2, is_default = $3,
			    source = $4, repo_url = $5, branch = $6, path = $7,
			    sync_schedule = $8, updated_at = NOW(),
			    auth_token = $10
			WHERE id = $9
		`, name, req.Description, req.IsDefault,
			source, repoURL, branch, req.Path,
			schedule, id, token)
	} else {
		_, err = m.pg.Exec(ctx, `
			UPDATE instruction_libraries
			SET name = $1, description = $2, is_default = $3,
			    source = $4, repo_url = $5, branch = $6, path = $7,
			    sync_schedule = $8, updated_at = NOW()
			WHERE id = $9
		`, name, req.Description, req.IsDefault,
			source, repoURL, branch, req.Path,
			schedule, id)
	}
	if err != nil {
		if strings.Contains(err.Error(), "idx_il_name_scope") {
			return nil, fmt.Errorf("a library named %q already exists in this scope", name)
		}
		return nil, fmt.Errorf("update library: %w", err)
	}
	return m.GetLibrary(ctx, id)
}

// DeleteLibrary removes a library and all its pages (cascading).
func (m *Manager) DeleteLibrary(ctx context.Context, id string) error {
	_, err := m.pg.Exec(ctx, `DELETE FROM instruction_libraries WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete library: %w", err)
	}
	return nil
}

// --- Page CRUD ---

// ListPages returns all pages for a given library.
func (m *Manager) ListPages(ctx context.Context, libraryID string) ([]*Page, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT id, library_id, name, description, content, always_include, sort_order,
		       source_path, source_hash, folder_id, COALESCE(created_by, ''), created_at, updated_at
		FROM instruction_pages
		WHERE library_id = $1
		ORDER BY sort_order ASC, name ASC
	`, libraryID)
	if err != nil {
		return nil, fmt.Errorf("list pages: %w", err)
	}
	defer rows.Close()

	var pages []*Page
	for rows.Next() {
		p, err := scanPage(rows)
		if err != nil {
			return nil, err
		}
		pages = append(pages, p)
	}
	return pages, nil
}

// GetPage returns a single page by ID.
func (m *Manager) GetPage(ctx context.Context, id string) (*Page, error) {
	row := m.pg.QueryRow(ctx, `
		SELECT id, library_id, name, description, content, always_include, sort_order,
		       source_path, source_hash, folder_id, COALESCE(created_by, ''), created_at, updated_at
		FROM instruction_pages
		WHERE id = $1
	`, id)
	return scanPageRow(row)
}

// GetPageByName returns a page by name within a set of libraries.
func (m *Manager) GetPageByName(ctx context.Context, libraryIDs []string, pageName string) (*Page, error) {
	if len(libraryIDs) == 0 {
		return nil, fmt.Errorf("no libraries specified")
	}

	placeholders := make([]string, len(libraryIDs))
	args := make([]interface{}, len(libraryIDs)+1)
	args[0] = pageName
	for i, id := range libraryIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = id
	}

	query := fmt.Sprintf(`
		SELECT id, library_id, name, description, content, always_include, sort_order,
		       source_path, source_hash, folder_id, COALESCE(created_by, ''), created_at, updated_at
		FROM instruction_pages
		WHERE name = $1 AND library_id IN (%s)
		LIMIT 1
	`, strings.Join(placeholders, ","))

	row := m.pg.QueryRow(ctx, query, args...)
	p, err := scanPageRow(row)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("page %q not found in linked libraries", pageName)
	}
	return p, err
}

// PageRef is a lightweight page reference (used for backlinks).
type PageRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// GetBacklinks returns pages in the same library whose content references
// [[pageName]] (case-insensitive). Within-library only: page names are unique
// per library, so resolution is deterministic.
func (m *Manager) GetBacklinks(ctx context.Context, libraryID, pageName string) ([]PageRef, error) {
	pages, err := m.ListPages(ctx, libraryID)
	if err != nil {
		return nil, err
	}
	target := normalizeLink(pageName)
	refs := make([]PageRef, 0)
	for _, p := range pages {
		if normalizeLink(p.Name) == target {
			continue // skip self-references
		}
		for _, link := range extractWikilinks(p.Content) {
			if link == target {
				refs = append(refs, PageRef{ID: p.ID, Name: p.Name})
				break
			}
		}
	}
	return refs, nil
}

// CreatePage creates a new page in a library.
func (m *Manager) CreatePage(ctx context.Context, libraryID string, req CreatePageRequest, createdBy string) (*Page, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	var id string
	err := m.pg.QueryRow(ctx, `
		INSERT INTO instruction_pages (library_id, name, description, content, always_include, sort_order, folder_id, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id
	`, libraryID, name, req.Description, req.Content, req.AlwaysInclude, req.SortOrder, req.FolderID, storage.NullableUser(createdBy)).Scan(&id)
	if err != nil {
		if strings.Contains(err.Error(), "ip_unique_name") {
			return nil, fmt.Errorf("a page named %q already exists in this library", name)
		}
		return nil, fmt.Errorf("create page: %w", err)
	}
	return m.GetPage(ctx, id)
}

// MovePage sets a page's folder (folderID=nil means library root) and position.
// It deliberately does NOT touch content/name so normal saves never clobber it.
func (m *Manager) MovePage(ctx context.Context, pageID string, folderID *string, sortOrder int) error {
	_, err := m.pg.Exec(ctx, `
		UPDATE instruction_pages SET folder_id = $1, sort_order = $2, updated_at = NOW() WHERE id = $3
	`, folderID, sortOrder, pageID)
	if err != nil {
		return fmt.Errorf("move page: %w", err)
	}
	return nil
}

// --- Folder CRUD (single-level) ---

func scanFolder(s scannable) (*Folder, error) {
	var f Folder
	if err := s.Scan(&f.ID, &f.LibraryID, &f.Name, &f.SortOrder, &f.CreatedBy, &f.CreatedAt, &f.UpdatedAt); err != nil {
		return nil, err
	}
	return &f, nil
}

// ListFolders returns all folders for a library, ordered.
func (m *Manager) ListFolders(ctx context.Context, libraryID string) ([]*Folder, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT id, library_id, name, sort_order, COALESCE(created_by, ''), created_at, updated_at
		FROM instruction_folders WHERE library_id = $1 ORDER BY sort_order ASC, name ASC
	`, libraryID)
	if err != nil {
		return nil, fmt.Errorf("list folders: %w", err)
	}
	defer rows.Close()
	folders := make([]*Folder, 0)
	for rows.Next() {
		f, err := scanFolder(rows)
		if err != nil {
			return nil, err
		}
		folders = append(folders, f)
	}
	return folders, nil
}

// CreateFolder creates a folder at the end of the library's folder list.
func (m *Manager) CreateFolder(ctx context.Context, libraryID, name, createdBy string) (*Folder, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	var nextOrder int
	_ = m.pg.QueryRow(ctx, `SELECT COALESCE(MAX(sort_order)+1, 0) FROM instruction_folders WHERE library_id = $1`, libraryID).Scan(&nextOrder)
	var id string
	err := m.pg.QueryRow(ctx, `
		INSERT INTO instruction_folders (library_id, name, sort_order, created_by)
		VALUES ($1, $2, $3, $4) RETURNING id
	`, libraryID, name, nextOrder, storage.NullableUser(createdBy)).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("create folder: %w", err)
	}
	return m.GetFolder(ctx, id)
}

// GetFolder returns a single folder by id.
func (m *Manager) GetFolder(ctx context.Context, id string) (*Folder, error) {
	row := m.pg.QueryRow(ctx, `
		SELECT id, library_id, name, sort_order, COALESCE(created_by, ''), created_at, updated_at
		FROM instruction_folders WHERE id = $1
	`, id)
	return scanFolder(row)
}

// UpdateFolder renames and/or reorders a folder.
func (m *Manager) UpdateFolder(ctx context.Context, id string, req UpdateFolderRequest) (*Folder, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}
	_, err := m.pg.Exec(ctx, `
		UPDATE instruction_folders SET name = $1, sort_order = $2, updated_at = NOW() WHERE id = $3
	`, name, req.SortOrder, id)
	if err != nil {
		return nil, fmt.Errorf("update folder: %w", err)
	}
	return m.GetFolder(ctx, id)
}

// DeleteFolder removes a folder. Its pages fall back to the root (FK ON DELETE SET NULL).
func (m *Manager) DeleteFolder(ctx context.Context, id string) error {
	_, err := m.pg.Exec(ctx, `DELETE FROM instruction_folders WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete folder: %w", err)
	}
	return nil
}

// UpdatePage updates an existing page.
func (m *Manager) UpdatePage(ctx context.Context, id string, req UpdatePageRequest) (*Page, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	_, err := m.pg.Exec(ctx, `
		UPDATE instruction_pages
		SET name = $1, description = $2, content = $3, always_include = $4, sort_order = $5, updated_at = NOW()
		WHERE id = $6
	`, name, req.Description, req.Content, req.AlwaysInclude, req.SortOrder, id)
	if err != nil {
		if strings.Contains(err.Error(), "ip_unique_name") {
			return nil, fmt.Errorf("a page named %q already exists in this library", name)
		}
		return nil, fmt.Errorf("update page: %w", err)
	}
	return m.GetPage(ctx, id)
}

// DeletePage removes a page.
func (m *Manager) DeletePage(ctx context.Context, id string) error {
	_, err := m.pg.Exec(ctx, `DELETE FROM instruction_pages WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete page: %w", err)
	}
	return nil
}

// --- Synced Page operations (used by syncer) ---

// CreateSyncedPage creates a page from a repo sync with source tracking fields.
func (m *Manager) CreateSyncedPage(ctx context.Context, libraryID, name, description, content string, alwaysInclude bool, sortOrder int, sourcePath, sourceHash string) (*Page, error) {
	var id string
	err := m.pg.QueryRow(ctx, `
		INSERT INTO instruction_pages (library_id, name, description, content, always_include, sort_order, source_path, source_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id
	`, libraryID, name, description, content, alwaysInclude, sortOrder, sourcePath, sourceHash).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("create synced page: %w", err)
	}
	return m.GetPage(ctx, id)
}

// UpdateSyncedPage updates a page from a repo sync with source tracking fields.
func (m *Manager) UpdateSyncedPage(ctx context.Context, id, name, description, content string, alwaysInclude bool, sortOrder int, sourcePath, sourceHash string) (*Page, error) {
	_, err := m.pg.Exec(ctx, `
		UPDATE instruction_pages
		SET name = $1, description = $2, content = $3, always_include = $4, sort_order = $5,
		    source_path = $6, source_hash = $7, updated_at = NOW()
		WHERE id = $8
	`, name, description, content, alwaysInclude, sortOrder, sourcePath, sourceHash, id)
	if err != nil {
		return nil, fmt.Errorf("update synced page: %w", err)
	}
	return m.GetPage(ctx, id)
}

// --- Conversation Library Association ---

// ErrLibraryOutOfScope means a library does not exist in the caller's fractal or prism.
var ErrLibraryOutOfScope = errors.New("library not found in this scope")

// conversationScopeJoin keeps only libraries in the conversation's own fractal or
// prism, so a link made before scope was checked on write never loads.
const conversationScopeJoin = `JOIN chat_conversations c ON c.id = cl.conversation_id
		  AND (l.fractal_id = c.fractal_id OR l.prism_id = c.prism_id)`

// GetConversationLibraries returns libraries linked to a conversation.
func (m *Manager) GetConversationLibraries(ctx context.Context, conversationID string) ([]*Library, error) {
	query := `
		SELECT l.id, l.name, l.description, l.is_default,
		       COALESCE(l.fractal_id::text, ''), COALESCE(l.prism_id::text, ''),
		       l.source, l.repo_url, l.branch, l.path, l.auth_token,
		       l.sync_schedule, l.last_synced_at, l.last_sync_status, l.last_sync_page_count,
		       COALESCE(l.created_by, ''), l.created_at, l.updated_at,
		       (SELECT COUNT(*) FROM instruction_pages p WHERE p.library_id = l.id) as page_count
		FROM conversation_libraries cl
		JOIN instruction_libraries l ON l.id = cl.library_id
		` + conversationScopeJoin + `
		WHERE cl.conversation_id = $1
		ORDER BY l.is_default DESC, l.name ASC
	`
	rows, err := m.pg.Query(ctx, query, conversationID)
	if err != nil {
		return nil, fmt.Errorf("get conversation libraries: %w", err)
	}
	defer rows.Close()

	var libs []*Library
	for rows.Next() {
		lib, err := scanLibrary(rows)
		if err != nil {
			return nil, err
		}
		libs = append(libs, lib)
	}
	return libs, nil
}

// GetConversationLibraryIDs returns just the library IDs linked to a conversation.
func (m *Manager) GetConversationLibraryIDs(ctx context.Context, conversationID string) ([]string, error) {
	rows, err := m.pg.Query(ctx, `
		SELECT l.id FROM conversation_libraries cl
		JOIN instruction_libraries l ON l.id = cl.library_id
		`+conversationScopeJoin+`
		WHERE cl.conversation_id = $1
	`, conversationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// CheckLibrariesInScope returns ErrLibraryOutOfScope unless every ID names a
// library in the given fractal or prism.
func (m *Manager) CheckLibrariesInScope(ctx context.Context, fractalID, prismID string, libraryIDs []string) error {
	column, scope := "fractal_id", fractalID
	if prismID != "" {
		column, scope = "prism_id", prismID
	}
	if scope == "" {
		return ErrLibraryOutOfScope
	}
	ids := uniqueIDs(libraryIDs)
	if len(ids) == 0 {
		return nil
	}
	// id::text so a malformed ID simply does not match.
	var found int
	err := m.pg.QueryRow(ctx, `SELECT COUNT(*) FROM instruction_libraries
		WHERE id::text = ANY($1) AND `+column+` = $2`, pq.Array(ids), scope).Scan(&found)
	if err != nil {
		return fmt.Errorf("check library scope: %w", err)
	}
	if found != len(ids) {
		return ErrLibraryOutOfScope
	}
	return nil
}

// SetConversationLibraries replaces a conversation's libraries, refusing any
// outside the conversation's fractal or prism.
func (m *Manager) SetConversationLibraries(ctx context.Context, conversationID, fractalID, prismID string, libraryIDs []string) error {
	if err := m.CheckLibrariesInScope(ctx, fractalID, prismID, libraryIDs); err != nil {
		return err
	}
	tx, err := m.pg.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM conversation_libraries WHERE conversation_id = $1`, conversationID); err != nil {
		return fmt.Errorf("clear conversation libraries: %w", err)
	}
	for _, libID := range uniqueIDs(libraryIDs) {
		if _, err := tx.Exec(ctx, `
			INSERT INTO conversation_libraries (conversation_id, library_id) VALUES ($1, $2)
		`, conversationID, libID); err != nil {
			return fmt.Errorf("set conversation library: %w", err)
		}
	}
	return tx.Commit(ctx)
}

func uniqueIDs(ids []string) []string {
	seen := make(map[string]bool, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// --- AI Context Resolution ---

// ResolveLibraryIDs returns the library IDs that should be used for a conversation.
// Uses conversation-explicit libraries if set, otherwise falls back to the fractal default.
func (m *Manager) ResolveLibraryIDs(ctx context.Context, conversationID, fractalID string) ([]string, error) {
	ids, err := m.GetConversationLibraryIDs(ctx, conversationID)
	if err != nil {
		return nil, err
	}
	if len(ids) > 0 {
		return ids, nil
	}

	// Fall back to default library
	lib, err := m.GetDefaultLibrary(ctx, fractalID)
	if err != nil {
		return nil, err
	}
	if lib != nil {
		return []string{lib.ID}, nil
	}
	return nil, nil
}

// GetPinnedPages returns all always_include pages from the given libraries.
func (m *Manager) GetPinnedPages(ctx context.Context, libraryIDs []string) ([]*Page, error) {
	if len(libraryIDs) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(libraryIDs))
	args := make([]interface{}, len(libraryIDs))
	for i, id := range libraryIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, library_id, name, description, content, always_include, sort_order,
		       source_path, source_hash, folder_id, COALESCE(created_by, ''), created_at, updated_at
		FROM instruction_pages
		WHERE library_id IN (%s) AND always_include = true
		ORDER BY sort_order ASC, name ASC
	`, strings.Join(placeholders, ","))

	rows, err := m.pg.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get pinned pages: %w", err)
	}
	defer rows.Close()

	var pages []*Page
	for rows.Next() {
		p, err := scanPage(rows)
		if err != nil {
			return nil, err
		}
		pages = append(pages, p)
	}
	return pages, nil
}

// GetPageIndex returns lightweight summaries of non-pinned pages for AI context.
func (m *Manager) GetPageIndex(ctx context.Context, libraryIDs []string) ([]PageSummary, error) {
	if len(libraryIDs) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(libraryIDs))
	args := make([]interface{}, len(libraryIDs))
	for i, id := range libraryIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, name, description, always_include
		FROM instruction_pages
		WHERE library_id IN (%s) AND always_include = false
		ORDER BY sort_order ASC, name ASC
	`, strings.Join(placeholders, ","))

	rows, err := m.pg.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get page index: %w", err)
	}
	defer rows.Close()

	var summaries []PageSummary
	for rows.Next() {
		var s PageSummary
		if err := rows.Scan(&s.ID, &s.Name, &s.Description, &s.AlwaysInclude); err != nil {
			return nil, err
		}
		summaries = append(summaries, s)
	}
	return summaries, nil
}

// UpdateSyncStatus updates the sync metadata for a library.
func (m *Manager) UpdateSyncStatus(ctx context.Context, id, status string, pageCount int) error {
	_, err := m.pg.Exec(ctx, `
		UPDATE instruction_libraries
		SET last_synced_at = NOW(), last_sync_status = $1, last_sync_page_count = $2, updated_at = NOW()
		WHERE id = $3
	`, status, pageCount, id)
	return err
}

// ListAllRepoLibraries returns all repo-source libraries with a sync schedule (for the syncer).
func (m *Manager) ListAllRepoLibraries(ctx context.Context) ([]*Library, error) {
	query := `
		SELECT l.id, l.name, l.description, l.is_default,
		       COALESCE(l.fractal_id::text, ''), COALESCE(l.prism_id::text, ''),
		       l.source, l.repo_url, l.branch, l.path, l.auth_token,
		       l.sync_schedule, l.last_synced_at, l.last_sync_status, l.last_sync_page_count,
		       COALESCE(l.created_by, ''), l.created_at, l.updated_at,
		       (SELECT COUNT(*) FROM instruction_pages p WHERE p.library_id = l.id) as page_count
		FROM instruction_libraries l
		WHERE l.source = 'repo' AND l.sync_schedule != 'never'
		ORDER BY l.name
	`
	rows, err := m.pg.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list repo libraries: %w", err)
	}
	defer rows.Close()

	var libs []*Library
	for rows.Next() {
		lib, err := scanLibrary(rows)
		if err != nil {
			return nil, err
		}
		libs = append(libs, lib)
	}
	return libs, nil
}

// GetDecryptedToken retrieves and decrypts the auth token for a library.
func (m *Manager) GetDecryptedToken(ctx context.Context, id string) (string, error) {
	var token string
	err := m.pg.QueryRow(ctx, `SELECT auth_token FROM instruction_libraries WHERE id = $1`, id).Scan(&token)
	if err != nil {
		return "", err
	}
	if token == "" {
		return "", nil
	}
	return decryptToken(token)
}

// --- Scanner helpers ---

type scannable interface {
	Scan(dest ...interface{}) error
}

func scanLibrary(s scannable) (*Library, error) {
	lib, err := scanLibraryRow(s)
	if err != nil {
		return nil, fmt.Errorf("scan library: %w", err)
	}
	return lib, nil
}

// scanLibraryRow returns the scan error as is, so callers can match sql.ErrNoRows.
func scanLibraryRow(s scannable) (*Library, error) {
	var lib Library
	var lastSyncedAt sql.NullTime
	var authToken string
	err := s.Scan(
		&lib.ID, &lib.Name, &lib.Description, &lib.IsDefault,
		&lib.FractalID, &lib.PrismID,
		&lib.Source, &lib.RepoURL, &lib.Branch, &lib.Path, &authToken,
		&lib.SyncSchedule, &lastSyncedAt, &lib.LastSyncStatus, &lib.LastSyncPageCount,
		&lib.CreatedBy, &lib.CreatedAt, &lib.UpdatedAt,
		&lib.PageCount,
	)
	if err != nil {
		return nil, err
	}
	if lastSyncedAt.Valid {
		lib.LastSyncedAt = &lastSyncedAt.Time
	}
	lib.RepoURL = feeds.WithoutUserinfo(lib.RepoURL)
	lib.HasAuthToken = authToken != ""
	return &lib, nil
}

func scanPage(s scannable) (*Page, error) {
	var p Page
	var folderID sql.NullString
	err := s.Scan(
		&p.ID, &p.LibraryID, &p.Name, &p.Description, &p.Content,
		&p.AlwaysInclude, &p.SortOrder,
		&p.SourcePath, &p.SourceHash, &folderID, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan page: %w", err)
	}
	if folderID.Valid {
		p.FolderID = &folderID.String
	}
	return &p, nil
}

func scanPageRow(row *sql.Row) (*Page, error) {
	var p Page
	var folderID sql.NullString
	err := row.Scan(
		&p.ID, &p.LibraryID, &p.Name, &p.Description, &p.Content,
		&p.AlwaysInclude, &p.SortOrder,
		&p.SourcePath, &p.SourceHash, &folderID, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	if folderID.Valid {
		p.FolderID = &folderID.String
	}
	return &p, nil
}

// --- Crypto helpers (reuse feeds encryption) ---

func encryptToken(plaintext string) (string, error) {
	return feeds.EncryptToken(plaintext)
}

func decryptToken(stored string) (string, error) {
	return feeds.DecryptToken(stored)
}
