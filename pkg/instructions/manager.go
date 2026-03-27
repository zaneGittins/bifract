package instructions

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"bifract/pkg/feeds"
	"bifract/pkg/storage"
)

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
	if source == SourceRepo && strings.TrimSpace(req.RepoURL) == "" {
		return nil, fmt.Errorf("repo_url is required for repo source")
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

	var authToken string
	if req.AuthToken != "" {
		encrypted, err := encryptToken(req.AuthToken)
		if err != nil {
			log.Printf("[Instructions] Failed to encrypt auth token: %v", err)
			authToken = req.AuthToken
		} else {
			authToken = encrypted
		}
	}

	var fID, pID interface{}
	if fractalID != "" {
		fID = fractalID
	}
	if prismID != "" {
		pID = prismID
	}

	var id string
	err := m.pg.QueryRow(ctx, `
		INSERT INTO instruction_libraries (name, description, is_default, fractal_id, prism_id,
		       source, repo_url, branch, path, auth_token, sync_schedule, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		RETURNING id
	`, name, req.Description, req.IsDefault, fID, pID,
		source, req.RepoURL, branch, req.Path, authToken, schedule, createdBy).Scan(&id)
	if err != nil {
		if strings.Contains(err.Error(), "idx_il_name_scope") {
			return nil, fmt.Errorf("a library named %q already exists in this scope", name)
		}
		return nil, fmt.Errorf("create library: %w", err)
	}
	return m.GetLibrary(ctx, id)
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

	// Handle token updates
	authTokenSQL := "auth_token"
	var authTokenArg interface{}
	if req.ClearToken {
		authTokenSQL = "$10"
		authTokenArg = ""
	} else if req.AuthToken != "" {
		encrypted, encErr := encryptToken(req.AuthToken)
		if encErr != nil {
			log.Printf("[Instructions] Failed to encrypt auth token: %v", encErr)
			encrypted = req.AuthToken
		}
		authTokenSQL = "$10"
		authTokenArg = encrypted
	} else {
		authTokenSQL = "$10"
		authTokenArg = nil // placeholder, won't be used
	}

	if req.ClearToken || req.AuthToken != "" {
		_, err = m.pg.Exec(ctx, fmt.Sprintf(`
			UPDATE instruction_libraries
			SET name = $1, description = $2, is_default = $3,
			    source = $4, repo_url = $5, branch = $6, path = $7,
			    sync_schedule = $8, updated_at = NOW(),
			    auth_token = %s
			WHERE id = $9
		`, authTokenSQL), name, req.Description, req.IsDefault,
			source, req.RepoURL, branch, req.Path,
			schedule, id, authTokenArg)
	} else {
		_, err = m.pg.Exec(ctx, `
			UPDATE instruction_libraries
			SET name = $1, description = $2, is_default = $3,
			    source = $4, repo_url = $5, branch = $6, path = $7,
			    sync_schedule = $8, updated_at = NOW()
			WHERE id = $9
		`, name, req.Description, req.IsDefault,
			source, req.RepoURL, branch, req.Path,
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
		       source_path, source_hash, COALESCE(created_by, ''), created_at, updated_at
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
		       source_path, source_hash, COALESCE(created_by, ''), created_at, updated_at
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
		       source_path, source_hash, COALESCE(created_by, ''), created_at, updated_at
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

// CreatePage creates a new page in a library.
func (m *Manager) CreatePage(ctx context.Context, libraryID string, req CreatePageRequest, createdBy string) (*Page, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return nil, fmt.Errorf("name is required")
	}

	var id string
	err := m.pg.QueryRow(ctx, `
		INSERT INTO instruction_pages (library_id, name, description, content, always_include, sort_order, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`, libraryID, name, req.Description, req.Content, req.AlwaysInclude, req.SortOrder, createdBy).Scan(&id)
	if err != nil {
		if strings.Contains(err.Error(), "ip_unique_name") {
			return nil, fmt.Errorf("a page named %q already exists in this library", name)
		}
		return nil, fmt.Errorf("create page: %w", err)
	}
	return m.GetPage(ctx, id)
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

// GetConversationLibraries returns libraries linked to a conversation.
func (m *Manager) GetConversationLibraries(ctx context.Context, conversationID string) ([]*Library, error) {
	query := `
		SELECT l.id, l.name, l.description, l.is_default,
		       COALESCE(l.fractal_id::text, ''), COALESCE(l.prism_id::text, ''),
		       l.source, l.repo_url, l.branch, l.path, l.auth_token,
		       l.sync_schedule, l.last_synced_at, l.last_sync_status, l.last_sync_page_count,
		       COALESCE(l.created_by, ''), l.created_at, l.updated_at,
		       (SELECT COUNT(*) FROM instruction_pages p WHERE p.library_id = l.id) as page_count
		FROM instruction_libraries l
		JOIN conversation_libraries cl ON cl.library_id = l.id
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
		SELECT library_id FROM conversation_libraries WHERE conversation_id = $1
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

// SetConversationLibraries replaces the library associations for a conversation.
func (m *Manager) SetConversationLibraries(ctx context.Context, conversationID string, libraryIDs []string) error {
	// Delete existing associations
	if _, err := m.pg.Exec(ctx, `DELETE FROM conversation_libraries WHERE conversation_id = $1`, conversationID); err != nil {
		return fmt.Errorf("clear conversation libraries: %w", err)
	}

	// Insert new associations
	for _, libID := range libraryIDs {
		if _, err := m.pg.Exec(ctx, `
			INSERT INTO conversation_libraries (conversation_id, library_id) VALUES ($1, $2)
			ON CONFLICT DO NOTHING
		`, conversationID, libID); err != nil {
			return fmt.Errorf("set conversation library: %w", err)
		}
	}
	return nil
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
		       source_path, source_hash, COALESCE(created_by, ''), created_at, updated_at
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
		return nil, fmt.Errorf("scan library: %w", err)
	}
	if lastSyncedAt.Valid {
		lib.LastSyncedAt = &lastSyncedAt.Time
	}
	lib.HasAuthToken = authToken != ""
	return &lib, nil
}

func scanLibraryRow(row *sql.Row) (*Library, error) {
	var lib Library
	var lastSyncedAt sql.NullTime
	var authToken string
	err := row.Scan(
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
	lib.HasAuthToken = authToken != ""
	return &lib, nil
}

func scanPage(s scannable) (*Page, error) {
	var p Page
	err := s.Scan(
		&p.ID, &p.LibraryID, &p.Name, &p.Description, &p.Content,
		&p.AlwaysInclude, &p.SortOrder,
		&p.SourcePath, &p.SourceHash, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan page: %w", err)
	}
	return &p, nil
}

func scanPageRow(row *sql.Row) (*Page, error) {
	var p Page
	err := row.Scan(
		&p.ID, &p.LibraryID, &p.Name, &p.Description, &p.Content,
		&p.AlwaysInclude, &p.SortOrder,
		&p.SourcePath, &p.SourceHash, &p.CreatedBy, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, err
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
