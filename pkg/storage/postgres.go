package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

type PostgresClient struct {
	db *sql.DB
}

type User struct {
	Username            string     `json:"username"`
	PasswordHash        string     `json:"-"`
	DisplayName         string     `json:"display_name"`
	GravatarColor       string     `json:"gravatar_color"`
	GravatarInitial     string     `json:"gravatar_initial"`
	CreatedAt           time.Time  `json:"created_at"`
	LastLogin           *time.Time `json:"last_login,omitempty"`
	IsAdmin             bool       `json:"is_admin"`
	InvitePending       bool       `json:"invite_pending"`
	AuthProvider        string     `json:"auth_provider"`
	OIDCSubject         string     `json:"-"`
	ForcePasswordChange bool       `json:"force_password_change,omitempty"`
}

type Comment struct {
	ID                    string    `json:"id"`
	LogID                 string    `json:"log_id"`
	LogTimestamp          time.Time `json:"log_timestamp"`
	Text                  string    `json:"text"`
	Author                string    `json:"author"`
	AuthorDisplayName     string    `json:"author_display_name"`
	AuthorGravatarColor   string    `json:"author_gravatar_color"`
	AuthorGravatarInitial string    `json:"author_gravatar_initial"`
	Tags                  []string  `json:"tags"`
	Query                 string    `json:"query"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
	FractalID             string    `json:"fractal_id"` // Fractal UUID for multi-tenant isolation
}

func (c *PostgresClient) Initialize(ctx context.Context, initSQL string) error {
	// Use an advisory lock so that when multiple replicas start simultaneously,
	// only one runs the schema initialization at a time. This prevents race
	// conditions on CREATE TYPE and other non-idempotent DDL.
	const schemaLockID int64 = 0x6269667261637400 // "bifract\0"
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection for schema lock: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", schemaLockID); err != nil {
		return fmt.Errorf("failed to acquire schema lock: %w", err)
	}
	defer conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", schemaLockID)

	// Always run the full SQL - all statements use IF NOT EXISTS / CREATE OR REPLACE,
	// so this is safe to run on an existing database and picks up new tables/triggers.
	if _, err := conn.ExecContext(ctx, initSQL); err != nil {
		return fmt.Errorf("failed to initialize postgres schema: %w", err)
	}
	return nil
}

func NewPostgresClient(host string, port int, database, user, password string) (*PostgresClient, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, database)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	return &PostgresClient{db: db}, nil
}

func (c *PostgresClient) Close() error {
	return c.db.Close()
}

func (c *PostgresClient) HealthCheck(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// DB returns the underlying database connection for direct access
func (c *PostgresClient) DB() *sql.DB {
	return c.db
}

// User methods

func (c *PostgresClient) GetUser(ctx context.Context, username string) (*User, error) {
	user := &User{}
	var passwordHash sql.NullString
	err := c.db.QueryRowContext(ctx, `
		SELECT username, password_hash, display_name, gravatar_color, gravatar_initial,
		       created_at, last_login, is_admin, COALESCE(auth_provider, 'local'),
		       COALESCE(force_password_change, FALSE)
		FROM users
		WHERE username = $1
	`, username).Scan(
		&user.Username,
		&passwordHash,
		&user.DisplayName,
		&user.GravatarColor,
		&user.GravatarInitial,
		&user.CreatedAt,
		&user.LastLogin,
		&user.IsAdmin,
		&user.AuthProvider,
		&user.ForcePasswordChange,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("user not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	if passwordHash.Valid {
		user.PasswordHash = passwordHash.String
	}

	return user, nil
}

func (c *PostgresClient) CreateUser(ctx context.Context, user User) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO users (username, password_hash, display_name, is_admin)
		VALUES ($1, $2, $3, $4)
	`, user.Username, user.PasswordHash, user.DisplayName, user.IsAdmin)

	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}

	return nil
}

func (c *PostgresClient) UpdateLastLogin(ctx context.Context, username string) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE users
		SET last_login = NOW()
		WHERE username = $1
	`, username)

	if err != nil {
		return fmt.Errorf("failed to update last login: %w", err)
	}

	return nil
}

func (c *PostgresClient) ListUsers(ctx context.Context) ([]User, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT username, display_name, gravatar_color, gravatar_initial,
		       created_at, last_login, is_admin,
		       (invite_token_hash IS NOT NULL) AS invite_pending,
		       COALESCE(auth_provider, 'local')
		FROM users
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
		var lastLogin sql.NullTime
		err := rows.Scan(
			&user.Username,
			&user.DisplayName,
			&user.GravatarColor,
			&user.GravatarInitial,
			&user.CreatedAt,
			&lastLogin,
			&user.IsAdmin,
			&user.InvitePending,
			&user.AuthProvider,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan user: %w", err)
		}
		if lastLogin.Valid {
			user.LastLogin = &lastLogin.Time
		}
		users = append(users, user)
	}

	return users, nil
}

func (c *PostgresClient) DeleteUser(ctx context.Context, username string) error {
	_, err := c.db.ExecContext(ctx, `
		DELETE FROM users WHERE username = $1
	`, username)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}
	return nil
}

// GetUserByOIDCSubject looks up a user by their OIDC subject identifier.
// Returns nil, nil if no user is found (distinct from an error).
func (c *PostgresClient) GetUserByOIDCSubject(ctx context.Context, subject string) (*User, error) {
	user := &User{}
	err := c.db.QueryRowContext(ctx, `
		SELECT username, display_name, gravatar_color, gravatar_initial,
		       created_at, last_login, is_admin, auth_provider, oidc_subject
		FROM users
		WHERE oidc_subject = $1
	`, subject).Scan(
		&user.Username,
		&user.DisplayName,
		&user.GravatarColor,
		&user.GravatarInitial,
		&user.CreatedAt,
		&user.LastLogin,
		&user.IsAdmin,
		&user.AuthProvider,
		&user.OIDCSubject,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get user by OIDC subject: %w", err)
	}
	return user, nil
}

// CreateOIDCUser creates a user provisioned via OIDC (no password).
func (c *PostgresClient) CreateOIDCUser(ctx context.Context, username, displayName, oidcSubject string, isAdmin bool) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO users (username, display_name, is_admin, auth_provider, oidc_subject)
		VALUES ($1, $2, $3, 'oidc', $4)
	`, username, displayName, isAdmin, oidcSubject)
	if err != nil {
		return fmt.Errorf("failed to create OIDC user: %w", err)
	}
	return nil
}

// UpdateUser updates a user's display name and/or role.
func (c *PostgresClient) UpdateUser(ctx context.Context, username string, displayName string, role string) error {
	if displayName != "" && role != "" {
		isAdmin := role == "admin"
		_, err := c.db.ExecContext(ctx,
			`UPDATE users SET display_name = $2, is_admin = $3 WHERE username = $1`,
			username, displayName, isAdmin)
		return err
	} else if displayName != "" {
		_, err := c.db.ExecContext(ctx,
			`UPDATE users SET display_name = $2 WHERE username = $1`,
			username, displayName)
		return err
	} else if role != "" {
		isAdmin := role == "admin"
		_, err := c.db.ExecContext(ctx,
			`UPDATE users SET is_admin = $2 WHERE username = $1`,
			username, isAdmin)
		return err
	}
	return nil
}

// CreateUserWithInvite creates a user with an invite token instead of a password.
// The password_hash is set to a marker value that will never match bcrypt verification.
func (c *PostgresClient) CreateUserWithInvite(ctx context.Context, user User, tokenHash string, expiresAt time.Time) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO users (username, password_hash, display_name, is_admin, invite_token_hash, invite_expires_at)
		VALUES ($1, '!invite', $2, $3, $4, $5)
	`, user.Username, user.DisplayName, user.IsAdmin, tokenHash, expiresAt)
	if err != nil {
		return fmt.Errorf("failed to create user with invite: %w", err)
	}
	return nil
}

// GetUserByInviteToken looks up a user by their invite token hash.
// Returns error if token not found or expired.
func (c *PostgresClient) GetUserByInviteToken(ctx context.Context, tokenHash string) (*User, error) {
	user := &User{}
	err := c.db.QueryRowContext(ctx, `
		SELECT username, display_name, gravatar_color, gravatar_initial,
		       created_at, is_admin
		FROM users
		WHERE invite_token_hash = $1 AND invite_expires_at > NOW()
	`, tokenHash).Scan(
		&user.Username,
		&user.DisplayName,
		&user.GravatarColor,
		&user.GravatarInitial,
		&user.CreatedAt,
		&user.IsAdmin,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("invalid or expired invite token")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to look up invite token: %w", err)
	}
	return user, nil
}

// AcceptInvite sets the user's password and clears the invite token.
func (c *PostgresClient) AcceptInvite(ctx context.Context, tokenHash string, passwordHash string) error {
	result, err := c.db.ExecContext(ctx, `
		UPDATE users
		SET password_hash = $1, invite_token_hash = NULL, invite_expires_at = NULL
		WHERE invite_token_hash = $2 AND invite_expires_at > NOW()
	`, passwordHash, tokenHash)
	if err != nil {
		return fmt.Errorf("failed to accept invite: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("invalid or expired invite token")
	}
	return nil
}

// RegenerateInvite updates the invite token for an existing pending user.
func (c *PostgresClient) RegenerateInvite(ctx context.Context, username string, tokenHash string, expiresAt time.Time) error {
	result, err := c.db.ExecContext(ctx, `
		UPDATE users
		SET invite_token_hash = $1, invite_expires_at = $2
		WHERE username = $3 AND invite_token_hash IS NOT NULL
	`, tokenHash, expiresAt, username)
	if err != nil {
		return fmt.Errorf("failed to regenerate invite: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("user not found or invite already accepted")
	}
	return nil
}

// UpdatePasswordHash updates the password hash for an existing user
// and clears any force_password_change flag.
func (c *PostgresClient) UpdatePasswordHash(ctx context.Context, username, passwordHash string) error {
	result, err := c.db.ExecContext(ctx, `
		UPDATE users SET password_hash = $1, force_password_change = FALSE WHERE username = $2
	`, passwordHash, username)
	if err != nil {
		return fmt.Errorf("failed to update password: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("user not found")
	}
	return nil
}

// ResetUserToInvite resets an existing user's password to invite state,
// replacing their password hash with a placeholder and setting a new invite token.
func (c *PostgresClient) ResetUserToInvite(ctx context.Context, username, tokenHash string, expiresAt time.Time) error {
	result, err := c.db.ExecContext(ctx, `
		UPDATE users
		SET password_hash = '!invite', invite_token_hash = $1, invite_expires_at = $2
		WHERE username = $3 AND COALESCE(auth_provider, 'local') != 'oidc'
	`, tokenHash, expiresAt, username)
	if err != nil {
		return fmt.Errorf("failed to reset user password: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("user not found or uses SSO")
	}
	return nil
}

// Comment methods

func (c *PostgresClient) InsertComment(ctx context.Context, comment Comment) (*Comment, error) {
	// Convert empty slice to nil for proper database handling
	var tags interface{}
	if len(comment.Tags) == 0 {
		tags = nil
	} else {
		tags = pq.Array(comment.Tags)
	}

	var newComment Comment
	err := c.db.QueryRowContext(ctx, `
		INSERT INTO comments (log_id, log_timestamp, text, author, tags, query, fractal_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, log_id, log_timestamp, text, author, tags, query, created_at, updated_at, fractal_id
	`, comment.LogID, comment.LogTimestamp, comment.Text, comment.Author, tags, comment.Query, comment.FractalID).Scan(
		&newComment.ID,
		&newComment.LogID,
		&newComment.LogTimestamp,
		&newComment.Text,
		&newComment.Author,
		pq.Array(&newComment.Tags),
		&newComment.Query,
		&newComment.CreatedAt,
		&newComment.UpdatedAt,
		&newComment.FractalID,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to insert comment: %w", err)
	}

	// Fetch author details
	author, err := c.GetUser(ctx, newComment.Author)
	if err == nil {
		newComment.AuthorDisplayName = author.DisplayName
		newComment.AuthorGravatarColor = author.GravatarColor
		newComment.AuthorGravatarInitial = author.GravatarInitial
	}

	return &newComment, nil
}

func (c *PostgresClient) GetComment(ctx context.Context, id string) (*Comment, error) {
	comment := &Comment{}
	err := c.db.QueryRowContext(ctx, `
		SELECT c.id, c.log_id, c.log_timestamp, c.text, c.author, c.tags, c.query, c.created_at, c.updated_at, c.fractal_id,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM comments c
		JOIN users u ON c.author = u.username
		WHERE c.id = $1
	`, id).Scan(
		&comment.ID,
		&comment.LogID,
		&comment.LogTimestamp,
		&comment.Text,
		&comment.Author,
		pq.Array(&comment.Tags),
		&comment.Query,
		&comment.CreatedAt,
		&comment.UpdatedAt,
		&comment.FractalID,
		&comment.AuthorDisplayName,
		&comment.AuthorGravatarColor,
		&comment.AuthorGravatarInitial,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("comment not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get comment: %w", err)
	}

	return comment, nil
}

func (c *PostgresClient) GetCommentsByLogID(ctx context.Context, logID string) ([]Comment, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT c.id, c.log_id, c.log_timestamp, c.text, c.author, c.tags, c.query, c.created_at, c.updated_at, c.fractal_id,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM comments c
		JOIN users u ON c.author = u.username
		WHERE c.log_id = $1
		ORDER BY c.created_at ASC
	`, logID)
	if err != nil {
		return nil, fmt.Errorf("failed to get comments by log ID: %w", err)
	}
	defer rows.Close()

	var comments []Comment
	for rows.Next() {
		var comment Comment
		err := rows.Scan(
			&comment.ID,
			&comment.LogID,
			&comment.LogTimestamp,
			&comment.Text,
			&comment.Author,
			pq.Array(&comment.Tags),
			&comment.Query,
			&comment.CreatedAt,
			&comment.UpdatedAt,
			&comment.FractalID,
			&comment.AuthorDisplayName,
			&comment.AuthorGravatarColor,
			&comment.AuthorGravatarInitial,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan comment: %w", err)
		}
		comments = append(comments, comment)
	}

	return comments, nil
}

// GetCommentsByLogIDAndFractal gets comments for a log within a specific fractal
func (c *PostgresClient) GetCommentsByLogIDAndFractal(ctx context.Context, logID, fractalID string) ([]Comment, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT c.id, c.log_id, c.log_timestamp, c.text, c.author, c.tags, c.query, c.created_at, c.updated_at, c.fractal_id,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM comments c
		JOIN users u ON c.author = u.username
		WHERE c.log_id = $1 AND c.fractal_id = $2
		ORDER BY c.created_at ASC
	`, logID, fractalID)
	if err != nil {
		return nil, fmt.Errorf("failed to get comments by log ID and fractal: %w", err)
	}
	defer rows.Close()

	var comments []Comment
	for rows.Next() {
		var comment Comment
		err := rows.Scan(
			&comment.ID,
			&comment.LogID,
			&comment.LogTimestamp,
			&comment.Text,
			&comment.Author,
			pq.Array(&comment.Tags),
			&comment.Query,
			&comment.CreatedAt,
			&comment.UpdatedAt,
			&comment.FractalID,
			&comment.AuthorDisplayName,
			&comment.AuthorGravatarColor,
			&comment.AuthorGravatarInitial,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan comment: %w", err)
		}
		comments = append(comments, comment)
	}

	return comments, nil
}

// GetCommentsByTagAndFractal returns all comments with the given tag in a fractal, ordered by log event time.
func (c *PostgresClient) GetCommentsByTagAndFractal(ctx context.Context, fractalID, tag string) ([]Comment, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT c.id, c.log_id, c.log_timestamp, c.text, c.author, c.tags, c.query, c.created_at, c.updated_at, c.fractal_id,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM comments c
		JOIN users u ON c.author = u.username
		WHERE c.fractal_id = $1 AND c.tags @> ARRAY[$2]::text[]
		ORDER BY c.log_timestamp ASC, c.created_at ASC
	`, fractalID, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get comments by tag and fractal: %w", err)
	}
	defer rows.Close()

	var comments []Comment
	for rows.Next() {
		var comment Comment
		err := rows.Scan(
			&comment.ID,
			&comment.LogID,
			&comment.LogTimestamp,
			&comment.Text,
			&comment.Author,
			pq.Array(&comment.Tags),
			&comment.Query,
			&comment.CreatedAt,
			&comment.UpdatedAt,
			&comment.FractalID,
			&comment.AuthorDisplayName,
			&comment.AuthorGravatarColor,
			&comment.AuthorGravatarInitial,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan comment: %w", err)
		}
		comments = append(comments, comment)
	}

	return comments, nil
}

// GetDistinctTagsByFractal returns all unique tags used in comments for a fractal.
func (c *PostgresClient) GetDistinctTagsByFractal(ctx context.Context, fractalID string) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT DISTINCT unnest(tags) AS tag
		FROM comments
		WHERE fractal_id = $1 AND tags IS NOT NULL AND array_length(tags, 1) > 0
		ORDER BY tag
	`, fractalID)
	if err != nil {
		return nil, fmt.Errorf("failed to get distinct tags: %w", err)
	}
	defer rows.Close()

	var tags []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, fmt.Errorf("failed to scan tag: %w", err)
		}
		tags = append(tags, tag)
	}
	return tags, nil
}

func (c *PostgresClient) UpdateComment(ctx context.Context, id string, authorUsername string, text string, tags []string) error {
	// Convert empty slice to nil for proper database handling
	var tagsVal interface{}
	if len(tags) == 0 {
		tagsVal = nil
	} else {
		tagsVal = pq.Array(tags)
	}

	result, err := c.db.ExecContext(ctx, `
		UPDATE comments
		SET text = $1, tags = $2
		WHERE id = $3 AND author = $4
	`, text, tagsVal, id, authorUsername)

	if err != nil {
		return fmt.Errorf("failed to update comment: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("comment not found or user not authorized")
	}

	return nil
}

func (c *PostgresClient) DeleteComment(ctx context.Context, id string, authorUsername string) error {
	result, err := c.db.ExecContext(ctx, `
		DELETE FROM comments
		WHERE id = $1 AND author = $2
	`, id, authorUsername)

	if err != nil {
		return fmt.Errorf("failed to delete comment: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("comment not found or user not authorized")
	}

	return nil
}

// DeleteCommentsByLogID deletes all comments associated with a specific log_id
func (c *PostgresClient) DeleteCommentsByLogID(ctx context.Context, logID string) error {
	result, err := c.db.ExecContext(ctx, `
		DELETE FROM comments
		WHERE log_id = $1
	`, logID)

	if err != nil {
		return fmt.Errorf("failed to delete comments for log_id %s: %w", logID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	// Log the number of comments deleted (but don't error if no comments found)
	if rowsAffected > 0 {
		fmt.Printf("Deleted %d comments for log_id: %s\n", rowsAffected, logID)
	}

	return nil
}

// DeleteCommentsByLogIDs deletes all comments associated with multiple log_ids
func (c *PostgresClient) DeleteCommentsByLogIDs(ctx context.Context, logIDs []string) error {
	if len(logIDs) == 0 {
		return nil
	}

	result, err := c.db.ExecContext(ctx, `
		DELETE FROM comments
		WHERE log_id = ANY($1)
	`, pq.Array(logIDs))

	if err != nil {
		return fmt.Errorf("failed to delete comments for %d log_ids: %w", len(logIDs), err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	// Log the number of comments deleted
	if rowsAffected > 0 {
		fmt.Printf("Deleted %d comments for %d log_ids\n", rowsAffected, len(logIDs))
	}

	return nil
}

// DeleteAllComments deletes all comments (used when clearing all logs)
func (c *PostgresClient) DeleteAllComments(ctx context.Context) error {
	result, err := c.db.ExecContext(ctx, "DELETE FROM comments")

	if err != nil {
		return fmt.Errorf("failed to delete all comments: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	// Log the number of comments deleted
	if rowsAffected > 0 {
		fmt.Printf("Deleted %d comments during log cleanup\n", rowsAffected)
	}

	return nil
}

// DeleteCommentsByFractalID deletes all comments for a specific fractal
func (c *PostgresClient) DeleteCommentsByFractalID(ctx context.Context, fractalID string) error {
	result, err := c.db.ExecContext(ctx, "DELETE FROM comments WHERE fractal_id = $1", fractalID)

	if err != nil {
		return fmt.Errorf("failed to delete comments for fractal %s: %w", fractalID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	// Log the number of comments deleted
	if rowsAffected > 0 {
		fmt.Printf("Deleted %d comments for fractal %s during log cleanup\n", rowsAffected, fractalID)
	}

	return nil
}

func (c *PostgresClient) GetCommentedLogIDs(ctx context.Context, start, end time.Time) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT DISTINCT log_id
		FROM comments
		WHERE log_timestamp >= $1 AND log_timestamp <= $2
		ORDER BY log_id
	`, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to get commented log IDs: %w", err)
	}
	defer rows.Close()

	var logIDs []string
	for rows.Next() {
		var logID string
		err := rows.Scan(&logID)
		if err != nil {
			return nil, fmt.Errorf("failed to scan log ID: %w", err)
		}
		logIDs = append(logIDs, logID)
	}

	return logIDs, nil
}

// GetCommentedLogIDsFiltered returns distinct log_ids matching the given comment filters.
// Used by the comment() BQL function to resolve log_ids at query time.
func (c *PostgresClient) GetCommentedLogIDsFiltered(ctx context.Context, fractalID string, fractalIDs []string, start, end time.Time, tags []string, keyword string) ([]string, error) {
	query := "SELECT DISTINCT log_id FROM comments WHERE log_timestamp >= $1 AND log_timestamp <= $2"
	args := []interface{}{start, end}
	argIdx := 3

	// Fractal scoping
	if len(fractalIDs) > 0 {
		query += fmt.Sprintf(" AND fractal_id = ANY($%d)", argIdx)
		args = append(args, pq.Array(fractalIDs))
		argIdx++
	} else if fractalID != "" {
		query += fmt.Sprintf(" AND fractal_id = $%d", argIdx)
		args = append(args, fractalID)
		argIdx++
	}

	// Tag filter (OR logic: at least one tag must match)
	if len(tags) > 0 {
		query += fmt.Sprintf(" AND tags && $%d", argIdx)
		args = append(args, pq.Array(tags))
		argIdx++
	}

	// Keyword filter (case-insensitive substring match on comment text)
	if keyword != "" {
		query += fmt.Sprintf(" AND text ILIKE $%d", argIdx)
		args = append(args, "%"+keyword+"%")
		argIdx++
	}

	query += " ORDER BY log_id LIMIT 50000"

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get filtered commented log IDs: %w", err)
	}
	defer rows.Close()

	var logIDs []string
	for rows.Next() {
		var logID string
		if err := rows.Scan(&logID); err != nil {
			return nil, fmt.Errorf("failed to scan log ID: %w", err)
		}
		logIDs = append(logIDs, logID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate comment log IDs: %w", err)
	}

	return logIDs, nil
}

func (c *PostgresClient) GetAllCommentedLogs(ctx context.Context, limit, offset int) ([]map[string]interface{}, int, error) {
	// First get total count
	var total int
	err := c.db.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT log_id) FROM comments
	`).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get total count: %w", err)
	}

	// Get paginated results
	rows, err := c.db.QueryContext(ctx, `
		SELECT
			c.log_id,
			c.log_timestamp,
			json_agg(json_build_object(
				'id', c.id,
				'text', c.text,
				'author', c.author,
				'author_display_name', u.display_name,
				'author_gravatar_color', u.gravatar_color,
				'author_gravatar_initial', u.gravatar_initial,
				'tags', c.tags,
				'query', c.query,
				'created_at', c.created_at,
				'updated_at', c.updated_at
			) ORDER BY c.created_at ASC) as comments
		FROM comments c
		JOIN users u ON c.author = u.username
		GROUP BY c.log_id, c.log_timestamp
		ORDER BY c.log_timestamp DESC
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get commented logs: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var logID string
		var logTimestamp time.Time
		var commentsJSON []byte

		err := rows.Scan(&logID, &logTimestamp, &commentsJSON)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan commented log: %w", err)
		}

		results = append(results, map[string]interface{}{
			"log_id":        logID,
			"log_timestamp": logTimestamp,
			"comments_json": string(commentsJSON),
			"tags":          []string{}, // Empty since we removed tags from UI
		})
	}

	return results, total, nil
}

// GetAllCommentedLogsByFractal gets all logs that have comments within a specific fractal
func (c *PostgresClient) GetAllCommentedLogsByFractal(ctx context.Context, fractalID string, limit, offset int) ([]map[string]interface{}, int, error) {
	// First get total count for the specific fractal
	var total int
	err := c.db.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT log_id) FROM comments WHERE fractal_id = $1
	`, fractalID).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get total count: %w", err)
	}

	// Get paginated results filtered by fractal
	rows, err := c.db.QueryContext(ctx, `
		SELECT
			c.log_id,
			c.log_timestamp,
			json_agg(json_build_object(
				'id', c.id,
				'text', c.text,
				'author', c.author,
				'author_display_name', u.display_name,
				'author_gravatar_color', u.gravatar_color,
				'author_gravatar_initial', u.gravatar_initial,
				'tags', c.tags,
				'query', c.query,
				'created_at', c.created_at,
				'updated_at', c.updated_at
			) ORDER BY c.created_at ASC) as comments
		FROM comments c
		JOIN users u ON c.author = u.username
		WHERE c.fractal_id = $1
		GROUP BY c.log_id, c.log_timestamp
		ORDER BY c.log_timestamp DESC
		LIMIT $2 OFFSET $3
	`, fractalID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get commented logs: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var logID string
		var logTimestamp time.Time
		var commentsJSON []byte

		err := rows.Scan(&logID, &logTimestamp, &commentsJSON)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan commented log: %w", err)
		}

		results = append(results, map[string]interface{}{
			"log_id":        logID,
			"log_timestamp": logTimestamp,
			"comments_json": string(commentsJSON),
			"tags":          []string{}, // Empty since we removed tags from UI
		})
	}

	if err = rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed to iterate commented logs: %w", err)
	}

	return results, total, nil
}

// GetAllCommentsByFractal returns individual comments for a fractal with author display info.
func (c *PostgresClient) GetAllCommentsByFractal(ctx context.Context, fractalID string, limit, offset int) ([]Comment, int, error) {
	var total int
	err := c.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM comments WHERE fractal_id = $1
	`, fractalID).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get total count: %w", err)
	}

	rows, err := c.db.QueryContext(ctx, `
		SELECT c.id, c.log_id, c.log_timestamp, c.text, c.author, c.tags, c.query,
		       c.created_at, c.updated_at, c.fractal_id,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM comments c
		JOIN users u ON c.author = u.username
		WHERE c.fractal_id = $1
		ORDER BY c.created_at DESC
		LIMIT $2 OFFSET $3
	`, fractalID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get comments: %w", err)
	}
	defer rows.Close()

	var comments []Comment
	for rows.Next() {
		var comment Comment
		var tags pq.StringArray
		err := rows.Scan(
			&comment.ID, &comment.LogID, &comment.LogTimestamp, &comment.Text,
			&comment.Author, &tags, &comment.Query,
			&comment.CreatedAt, &comment.UpdatedAt, &comment.FractalID,
			&comment.AuthorDisplayName, &comment.AuthorGravatarColor, &comment.AuthorGravatarInitial,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan comment: %w", err)
		}
		comment.Tags = tags
		comments = append(comments, comment)
	}

	if err = rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("failed to iterate comments: %w", err)
	}

	return comments, total, nil
}

// BulkAddTagToComments adds a tag to multiple comments by ID.
// Non-admin users can only modify comments they authored.
func (c *PostgresClient) BulkAddTagToComments(ctx context.Context, commentIDs []string, tag string, authorUsername string, isAdmin bool) (int64, error) {
	var result sql.Result
	var err error
	if isAdmin {
		result, err = c.db.ExecContext(ctx, `
			UPDATE comments
			SET tags = array_append(tags, $1), updated_at = NOW()
			WHERE id = ANY($2) AND NOT ($1 = ANY(COALESCE(tags, '{}')))
		`, tag, pq.Array(commentIDs))
	} else {
		result, err = c.db.ExecContext(ctx, `
			UPDATE comments
			SET tags = array_append(tags, $1), updated_at = NOW()
			WHERE id = ANY($2) AND author = $3 AND NOT ($1 = ANY(COALESCE(tags, '{}')))
		`, tag, pq.Array(commentIDs), authorUsername)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to bulk add tag: %w", err)
	}
	return result.RowsAffected()
}

// BulkRemoveTagFromComments removes a tag from multiple comments by ID.
// Non-admin users can only modify comments they authored.
func (c *PostgresClient) BulkRemoveTagFromComments(ctx context.Context, commentIDs []string, tag string, authorUsername string, isAdmin bool) (int64, error) {
	var result sql.Result
	var err error
	if isAdmin {
		result, err = c.db.ExecContext(ctx, `
			UPDATE comments
			SET tags = array_remove(tags, $1), updated_at = NOW()
			WHERE id = ANY($2)
		`, tag, pq.Array(commentIDs))
	} else {
		result, err = c.db.ExecContext(ctx, `
			UPDATE comments
			SET tags = array_remove(tags, $1), updated_at = NOW()
			WHERE id = ANY($2) AND author = $3
		`, tag, pq.Array(commentIDs), authorUsername)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to bulk remove tag: %w", err)
	}
	return result.RowsAffected()
}

// BulkDeleteComments deletes multiple comments by ID.
// Non-admin users can only delete comments they authored.
func (c *PostgresClient) BulkDeleteComments(ctx context.Context, commentIDs []string, authorUsername string, isAdmin bool) (int64, error) {
	var result sql.Result
	var err error
	if isAdmin {
		result, err = c.db.ExecContext(ctx, `
			DELETE FROM comments WHERE id = ANY($1)
		`, pq.Array(commentIDs))
	} else {
		result, err = c.db.ExecContext(ctx, `
			DELETE FROM comments WHERE id = ANY($1) AND author = $2
		`, pq.Array(commentIDs), authorUsername)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to bulk delete comments: %w", err)
	}
	return result.RowsAffected()
}

// Settings methods

func (c *PostgresClient) GetSetting(ctx context.Context, key string) (string, error) {
	var value string
	err := c.db.QueryRowContext(ctx, `
		SELECT value FROM settings WHERE key = $1
	`, key).Scan(&value)

	if err == sql.ErrNoRows {
		return "", fmt.Errorf("setting not found")
	}
	if err != nil {
		return "", fmt.Errorf("failed to get setting: %w", err)
	}

	return value, nil
}

func (c *PostgresClient) SetSetting(ctx context.Context, key string, value string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO settings (key, value, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()
	`, key, value)

	if err != nil {
		return fmt.Errorf("failed to set setting: %w", err)
	}

	return nil
}

// Transaction interface for database transactions
type Tx interface {
	Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row
	Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// PostgresTx wraps sql.Tx to implement our Tx interface
type PostgresTx struct {
	tx *sql.Tx
}

func (ptx *PostgresTx) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return ptx.tx.QueryContext(ctx, query, args...)
}

func (ptx *PostgresTx) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return ptx.tx.QueryRowContext(ctx, query, args...)
}

func (ptx *PostgresTx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return ptx.tx.ExecContext(ctx, query, args...)
}

func (ptx *PostgresTx) Commit(ctx context.Context) error {
	return ptx.tx.Commit()
}

func (ptx *PostgresTx) Rollback(ctx context.Context) error {
	return ptx.tx.Rollback()
}

// Begin starts a new database transaction
func (c *PostgresClient) Begin(ctx context.Context) (Tx, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &PostgresTx{tx: tx}, nil
}

// Query executes a query that returns rows
func (c *PostgresClient) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query, args...)
}

// QueryRow executes a query that returns at most one row
func (c *PostgresClient) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.db.QueryRowContext(ctx, query, args...)
}

// Exec executes a query without returning any rows
func (c *PostgresClient) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}

// TryAdvisoryLock attempts to acquire a PostgreSQL session-level advisory lock
// using a dedicated connection. Returns the unlock function (must be called to
// release) and true if the lock was acquired, or nil and false if another
// session already holds it. The lock is automatically released if the returned
// connection is closed (e.g. on process crash).
func (c *PostgresClient) TryAdvisoryLock(ctx context.Context, lockID int64) (unlock func(), acquired bool) {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return nil, false
	}
	var ok bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, lockID).Scan(&ok); err != nil {
		conn.Close()
		return nil, false
	}
	if !ok {
		conn.Close()
		return nil, false
	}
	return func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer unlockCancel()
		var unlocked bool
		conn.QueryRowContext(unlockCtx, `SELECT pg_advisory_unlock($1)`, lockID).Scan(&unlocked)
		conn.Close()
	}, true
}

// ============================
// Notebooks Database Operations
// ============================

// Notebook represents a notebook document
type Notebook struct {
	ID                    string     `json:"id"`
	Name                  string     `json:"name"`
	Description           string     `json:"description"`
	TimeRangeType         string     `json:"time_range_type"`
	TimeRangeStart        *time.Time `json:"time_range_start,omitempty"`
	TimeRangeEnd          *time.Time `json:"time_range_end,omitempty"`
	MaxResultsPerSection  int        `json:"max_results_per_section"`
	FractalID             string          `json:"fractal_id,omitempty"`
	PrismID               string          `json:"prism_id,omitempty"`
	Variables             json.RawMessage `json:"variables"`
	CreatedBy             string          `json:"created_by"`
	AuthorDisplayName     string          `json:"author_display_name"`
	AuthorGravatarColor   string          `json:"author_gravatar_color"`
	AuthorGravatarInitial string          `json:"author_gravatar_initial"`
	CreatedAt             time.Time       `json:"created_at"`
	UpdatedAt             time.Time       `json:"updated_at"`
}

// NotebookSection represents a section within a notebook
type NotebookSection struct {
	ID              string      `json:"id"`
	NotebookID      string      `json:"notebook_id"`
	SectionType     string      `json:"section_type"`
	Title           *string     `json:"title,omitempty"`
	Content         string      `json:"content"`
	RenderedContent *string     `json:"rendered_content,omitempty"`
	OrderIndex      int         `json:"order_index"`
	LastExecutedAt  *time.Time  `json:"last_executed_at,omitempty"`
	LastResults     json.RawMessage `json:"last_results,omitempty"`
	ChartType       *string         `json:"chart_type,omitempty"`
	ChartConfig     json.RawMessage `json:"chart_config,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

// NotebookPresence represents a user's presence in a notebook
type NotebookPresence struct {
	NotebookID          string    `json:"notebook_id"`
	Username            string    `json:"username"`
	LastSeenAt          time.Time `json:"last_seen_at"`
	UserDisplayName     string    `json:"user_display_name"`
	UserGravatarColor   string    `json:"user_gravatar_color"`
	UserGravatarInitial string    `json:"user_gravatar_initial"`
}

// InsertNotebook creates a new notebook (scoped to fractal or prism; set one, leave other empty).
func (c *PostgresClient) InsertNotebook(ctx context.Context, notebook Notebook) (*Notebook, error) {
	var fractalIDPtr, prismIDPtr interface{}
	if notebook.PrismID != "" {
		prismIDPtr = notebook.PrismID
	} else {
		fractalIDPtr = notebook.FractalID
	}

	varsJSON := notebook.Variables
	if varsJSON == nil {
		varsJSON = json.RawMessage("[]")
	}

	var newNotebook Notebook
	var scanFractalID, scanPrismID sql.NullString
	err := c.db.QueryRowContext(ctx, `
		INSERT INTO notebooks (name, description, time_range_type, time_range_start, time_range_end, max_results_per_section, fractal_id, prism_id, variables, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
		RETURNING id, name, description, time_range_type, time_range_start, time_range_end, max_results_per_section, fractal_id, prism_id, COALESCE(variables, '[]'), created_by, created_at, updated_at
	`, notebook.Name, notebook.Description, notebook.TimeRangeType, notebook.TimeRangeStart, notebook.TimeRangeEnd, notebook.MaxResultsPerSection, fractalIDPtr, prismIDPtr, string(varsJSON), notebook.CreatedBy).Scan(
		&newNotebook.ID,
		&newNotebook.Name,
		&newNotebook.Description,
		&newNotebook.TimeRangeType,
		&newNotebook.TimeRangeStart,
		&newNotebook.TimeRangeEnd,
		&newNotebook.MaxResultsPerSection,
		&scanFractalID,
		&scanPrismID,
		&newNotebook.Variables,
		&newNotebook.CreatedBy,
		&newNotebook.CreatedAt,
		&newNotebook.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert notebook: %w", err)
	}
	newNotebook.FractalID = scanFractalID.String
	newNotebook.PrismID = scanPrismID.String

	author, err := c.GetUser(ctx, newNotebook.CreatedBy)
	if err == nil {
		newNotebook.AuthorDisplayName = author.DisplayName
		newNotebook.AuthorGravatarColor = author.GravatarColor
		newNotebook.AuthorGravatarInitial = author.GravatarInitial
	}

	return &newNotebook, nil
}

// GetNotebook retrieves a notebook by ID
func (c *PostgresClient) GetNotebook(ctx context.Context, id string) (*Notebook, error) {
	var notebook Notebook
	err := c.db.QueryRowContext(ctx, `
		SELECT n.id, n.name, COALESCE(n.description, ''), n.time_range_type, n.time_range_start, n.time_range_end,
		       n.max_results_per_section, COALESCE(n.fractal_id::text, ''), COALESCE(n.prism_id::text, ''),
		       COALESCE(n.variables, '[]'), n.created_by, n.created_at, n.updated_at,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM notebooks n
		JOIN users u ON n.created_by = u.username
		WHERE n.id = $1
	`, id).Scan(
		&notebook.ID,
		&notebook.Name,
		&notebook.Description,
		&notebook.TimeRangeType,
		&notebook.TimeRangeStart,
		&notebook.TimeRangeEnd,
		&notebook.MaxResultsPerSection,
		&notebook.FractalID,
		&notebook.PrismID,
		&notebook.Variables,
		&notebook.CreatedBy,
		&notebook.CreatedAt,
		&notebook.UpdatedAt,
		&notebook.AuthorDisplayName,
		&notebook.AuthorGravatarColor,
		&notebook.AuthorGravatarInitial,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("notebook not found")
		}
		return nil, fmt.Errorf("failed to get notebook: %w", err)
	}

	return &notebook, nil
}

// GetNotebookByNameAndFractal finds a notebook by exact name within a fractal.
// Returns nil, nil if not found.
func (c *PostgresClient) GetNotebookByNameAndFractal(ctx context.Context, name, fractalID string) (*Notebook, error) {
	var notebook Notebook
	var scanPrismID sql.NullString
	err := c.db.QueryRowContext(ctx, `
		SELECT id, name, description, time_range_type, time_range_start, time_range_end,
		       max_results_per_section, fractal_id, prism_id, COALESCE(variables, '[]'),
		       created_by, created_at, updated_at
		FROM notebooks
		WHERE name = $1 AND fractal_id = $2
	`, name, fractalID).Scan(
		&notebook.ID,
		&notebook.Name,
		&notebook.Description,
		&notebook.TimeRangeType,
		&notebook.TimeRangeStart,
		&notebook.TimeRangeEnd,
		&notebook.MaxResultsPerSection,
		&notebook.FractalID,
		&scanPrismID,
		&notebook.Variables,
		&notebook.CreatedBy,
		&notebook.CreatedAt,
		&notebook.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get notebook by name: %w", err)
	}
	notebook.PrismID = scanPrismID.String

	return &notebook, nil
}

// DeleteNotebookByID deletes a notebook by ID without author check.
// Caller must verify authorization before calling.
func (c *PostgresClient) DeleteNotebookByID(ctx context.Context, id string) error {
	result, err := c.db.ExecContext(ctx, `DELETE FROM notebooks WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("failed to delete notebook: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("notebook not found")
	}

	return nil
}

// GetNotebooksByFractal retrieves notebooks for a specific fractal with pagination
func (c *PostgresClient) GetNotebooksByFractal(ctx context.Context, fractalID string, limit, offset int) ([]Notebook, int, error) {
	// Get total count
	var total int
	err := c.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM notebooks WHERE fractal_id = $1
	`, fractalID).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count notebooks: %w", err)
	}

	// Get notebooks with pagination
	rows, err := c.db.QueryContext(ctx, `
		SELECT n.id, n.name, n.description, n.time_range_type, n.time_range_start, n.time_range_end,
		       n.max_results_per_section, n.fractal_id, COALESCE(n.variables, '[]'),
		       n.created_by, n.created_at, n.updated_at,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM notebooks n
		JOIN users u ON n.created_by = u.username
		WHERE n.fractal_id = $1
		ORDER BY n.updated_at DESC
		LIMIT $2 OFFSET $3
	`, fractalID, limit, offset)

	if err != nil {
		return nil, 0, fmt.Errorf("failed to query notebooks: %w", err)
	}
	defer rows.Close()

	var notebooks []Notebook
	for rows.Next() {
		var notebook Notebook
		err := rows.Scan(
			&notebook.ID,
			&notebook.Name,
			&notebook.Description,
			&notebook.TimeRangeType,
			&notebook.TimeRangeStart,
			&notebook.TimeRangeEnd,
			&notebook.MaxResultsPerSection,
			&notebook.FractalID,
			&notebook.Variables,
			&notebook.CreatedBy,
			&notebook.CreatedAt,
			&notebook.UpdatedAt,
			&notebook.AuthorDisplayName,
			&notebook.AuthorGravatarColor,
			&notebook.AuthorGravatarInitial,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan notebook: %w", err)
		}
		notebooks = append(notebooks, notebook)
	}

	return notebooks, total, nil
}

// GetNotebooksByPrism retrieves notebooks scoped to a prism with pagination.
func (c *PostgresClient) GetNotebooksByPrism(ctx context.Context, prismID string, limit, offset int) ([]Notebook, int, error) {
	var total int
	err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM notebooks WHERE prism_id = $1`, prismID).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count notebooks: %w", err)
	}

	rows, err := c.db.QueryContext(ctx, `
		SELECT n.id, n.name, n.description, n.time_range_type, n.time_range_start, n.time_range_end,
		       n.max_results_per_section, n.prism_id, COALESCE(n.variables, '[]'),
		       n.created_by, n.created_at, n.updated_at,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM notebooks n
		JOIN users u ON n.created_by = u.username
		WHERE n.prism_id = $1
		ORDER BY n.updated_at DESC
		LIMIT $2 OFFSET $3
	`, prismID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query notebooks: %w", err)
	}
	defer rows.Close()

	var notebooks []Notebook
	for rows.Next() {
		var notebook Notebook
		err := rows.Scan(
			&notebook.ID,
			&notebook.Name,
			&notebook.Description,
			&notebook.TimeRangeType,
			&notebook.TimeRangeStart,
			&notebook.TimeRangeEnd,
			&notebook.MaxResultsPerSection,
			&notebook.PrismID,
			&notebook.Variables,
			&notebook.CreatedBy,
			&notebook.CreatedAt,
			&notebook.UpdatedAt,
			&notebook.AuthorDisplayName,
			&notebook.AuthorGravatarColor,
			&notebook.AuthorGravatarInitial,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan notebook: %w", err)
		}
		notebooks = append(notebooks, notebook)
	}
	return notebooks, total, nil
}

// UpdateNotebook updates a notebook's metadata
func (c *PostgresClient) UpdateNotebook(ctx context.Context, id, username string, name, description, timeRangeType *string, timeRangeStart, timeRangeEnd *time.Time, maxResults *int) error {
	// Build dynamic query based on provided fields
	setParts := []string{}
	args := []interface{}{}
	argIndex := 1

	if name != nil {
		setParts = append(setParts, fmt.Sprintf("name = $%d", argIndex))
		args = append(args, *name)
		argIndex++
	}
	if description != nil {
		setParts = append(setParts, fmt.Sprintf("description = $%d", argIndex))
		args = append(args, *description)
		argIndex++
	}
	if timeRangeType != nil {
		setParts = append(setParts, fmt.Sprintf("time_range_type = $%d", argIndex))
		args = append(args, *timeRangeType)
		argIndex++
	}
	if timeRangeStart != nil {
		setParts = append(setParts, fmt.Sprintf("time_range_start = $%d", argIndex))
		args = append(args, timeRangeStart)
		argIndex++
	}
	if timeRangeEnd != nil {
		setParts = append(setParts, fmt.Sprintf("time_range_end = $%d", argIndex))
		args = append(args, timeRangeEnd)
		argIndex++
	}
	if maxResults != nil {
		setParts = append(setParts, fmt.Sprintf("max_results_per_section = $%d", argIndex))
		args = append(args, *maxResults)
		argIndex++
	}

	if len(setParts) == 0 {
		return fmt.Errorf("no fields to update")
	}

	// Add WHERE clause arguments
	args = append(args, id, username)
	query := fmt.Sprintf(`
		UPDATE notebooks
		SET %s, updated_at = NOW()
		WHERE id = $%d AND created_by = $%d
	`, strings.Join(setParts, ", "), argIndex, argIndex+1)

	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to update notebook: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("notebook not found or unauthorized")
	}

	return nil
}

func (c *PostgresClient) UpdateNotebookVariables(ctx context.Context, id, username string, variables json.RawMessage) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE notebooks SET variables = $1::jsonb, updated_at = NOW()
		WHERE id = $2 AND created_by = $3
	`, string(variables), id, username)
	if err != nil {
		return fmt.Errorf("failed to update notebook variables: %w", err)
	}
	return nil
}

// DeleteNotebook deletes a notebook (author only)
func (c *PostgresClient) DeleteNotebook(ctx context.Context, id, username string) error {
	result, err := c.db.ExecContext(ctx, `
		DELETE FROM notebooks WHERE id = $1 AND created_by = $2
	`, id, username)

	if err != nil {
		return fmt.Errorf("failed to delete notebook: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("notebook not found or unauthorized")
	}

	return nil
}

// InsertNotebookSection creates a new notebook section
func (c *PostgresClient) InsertNotebookSection(ctx context.Context, section NotebookSection) (*NotebookSection, error) {
	var newSection NotebookSection
	err := c.db.QueryRowContext(ctx, `
		INSERT INTO notebook_sections (notebook_id, section_type, title, content, rendered_content, order_index, chart_type, chart_config)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, notebook_id, section_type, title, content, rendered_content, order_index,
		          last_executed_at, COALESCE(last_results, 'null'::jsonb), chart_type, COALESCE(chart_config, 'null'::jsonb), created_at, updated_at
	`, section.NotebookID, section.SectionType, section.Title, section.Content, section.RenderedContent, section.OrderIndex, section.ChartType, section.ChartConfig).Scan(
		&newSection.ID,
		&newSection.NotebookID,
		&newSection.SectionType,
		&newSection.Title,
		&newSection.Content,
		&newSection.RenderedContent,
		&newSection.OrderIndex,
		&newSection.LastExecutedAt,
		&newSection.LastResults,
		&newSection.ChartType,
		&newSection.ChartConfig,
		&newSection.CreatedAt,
		&newSection.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to insert notebook section: %w", err)
	}

	return &newSection, nil
}

// GetNotebookSections retrieves all sections for a notebook
func (c *PostgresClient) GetNotebookSections(ctx context.Context, notebookID string) ([]NotebookSection, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT id, notebook_id, section_type, title, content, rendered_content, order_index,
		       last_executed_at, COALESCE(last_results, 'null'::jsonb), chart_type, COALESCE(chart_config, 'null'::jsonb), created_at, updated_at
		FROM notebook_sections
		WHERE notebook_id = $1
		ORDER BY order_index ASC
	`, notebookID)

	if err != nil {
		return nil, fmt.Errorf("failed to query notebook sections: %w", err)
	}
	defer rows.Close()

	var sections []NotebookSection
	for rows.Next() {
		var section NotebookSection
		err := rows.Scan(
			&section.ID,
			&section.NotebookID,
			&section.SectionType,
			&section.Title,
			&section.Content,
			&section.RenderedContent,
			&section.OrderIndex,
			&section.LastExecutedAt,
			&section.LastResults,
			&section.ChartType,
			&section.ChartConfig,
			&section.CreatedAt,
			&section.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan notebook section: %w", err)
		}
		sections = append(sections, section)
	}

	return sections, nil
}

// GetNotebookSection retrieves a specific notebook section by ID
func (c *PostgresClient) GetNotebookSection(ctx context.Context, sectionID string) (*NotebookSection, error) {
	var section NotebookSection
	err := c.db.QueryRowContext(ctx, `
		SELECT id, notebook_id, section_type, title, content, rendered_content, order_index,
		       last_executed_at, COALESCE(last_results, 'null'::jsonb), chart_type, COALESCE(chart_config, 'null'::jsonb), created_at, updated_at
		FROM notebook_sections
		WHERE id = $1
	`, sectionID).Scan(
		&section.ID,
		&section.NotebookID,
		&section.SectionType,
		&section.Title,
		&section.Content,
		&section.RenderedContent,
		&section.OrderIndex,
		&section.LastExecutedAt,
		&section.LastResults,
		&section.ChartType,
		&section.ChartConfig,
		&section.CreatedAt,
		&section.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("notebook section not found")
		}
		return nil, fmt.Errorf("failed to get notebook section: %w", err)
	}

	return &section, nil
}

// UpdateNotebookSection updates a notebook section
func (c *PostgresClient) UpdateNotebookSection(ctx context.Context, sectionID string, title *string, content *string, renderedContent *string, chartConfig *string) error {
	setParts := []string{}
	args := []interface{}{}
	argIndex := 1

	if title != nil {
		setParts = append(setParts, fmt.Sprintf("title = $%d", argIndex))
		args = append(args, title)
		argIndex++
	}
	if content != nil {
		setParts = append(setParts, fmt.Sprintf("content = $%d", argIndex))
		args = append(args, *content)
		argIndex++
	}
	if renderedContent != nil {
		setParts = append(setParts, fmt.Sprintf("rendered_content = $%d", argIndex))
		args = append(args, renderedContent)
		argIndex++
	}
	if chartConfig != nil {
		setParts = append(setParts, fmt.Sprintf("chart_config = $%d::jsonb", argIndex))
		args = append(args, chartConfig)
		argIndex++
	}

	if len(setParts) == 0 {
		return fmt.Errorf("no fields to update")
	}

	args = append(args, sectionID)
	query := fmt.Sprintf(`
		UPDATE notebook_sections
		SET %s, updated_at = NOW()
		WHERE id = $%d
	`, strings.Join(setParts, ", "), argIndex)

	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to update notebook section: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("notebook section not found")
	}

	return nil
}

// UpdateSectionResults updates the execution results for a query section
func (c *PostgresClient) UpdateSectionResults(ctx context.Context, sectionID string, lastExecutedAt *time.Time, lastResults string) error {
	query := `
		UPDATE notebook_sections
		SET last_executed_at = $2, last_results = $3, updated_at = NOW()
		WHERE id = $1
	`

	result, err := c.db.ExecContext(ctx, query, sectionID, lastExecutedAt, lastResults)
	if err != nil {
		return fmt.Errorf("failed to update section results: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("notebook section not found")
	}

	return nil
}

// DeleteNotebookSection deletes a notebook section
func (c *PostgresClient) DeleteNotebookSection(ctx context.Context, sectionID string) error {
	result, err := c.db.ExecContext(ctx, `
		DELETE FROM notebook_sections WHERE id = $1
	`, sectionID)

	if err != nil {
		return fmt.Errorf("failed to delete notebook section: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("notebook section not found")
	}

	return nil
}

// UpdateSectionQueryResults updates the cached results for a query section
func (c *PostgresClient) UpdateSectionQueryResults(ctx context.Context, sectionID string, results interface{}, chartType *string, chartConfig interface{}) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE notebook_sections
		SET last_executed_at = NOW(), last_results = $2, chart_type = $3, chart_config = $4, updated_at = NOW()
		WHERE id = $1
	`, sectionID, results, chartType, chartConfig)

	if err != nil {
		return fmt.Errorf("failed to update section query results: %w", err)
	}

	return nil
}

// ReorderNotebookSections updates the order of sections in a notebook
func (c *PostgresClient) ReorderNotebookSections(ctx context.Context, notebookID string, sectionOrder []string) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for i, sectionID := range sectionOrder {
		_, err := tx.ExecContext(ctx, `
			UPDATE notebook_sections
			SET order_index = $1, updated_at = NOW()
			WHERE id = $2 AND notebook_id = $3
		`, i, sectionID, notebookID)
		if err != nil {
			return fmt.Errorf("failed to update section order: %w", err)
		}
	}

	return tx.Commit()
}

// UpdateNotebookPresence updates or inserts user presence for a notebook
func (c *PostgresClient) UpdateNotebookPresence(ctx context.Context, notebookID, username string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO notebook_presence (notebook_id, username, last_seen_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (notebook_id, username)
		DO UPDATE SET last_seen_at = NOW()
	`, notebookID, username)

	if err != nil {
		return fmt.Errorf("failed to update notebook presence: %w", err)
	}

	return nil
}

// GetNotebookPresence gets active users for a notebook (seen within 30 seconds)
func (c *PostgresClient) GetNotebookPresence(ctx context.Context, notebookID string) ([]NotebookPresence, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT np.notebook_id, np.username, np.last_seen_at,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM notebook_presence np
		JOIN users u ON np.username = u.username
		WHERE np.notebook_id = $1 AND np.last_seen_at > NOW() - INTERVAL '30 seconds'
		ORDER BY np.last_seen_at DESC
	`, notebookID)

	if err != nil {
		return nil, fmt.Errorf("failed to query notebook presence: %w", err)
	}
	defer rows.Close()

	var presence []NotebookPresence
	for rows.Next() {
		var p NotebookPresence
		err := rows.Scan(
			&p.NotebookID,
			&p.Username,
			&p.LastSeenAt,
			&p.UserDisplayName,
			&p.UserGravatarColor,
			&p.UserGravatarInitial,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan notebook presence: %w", err)
		}
		presence = append(presence, p)
	}

	return presence, nil
}

// CleanupStalePresence removes presence records older than 5 minutes
func (c *PostgresClient) CleanupStalePresence(ctx context.Context) error {
	_, err := c.db.ExecContext(ctx, `
		DELETE FROM notebook_presence
		WHERE last_seen_at < NOW() - INTERVAL '5 minutes'
	`)

	if err != nil {
		return fmt.Errorf("failed to cleanup stale presence: %w", err)
	}

	return nil
}

// ResourcePresence is a generic presence record used for chat conversations and dashboards.
type ResourcePresence struct {
	ResourceID          string    `json:"resource_id"`
	Username            string    `json:"username"`
	LastSeenAt          time.Time `json:"last_seen_at"`
	UserDisplayName     string    `json:"user_display_name"`
	UserGravatarColor   string    `json:"user_gravatar_color"`
	UserGravatarInitial string    `json:"user_gravatar_initial"`
}

func (c *PostgresClient) UpdateChatPresence(ctx context.Context, conversationID, username string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO chat_presence (conversation_id, username, last_seen_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (conversation_id, username)
		DO UPDATE SET last_seen_at = NOW()
	`, conversationID, username)
	return err
}

func (c *PostgresClient) GetChatPresence(ctx context.Context, conversationID string) ([]ResourcePresence, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT cp.conversation_id, cp.username, cp.last_seen_at,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM chat_presence cp
		JOIN users u ON cp.username = u.username
		WHERE cp.conversation_id = $1 AND cp.last_seen_at > NOW() - INTERVAL '30 seconds'
		ORDER BY cp.last_seen_at DESC
	`, conversationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var presence []ResourcePresence
	for rows.Next() {
		var p ResourcePresence
		if err := rows.Scan(&p.ResourceID, &p.Username, &p.LastSeenAt,
			&p.UserDisplayName, &p.UserGravatarColor, &p.UserGravatarInitial); err != nil {
			return nil, err
		}
		presence = append(presence, p)
	}
	return presence, nil
}

func (c *PostgresClient) UpdateDashboardPresence(ctx context.Context, dashboardID, username string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO dashboard_presence (dashboard_id, username, last_seen_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (dashboard_id, username)
		DO UPDATE SET last_seen_at = NOW()
	`, dashboardID, username)
	return err
}

func (c *PostgresClient) GetDashboardPresence(ctx context.Context, dashboardID string) ([]ResourcePresence, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT dp.dashboard_id, dp.username, dp.last_seen_at,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM dashboard_presence dp
		JOIN users u ON dp.username = u.username
		WHERE dp.dashboard_id = $1 AND dp.last_seen_at > NOW() - INTERVAL '30 seconds'
		ORDER BY dp.last_seen_at DESC
	`, dashboardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var presence []ResourcePresence
	for rows.Next() {
		var p ResourcePresence
		if err := rows.Scan(&p.ResourceID, &p.Username, &p.LastSeenAt,
			&p.UserDisplayName, &p.UserGravatarColor, &p.UserGravatarInitial); err != nil {
			return nil, err
		}
		presence = append(presence, p)
	}
	return presence, nil
}

// ============================
// Dashboard Storage Types
// ============================

// Dashboard represents a fractal dashboard document
type Dashboard struct {
	ID                    string            `json:"id"`
	Name                  string            `json:"name"`
	Description           string            `json:"description"`
	TimeRangeType         string            `json:"time_range_type"`
	TimeRangeStart        *time.Time        `json:"time_range_start,omitempty"`
	TimeRangeEnd          *time.Time        `json:"time_range_end,omitempty"`
	FractalID             string            `json:"fractal_id,omitempty"`
	PrismID               string            `json:"prism_id,omitempty"`
	Variables             json.RawMessage   `json:"variables"`
	CreatedBy             string            `json:"created_by"`
	AuthorDisplayName     string            `json:"author_display_name"`
	AuthorGravatarColor   string            `json:"author_gravatar_color"`
	AuthorGravatarInitial string            `json:"author_gravatar_initial"`
	CreatedAt             time.Time         `json:"created_at"`
	UpdatedAt             time.Time         `json:"updated_at"`
	Widgets               []DashboardWidget `json:"widgets,omitempty"`
}

// DashboardWidget represents a query widget on a dashboard
type DashboardWidget struct {
	ID             string          `json:"id"`
	DashboardID    string          `json:"dashboard_id"`
	Title          *string         `json:"title,omitempty"`
	QueryContent   string          `json:"query_content"`
	ChartType      string          `json:"chart_type"`
	ChartConfig    json.RawMessage `json:"chart_config,omitempty"`
	PosX           int             `json:"pos_x"`
	PosY           int             `json:"pos_y"`
	Width          int             `json:"width"`
	Height         int             `json:"height"`
	LastExecutedAt *time.Time      `json:"last_executed_at,omitempty"`
	LastResults    json.RawMessage `json:"last_results,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// ============================
// Dashboard Storage Methods
// ============================

func (c *PostgresClient) InsertDashboard(ctx context.Context, d Dashboard) (*Dashboard, error) {
	var fractalIDPtr, prismIDPtr interface{}
	if d.PrismID != "" {
		prismIDPtr = d.PrismID
	} else {
		fractalIDPtr = d.FractalID
	}

	varsJSON := d.Variables
	if varsJSON == nil {
		varsJSON = json.RawMessage("[]")
	}

	var nd Dashboard
	var scanFractalID, scanPrismID sql.NullString
	err := c.db.QueryRowContext(ctx, `
		INSERT INTO dashboards (name, description, time_range_type, time_range_start, time_range_end, fractal_id, prism_id, variables, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)
		RETURNING id, name, description, time_range_type, time_range_start, time_range_end, fractal_id, prism_id, COALESCE(variables, '[]'), created_by, created_at, updated_at
	`, d.Name, d.Description, d.TimeRangeType, d.TimeRangeStart, d.TimeRangeEnd, fractalIDPtr, prismIDPtr, string(varsJSON), d.CreatedBy).Scan(
		&nd.ID, &nd.Name, &nd.Description, &nd.TimeRangeType, &nd.TimeRangeStart, &nd.TimeRangeEnd,
		&scanFractalID, &scanPrismID, &nd.Variables, &nd.CreatedBy, &nd.CreatedAt, &nd.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert dashboard: %w", err)
	}
	nd.FractalID = scanFractalID.String
	nd.PrismID = scanPrismID.String

	author, err := c.GetUser(ctx, nd.CreatedBy)
	if err == nil {
		nd.AuthorDisplayName = author.DisplayName
		nd.AuthorGravatarColor = author.GravatarColor
		nd.AuthorGravatarInitial = author.GravatarInitial
	}
	return &nd, nil
}

func (c *PostgresClient) GetDashboard(ctx context.Context, id string) (*Dashboard, error) {
	var d Dashboard
	var scanFractalID, scanPrismID sql.NullString
	err := c.db.QueryRowContext(ctx, `
		SELECT d.id, d.name, d.description, d.time_range_type, d.time_range_start, d.time_range_end,
		       d.fractal_id, d.prism_id, COALESCE(d.variables, '[]'), d.created_by, d.created_at, d.updated_at,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM dashboards d
		JOIN users u ON d.created_by = u.username
		WHERE d.id = $1
	`, id).Scan(
		&d.ID, &d.Name, &d.Description, &d.TimeRangeType, &d.TimeRangeStart, &d.TimeRangeEnd,
		&scanFractalID, &scanPrismID, &d.Variables, &d.CreatedBy, &d.CreatedAt, &d.UpdatedAt,
		&d.AuthorDisplayName, &d.AuthorGravatarColor, &d.AuthorGravatarInitial,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("dashboard not found")
		}
		return nil, fmt.Errorf("failed to get dashboard: %w", err)
	}
	d.FractalID = scanFractalID.String
	d.PrismID = scanPrismID.String
	return &d, nil
}

func (c *PostgresClient) GetDashboardsByFractal(ctx context.Context, fractalID string, limit, offset int) ([]Dashboard, int, error) {
	var total int
	err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dashboards WHERE fractal_id = $1`, fractalID).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count dashboards: %w", err)
	}

	rows, err := c.db.QueryContext(ctx, `
		SELECT d.id, d.name, d.description, d.time_range_type, d.time_range_start, d.time_range_end,
		       d.fractal_id, COALESCE(d.variables, '[]'), d.created_by, d.created_at, d.updated_at,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM dashboards d
		JOIN users u ON d.created_by = u.username
		WHERE d.fractal_id = $1
		ORDER BY d.updated_at DESC
		LIMIT $2 OFFSET $3
	`, fractalID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query dashboards: %w", err)
	}
	defer rows.Close()

	var dashboards []Dashboard
	for rows.Next() {
		var d Dashboard
		err := rows.Scan(
			&d.ID, &d.Name, &d.Description, &d.TimeRangeType, &d.TimeRangeStart, &d.TimeRangeEnd,
			&d.FractalID, &d.Variables, &d.CreatedBy, &d.CreatedAt, &d.UpdatedAt,
			&d.AuthorDisplayName, &d.AuthorGravatarColor, &d.AuthorGravatarInitial,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan dashboard: %w", err)
		}
		dashboards = append(dashboards, d)
	}
	return dashboards, total, nil
}

// GetDashboardsByPrism retrieves dashboards scoped to a prism with pagination.
func (c *PostgresClient) GetDashboardsByPrism(ctx context.Context, prismID string, limit, offset int) ([]Dashboard, int, error) {
	var total int
	err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dashboards WHERE prism_id = $1`, prismID).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count dashboards: %w", err)
	}

	rows, err := c.db.QueryContext(ctx, `
		SELECT d.id, d.name, d.description, d.time_range_type, d.time_range_start, d.time_range_end,
		       d.prism_id, COALESCE(d.variables, '[]'), d.created_by, d.created_at, d.updated_at,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM dashboards d
		JOIN users u ON d.created_by = u.username
		WHERE d.prism_id = $1
		ORDER BY d.updated_at DESC
		LIMIT $2 OFFSET $3
	`, prismID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query dashboards: %w", err)
	}
	defer rows.Close()

	var dashboards []Dashboard
	for rows.Next() {
		var d Dashboard
		err := rows.Scan(
			&d.ID, &d.Name, &d.Description, &d.TimeRangeType, &d.TimeRangeStart, &d.TimeRangeEnd,
			&d.PrismID, &d.Variables, &d.CreatedBy, &d.CreatedAt, &d.UpdatedAt,
			&d.AuthorDisplayName, &d.AuthorGravatarColor, &d.AuthorGravatarInitial,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan dashboard: %w", err)
		}
		dashboards = append(dashboards, d)
	}
	return dashboards, total, nil
}

func (c *PostgresClient) UpdateDashboard(ctx context.Context, id, username string, name, description, timeRangeType *string, timeRangeStart, timeRangeEnd *time.Time) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE dashboards SET
			name = COALESCE($1, name),
			description = COALESCE($2, description),
			time_range_type = COALESCE($3, time_range_type),
			time_range_start = COALESCE($4, time_range_start),
			time_range_end = COALESCE($5, time_range_end)
		WHERE id = $6 AND created_by = $7
	`, name, description, timeRangeType, timeRangeStart, timeRangeEnd, id, username)
	if err != nil {
		return fmt.Errorf("failed to update dashboard: %w", err)
	}
	return nil
}

func (c *PostgresClient) UpdateDashboardVariables(ctx context.Context, id, username string, variables json.RawMessage) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE dashboards SET variables = $1::jsonb, updated_at = NOW()
		WHERE id = $2 AND created_by = $3
	`, string(variables), id, username)
	if err != nil {
		return fmt.Errorf("failed to update dashboard variables: %w", err)
	}
	return nil
}

func (c *PostgresClient) DeleteDashboard(ctx context.Context, id, username string) error {
	_, err := c.db.ExecContext(ctx, `DELETE FROM dashboards WHERE id = $1 AND created_by = $2`, id, username)
	if err != nil {
		return fmt.Errorf("failed to delete dashboard: %w", err)
	}
	return nil
}

func (c *PostgresClient) InsertDashboardWidget(ctx context.Context, w DashboardWidget) (*DashboardWidget, error) {
	var nw DashboardWidget
	err := c.db.QueryRowContext(ctx, `
		INSERT INTO dashboard_widgets (dashboard_id, title, query_content, chart_type, chart_config, pos_x, pos_y, width, height)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id, dashboard_id, title, query_content, chart_type, COALESCE(chart_config, 'null'::jsonb), pos_x, pos_y, width, height, last_executed_at, COALESCE(last_results, 'null'::jsonb), created_at, updated_at
	`, w.DashboardID, w.Title, w.QueryContent, w.ChartType, w.ChartConfig, w.PosX, w.PosY, w.Width, w.Height).Scan(
		&nw.ID, &nw.DashboardID, &nw.Title, &nw.QueryContent, &nw.ChartType, &nw.ChartConfig,
		&nw.PosX, &nw.PosY, &nw.Width, &nw.Height, &nw.LastExecutedAt, &nw.LastResults,
		&nw.CreatedAt, &nw.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert dashboard widget: %w", err)
	}
	return &nw, nil
}

func (c *PostgresClient) GetDashboardWidgets(ctx context.Context, dashboardID string) ([]DashboardWidget, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT id, dashboard_id, title, query_content, chart_type, COALESCE(chart_config, 'null'::jsonb), pos_x, pos_y, width, height, last_executed_at, COALESCE(last_results, 'null'::jsonb), created_at, updated_at
		FROM dashboard_widgets
		WHERE dashboard_id = $1
		ORDER BY pos_y ASC, pos_x ASC
	`, dashboardID)
	if err != nil {
		return nil, fmt.Errorf("failed to query dashboard widgets: %w", err)
	}
	defer rows.Close()

	var widgets []DashboardWidget
	for rows.Next() {
		var w DashboardWidget
		err := rows.Scan(
			&w.ID, &w.DashboardID, &w.Title, &w.QueryContent, &w.ChartType, &w.ChartConfig,
			&w.PosX, &w.PosY, &w.Width, &w.Height, &w.LastExecutedAt, &w.LastResults,
			&w.CreatedAt, &w.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan dashboard widget: %w", err)
		}
		widgets = append(widgets, w)
	}
	return widgets, nil
}

func (c *PostgresClient) UpdateDashboardWidget(ctx context.Context, widgetID string, title, queryContent, chartType, chartConfig *string) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE dashboard_widgets SET
			title = COALESCE($1, title),
			query_content = COALESCE($2, query_content),
			chart_type = COALESCE($3, chart_type),
			chart_config = COALESCE($4::jsonb, chart_config)
		WHERE id = $5
	`, title, queryContent, chartType, chartConfig, widgetID)
	if err != nil {
		return fmt.Errorf("failed to update dashboard widget: %w", err)
	}
	return nil
}

func (c *PostgresClient) UpdateDashboardWidgetResults(ctx context.Context, widgetID, lastResults string, chartType *string) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE dashboard_widgets SET
			last_results = $1::jsonb,
			last_executed_at = NOW(),
			chart_type = COALESCE($2, chart_type)
		WHERE id = $3
	`, lastResults, chartType, widgetID)
	if err != nil {
		return fmt.Errorf("failed to update dashboard widget results: %w", err)
	}
	return nil
}

func (c *PostgresClient) UpdateDashboardWidgetLayout(ctx context.Context, widgetID string, posX, posY, width, height int) error {
	_, err := c.db.ExecContext(ctx, `
		UPDATE dashboard_widgets SET pos_x = $1, pos_y = $2, width = $3, height = $4 WHERE id = $5
	`, posX, posY, width, height, widgetID)
	if err != nil {
		return fmt.Errorf("failed to update dashboard widget layout: %w", err)
	}
	return nil
}

func (c *PostgresClient) DeleteDashboardWidget(ctx context.Context, widgetID string) error {
	_, err := c.db.ExecContext(ctx, `DELETE FROM dashboard_widgets WHERE id = $1`, widgetID)
	if err != nil {
		return fmt.Errorf("failed to delete dashboard widget: %w", err)
	}
	return nil
}

// ============================
// RBAC: Groups
// ============================

type Group struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedBy   string    `json:"created_by"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	MemberCount int       `json:"member_count"`
}

type GroupMember struct {
	GroupID         string    `json:"group_id"`
	Username        string    `json:"username"`
	AddedBy         string    `json:"added_by"`
	AddedAt         time.Time `json:"added_at"`
	DisplayName     string    `json:"display_name"`
	GravatarColor   string    `json:"gravatar_color"`
	GravatarInitial string    `json:"gravatar_initial"`
}

func (c *PostgresClient) CreateGroup(ctx context.Context, name, description, createdBy string) (*Group, error) {
	g := &Group{}
	err := c.db.QueryRowContext(ctx, `
		INSERT INTO groups (name, description, created_by)
		VALUES ($1, $2, $3)
		RETURNING id, name, description, created_by, created_at, updated_at
	`, name, description, createdBy).Scan(
		&g.ID, &g.Name, &g.Description, &g.CreatedBy, &g.CreatedAt, &g.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create group: %w", err)
	}
	return g, nil
}

func (c *PostgresClient) GetGroup(ctx context.Context, id string) (*Group, error) {
	g := &Group{}
	err := c.db.QueryRowContext(ctx, `
		SELECT g.id, g.name, g.description, g.created_by, g.created_at, g.updated_at,
		       (SELECT COUNT(*) FROM group_members WHERE group_id = g.id) AS member_count
		FROM groups g
		WHERE g.id = $1
	`, id).Scan(
		&g.ID, &g.Name, &g.Description, &g.CreatedBy, &g.CreatedAt, &g.UpdatedAt, &g.MemberCount,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("group not found")
		}
		return nil, fmt.Errorf("failed to get group: %w", err)
	}
	return g, nil
}

func (c *PostgresClient) ListGroups(ctx context.Context) ([]Group, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT g.id, g.name, g.description, g.created_by, g.created_at, g.updated_at,
		       (SELECT COUNT(*) FROM group_members WHERE group_id = g.id) AS member_count
		FROM groups g
		ORDER BY g.name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list groups: %w", err)
	}
	defer rows.Close()

	var groups []Group
	for rows.Next() {
		var g Group
		if err := rows.Scan(&g.ID, &g.Name, &g.Description, &g.CreatedBy, &g.CreatedAt, &g.UpdatedAt, &g.MemberCount); err != nil {
			return nil, fmt.Errorf("failed to scan group: %w", err)
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

func (c *PostgresClient) UpdateGroup(ctx context.Context, id, name, description string) (*Group, error) {
	g := &Group{}
	err := c.db.QueryRowContext(ctx, `
		UPDATE groups SET name = $2, description = $3, updated_at = NOW()
		WHERE id = $1
		RETURNING id, name, description, created_by, created_at, updated_at
	`, id, name, description).Scan(
		&g.ID, &g.Name, &g.Description, &g.CreatedBy, &g.CreatedAt, &g.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to update group: %w", err)
	}
	return g, nil
}

func (c *PostgresClient) DeleteGroup(ctx context.Context, id string) error {
	result, err := c.db.ExecContext(ctx, `DELETE FROM groups WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("failed to delete group: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("group not found")
	}
	return nil
}

func (c *PostgresClient) AddGroupMember(ctx context.Context, groupID, username, addedBy string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT INTO group_members (group_id, username, added_by)
		VALUES ($1, $2, $3)
	`, groupID, username, addedBy)
	if err != nil {
		return fmt.Errorf("failed to add group member: %w", err)
	}
	return nil
}

func (c *PostgresClient) RemoveGroupMember(ctx context.Context, groupID, username string) error {
	result, err := c.db.ExecContext(ctx, `
		DELETE FROM group_members WHERE group_id = $1 AND username = $2
	`, groupID, username)
	if err != nil {
		return fmt.Errorf("failed to remove group member: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("member not found in group")
	}
	return nil
}

func (c *PostgresClient) ListGroupMembers(ctx context.Context, groupID string) ([]GroupMember, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT gm.group_id, gm.username, gm.added_by, gm.added_at,
		       u.display_name, u.gravatar_color, u.gravatar_initial
		FROM group_members gm
		JOIN users u ON gm.username = u.username
		WHERE gm.group_id = $1
		ORDER BY u.display_name ASC
	`, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to list group members: %w", err)
	}
	defer rows.Close()

	var members []GroupMember
	for rows.Next() {
		var m GroupMember
		if err := rows.Scan(&m.GroupID, &m.Username, &m.AddedBy, &m.AddedAt,
			&m.DisplayName, &m.GravatarColor, &m.GravatarInitial); err != nil {
			return nil, fmt.Errorf("failed to scan group member: %w", err)
		}
		members = append(members, m)
	}
	return members, rows.Err()
}

func (c *PostgresClient) GetUserGroups(ctx context.Context, username string) ([]Group, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT g.id, g.name, g.description, g.created_by, g.created_at, g.updated_at,
		       (SELECT COUNT(*) FROM group_members WHERE group_id = g.id) AS member_count
		FROM groups g
		JOIN group_members gm ON g.id = gm.group_id
		WHERE gm.username = $1
		ORDER BY g.name ASC
	`, username)
	if err != nil {
		return nil, fmt.Errorf("failed to get user groups: %w", err)
	}
	defer rows.Close()

	var groups []Group
	for rows.Next() {
		var g Group
		if err := rows.Scan(&g.ID, &g.Name, &g.Description, &g.CreatedBy, &g.CreatedAt, &g.UpdatedAt, &g.MemberCount); err != nil {
			return nil, fmt.Errorf("failed to scan group: %w", err)
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

// ============================
// RBAC: Fractal Permissions
// ============================

type FractalPermission struct {
	ID          string    `json:"id"`
	FractalID   string    `json:"fractal_id"`
	Username    *string   `json:"username,omitempty"`
	GroupID     *string   `json:"group_id,omitempty"`
	Role        string    `json:"role"`
	GrantedBy   string    `json:"granted_by"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	DisplayName string    `json:"display_name,omitempty"`
}

func (c *PostgresClient) GrantFractalPermission(ctx context.Context, fractalID string, username *string, groupID *string, role, grantedBy string) (*FractalPermission, error) {
	fp := &FractalPermission{}
	err := c.db.QueryRowContext(ctx, `
		INSERT INTO fractal_permissions (fractal_id, username, group_id, role, granted_by)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (fractal_id, username) WHERE username IS NOT NULL
			DO UPDATE SET role = EXCLUDED.role, granted_by = EXCLUDED.granted_by, updated_at = NOW()
		RETURNING id, fractal_id, username, group_id, role, granted_by, created_at, updated_at
	`, fractalID, username, groupID, role, grantedBy).Scan(
		&fp.ID, &fp.FractalID, &fp.Username, &fp.GroupID, &fp.Role, &fp.GrantedBy, &fp.CreatedAt, &fp.UpdatedAt,
	)
	if err != nil {
		// Try group conflict path
		if groupID != nil {
			err2 := c.db.QueryRowContext(ctx, `
				INSERT INTO fractal_permissions (fractal_id, username, group_id, role, granted_by)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (fractal_id, group_id) WHERE group_id IS NOT NULL
					DO UPDATE SET role = EXCLUDED.role, granted_by = EXCLUDED.granted_by, updated_at = NOW()
				RETURNING id, fractal_id, username, group_id, role, granted_by, created_at, updated_at
			`, fractalID, username, groupID, role, grantedBy).Scan(
				&fp.ID, &fp.FractalID, &fp.Username, &fp.GroupID, &fp.Role, &fp.GrantedBy, &fp.CreatedAt, &fp.UpdatedAt,
			)
			if err2 != nil {
				return nil, fmt.Errorf("failed to grant fractal permission: %w", err2)
			}
			return fp, nil
		}
		return nil, fmt.Errorf("failed to grant fractal permission: %w", err)
	}
	return fp, nil
}

func (c *PostgresClient) RevokeFractalPermission(ctx context.Context, id string) error {
	result, err := c.db.ExecContext(ctx, `DELETE FROM fractal_permissions WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("failed to revoke fractal permission: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("permission not found")
	}
	return nil
}

func (c *PostgresClient) UpdateFractalPermissionRole(ctx context.Context, id, role string) error {
	result, err := c.db.ExecContext(ctx, `
		UPDATE fractal_permissions SET role = $2, updated_at = NOW() WHERE id = $1
	`, id, role)
	if err != nil {
		return fmt.Errorf("failed to update fractal permission: %w", err)
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("permission not found")
	}
	return nil
}

func (c *PostgresClient) ListFractalPermissions(ctx context.Context, fractalID string) ([]FractalPermission, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT fp.id, fp.fractal_id, fp.username, fp.group_id, fp.role, fp.granted_by,
		       fp.created_at, fp.updated_at,
		       COALESCE(u.display_name, g.name, '') AS display_name
		FROM fractal_permissions fp
		LEFT JOIN users u ON fp.username = u.username
		LEFT JOIN groups g ON fp.group_id = g.id
		WHERE fp.fractal_id = $1
		ORDER BY fp.created_at ASC
	`, fractalID)
	if err != nil {
		return nil, fmt.Errorf("failed to list fractal permissions: %w", err)
	}
	defer rows.Close()

	var perms []FractalPermission
	for rows.Next() {
		var fp FractalPermission
		if err := rows.Scan(&fp.ID, &fp.FractalID, &fp.Username, &fp.GroupID, &fp.Role,
			&fp.GrantedBy, &fp.CreatedAt, &fp.UpdatedAt, &fp.DisplayName); err != nil {
			return nil, fmt.Errorf("failed to scan fractal permission: %w", err)
		}
		perms = append(perms, fp)
	}
	return perms, rows.Err()
}

func (c *PostgresClient) GetFractalPermission(ctx context.Context, id string) (*FractalPermission, error) {
	fp := &FractalPermission{}
	err := c.db.QueryRowContext(ctx, `
		SELECT fp.id, fp.fractal_id, fp.username, fp.group_id, fp.role, fp.granted_by,
		       fp.created_at, fp.updated_at,
		       COALESCE(u.display_name, g.name, '') AS display_name
		FROM fractal_permissions fp
		LEFT JOIN users u ON fp.username = u.username
		LEFT JOIN groups g ON fp.group_id = g.id
		WHERE fp.id = $1
	`, id).Scan(&fp.ID, &fp.FractalID, &fp.Username, &fp.GroupID, &fp.Role,
		&fp.GrantedBy, &fp.CreatedAt, &fp.UpdatedAt, &fp.DisplayName)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("permission not found")
		}
		return nil, fmt.Errorf("failed to get fractal permission: %w", err)
	}
	return fp, nil
}
