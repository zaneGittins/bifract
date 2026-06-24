package fractals

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	"bifract/pkg/storage"
)

// Manager handles business logic for index operations
type Manager struct {
	storage      *Storage
	onCreateHook func(ctx context.Context, fractal *Fractal)
}

// NewManager creates a new index manager
func NewManager(pg *storage.PostgresClient, ch *storage.ClickHouseClient) *Manager {
	return &Manager{
		storage: NewStorage(pg, ch),
	}
}

// SetOnCreateHook registers a callback invoked after a fractal is created.
func (m *Manager) SetOnCreateHook(hook func(ctx context.Context, fractal *Fractal)) {
	m.onCreateHook = hook
}

// CreateFractal creates a new index with validation
func (m *Manager) CreateFractal(ctx context.Context, req CreateFractalRequest, createdBy string) (*Fractal, error) {
	// Validate request
	if err := m.validateCreateRequest(req); err != nil {
		return nil, err
	}

	// Check if index name already exists
	existing, err := m.storage.GetFractalByName(ctx, req.Name)
	if err == nil && existing != nil {
		return nil, fmt.Errorf("index with name '%s' already exists", req.Name)
	}

	// Create the index
	fractal, err := m.storage.CreateFractal(ctx, req, createdBy)
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	if m.onCreateHook != nil {
		m.onCreateHook(ctx, fractal)
	}

	return fractal, nil
}

// GetFractal retrieves an index by ID
func (m *Manager) GetFractal(ctx context.Context, fractalID string) (*Fractal, error) {
	if fractalID == "" {
		return nil, fmt.Errorf("index ID is required")
	}

	return m.storage.GetFractal(ctx, fractalID)
}

// GetFractalByName retrieves an index by name
func (m *Manager) GetFractalByName(ctx context.Context, name string) (*Fractal, error) {
	if name == "" {
		return nil, fmt.Errorf("index name is required")
	}

	return m.storage.GetFractalByName(ctx, name)
}

// GetDefaultFractal retrieves the default index
func (m *Manager) GetDefaultFractal(ctx context.Context) (*Fractal, error) {
	return m.storage.GetDefaultFractal(ctx)
}

// ListFractals retrieves all fractals
func (m *Manager) ListFractals(ctx context.Context) ([]*Fractal, error) {
	return m.storage.ListFractals(ctx)
}

// UpdateFractal updates an existing index with validation
func (m *Manager) UpdateFractal(ctx context.Context, fractalID string, req UpdateFractalRequest) (*Fractal, error) {
	if fractalID == "" {
		return nil, fmt.Errorf("index ID is required")
	}

	// Validate request
	if err := m.validateUpdateRequest(req); err != nil {
		return nil, err
	}

	// Check if the index exists
	existing, err := m.storage.GetFractal(ctx, fractalID)
	if err != nil {
		return nil, fmt.Errorf("index not found: %w", err)
	}

	// Don't allow renaming default or system fractals
	if (existing.IsDefault || existing.IsSystem) && req.Name != existing.Name {
		return nil, fmt.Errorf("cannot rename system fractal '%s'", existing.Name)
	}

	// Check if the new name conflicts with another index
	if req.Name != existing.Name {
		conflicting, err := m.storage.GetFractalByName(ctx, req.Name)
		if err == nil && conflicting != nil {
			return nil, fmt.Errorf("index with name '%s' already exists", req.Name)
		}
	}

	return m.storage.UpdateFractal(ctx, fractalID, req)
}

// DeleteFractal deletes an index with validation
func (m *Manager) DeleteFractal(ctx context.Context, fractalID string) error {
	if fractalID == "" {
		return fmt.Errorf("index ID is required")
	}

	// Check if the index exists
	fractal, err := m.storage.GetFractal(ctx, fractalID)
	if err != nil {
		return fmt.Errorf("index not found: %w", err)
	}

	// Cannot delete default or system fractals
	if fractal.IsDefault || fractal.IsSystem {
		return fmt.Errorf("cannot delete system fractal '%s'", fractal.Name)
	}

	return m.storage.DeleteFractal(ctx, fractalID)
}

// ResolveFractalID resolves an index name or ID to a valid index ID
// This is used when users specify an index by name in API calls
func (m *Manager) ResolveFractalID(ctx context.Context, nameOrID string) (string, error) {
	if nameOrID == "" || nameOrID == "default" {
		// Return default index ID
		defaultFractal, err := m.storage.GetDefaultFractal(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get default index: %w", err)
		}
		return defaultFractal.ID, nil
	}

	// Check if it's already a valid UUID (index ID)
	if isValidUUID(nameOrID) {
		// Validate that the index exists
		exists, err := m.storage.ValidateFractalExists(ctx, nameOrID)
		if err != nil {
			return "", fmt.Errorf("failed to validate index: %w", err)
		}
		if !exists {
			return "", fmt.Errorf("index with ID '%s' not found", nameOrID)
		}
		return nameOrID, nil
	}

	// Try to resolve by name
	fractal, err := m.storage.GetFractalByName(ctx, nameOrID)
	if err != nil {
		return "", fmt.Errorf("index with name '%s' not found", nameOrID)
	}

	return fractal.ID, nil
}

// RefreshFractalStats updates the cached statistics for all fractals.
// Uses a single batched ClickHouse query instead of one per fractal.
func (m *Manager) RefreshFractalStats(ctx context.Context) error {
	fractals, err := m.storage.ListFractals(ctx)
	if err != nil {
		return fmt.Errorf("failed to list fractals: %w", err)
	}

	statsMap, err := m.storage.ComputeAllFractalStats(ctx, fractals)
	if err != nil {
		return fmt.Errorf("failed to compute fractal stats: %w", err)
	}

	for _, fractal := range fractals {
		stats, ok := statsMap[fractal.ID]
		if !ok {
			continue
		}
		if err := m.storage.UpdateFractalStats(ctx, fractal.ID, *stats); err != nil {
			fmt.Printf("Failed to update stats for index %s: %v\n", fractal.Name, err)
		}
	}

	return nil
}

// SetRetention sets the retention period for a fractal (nil = unlimited)
func (m *Manager) SetRetention(ctx context.Context, fractalID string, days *int) error {
	if fractalID == "" {
		return fmt.Errorf("fractal ID is required")
	}
	if days != nil && *days < 1 {
		return fmt.Errorf("retention days must be at least 1")
	}
	return m.storage.SetRetention(ctx, fractalID, days)
}

// SetColdDays sets the cold-storage age threshold (in days) for a fractal.
// Logs older than this are moved to the cold object-storage tier. nil disables
// tiering. When retention is also set, cold_days must be strictly less than
// retention_days, otherwise logs would be deleted before they ever tier.
func (m *Manager) SetColdDays(ctx context.Context, fractalID string, days *int) error {
	if fractalID == "" {
		return fmt.Errorf("fractal ID is required")
	}
	if days != nil && *days < 1 {
		return fmt.Errorf("cold days must be at least 1")
	}
	if days != nil {
		fractal, err := m.storage.GetFractal(ctx, fractalID)
		if err != nil {
			return fmt.Errorf("failed to load fractal: %w", err)
		}
		if fractal.RetentionDays != nil && *days >= *fractal.RetentionDays {
			return fmt.Errorf("cold days (%d) must be less than retention days (%d)", *days, *fractal.RetentionDays)
		}
	}
	return m.storage.SetColdDays(ctx, fractalID, days)
}

// SetDiskQuota sets the disk quota and enforcement action for a fractal.
func (m *Manager) SetDiskQuota(ctx context.Context, fractalID string, quotaBytes *int64, action string) error {
	if fractalID == "" {
		return fmt.Errorf("fractal ID is required")
	}
	if action != "reject" && action != "rollover" {
		return fmt.Errorf("invalid disk quota action: must be reject or rollover")
	}
	if quotaBytes != nil && *quotaBytes < 1 {
		return fmt.Errorf("quota_bytes must be positive")
	}
	return m.storage.SetDiskQuota(ctx, fractalID, quotaBytes, action)
}

// EnforceRetention deletes logs that exceed the configured retention period for each fractal.
func (m *Manager) EnforceRetention(ctx context.Context) error {
	fractals, err := m.storage.ListFractals(ctx)
	if err != nil {
		return fmt.Errorf("failed to list fractals: %w", err)
	}

	for _, fractal := range fractals {
		if fractal.RetentionDays == nil {
			continue
		}
		if err := m.storage.DeleteOldLogs(ctx, fractal.ID, *fractal.RetentionDays, fractal.IsDefault); err != nil {
			fmt.Printf("Retention enforcement failed for fractal %s: %v\n", fractal.Name, err)
		}
	}

	return nil
}

// EnforceColdStorage moves log partitions older than each fractal's cold_days
// threshold to the cold object-storage tier. It is a no-op for fractals without
// cold_days set. Runs before EnforceRetention so data tiers before it is deleted.
func (m *Manager) EnforceColdStorage(ctx context.Context) error {
	fractals, err := m.storage.ListFractals(ctx)
	if err != nil {
		return fmt.Errorf("failed to list fractals: %w", err)
	}

	for _, fractal := range fractals {
		if fractal.ColdDays == nil {
			continue
		}
		if err := m.storage.MoveOldLogsToCold(ctx, fractal.ID, *fractal.ColdDays, fractal.IsDefault); err != nil {
			fmt.Printf("Cold-storage tiering failed for fractal %s: %v\n", fractal.Name, err)
		}
	}

	return nil
}

// GetFractalStats returns cached statistics for a fractal from PostgreSQL.
// Stats are kept up-to-date by a background job that computes them from ClickHouse periodically.
func (m *Manager) GetFractalStats(ctx context.Context, fractalID string) (*FractalStats, error) {
	if fractalID == "" {
		return nil, fmt.Errorf("index ID is required")
	}

	fractal, err := m.storage.GetFractal(ctx, fractalID)
	if err != nil {
		return nil, err
	}

	return &FractalStats{
		ID:          fractal.ID,
		Name:        fractal.Name,
		LogCount:    fractal.LogCount,
		SizeBytes:   fractal.SizeBytes,
		EarliestLog: fractal.EarliestLog,
		LatestLog:   fractal.LatestLog,
		LastUpdated: fractal.UpdatedAt,
	}, nil
}

// validateCreateRequest validates the create index request
func (m *Manager) validateCreateRequest(req CreateFractalRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return fmt.Errorf("index name is required")
	}

	req.Name = strings.TrimSpace(req.Name)

	// Validate name format
	if len(req.Name) > 100 {
		return fmt.Errorf("index name must be 100 characters or less")
	}

	// Check for valid characters (alphanumeric, hyphens, underscores)
	if !isValidFractalName(req.Name) {
		return fmt.Errorf("index name can only contain letters, numbers, hyphens, and underscores")
	}

	// Reserved names
	reservedNames := []string{"default", "admin", "system", "logs", "events", "audit", "alerts"}
	lowerName := strings.ToLower(req.Name)
	for _, reserved := range reservedNames {
		if lowerName == reserved {
			return fmt.Errorf("'%s' is a reserved index name", req.Name)
		}
	}

	// Validate description length
	if len(req.Description) > 500 {
		return fmt.Errorf("description must be 500 characters or less")
	}

	return nil
}

// validateUpdateRequest validates the update index request
func (m *Manager) validateUpdateRequest(req UpdateFractalRequest) error {
	// Use the same validation as create
	createReq := CreateFractalRequest{
		Name:        req.Name,
		Description: req.Description,
	}
	return m.validateCreateRequest(createReq)
}

// isValidFractalName checks if an index name contains only valid characters
func isValidFractalName(name string) bool {
	if name == "" {
		return false
	}

	// Must start with a letter
	if !unicode.IsLetter(rune(name[0])) {
		return false
	}

	// Only letters, numbers, hyphens, and underscores allowed
	for _, char := range name {
		if !unicode.IsLetter(char) && !unicode.IsDigit(char) && char != '-' && char != '_' {
			return false
		}
	}

	return true
}

// isValidUUID checks if a string looks like a UUID
func isValidUUID(str string) bool {
	// Simple UUID format check (8-4-4-4-12)
	if len(str) != 36 {
		return false
	}

	// Check hyphen positions
	if str[8] != '-' || str[13] != '-' || str[18] != '-' || str[23] != '-' {
		return false
	}

	// Check if all other characters are hex digits
	for i, char := range str {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue // Skip hyphens
		}
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return false
		}
	}

	return true
}
