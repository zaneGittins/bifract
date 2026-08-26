package apikeys

import (
	"fmt"
	"time"
)

// APIKey represents an API key for programmatic access to a fractal or prism
type APIKey struct {
	ID          string     `json:"id" db:"id"`
	Name        string     `json:"name" db:"name"`
	Description string     `json:"description,omitempty" db:"description"`
	KeyID       string     `json:"key_id" db:"key_id"` // Public identifier (first 8 chars)
	FractalID   string     `json:"fractal_id,omitempty" db:"fractal_id"`
	FractalName string     `json:"fractal_name,omitempty"` // Populated in responses
	PrismID     string     `json:"prism_id,omitempty" db:"prism_id"`
	PrismName   string     `json:"prism_name,omitempty"` // Populated in responses
	CreatedBy   string     `json:"created_by" db:"created_by"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty" db:"expires_at"`
	IsActive    bool       `json:"is_active" db:"is_active"`
	// Role is the RBAC role the key holds on its fractal or prism, the same
	// ladder a person is authorized by.
	Role string `json:"role" db:"role"`
	// TenantAdmin grants instance-wide administration. It is never derived, only
	// granted explicitly, and the schema requires such a key to carry an expiry.
	TenantAdmin bool       `json:"tenant_admin" db:"tenant_admin"`
	CreatedAt   time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at" db:"updated_at"`
	LastUsedAt  *time.Time `json:"last_used_at,omitempty" db:"last_used_at"`
	UsageCount  int        `json:"usage_count" db:"usage_count"`
}

// CreateAPIKeyRequest represents a request to create a new API key
type CreateAPIKeyRequest struct {
	Name        string     `json:"name" validate:"required,max=255"`
	Description string     `json:"description,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	Role        string     `json:"role,omitempty"`
	TenantAdmin bool       `json:"tenant_admin,omitempty"`
}

// CreateAPIKeyResponse represents the response after creating an API key
// The Key field is only returned once during creation
type CreateAPIKeyResponse struct {
	Key    string `json:"key"`     // Full key, shown only once
	KeyID  string `json:"key_id"`  // Public identifier
	APIKey APIKey `json:"api_key"` // Full API key object
}

// UpdateAPIKeyRequest represents a request to update an existing API key
type UpdateAPIKeyRequest struct {
	Name        *string    `json:"name,omitempty"`
	Description *string    `json:"description,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	IsActive    *bool      `json:"is_active,omitempty"`
	Role        *string    `json:"role,omitempty"`
	TenantAdmin *bool      `json:"tenant_admin,omitempty"`
}

// ValidatedAPIKey represents an API key that has been validated for authentication.
// FractalName and PrismName are populated from the embedded APIKey fields.
type ValidatedAPIKey struct {
	APIKey
}

// ValidRoles are the roles a key may hold on its scope.
var ValidRoles = map[string]bool{"": true, "viewer": true, "analyst": true, "admin": true}

// Validate rejects a grant that cannot be honoured: an unknown role, or an
// instance-wide key with no expiry.
func (r CreateAPIKeyRequest) Validate() error {
	if !ValidRoles[r.Role] {
		return fmt.Errorf("role must be viewer, analyst or admin")
	}
	if r.TenantAdmin && r.ExpiresAt == nil {
		return fmt.Errorf("a tenant admin key must have an expiry")
	}
	return nil
}

// Validate rejects an update that cannot be honoured.
func (r UpdateAPIKeyRequest) Validate(current *APIKey) error {
	if r.Role != nil && !ValidRoles[*r.Role] {
		return fmt.Errorf("role must be viewer, analyst or admin")
	}
	tenant := current.TenantAdmin
	if r.TenantAdmin != nil {
		tenant = *r.TenantAdmin
	}
	expires := current.ExpiresAt
	if r.ExpiresAt != nil {
		expires = r.ExpiresAt
	}
	if tenant && expires == nil {
		return fmt.Errorf("a tenant admin key must have an expiry")
	}
	return nil
}

// IsExpired checks if the API key has expired
func (k *APIKey) IsExpired() bool {
	if k.ExpiresAt == nil {
		return false // Never expires
	}
	return time.Now().After(*k.ExpiresAt)
}

// IsValid checks if the API key is valid for use
func (k *APIKey) IsValid() bool {
	return k.IsActive && !k.IsExpired()
}
