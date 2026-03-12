package apikeys

import (
	"fmt"
	"time"
)

// APIKey represents an API key for programmatic access to a specific fractal
type APIKey struct {
	ID          string                 `json:"id" db:"id"`
	Name        string                 `json:"name" db:"name"`
	Description string                 `json:"description,omitempty" db:"description"`
	KeyID       string                 `json:"key_id" db:"key_id"`        // Public identifier (first 8 chars)
	FractalID   string                 `json:"fractal_id" db:"fractal_id"`
	FractalName string                 `json:"fractal_name,omitempty"`    // Populated in responses
	CreatedBy   string                 `json:"created_by" db:"created_by"`
	ExpiresAt   *time.Time             `json:"expires_at,omitempty" db:"expires_at"`
	IsActive    bool                   `json:"is_active" db:"is_active"`
	Permissions map[string]interface{} `json:"permissions" db:"permissions"`
	CreatedAt   time.Time              `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at" db:"updated_at"`
	LastUsedAt  *time.Time             `json:"last_used_at,omitempty" db:"last_used_at"`
	UsageCount  int                    `json:"usage_count" db:"usage_count"`
}

// CreateAPIKeyRequest represents a request to create a new API key
type CreateAPIKeyRequest struct {
	Name        string                 `json:"name" validate:"required,max=255"`
	Description string                 `json:"description,omitempty"`
	ExpiresAt   *time.Time             `json:"expires_at,omitempty"`
	Permissions map[string]interface{} `json:"permissions,omitempty"`
}

// CreateAPIKeyResponse represents the response after creating an API key
// The Key field is only returned once during creation
type CreateAPIKeyResponse struct {
	Key    string `json:"key"`           // Full key, shown only once
	KeyID  string `json:"key_id"`        // Public identifier
	APIKey APIKey `json:"api_key"`       // Full API key object
}

// UpdateAPIKeyRequest represents a request to update an existing API key
type UpdateAPIKeyRequest struct {
	Name        *string                `json:"name,omitempty"`
	Description *string                `json:"description,omitempty"`
	ExpiresAt   *time.Time             `json:"expires_at,omitempty"`
	IsActive    *bool                  `json:"is_active,omitempty"`
	Permissions map[string]interface{} `json:"permissions,omitempty"`
}

// APIKeyListResponse represents a response containing multiple API keys
type APIKeyListResponse struct {
	Success bool     `json:"success"`
	Data    []APIKey `json:"data"`
	Error   string   `json:"error,omitempty"`
}

// APIKeyResponse represents a response containing a single API key
type APIKeyResponse struct {
	Success bool   `json:"success"`
	Data    APIKey `json:"data"`
	Error   string `json:"error,omitempty"`
}

// ValidatedAPIKey represents an API key that has been validated for authentication
// It includes additional context needed during request processing
type ValidatedAPIKey struct {
	APIKey
	// FractalName is always populated for validated keys
	FractalName string `json:"fractal_name"`
}

// DefaultPermissions returns the default permissions for a new API key
func DefaultPermissions() map[string]interface{} {
	return map[string]interface{}{
		"query":        true,
		"comment":      true,
		"alert_manage": false,
		"notebook":     false,
		"dashboard":    false,
	}
}

// validPermissionKeys defines the only permission keys allowed on API keys.
var validPermissionKeys = map[string]bool{
	"query":        true,
	"comment":      true,
	"alert_manage": true,
	"notebook":     true,
	"dashboard":    true,
}

// ValidatePermissions checks that a permissions map contains only known keys
// with boolean values. Returns a sanitized copy merged over defaults.
func ValidatePermissions(perms map[string]interface{}) (map[string]interface{}, error) {
	result := DefaultPermissions()
	if perms == nil {
		return result, nil
	}
	for k, v := range perms {
		if !validPermissionKeys[k] {
			return nil, fmt.Errorf("unknown permission: %s", k)
		}
		boolVal, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("permission %s must be a boolean", k)
		}
		result[k] = boolVal
	}
	return result, nil
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

// CanQuery checks if the API key has query permissions
func (k *APIKey) CanQuery() bool {
	if k.Permissions == nil {
		return false
	}
	query, ok := k.Permissions["query"].(bool)
	return ok && query
}

// CanComment checks if the API key has comment permissions
func (k *APIKey) CanComment() bool {
	if k.Permissions == nil {
		return false
	}
	comment, ok := k.Permissions["comment"].(bool)
	return ok && comment
}

// CanManageAlerts checks if the API key has alert management permissions
func (k *APIKey) CanManageAlerts() bool {
	if k.Permissions == nil {
		return false
	}
	alertManage, ok := k.Permissions["alert_manage"].(bool)
	return ok && alertManage
}

// CanAccessNotebooks checks if the API key has notebook permissions
func (k *APIKey) CanAccessNotebooks() bool {
	if k.Permissions == nil {
		return false
	}
	v, ok := k.Permissions["notebook"].(bool)
	return ok && v
}

// CanAccessDashboards checks if the API key has dashboard permissions
func (k *APIKey) CanAccessDashboards() bool {
	if k.Permissions == nil {
		return false
	}
	v, ok := k.Permissions["dashboard"].(bool)
	return ok && v
}