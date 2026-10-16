package apikeys

import (
	"strings"
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }

func TestCreateRequestValidate(t *testing.T) {
	expiry := ptr(time.Now().Add(time.Hour))
	tests := []struct {
		name    string
		req     CreateAPIKeyRequest
		wantErr string
	}{
		{name: "scoped viewer", req: CreateAPIKeyRequest{Role: "viewer"}},
		{name: "scoped analyst", req: CreateAPIKeyRequest{Role: "analyst"}},
		{name: "scoped admin", req: CreateAPIKeyRequest{Role: "admin"}},
		{name: "scoped with no role", req: CreateAPIKeyRequest{}},
		// The role ladder is exact: anything else is an unknown grant.
		{name: "unknown role", req: CreateAPIKeyRequest{Role: "owner"}, wantErr: "role must be"},
		{name: "role is case sensitive", req: CreateAPIKeyRequest{Role: "Admin"}, wantErr: "role must be"},
		{name: "tenant admin with expiry", req: CreateAPIKeyRequest{TenantAdmin: true, ExpiresAt: expiry}},
		// An instance-wide credential that never expires is refused outright.
		{name: "tenant admin without expiry", req: CreateAPIKeyRequest{TenantAdmin: true}, wantErr: "must have an expiry"},
		{name: "tenant admin with scope role", req: CreateAPIKeyRequest{TenantAdmin: true, ExpiresAt: expiry, Role: "viewer"}, wantErr: "cannot also hold a scope role"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestUpdateRequestValidate(t *testing.T) {
	expiry := ptr(time.Now().Add(time.Hour))
	scoped := &APIKey{Role: "viewer", FractalID: fractalA}
	tenant := &APIKey{TenantAdmin: true, ExpiresAt: expiry}
	legacyTenant := &APIKey{TenantAdmin: true} // issued before expiry was required

	tests := []struct {
		name    string
		current *APIKey
		req     UpdateAPIKeyRequest
		wantErr string
	}{
		{name: "rename scoped", current: scoped, req: UpdateAPIKeyRequest{Name: ptr("x")}},
		{name: "raise scoped role", current: scoped, req: UpdateAPIKeyRequest{Role: ptr("admin")}},
		{name: "unknown role", current: scoped, req: UpdateAPIKeyRequest{Role: ptr("root")}, wantErr: "role must be"},
		// A key never changes kind in either direction.
		{name: "promote scoped to tenant admin", current: scoped, req: UpdateAPIKeyRequest{TenantAdmin: ptr(true), ExpiresAt: expiry}, wantErr: "cannot change between"},
		{name: "demote tenant admin to scoped", current: tenant, req: UpdateAPIKeyRequest{TenantAdmin: ptr(false)}, wantErr: "cannot change between"},
		{name: "restating the same kind is fine", current: tenant, req: UpdateAPIKeyRequest{TenantAdmin: ptr(true)}},
		{name: "tenant admin gains scope role", current: tenant, req: UpdateAPIKeyRequest{Role: ptr("analyst")}, wantErr: "cannot also hold a scope role"},
		{name: "tenant admin with empty role", current: tenant, req: UpdateAPIKeyRequest{Role: ptr("")}},
		// An update to a never-expiring tenant key must give it an expiry first.
		{name: "legacy tenant key without expiry", current: legacyTenant, req: UpdateAPIKeyRequest{Name: ptr("x")}, wantErr: "must have an expiry"},
		{name: "legacy tenant key given expiry", current: legacyTenant, req: UpdateAPIKeyRequest{ExpiresAt: expiry}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate(tt.current)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestKeyValidity(t *testing.T) {
	past := ptr(time.Now().Add(-time.Minute))
	future := ptr(time.Now().Add(time.Hour))
	tests := []struct {
		name        string
		key         APIKey
		expired, ok bool
	}{
		{name: "active, no expiry", key: APIKey{IsActive: true}, ok: true},
		{name: "active, future expiry", key: APIKey{IsActive: true, ExpiresAt: future}, ok: true},
		{name: "active, expired", key: APIKey{IsActive: true, ExpiresAt: past}, expired: true},
		{name: "inactive", key: APIKey{IsActive: false}},
		{name: "inactive and expired", key: APIKey{ExpiresAt: past}, expired: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.key.IsExpired(); got != tt.expired {
				t.Errorf("IsExpired() = %v, want %v", got, tt.expired)
			}
			if got := tt.key.IsValid(); got != tt.ok {
				t.Errorf("IsValid() = %v, want %v", got, tt.ok)
			}
		})
	}
}
