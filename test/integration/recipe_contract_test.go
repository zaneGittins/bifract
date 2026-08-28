//go:build integration

// The contract every client codes against: what a failure looks like, and how a
// collection is paged. These are the parts of the API that are easy to describe
// and easy to let drift, so they are asserted against a running server rather
// than against the description alone.
//
//	go test -tags integration ./test/integration/ -run TestContract -v

package integration

import (
	"net/http"
	"testing"

	"bifract/pkg/api"
)

func TestContractErrors(t *testing.T) {
	c := New(t)

	cases := []struct {
		name       string
		client     *Client
		method     string
		path       string
		body       any
		wantStatus int
		wantCode   api.ErrorCode
	}{
		{
			name:       "no credential",
			client:     c.WithKey(""),
			method:     "GET",
			path:       "/settings",
			wantStatus: http.StatusUnauthorized,
			wantCode:   api.CodeUnauthenticated,
		},
		{
			name:       "a credential that is not a key",
			client:     c.WithKey("bifract_not_a_real_key"),
			method:     "GET",
			path:       "/settings",
			wantStatus: http.StatusUnauthorized,
			wantCode:   api.CodeUnauthenticated,
		},
		{
			name:       "a resource that does not exist",
			client:     c,
			method:     "GET",
			path:       "/fractals/00000000-0000-0000-0000-000000000000",
			wantStatus: http.StatusNotFound,
			wantCode:   api.CodeNotFound,
		},
		{
			name:       "a body that fails validation",
			client:     c,
			method:     "POST",
			path:       "/groups",
			body:       map[string]any{"name": ""},
			wantStatus: http.StatusBadRequest,
			wantCode:   api.CodeBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, code := tc.client.Failure(t, tc.method, tc.path, tc.body)
			if status != tc.wantStatus {
				t.Errorf("status = %d, want %d", status, tc.wantStatus)
			}
			if code != tc.wantCode {
				t.Errorf("code = %q, want %q", code, tc.wantCode)
			}
		})
	}
}

// TestContractPagination checks that a collection answers the documented page
// envelope and that limit and offset actually move the window, rather than
// being accepted and ignored.
func TestContractPagination(t *testing.T) {
	c := New(t)

	var all struct {
		Data []map[string]any `json:"data"`
		Page *api.Page        `json:"page"`
	}
	c.DoRaw(t, "GET", "/users", nil, &all)

	if all.Page == nil {
		t.Fatal("a paged collection answered without a page object")
	}
	if all.Page.Total < len(all.Data) {
		t.Errorf("page.total (%d) is less than the rows returned (%d)", all.Page.Total, len(all.Data))
	}
	if all.Data == nil {
		t.Error("data was absent; an empty page must still answer []")
	}

	// One row at a time must return one row, and say so.
	var first struct {
		Data []map[string]any `json:"data"`
		Page *api.Page        `json:"page"`
	}
	c.DoRaw(t, "GET", "/users?limit=1&offset=0", nil, &first)
	if len(first.Data) > 1 {
		t.Errorf("limit=1 returned %d rows", len(first.Data))
	}
	if first.Page == nil || first.Page.Limit != 1 {
		t.Errorf("page did not echo the limit it applied: %+v", first.Page)
	}
	if first.Page != nil && first.Page.Total != all.Page.Total {
		t.Errorf("total changed with the window: %d then %d", all.Page.Total, first.Page.Total)
	}
}
