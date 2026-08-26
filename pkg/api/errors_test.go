package api

import (
	"net/http"
	"testing"
)

// TestCodeForStatus pins the status-to-code mapping every error without an
// explicit code relies on.
func TestCodeForStatus(t *testing.T) {
	cases := map[int]ErrorCode{
		http.StatusBadRequest:            CodeBadRequest,
		http.StatusUnauthorized:          CodeUnauthenticated,
		http.StatusForbidden:             CodeForbidden,
		http.StatusNotFound:              CodeNotFound,
		http.StatusConflict:              CodeConflict,
		http.StatusTooManyRequests:       CodeRateLimited,
		http.StatusRequestEntityTooLarge: CodePayloadTooLarge,
		http.StatusInternalServerError:   CodeInternal,
		http.StatusBadGateway:            CodeInternal,
		http.StatusTeapot:                CodeBadRequest,
	}
	for status, want := range cases {
		if got := CodeForStatus(status); got != want {
			t.Errorf("CodeForStatus(%d) = %q, want %q", status, got, want)
		}
	}
}
