package comments

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/storage"
	"bifract/pkg/storage/storagetest"

	"github.com/go-chi/chi/v5"
)

// Editing or deleting a comment by id is confined to the caller's scope and
// role, as reading and tagging one are. Authorship alone would let a key issued
// for one fractal change its creator's comments in every other, and let a user
// removed from a fractal keep editing there.
func TestCommentEditAndDeleteStayInScope(t *testing.T) {
	ts := time.Date(2026, 3, 4, 4, 6, 0, 0, time.UTC)
	commentIn := func(fractal string) fakedb.PGResult {
		return fakedb.Row("C1", "L1", ts, "text", "alice", "{}", "q", ts, ts, fractal, "", "Alice", "#fff", "A")
	}
	request := func(method, body, role string) *http.Request {
		r := httptest.NewRequest(method, "/", strings.NewReader(body))
		ctx := context.WithValue(r.Context(), "user", &storage.User{Username: "alice"})
		ctx = context.WithValue(ctx, "selected_fractal", "F1")
		ctx = context.WithValue(ctx, "fractal_role", role)
		rc := chi.NewRouteContext()
		rc.URLParams.Add("id", "C1")
		return r.WithContext(context.WithValue(ctx, chi.RouteCtxKey, rc))
	}

	cases := []struct {
		name    string
		fractal string
		role    string
		want    int
	}{
		{"comment in another fractal", "F2", "analyst", http.StatusNotFound},
		{"viewer in the comment's fractal", "F1", "viewer", http.StatusForbidden},
	}
	for _, c := range cases {
		for verb, handle := range map[string]func(h *CommentHandler) http.HandlerFunc{
			http.MethodPut:    func(h *CommentHandler) http.HandlerFunc { return h.HandleUpdateComment },
			http.MethodDelete: func(h *CommentHandler) http.HandlerFunc { return h.HandleDeleteComment },
		} {
			pg, fake := storagetest.Postgres()
			fake.On("FROM comments c", commentIn(c.fractal))
			rec := httptest.NewRecorder()
			handle(&CommentHandler{pg: pg})(rec, request(verb, `{"text":"changed"}`, c.role))
			if rec.Code != c.want {
				t.Errorf("%s %s: status %d, want %d", c.name, verb, rec.Code, c.want)
			}
			for _, call := range fake.Calls() {
				if strings.Contains(call.Query, "UPDATE comments") || strings.Contains(call.Query, "DELETE FROM comments") {
					t.Errorf("%s %s: wrote %q", c.name, verb, call.Query)
				}
			}
		}
	}
}
