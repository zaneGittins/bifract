package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestEnvelopeWireShape pins the envelope's JSON contract. Every client and the
// generated schema depend on these exact keys and on absent fields staying
// absent, so a change here is a change to the public API.
func TestEnvelopeWireShape(t *testing.T) {
	cases := []struct {
		name string
		resp any
		want string
	}{
		{"zero value", Response[any]{}, `{"success":false}`},
		{"success only", Response[any]{Success: true}, `{"success":true}`},
		{"with data", Response[any]{Success: true, Data: map[string]string{"id": "x"}},
			`{"success":true,"data":{"id":"x"}}`},
		{"with message", Response[any]{Success: true, Message: "done"},
			`{"success":true,"message":"done"}`},
		{"error", Response[any]{Error: "boom"}, `{"success":false,"error":"boom"}`},
		{"error with code", Response[any]{Error: "boom", Code: CodeNotFound},
			`{"success":false,"error":"boom","code":"not_found"}`},
		{"typed payload", Response[[]string]{Success: true, Data: []string{"a"}},
			`{"success":true,"data":["a"]}`},
		{"empty slice payload is omitted", Response[[]string]{Success: true, Data: []string{}},
			`{"success":true}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.resp)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Errorf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

func TestWriters(t *testing.T) {
	cases := []struct {
		name       string
		write      func(http.ResponseWriter)
		wantStatus int
		wantBody   string
	}{
		{"success", func(w http.ResponseWriter) { WriteSuccess(w, []int{1, 2}) },
			http.StatusOK, `{"success":true,"data":[1,2]}` + "\n"},
		{"message", func(w http.ResponseWriter) { WriteMessage(w, "saved", map[string]int{"n": 1}) },
			http.StatusOK, `{"success":true,"data":{"n":1},"message":"saved"}` + "\n"},
		{"error carries a code derived from status", func(w http.ResponseWriter) { WriteError(w, http.StatusForbidden, "nope") },
			http.StatusForbidden, `{"success":false,"error":"nope","code":"forbidden"}` + "\n"},
		{"envelope built by a package still gets a code", func(w http.ResponseWriter) {
			WriteJSON(w, http.StatusNotFound, Response[any]{Error: "gone"})
		}, http.StatusNotFound, `{"success":false,"error":"gone","code":"not_found"}` + "\n"},
		{"a success envelope is left alone", func(w http.ResponseWriter) {
			WriteJSON(w, http.StatusOK, Response[any]{Success: true})
		}, http.StatusOK, `{"success":true}` + "\n"},
		{"explicit code wins", func(w http.ResponseWriter) {
			WriteErrorCode(w, http.StatusBadRequest, CodeConflict, "clash")
		}, http.StatusBadRequest, `{"success":false,"error":"clash","code":"conflict"}` + "\n"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.write(rec)

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}
			if rec.Body.String() != tc.wantBody {
				t.Errorf("body = %q, want %q", rec.Body.String(), tc.wantBody)
			}
		})
	}
}
