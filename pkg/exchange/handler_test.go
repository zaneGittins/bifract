package exchange

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"bifract/pkg/api"
)

func serve(h http.HandlerFunc, path, pattern string) *httptest.ResponseRecorder {
	r := chi.NewRouter()
	r.Get(pattern, h)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestHandleList(t *testing.T) {
	h := NewHandler(exchangeServer(t).client())

	rec := serve(h.HandleList, "/exchange", "/exchange")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var resp api.Response[Listing]
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Data.Enabled || len(resp.Data.Items) != 3 || resp.Data.Source == "" {
		t.Errorf("unexpected listing: %+v", resp.Data)
	}
}

func TestHandleListDisabled(t *testing.T) {
	h := NewHandler(NewClient("", false))
	rec := serve(h.HandleList, "/exchange", "/exchange")
	var resp api.Response[Listing]
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || resp.Data.Enabled || resp.Data.Items == nil {
		t.Errorf("a disabled exchange should answer an empty, disabled listing: %d %s", rec.Code, rec.Body)
	}
}

func TestHandleListUnreachable(t *testing.T) {
	f := exchangeServer(t)
	h := NewHandler(NewClient(f.srv.URL+"/repo/missing/index.yaml", true))
	if rec := serve(h.HandleList, "/exchange", "/exchange"); rec.Code != http.StatusBadGateway {
		t.Errorf("status %d, want 502", rec.Code)
	}
}

func TestHandleItem(t *testing.T) {
	h := NewHandler(exchangeServer(t).client())

	rec := serve(h.HandleItem, "/exchange/items/sysmon", "/exchange/items/{id}")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var resp api.Response[ItemContent]
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Data.Item.ID != "sysmon" || resp.Data.Content != "name: Sysmon\n" {
		t.Errorf("unexpected item: %+v", resp.Data)
	}

	if rec := serve(h.HandleItem, "/exchange/items/nope", "/exchange/items/{id}"); rec.Code != http.StatusNotFound {
		t.Errorf("unknown id: status %d, want 404", rec.Code)
	}
}

// A search arrives parsed, in the shape a saved query is created with.
func TestHandleItemSearch(t *testing.T) {
	h := NewHandler(exchangeServer(t).client())
	rec := serve(h.HandleItem, "/exchange/items/rmm-search", "/exchange/items/{id}")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"search":{"name":"RMM tools run","query_text":"event_id=1"}`) {
		t.Errorf("unexpected body: %s", rec.Body)
	}
}

func TestHandleItemDisabled(t *testing.T) {
	h := NewHandler(NewClient("", false))
	if rec := serve(h.HandleItem, "/exchange/items/sysmon", "/exchange/items/{id}"); rec.Code != http.StatusNotFound {
		t.Errorf("status %d, want 404", rec.Code)
	}
}
