package chat

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The model's answer is advisory: only IDs the embedded matrix knows survive,
// tactic aliases resolve, and labels already on the rule are not repeated.
func TestSuggestAttackLabelsFiltersThroughMatrix(t *testing.T) {
	answer := "```json\n" + `{"techniques":["T1059.001","T9999","t1003"],"tactics":["defense-evasion","execution","nonsense"]}` + "\n```"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req llmRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Stream {
			t.Errorf("unexpected request: err=%v stream=%v", err, req.Stream)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]any{"content": answer}}},
		})
	}))
	defer srv.Close()

	m := &Manager{litellmURL: srv.URL, httpClient: srv.Client()}
	got, err := m.SuggestAttackLabels(context.Background(), SuggestLabelsRequest{
		Name:        "PowerShell download cradle",
		QueryString: `image=~powershell`,
		Labels:      []string{"attack.T1003"},
	})
	if err != nil {
		t.Fatal(err)
	}

	labels := make([]string, 0, len(got))
	for _, s := range got {
		labels = append(labels, s.Label)
		if s.Name == "" || s.Kind == "" {
			t.Errorf("%s: missing name or kind", s.Label)
		}
	}
	want := []string{"attack.t1059.001", "attack.stealth", "attack.execution"}
	if len(labels) != len(want) {
		t.Fatalf("got %v, want %v", labels, want)
	}
	for i := range want {
		if labels[i] != want[i] {
			t.Fatalf("got %v, want %v", labels, want)
		}
	}
}

func TestSuggestAttackLabelsNeedsSomethingToMap(t *testing.T) {
	m := &Manager{}
	if _, err := m.SuggestAttackLabels(context.Background(), SuggestLabelsRequest{}); err == nil {
		t.Fatal("expected an error for an empty rule")
	}
}
