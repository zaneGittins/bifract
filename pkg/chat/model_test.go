package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bifract/internal/fakedb"
	"bifract/pkg/llm"
	"bifract/pkg/storage/storagetest"
)

func modelClient(t *testing.T, baseURL string) *llm.Client {
	t.Helper()
	c, err := llm.New(llm.Config{Provider: llm.ProviderOpenAI, APIKey: "k", Model: "small", Models: []string{"large"}, BaseURL: baseURL})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSetConversationModelOnlyStoresListedModels(t *testing.T) {
	pg, fake := storagetest.Postgres()
	t.Cleanup(func() { pg.Close() })
	m := &Manager{pg: pg, llm: modelClient(t, "http://unused")}
	fake.On("UPDATE chat_conversations SET model", fakedb.Affected(1))

	if err := m.SetConversationModel(context.Background(), "c1", "unlisted"); !errors.Is(err, llm.ErrModelNotAllowed) {
		t.Fatalf("err = %v, want ErrModelNotAllowed", err)
	}
	if n := len(fake.Matching("UPDATE chat_conversations SET model")); n != 0 {
		t.Fatalf("unlisted model stored %d times", n)
	}
	for _, model := range []string{"large", ""} {
		if err := m.SetConversationModel(context.Background(), "c1", model); err != nil {
			t.Fatalf("%q: %v", model, err)
		}
	}
	calls := fake.Matching("UPDATE chat_conversations SET model")
	if len(calls) != 2 || calls[0].Args[0] != "large" || calls[1].Args[0] != nil {
		t.Fatalf("updates: %+v", calls)
	}
}

// A conversation reports the model that will answer it, so one whose choice
// was removed from the list shows the default rather than a dead name.
func TestConversationReportsTheModelThatAnswers(t *testing.T) {
	pg, fake := storagetest.Postgres()
	t.Cleanup(func() { pg.Close() })
	m := &Manager{pg: pg, llm: modelClient(t, "http://unused")}
	now := time.Now()
	fake.OnOnce("FROM chat_conversations WHERE id", fakedb.Row("c1", "f1", "", "t", "alice", now, now, "large"))
	fake.OnOnce("FROM chat_conversations WHERE id", fakedb.Row("c2", "f1", "", "t", "alice", now, now, "retired"))

	for _, want := range []string{"large", "small"} {
		conv, err := m.GetConversation(context.Background(), "c")
		if err != nil {
			t.Fatal(err)
		}
		if conv.Model != want {
			t.Errorf("model = %q, want %q", conv.Model, want)
		}
	}
}

func TestStreamUsesTheConversationsModel(t *testing.T) {
	var sent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model string `json:"model"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		sent = req.Model
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	m := &Manager{llm: modelClient(t, srv.URL)}
	rec := httptest.NewRecorder()
	if _, _, err := m.streamFromLLM(context.Background(), rec, rec, "large", []llmMessage{{Role: "user", Content: "x"}}, nil); err != nil {
		t.Fatal(err)
	}
	if sent != "large" {
		t.Errorf("sent model %q, want large", sent)
	}
}
