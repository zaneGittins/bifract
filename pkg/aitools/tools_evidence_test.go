package aitools

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"testing"
)

// Marking an event is one verb. add_comment writes the record every surface
// reads: the row's mark in search, what comments() finds, and, with a notebook,
// the entry in that investigation. A second way to write evidence is what made
// the notebook and the comments tab disagree in the first place.
func TestAddCommentFilesIntoTheNotebookItIsGiven(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{"id":"c1"}}`)

	if _, err := addComment(context.Background(), s, addCommentArgs{
		LogID:      "a3f5c1d2e4b60718",
		Text:       "beaconing",
		Tags:       []string{"IR-Beacon"},
		NotebookID: "nb-1",
		Title:      "WKSTN - curl",
	}); err != nil {
		t.Fatal(err)
	}

	var sent map[string]any
	if err := json.Unmarshal(s.sent, &sent); err != nil {
		t.Fatal(err)
	}
	if sent["notebook_id"] != "nb-1" {
		t.Errorf("notebook_id = %v, want nb-1", sent["notebook_id"])
	}
	if sent["title"] != "WKSTN - curl" {
		t.Errorf("title = %v, want the outline line", sent["title"])
	}
	// The AI tag is what tells a reader which findings came from a model.
	tags, _ := sent["tags"].([]any)
	if len(tags) != 2 || tags[0] != aiTag || tags[1] != "IR-Beacon" {
		t.Errorf("tags = %v, want [%s IR-Beacon]", tags, aiTag)
	}
}

// An explicit notebook must not cost a lookup of the analyst's own.
func TestAddCommentDoesNotAskForTheActiveNotebookWhenGivenOne(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{"id":"c1"}}`)

	if _, err := addComment(context.Background(), s, addCommentArgs{
		LogID: "a3f5c1d2", Text: "x", NotebookID: "nb-1",
	}); err != nil {
		t.Fatal(err)
	}
	for _, call := range s.seen {
		if call.Path == "/notebooks/active" {
			t.Fatal("looked up the active notebook despite being given one")
		}
	}
}

// In a chat session the tool runs as the analyst, so evidence lands where they
// are already collecting and reaches their rail live.
func TestAddCommentFilesIntoTheAnalystsActiveNotebook(t *testing.T) {
	s := serve(t, 200, `{"success":true,"data":{"id":"c1"}}`)
	s.byPath = map[string]string{
		"/notebooks/active": `{"success":true,"data":{"notebook_id":"nb-active","has_notebooks":true}}`,
	}

	if _, err := addComment(context.Background(), s, addCommentArgs{LogID: "a3f5c1d2", Text: "x"}); err != nil {
		t.Fatal(err)
	}

	var sent map[string]any
	if err := json.Unmarshal(s.sent, &sent); err != nil {
		t.Fatal(err)
	}
	if sent["notebook_id"] != "nb-active" {
		t.Errorf("notebook_id = %v, want nb-active", sent["notebook_id"])
	}
}

// Over MCP the credential is a machine principal with no active notebook, and
// that endpoint refuses it. The comment still has to be written: an unfiled
// annotation is worth more than a lost one.
func TestAddCommentStillWritesWhenThereIsNoActiveNotebook(t *testing.T) {
	s := &refusingActive{fakeClient: fakeClient{status: 200, body: `{"success":true,"data":{"id":"c1"}}`}}

	if _, err := addComment(context.Background(), s, addCommentArgs{LogID: "a3f5c1d2", Text: "x"}); err != nil {
		t.Fatal(err)
	}

	var sent map[string]any
	if err := json.Unmarshal(s.sent, &sent); err != nil {
		t.Fatal(err)
	}
	if _, filed := sent["notebook_id"]; filed {
		t.Errorf("filed into a notebook nobody named: %v", sent["notebook_id"])
	}
	if sent["text"] != "x" {
		t.Errorf("text = %v, want the comment to have been written anyway", sent["text"])
	}
}

// refusingActive answers the active-notebook lookup the way the API does for an
// API key, and everything else normally.
type refusingActive struct{ fakeClient }

func (r *refusingActive) Get(ctx context.Context, path string, query url.Values) (any, error) {
	if path == "/notebooks/active" {
		return nil, errors.New("the active notebook is per-user and not available for API key authentication")
	}
	return r.fakeClient.Get(ctx, path, query)
}
