//go:build integration

// Moving work between fractals. A notebook exports as a YAML document, which is
// what makes an investigation reviewable in a pull request and reusable in a
// second environment.
//
//	go test -tags integration ./test/integration/ -run TestNotebookPortability -v

package integration

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestNotebookPortability(t *testing.T) {
	c := New(t)
	source := requireFractal(t, c)
	scoped := c.InScope(source)

	// 1. A notebook holds an investigation: prose, queries, and their results.
	name := fmt.Sprintf("api-suite-%d", time.Now().UnixNano())
	var notebook struct {
		ID string `json:"id"`
	}
	scoped.Do(t, "POST", "/notebooks", map[string]any{
		"name":            name,
		"description":     "Created by the API test suite",
		"time_range_type": "24h",
	}, &notebook)

	if notebook.ID == "" {
		t.Fatal("the created notebook came back without an id")
	}
	t.Cleanup(func() {
		scoped.Status(t, "DELETE", "/notebooks/"+notebook.ID, nil)
	})

	// 2. Sections are the content. A markdown section carries the reasoning; a
	//    query section carries the evidence and can be re-run later.
	scoped.Do(t, "POST", "/notebooks/"+notebook.ID+"/sections", map[string]any{
		"title":        "What we saw",
		"section_type": "markdown",
		"content":      "Errors climbed sharply at 03:00. Recorded by the API test suite.",
		"order_index":  0,
		"tags":         []string{"suite:api"},
	}, nil)

	scoped.Do(t, "POST", "/notebooks/"+notebook.ID+"/sections", map[string]any{
		"title":        "The errors",
		"section_type": "query",
		"content":      `level="error"`,
		"order_index":  1,
		"tags":         []string{"suite:api"},
	}, nil)

	// 3. Export it. The result is a YAML document, so it can live in a
	//    repository and be reviewed like any other change.
	exported := scoped.Raw(t, "GET", "/notebooks/"+notebook.ID+"/export", "", nil)
	if len(exported) == 0 {
		t.Fatal("the export was empty")
	}
	for _, want := range []string{"What we saw", "The errors"} {
		if !bytes.Contains(exported, []byte(want)) {
			t.Errorf("the export is missing the %q section", want)
		}
	}

	// 4. Import it back. Sending the same document to a different fractal is how
	//    an investigation template moves between environments; here it lands in
	//    the same one, which is why the imported copy takes a new name.
	imported := scoped.Raw(t, "POST", "/notebooks/import", "application/yaml", exported)
	if len(imported) == 0 {
		t.Fatal("the import answered nothing")
	}

	// 5. Both are now listed.
	var listed []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	scoped.Do(t, "GET", "/notebooks?limit=100", nil, &listed)

	var matches int
	for _, n := range listed {
		if n.Name == name || n.Name == name+" (imported)" {
			matches++
			if n.ID != notebook.ID {
				id := n.ID
				t.Cleanup(func() { scoped.Status(t, "DELETE", "/notebooks/"+id, nil) })
			}
		}
	}
	if matches < 1 {
		t.Errorf("neither the original nor the imported notebook was listed")
	}
}
