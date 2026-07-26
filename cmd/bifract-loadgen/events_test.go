package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func testSampler(t *testing.T) (*corpus, *sampler, *host) {
	t.Helper()
	c := buildCorpus(2000, 500, 7)
	s := newSampler(c, 7)
	s.card = newCardinality(c, 16)
	return c, s, newHost(c, s, 0)
}

// The hot path writes JSON by hand with pre-escaped corpus values. A single missed escape
// would be rejected by the server for a whole run, so this is the guardrail.
func TestEventsAreValidJSON(t *testing.T) {
	c, s, h := testSampler(t)
	base := utcBase(time.Now())
	for i := 0; i < 20000; i++ {
		var j jbuf
		s.emit(&j, c, h, base, i%1000, 0.002)
		var m map[string]any
		if err := json.Unmarshal(j.b, &m); err != nil {
			t.Fatalf("event %d is not valid JSON: %v\n%s", i, err, j.b)
		}
		if _, ok := m["EventID"]; !ok {
			t.Fatalf("event %d missing EventID: %s", i, j.b)
		}
	}
}

// Windows paths are the escaping hazard. They must survive as single backslashes.
func TestPathEscaping(t *testing.T) {
	c, s, h := testSampler(t)
	base := utcBase(time.Now())
	var j jbuf
	for i := 0; i < 500; i++ {
		j.b = j.b[:0]
		if s.emit(&j, c, h, base, i, 0.002) == kindFile {
			break
		}
	}
	var m map[string]any
	if err := json.Unmarshal(j.b, &m); err != nil {
		t.Fatalf("file event invalid: %v", err)
	}
	img, _ := m["Image"].(string)
	if strings.Contains(img, `\\`) {
		t.Errorf("Image has double backslashes after decode: %q", img)
	}
	if !strings.Contains(img, `\`) {
		t.Errorf("Image lost its backslashes: %q", img)
	}
}

// pgr() traverses process GUIDs shared between a spawn and that process's later activity.
// If reuse breaks, the benchmark still ingests but the provenance data is meaningless.
func TestProcessGUIDReuse(t *testing.T) {
	c, s, h := testSampler(t)
	base := utcBase(time.Now())
	spawned := map[string]bool{}
	reused := 0
	for i := 0; i < 20000; i++ {
		var j jbuf
		kind := s.emit(&j, c, h, base, i%1000, 0.002)
		var m map[string]any
		if err := json.Unmarshal(j.b, &m); err != nil {
			t.Fatalf("invalid JSON: %v", err)
		}
		guid, _ := m["ProcessGuid"].(string)
		if guid == "" {
			continue
		}
		if kind == kindProcess {
			spawned[guid] = true
		} else if spawned[guid] {
			reused++
		}
	}
	if reused == 0 {
		t.Fatal("no activity event reused a spawned process GUID; provenance edges would be empty")
	}
	t.Logf("%d activity events reused one of %d spawned GUIDs", reused, len(spawned))
}

// The realized mix drives the published event-shape disclosure; drift means the weights
// and the documented table disagree.
func TestEventMixMatchesWeights(t *testing.T) {
	c, s, h := testSampler(t)
	base := utcBase(time.Now())
	const n = 50000
	counts := [kindCount]int{}
	for i := 0; i < n; i++ {
		var j jbuf
		counts[s.emit(&j, c, h, base, i%1000, 0.002)]++
	}
	want := [kindCount]float64{35, 30, 20, 10, 3, 2}
	for k := 0; k < kindCount; k++ {
		got := float64(counts[k]) / n * 100
		if got < want[k]-1.5 || got > want[k]+1.5 {
			t.Errorf("%s: got %.1f%%, want %.1f%% (+/-1.5)", kindNames[k], got, want[k])
		}
	}
}

// Hashes identify a binary, not an event. Randomizing per event would inflate cardinality
// and understate the compression ratio the benchmark reports.
func TestHashesAreStablePerImage(t *testing.T) {
	c, s, h := testSampler(t)
	base := utcBase(time.Now())
	seen := map[string]string{}
	for i := 0; i < 20000; i++ {
		var j jbuf
		if s.emit(&j, c, h, base, i%1000, 0.002) != kindProcess {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(j.b, &m); err != nil {
			t.Fatalf("invalid JSON: %v", err)
		}
		img, _ := m["Image"].(string)
		hash, _ := m["Hashes"].(string)
		if prev, ok := seen[img]; ok && prev != hash {
			t.Fatalf("image %q reported two hashes:\n  %s\n  %s", img, prev, hash)
		}
		seen[img] = hash
	}
}
