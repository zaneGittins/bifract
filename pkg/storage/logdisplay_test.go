package storage

import (
	"reflect"
	"testing"
)

// TestMissingLogIDs pins the set the unbounded retry pass runs over: the bounded
// window is an optimisation, so anything it did not find has to be retried, in
// the original order and without duplicates.
func TestMissingLogIDs(t *testing.T) {
	found := map[string]map[string]interface{}{
		"aa": {"log_id": "aa"},
		"cc": {"log_id": "cc"},
	}

	tests := []struct {
		name  string
		ids   []string
		found map[string]map[string]interface{}
		want  []string
	}{
		{"some missing", []string{"aa", "bb", "cc", "dd"}, found, []string{"bb", "dd"}},
		{"none missing", []string{"aa", "cc"}, found, nil},
		{"all missing", []string{"xx", "yy"}, found, []string{"xx", "yy"}},
		{"nothing found at all", []string{"aa"}, map[string]map[string]interface{}{}, []string{"aa"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := missingLogIDs(tt.ids, tt.found); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("missingLogIDs(%v) = %v, want %v", tt.ids, got, tt.want)
			}
		})
	}
}

// TestChunkStringsCoversEveryID guards the chunking the batched lookup relies on:
// dropping a chunk would silently lose logs from a generated notebook.
func TestChunkStringsCoversEveryID(t *testing.T) {
	for _, n := range []int{0, 1, commentLogChunkSize - 1, commentLogChunkSize, commentLogChunkSize + 1, commentLogChunkSize*2 + 7} {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = string(rune('a'+i%26)) + string(rune('0'+i%10))
		}

		seen := 0
		for _, chunk := range chunkStrings(ids, commentLogChunkSize) {
			if len(chunk) > commentLogChunkSize {
				t.Fatalf("n=%d: chunk of %d exceeds the cap", n, len(chunk))
			}
			seen += len(chunk)
		}
		if seen != n {
			t.Errorf("n=%d: chunks covered %d ids", n, seen)
		}
	}
}

// TestLogKeysFromIDs checks the adapter feeding dedupeLogIDs drops blanks and
// duplicates, so neither reaches an IN list.
func TestLogKeysFromIDs(t *testing.T) {
	got := dedupeLogIDs(logKeysFromIDs([]string{"aa", "", "bb", "aa", "cc", ""}))
	want := []string{"aa", "bb", "cc"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
