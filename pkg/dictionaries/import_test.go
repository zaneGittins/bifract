package dictionaries

import (
	"fmt"
	"strings"
	"testing"
)

// collect runs an import over text and records what each insert received.
func collect(t *testing.T, text, key string, firstSeq uint64, batchSize int) ([][]DictionaryRow, [][]uint64, int, error) {
	t.Helper()
	reader, headers, err := newCSVImport(strings.NewReader(text))
	if err != nil {
		return nil, nil, 0, err
	}
	var batches [][]DictionaryRow
	var seqs [][]uint64
	imported, err := streamCSVBatches(reader, headers, key, firstSeq, batchSize,
		func(batch []DictionaryRow, s []uint64) error {
			// The slices are reused between batches, so keep a copy.
			batches = append(batches, append([]DictionaryRow(nil), batch...))
			seqs = append(seqs, append([]uint64(nil), s...))
			return nil
		})
	return batches, seqs, imported, err
}

// A file is never held whole: it reaches the table in batches as it is read, so
// a load of any size costs one batch of memory.
func TestImportInsertsInBatchesAsItReads(t *testing.T) {
	var file strings.Builder
	file.WriteString("ioc,source\n")
	for i := 0; i < 2500; i++ {
		fmt.Fprintf(&file, "value-%d,feed\n", i)
	}

	batches, _, imported, err := collect(t, file.String(), "ioc", 0, 1000)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if imported != 2500 {
		t.Errorf("imported %d rows, want 2500", imported)
	}
	sizes := []int{}
	for _, batch := range batches {
		sizes = append(sizes, len(batch))
	}
	if fmt.Sprint(sizes) != "[1000 1000 500]" {
		t.Errorf("batch sizes %v, want [1000 1000 500]", sizes)
	}
}

// Ordinals continue from what the dictionary already holds. A file sent as
// several requests would otherwise restart at 1 in every one of them and
// interleave its rows with the ones already loaded.
func TestImportNumbersRowsAfterTheOnesAlreadyLoaded(t *testing.T) {
	_, seqs, _, err := collect(t, "ioc\na\nb\nc\n", "ioc", 40, 1000)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if got := fmt.Sprint(seqs[0]); got != "[41 42 43]" {
		t.Errorf("ordinals %s, want [41 42 43]", got)
	}
}

func TestImportSkipsRowsWithNoKey(t *testing.T) {
	batches, _, imported, err := collect(t, "ioc,source\na,feed\n,feed\nb,feed\n", "ioc", 0, 1000)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if imported != 2 {
		t.Fatalf("imported %d rows, want 2: a row with no key cannot be looked up", imported)
	}
	if batches[0][1].Key != "b" {
		t.Errorf("second row is %q, want b", batches[0][1].Key)
	}
}

// A short line pads and a long one is truncated. One ragged line in a large file
// costs that line, not the load.
func TestImportToleratesRaggedRows(t *testing.T) {
	batches, _, imported, err := collect(t, "ioc,source,note\na,feed\nb,feed,seen,extra\n", "ioc", 0, 1000)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if imported != 2 {
		t.Fatalf("imported %d rows, want 2", imported)
	}
	if got := batches[0][0].Fields["note"]; got != "" {
		t.Errorf("short row filled note with %q, want empty", got)
	}
	if got := batches[0][1].Fields["note"]; got != "seen" {
		t.Errorf("long row lost note: %q", got)
	}
}

// A file that goes wrong part way through keeps what came before it: the rows
// already inserted are committed, and the count reports them.
func TestImportKeepsWhatLandedBeforeAParseError(t *testing.T) {
	broken := "ioc\ngood\n\"unterminated\n"
	_, _, imported, err := collect(t, broken, "ioc", 0, 1000)
	if err == nil {
		t.Fatal("a malformed file should report the error")
	}
	if !IsInvalidInput(err) {
		t.Errorf("a malformed file is the caller's fault, so it must be invalid input: %v", err)
	}
	if imported != 1 {
		t.Errorf("imported %d rows, want the 1 that landed before the error", imported)
	}
}

func TestImportRejectsAFileWithNoHeader(t *testing.T) {
	_, _, _, err := collect(t, "", "ioc", 0, 1000)
	if err == nil || !IsInvalidInput(err) {
		t.Errorf("an empty file must be reported as invalid input, got %v", err)
	}
}
