package archive

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"

	"bifract/pkg/storage"
)

// tasksOfSizes builds one scan task per size, all in one ingest_date partition,
// standing in for a sealed day the archiver has finished writing. Paths are
// unique per file: the plan identifies files by path, so colliding paths make a
// multi-file partition indistinguishable from a single file.
func tasksOfSizes(t *testing.T, day iceberg.Date, sizes []int64) []icetable.FileScanTask {
	t.Helper()
	sc, err := icebergSchema()
	if err != nil {
		t.Fatalf("iceberg schema: %v", err)
	}
	spec, ok := partitionSpec(sc)
	if !ok {
		t.Fatal("partition spec not found")
	}
	tasks := make([]icetable.FileScanTask, 0, len(sizes))
	for i, size := range sizes {
		b, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
			fmt.Sprintf("s3://bucket/table/data/ingest_date=%d/%d-%d.parquet", day, i, size),
			iceberg.ParquetFile,
			map[int]any{partitionFieldID: day}, nil, nil, 1000, size)
		if err != nil {
			t.Fatalf("data file builder: %v", err)
		}
		tasks = append(tasks, icetable.FileScanTask{File: b.Build()})
	}
	return tasks
}

// sealedTasks is tasksOfSizes for n identically-sized files.
func sealedTasks(t *testing.T, day iceberg.Date, n int, size int64) []icetable.FileScanTask {
	t.Helper()
	return tasksOfSizes(t, day, repeatSize(n, size))
}

func planGroups(t *testing.T, cfg compaction.Config, tasks []icetable.FileScanTask) int {
	t.Helper()
	plan, err := cfg.PlanCompaction(tasks)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return len(plan.Groups)
}

// A sealed partition never gains another file, so the library's MinInputFiles of
// 5 strands any partition holding 2-4 small files forever. That is the quiet
// low-volume fractal that "never compacts", so pin the difference.
func TestCompactionConfigCompactsSmallSealedPartitions(t *testing.T) {
	const small = 20 << 20
	for n := 2; n <= 4; n++ {
		tasks := sealedTasks(t, 20000, n, small)
		if got := planGroups(t, compaction.DefaultConfig(), tasks); got != 0 {
			t.Fatalf("premise changed: library default planned %d group(s) for %d files", got, n)
		}
		if got := planGroups(t, compactionConfig(), tasks); got != 1 {
			t.Errorf("%d small sealed files planned %d group(s), want 1", n, got)
		}
	}
}

// The remainder bin of a partition is subject to the same floor, so a file count
// that does not divide evenly into target-sized bins used to strand its tail.
func TestCompactionConfigCompactsRemainderBin(t *testing.T) {
	// 27 x 20MB packs to 512MB as 25 + a remainder of 2.
	if got := planGroups(t, compactionConfig(), sealedTasks(t, 20001, 27, 20<<20)); got != 2 {
		t.Errorf("planned %d group(s) for 27 files, want 2 (full bin + remainder)", got)
	}
}

// Convergence: files at or above the optimal floor are not candidates, so a
// compacted partition is not rewritten again on the next pass.
func TestCompactionConfigLeavesOptimalFilesAlone(t *testing.T) {
	cfg := compactionConfig()
	if got := planGroups(t, cfg, sealedTasks(t, 20002, 4, cfg.MinFileSizeBytes)); got != 0 {
		t.Errorf("optimal files planned %d group(s), want 0", got)
	}
}

// A single file has nothing to merge with, whatever its size.
func TestCompactionConfigSkipsLoneFile(t *testing.T) {
	if got := planGroups(t, compactionConfig(), sealedTasks(t, 20003, 1, 1<<20)); got != 0 {
		t.Errorf("lone file planned %d group(s), want 0", got)
	}
}

func TestLookbackLabel(t *testing.T) {
	if got := lookbackLabel(0); got != "whole table (deep pass)" {
		t.Errorf("lookbackLabel(0) = %q", got)
	}
	if got := lookbackLabel(defaultCompactLookback); got != "72h0m0s" {
		t.Errorf("lookbackLabel(72h) = %q", got)
	}
}

// payloadSize must depend only on the data, so the roll threshold means the same
// volume for a field-dense fractal as for a raw-heavy one. approxSize must not:
// it exists to predict memory, and field-dense entries genuinely cost more.
func TestPayloadSizeIgnoresFieldDensity(t *testing.T) {
	raw := storage.LogEntry{LogID: "a", FractalID: "f", RawLog: string(make([]byte, 600))}
	dense := storage.LogEntry{LogID: "a", FractalID: "f", Fields: map[string]string{}}
	for i := range 60 {
		dense.Fields[fmt.Sprintf("k%02d", i)] = string(make([]byte, 7))
	}

	if p1, p2 := payloadSize(&raw), payloadSize(&dense); p1 != p2 {
		t.Errorf("equal payloads measured differently: raw %d, dense %d", p1, p2)
	}
	if a1, a2 := approxSize(&raw), approxSize(&dense); a2 <= a1 {
		t.Errorf("approxSize should charge the dense entry more: raw %d, dense %d", a1, a2)
	}
}

// simulateCompaction runs plan-then-rewrite rounds over one partition's file
// sizes until the plan comes back empty, returning the rounds taken and the
// resulting sizes. A group is modelled the way ExecuteCompactionGroup writes it:
// its inputs become ceil(total/target) evenly-sized output files.
func simulateCompaction(t *testing.T, cfg compaction.Config, day iceberg.Date, sizes []int64) (rounds int, out []int64) {
	t.Helper()
	out = append([]int64(nil), sizes...)
	for rounds = 0; rounds < 20; rounds++ {
		tasks := tasksOfSizes(t, day, out)
		plan, err := cfg.PlanCompaction(tasks)
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		if len(plan.Groups) == 0 {
			return rounds, out
		}
		// Everything not in a group survives; each group collapses to its outputs.
		grouped := map[string]bool{}
		var next []int64
		for _, g := range plan.Groups {
			for _, task := range g.Tasks {
				grouped[task.File.FilePath()] = true
			}
			n := int64(max(1, int((g.TotalSizeBytes+cfg.TargetFileSizeBytes-1)/cfg.TargetFileSizeBytes)))
			for range n {
				next = append(next, g.TotalSizeBytes/n)
			}
		}
		for _, task := range tasks {
			if !grouped[task.File.FilePath()] {
				next = append(next, task.File.FileSizeBytes())
			}
		}
		out = next
	}
	t.Fatalf("compaction did not converge in %d rounds; sizes now %v", rounds, out)
	return 0, nil
}

// The risk of lowering MinInputFiles to 2 is thrash: a partition whose rewrite
// output is itself undersized becomes an input again next pass, and compaction
// rewrites the same bytes forever. Prove a fixed point exists across the shapes
// the archiver actually produces, including the boundary sizes around
// MinFileSizeBytes where a rewrite output lands just short of optimal.
func TestCompactionConvergesToFixedPoint(t *testing.T) {
	cfg := compactionConfig()
	mb := int64(1 << 20)
	cases := []struct {
		name  string
		sizes []int64
	}{
		{"two tiny", []int64{5 * mb, 5 * mb}},
		{"handful of small", []int64{20 * mb, 20 * mb, 20 * mb, 25 * mb}},
		{"a day of rolls", repeatSize(48, 25*mb)},
		{"remainder tail", repeatSize(27, 20*mb)},
		{"outputs land just short of optimal", []int64{190 * mb, 190 * mb, 190 * mb}},
		{"mixed optimal and small", []int64{400 * mb, 30 * mb, 30 * mb, 600 * mb}},
		{"already optimal", []int64{400 * mb, 450 * mb, 500 * mb}},
		{"one lone small file", []int64{10 * mb}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rounds, out := simulateCompaction(t, cfg, iceberg.Date(21000+i), tc.sizes)
			t.Logf("converged in %d round(s): %d file(s) from %d", rounds, len(out), len(tc.sizes))
			// A second plan on the settled layout must also be empty, which is what
			// "compaction stops doing work" actually means.
			if rounds > 2 {
				t.Errorf("took %d rounds to converge; compaction is rewriting its own output", rounds)
			}
			if len(out) > len(tc.sizes) {
				t.Errorf("compaction produced MORE files than it started with: %d -> %d", len(tc.sizes), len(out))
			}
		})
	}
}

func repeatSize(n int, size int64) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = size
	}
	return out
}
