package archive

import (
	"os"
	"testing"
)

// Compaction's file-decode fan-out is its dominant memory term. It used to be a
// hard-coded 4, unrelated to the container's memory limit, which OOMKilled the
// maintainer on every pass that found real work on a 2GB limit. These cases pin
// the relationship that replaced it: never more workers than the memory budget
// affords, never more than there are cores, and never zero.
func TestScanConcurrencyFor(t *testing.T) {
	tests := []struct {
		name     string
		memLimit int64
		cpus     int
		want     int
	}{
		{"2GB docker limit stays within one worker", 2 << 30, 2, 1},
		{"unknown limit falls back rather than fanning out", 0, 16, fallbackScanConcurrency},
		{"cpu count caps an ample memory budget", 64 << 30, 2, 2},
		{"memory caps a wide cpu budget", 6 << 30, 32, 2},
		{"8Gi maintainer earns real parallelism", 8 << 30, 4, 3},
		{"a tiny limit still attempts the pass", 128 << 20, 4, 1},
		{"a nonsense cpu count never yields zero workers", 8 << 30, 0, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := scanConcurrencyFor(tt.memLimit, tt.cpus); got != tt.want {
				t.Errorf("scanConcurrencyFor(%d, %d) = %d, want %d", tt.memLimit, tt.cpus, got, tt.want)
			}
		})
	}
}

// The env override is the operator's escape hatch and must win over the derived
// value, including when it asks for more than the memory budget would allow.
func TestMaintainScanConcurrencyEnvOverride(t *testing.T) {
	t.Setenv("BIFRACT_ARCHIVE_MAINTAIN_SCAN_CONCURRENCY", "7")
	if got := MaintainScanConcurrency(); got != 7 {
		t.Fatalf("MaintainScanConcurrency() = %d, want 7", got)
	}
	os.Unsetenv("BIFRACT_ARCHIVE_MAINTAIN_SCAN_CONCURRENCY")
	if got := MaintainScanConcurrency(); got < 1 {
		t.Fatalf("MaintainScanConcurrency() = %d, want at least 1", got)
	}
}

// Every pass must resolve a usable concurrency: a zero here would reach
// iceberg-go as "no limit" and reinstate the unbounded fan-out.
func TestDefaultMaintainOptionsSetsScanConcurrency(t *testing.T) {
	if got := DefaultMaintainOptions().ScanConcurrency; got < 1 {
		t.Fatalf("DefaultMaintainOptions().ScanConcurrency = %d, want at least 1", got)
	}
	if got := MaintainOptionsFromEnv().ScanConcurrency; got < 1 {
		t.Fatalf("MaintainOptionsFromEnv().ScanConcurrency = %d, want at least 1", got)
	}
}

// A limit below one worker's measured requirement must be reported rather than
// silently accepted: that configuration is an OOMKill every pass, and the
// warning is the only signal an operator gets before the process is killed.
func TestMaintainMemoryUndersized(t *testing.T) {
	undersized, _, need := MaintainMemoryUndersized()
	if need != scanWorkerMemoryBudget {
		t.Errorf("reported need = %d, want %d", need, scanWorkerMemoryBudget)
	}
	// The test process has no cgroup memory limit of its own, so this must not
	// claim undersized: an unknown limit is not evidence of a small one.
	if limit := processMemoryLimit(); limit == 0 && undersized {
		t.Error("reported undersized with no memory limit set")
	}
}
