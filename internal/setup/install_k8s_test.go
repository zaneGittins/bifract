package setup

import (
	"strconv"
	"strings"
	"testing"
)

func TestParseK8sMemToBytes(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		{"56Gi", 56 << 30},
		{"512Mi", 512 << 20},
		{"2Ti", 2 << 40},
		{"64Ki", 64 << 10},
		{"1048576", 1 << 20},
		{" 4Gi ", 4 << 30},
		// Unparseable means unknown, which omits the memory block entirely so
		// ClickHouse falls back to its own defaults rather than a wrong cap.
		{"", 0},
		{"lots", 0},
		{"-1Gi", 0},
		{"1.5Gi", 0},
		{"8G", 0},
	}
	for _, tt := range tests {
		if got := parseK8sMemToBytes(tt.in); got != tt.want {
			t.Errorf("parseK8sMemToBytes(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestFormatStorageSize(t *testing.T) {
	for gb, want := range map[int]string{10: "10Gi", 1023: "1023Gi", 1024: "1Ti", 1536: "1536Gi", 3072: "3Ti"} {
		if got := formatStorageSize(gb); got != want {
			t.Errorf("formatStorageSize(%d) = %q, want %q", gb, got, want)
		}
	}
}

// The ClickHouse memory ceilings are derived from the pod limit; an install
// whose limit cannot be read gets no ceilings rather than a wrong one.
func TestClickHouseMemoryCeilingsRender(t *testing.T) {
	large, _ := lookupSizeProfile("Large")
	dir := t.TempDir()
	if err := writeK8sManifests(freshK8sConfig(large, dir)); err != nil {
		t.Fatal(err)
	}
	limit := parseK8sMemToBytes(large.ClickHouse.MemLimit)
	server := limit * 8 / 10
	chi := readFile(t, dir, "clickhouse", "clickhouse-installation.yaml")
	for _, want := range []string{
		"max_server_memory_usage: " + strconv.FormatInt(server, 10),
		"merges_mutations_memory_usage_soft_limit: " + strconv.FormatInt(server/2, 10),
		"max_bytes_to_merge_at_max_space_in_pool: " + strconv.FormatInt(server/4, 10),
	} {
		if !strings.Contains(chi, want) {
			t.Errorf("rendered ClickHouseCluster lacks %q", want)
		}
	}

	unknown := large
	unknown.ClickHouse.MemLimit = "lots"
	dir = t.TempDir()
	if err := writeK8sManifests(freshK8sConfig(unknown, dir)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(readFile(t, dir, "clickhouse", "clickhouse-installation.yaml"), "max_server_memory_usage") {
		t.Error("rendered a memory ceiling from an unreadable limit")
	}
}
