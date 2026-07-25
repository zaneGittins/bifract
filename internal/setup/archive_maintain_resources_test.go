package setup

import "testing"

// The upgrade path reads the maintainer's resources back out of the rendered
// manifest. If extractResources cannot find that container, every upgrade
// silently falls back to the default profile and quietly discards an operator's
// deliberate sizing -- a regression that no other test would catch, since the
// fallback still renders valid YAML.
func TestExtractResourcesFindsMaintainContainer(t *testing.T) {
	want := ResourceProfile{"500m", "2", "3Gi", "5Gi"}
	manifest, err := renderK8sTemplate("templates/k8s/bifract-archive-maintain-deployment.yaml.tmpl",
		k8sTemplateData{ImageTag: "test", Domain: "example.com", ArchiveMaintainRes: want})
	if err != nil {
		t.Fatalf("render failed: %v", err)
	}
	got := extractResources(manifest, "bifract-archive-maintain")
	if got != want {
		t.Fatalf("extractResources = %+v, want %+v", got, want)
	}
	if parseK8sQuantityBytes(got.MemLimit) < minArchiveMaintainMemLimit {
		t.Errorf("round-tripped limit %q is below the floor, so an upgrade would discard it", got.MemLimit)
	}
}

func TestParseK8sQuantityBytes(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		{"1Gi", 1 << 30},
		{"8Gi", 8 << 30},
		{"512Mi", 512 << 20},
		{"3Gi", 3 << 30},
		{"2G", 2e9},
		{"1048576", 1 << 20},
		{"", 0},
		{"garbage", 0},
		{"12Xi", 0},
	}
	for _, tt := range tests {
		if got := parseK8sQuantityBytes(tt.in); got != tt.want {
			t.Errorf("parseK8sQuantityBytes(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

// Upgrades must repair a maintainer that inherited the app's memory profile
// (too small for compaction, OOMKilled every pass) while preserving a limit an
// operator deliberately raised. The floor is what separates the two.
func TestArchiveMaintainResourcesFloorOnUpgrade(t *testing.T) {
	fb := sizeProfiles[0]
	tests := []struct {
		name     string
		parsed   ResourceProfile
		wantKept bool
	}{
		{"app-sized limit is inherited breakage", ResourceProfile{"500m", "1", "512Mi", "1Gi"}, false},
		{"just under the floor is still breakage", ResourceProfile{"500m", "2", "2Gi", "3Gi"}, false},
		{"exactly at the floor counts as deliberate", ResourceProfile{"500m", "2", "2Gi", "4Gi"}, true},
		{"a large deliberate limit survives", ResourceProfile{"1", "4", "6Gi", "8Gi"}, true},
		{"nothing parsed falls back", ResourceProfile{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var settings k8sSettings
			if !resourceProfileEmpty(tt.parsed) &&
				parseK8sQuantityBytes(tt.parsed.MemLimit) >= minArchiveMaintainMemLimit {
				settings.archiveMaintainResources = tt.parsed
			}
			got := fallbackProfile(settings.archiveMaintainResources, fb.ArchiveMaintain)
			if tt.wantKept && got != tt.parsed {
				t.Errorf("deliberate sizing %v was replaced by %v", tt.parsed, got)
			}
			if !tt.wantKept && got != fb.ArchiveMaintain {
				t.Errorf("undersized profile %v was kept as %v, want fallback %v", tt.parsed, got, fb.ArchiveMaintain)
			}
		})
	}
}

// Every profile must give the maintainer enough memory to run a pass; the whole
// class of failure this guards against was a profile shipping one it could not.
func TestSizeProfilesMeetArchiveMaintainFloor(t *testing.T) {
	for _, p := range sizeProfiles {
		if got := parseK8sQuantityBytes(p.ArchiveMaintain.MemLimit); got < minArchiveMaintainMemLimit {
			t.Errorf("profile %s: archive-maintain memory limit %q (%d bytes) is below the %d byte floor",
				p.Name, p.ArchiveMaintain.MemLimit, got, minArchiveMaintainMemLimit)
		}
	}
}
