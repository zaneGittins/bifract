package setup

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// parseQty is parseK8sQuantityBytes with a test-visible failure, so a typo in a
// profile's memory string ("8G" vs "8Gi") surfaces as a named error rather than
// silently reading as zero and passing every comparison below.
func parseQty(t *testing.T, profile, field, q string) int64 {
	t.Helper()
	n := parseK8sQuantityBytes(q)
	if n <= 0 {
		t.Fatalf("%s profile: %s %q did not parse as a memory quantity", profile, field, q)
	}
	return n
}

// The archiver's roll thresholds are uncompressed heap while compaction judges
// files by compressed size on disk, so these three numbers only work as a set.
// A profile added without them, or with a memory limit that cannot hold the
// buffer it is told to accumulate, is an OOMKill or a permanent compaction
// backlog on somebody's cluster. Pin the relationships rather than the values.
func TestArchiveSizingProfilesAreCoherent(t *testing.T) {
	for _, p := range sizeProfiles {
		t.Run(p.Name, func(t *testing.T) {
			if p.ArchiveRollBytes <= 0 {
				t.Fatalf("ArchiveRollBytes unset; a zero here silently falls back to the archiver's flat code default")
			}
			if p.ArchiveMaxPendingBytes <= 0 {
				t.Fatalf("ArchiveMaxPendingBytes unset; a zero here silently falls back to the archiver's flat code default")
			}
			if resourceProfileEmpty(p.Archiver) {
				t.Fatalf("Archiver resource profile unset; the sidecar would render with empty requests/limits")
			}

			// Below 4x, several busy fractals buffering at once trip the total cap
			// and force an early commit under the roll target, which is the same
			// small-file outcome the roll threshold exists to prevent.
			if min := 4 * p.ArchiveRollBytes; p.ArchiveMaxPendingBytes < min {
				t.Errorf("ArchiveMaxPendingBytes = %d, want >= %d (4x ArchiveRollBytes %d)",
					p.ArchiveMaxPendingBytes, min, p.ArchiveRollBytes)
			}

			// The sidecar holds the whole pending buffer, plus the Arrow copy and
			// Parquet encode of the fractal currently flushing.
			memLimit := parseQty(t, p.Name, "Archiver.MemLimit", p.Archiver.MemLimit)
			if need := p.ArchiveMaxPendingBytes + 2*p.ArchiveRollBytes; memLimit < need {
				t.Errorf("Archiver.MemLimit = %d bytes, want >= %d (ArchiveMaxPendingBytes + 2x ArchiveRollBytes)",
					memLimit, need)
			}

			// A request far under real steady-state usage makes the pod Burstable
			// and a prime eviction target on a node under memory pressure.
			memRequest := parseQty(t, p.Name, "Archiver.MemRequest", p.Archiver.MemRequest)
			if memRequest > memLimit {
				t.Errorf("Archiver.MemRequest %d exceeds MemLimit %d", memRequest, memLimit)
			}
		})
	}
}

// The profile values only reach a cluster through the secret, so a missing
// template key or map entry makes the whole sizing exercise a no-op.
func TestArchiveSizingReachesRenderedSecret(t *testing.T) {
	large, ok := lookupSizeProfile("Large")
	if !ok {
		t.Fatal("Large size profile not found")
	}

	cfg := &K8sConfig{SizeProfile: large}
	if cfg.UserSecrets == nil {
		cfg.UserSecrets = make(map[string]string)
	}
	defaultUserSecretBytes(cfg.UserSecrets, "ARCHIVE_ROLL_BYTES", cfg.SizeProfile.ArchiveRollBytes)
	defaultUserSecretBytes(cfg.UserSecrets, "ARCHIVE_MAX_PENDING_BYTES", cfg.SizeProfile.ArchiveMaxPendingBytes)

	sec, err := renderK8sTemplate("templates/k8s/bifract-secrets.yaml.tmpl", k8sTemplateData{
		UserSecrets: cfg.UserSecrets,
	})
	if err != nil {
		t.Fatalf("secrets render failed: %v", err)
	}
	for _, want := range []string{
		"ARCHIVE_ROLL_BYTES: \"" + strconv.FormatInt(large.ArchiveRollBytes, 10) + "\"",
		"ARCHIVE_MAX_PENDING_BYTES: \"" + strconv.FormatInt(large.ArchiveMaxPendingBytes, 10) + "\"",
	} {
		if !strings.Contains(sec, want) {
			t.Errorf("rendered secrets missing %q", want)
		}
	}

	// The ingest deployment is what actually consumes the key.
	ingest, err := renderK8sTemplate("templates/k8s/bifract-ingest-deployment.yaml.tmpl", k8sTemplateData{
		ImageTag:    "test",
		ArchiverRes: large.Archiver,
		BifractRes:  large.Bifract,
	})
	if err != nil {
		t.Fatalf("ingest render failed: %v", err)
	}
	if !strings.Contains(ingest, "BIFRACT_ARCHIVE_MAX_PENDING_BYTES") {
		t.Error("ingest deployment does not consume BIFRACT_ARCHIVE_MAX_PENDING_BYTES")
	}
}

// An operator's explicit value must survive regeneration, and the zero-valued
// "custom" profile that upgrade/reconfigure build from parsed manifests must
// never blank a key the running deployment already depends on.
func TestDefaultUserSecretBytes(t *testing.T) {
	t.Run("fills an unset key from the profile", func(t *testing.T) {
		s := map[string]string{}
		defaultUserSecretBytes(s, "K", 1<<30)
		if s["K"] != "1073741824" {
			t.Errorf("K = %q, want 1073741824", s["K"])
		}
	})

	t.Run("an operator value wins over the profile", func(t *testing.T) {
		s := map[string]string{"K": "42"}
		defaultUserSecretBytes(s, "K", 1<<30)
		if s["K"] != "42" {
			t.Errorf("K = %q, want the operator's 42", s["K"])
		}
	})

	t.Run("a zero profile value writes nothing", func(t *testing.T) {
		s := map[string]string{}
		defaultUserSecretBytes(s, "K", 0)
		if _, ok := s["K"]; ok {
			t.Errorf("K was written from a zero profile value: %q", s["K"])
		}
	})

	t.Run("a zero profile value never blanks an existing key", func(t *testing.T) {
		s := map[string]string{"K": "42"}
		defaultUserSecretBytes(s, "K", 0)
		if s["K"] != "42" {
			t.Errorf("K = %q, want the existing 42 preserved", s["K"])
		}
	})
}

// End-to-end through the real install path: a fresh install must land the
// profile's roll thresholds in the secret on disk and the profile's Archiver
// resources on the sidecar. Rendering the templates directly (as the tests above
// do) would miss a break in writeK8sManifests' wiring, which is where both
// values are actually chosen.
func TestInstallK8sWritesArchiveSizing(t *testing.T) {
	large, ok := lookupSizeProfile("Large")
	if !ok {
		t.Fatal("Large size profile not found")
	}

	dir := t.TempDir()
	cfg := &K8sConfig{
		SizeProfile: large,
		CHShards:    large.CHShards,
		OutputDir:   dir,
	}
	cfg.ImageTag = "test"
	cfg.Domain = "example.test"
	if err := writeK8sManifests(cfg); err != nil {
		t.Fatalf("writeK8sManifests: %v", err)
	}

	secret, err := os.ReadFile(filepath.Join(dir, "bifract", "secrets.yaml"))
	if err != nil {
		t.Fatalf("read rendered secrets: %v", err)
	}
	for _, want := range []string{
		"ARCHIVE_ROLL_BYTES: \"" + strconv.FormatInt(large.ArchiveRollBytes, 10) + "\"",
		"ARCHIVE_MAX_PENDING_BYTES: \"" + strconv.FormatInt(large.ArchiveMaxPendingBytes, 10) + "\"",
	} {
		if !strings.Contains(string(secret), want) {
			t.Errorf("rendered secrets missing %q", want)
		}
	}

	ingest, err := os.ReadFile(filepath.Join(dir, "bifract", "ingest-deployment.yaml"))
	if err != nil {
		t.Fatalf("read rendered ingest deployment: %v", err)
	}
	// extractResources is the same parser the upgrade path uses to preserve these,
	// so agreeing with it here also proves the preservation round-trips.
	got := extractResources(string(ingest), "bifract-archiver")
	if got != large.Archiver {
		t.Errorf("archiver sidecar resources = %+v, want the profile's %+v", got, large.Archiver)
	}
	if got == large.Bifract {
		t.Error("archiver sidecar is still inheriting the Bifract profile")
	}
}

// The upgrade path has to tell "the operator sized this sidecar" apart from "the
// old code rendered it from the Bifract profile". Getting that wrong in either
// direction is bad: preserve the inherited value and no existing install ever
// adopts the archiver profile; discard a real one and an operator's deliberate
// sizing is silently reverted on every upgrade.
func TestPreservedArchiverResources(t *testing.T) {
	large, _ := lookupSizeProfile("Large")

	tests := []struct {
		name    string
		parsed  ResourceProfile
		bifract ResourceProfile
		want    ResourceProfile
	}{
		{
			// Exactly what the pre-fix wiring produced: ArchiverRes = SizeProfile.Bifract.
			name:    "inherited Bifract sizing gives way to the profile",
			parsed:  large.Bifract,
			bifract: large.Bifract,
			want:    ResourceProfile{},
		},
		{
			name:    "a deliberately sized archiver is preserved",
			parsed:  ResourceProfile{"2", "4", "5Gi", "10Gi"},
			bifract: large.Bifract,
			want:    ResourceProfile{"2", "4", "5Gi", "10Gi"},
		},
		{
			// One field of difference is a decision, not inherited wiring.
			name:    "the inheritance test is exact, not fuzzy",
			parsed:  ResourceProfile{large.Bifract.CPURequest, large.Bifract.CPULimit, "3Gi", large.Bifract.MemLimit},
			bifract: large.Bifract,
			want:    ResourceProfile{large.Bifract.CPURequest, large.Bifract.CPULimit, "3Gi", large.Bifract.MemLimit},
		},
		{
			name:    "a limit under the floor is corrected, not preserved",
			parsed:  ResourceProfile{"100m", "500m", "128Mi", "256Mi"},
			bifract: large.Bifract,
			want:    ResourceProfile{},
		},
		{
			name:    "nothing parsed means nothing to preserve",
			parsed:  ResourceProfile{},
			bifract: large.Bifract,
			want:    ResourceProfile{},
		},
		{
			// Without an app-tier reading to compare against, inheritance cannot be
			// proven, so a real-looking value is kept rather than silently reset.
			name:    "an unreadable app tier does not force a reset",
			parsed:  ResourceProfile{"2", "4", "5Gi", "10Gi"},
			bifract: ResourceProfile{},
			want:    ResourceProfile{"2", "4", "5Gi", "10Gi"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := preservedArchiverResources(tt.parsed, tt.bifract); got != tt.want {
				t.Errorf("preservedArchiverResources(%+v, %+v) = %+v, want %+v",
					tt.parsed, tt.bifract, got, tt.want)
			}
		})
	}
}

// Regression: an upgrade must never hand the archiver a memory limit below the
// buffer its ARCHIVE_MAX_PENDING_BYTES tells it to accumulate. Before
// inferSizeProfile, a Large install upgrading got Dev's 1Gi limit against a 4GiB
// buffer, which is not a smaller deployment, it is an OOMKill loop that stops
// archiving and backs the spool up into ingest.
func TestUpgradeNeverUnderprovisionsArchiver(t *testing.T) {
	for _, p := range sizeProfiles {
		t.Run(p.Name, func(t *testing.T) {
			// What an untouched install of this profile has on disk: the sidecar
			// rendered from the Bifract profile by the pre-fix wiring.
			parsedArchiver := p.Bifract

			fb := sizeProfiles[0]
			if inferred, ok := inferSizeProfile(p.ClickHouse, p.CHShards); ok {
				fb = inferred
			}
			preserved := preservedArchiverResources(parsedArchiver, p.Bifract)
			got := archiverProfileFor(fallbackProfile(preserved, fb.Archiver), parsedArchiver)

			limit := parseK8sQuantityBytes(got.MemLimit)
			if limit < p.ArchiveMaxPendingBytes {
				t.Errorf("archiver limit %s (%d bytes) is below the %d bytes this profile buffers",
					got.MemLimit, limit, p.ArchiveMaxPendingBytes)
			}
			if limit < parseK8sQuantityBytes(parsedArchiver.MemLimit) {
				t.Errorf("upgrade downsized the archiver from %s to %s", parsedArchiver.MemLimit, got.MemLimit)
			}
		})
	}
}

// Inference is what makes the fallback appropriate to the install rather than
// always Dev, so it has to be exact and has to separate the profiles that share
// ClickHouse resources.
func TestInferSizeProfile(t *testing.T) {
	large, _ := lookupSizeProfile("Large")
	xlarge, _ := lookupSizeProfile("X-Large")

	if large.ClickHouse != xlarge.ClickHouse {
		t.Skip("Large and X-Large no longer share ClickHouse resources; the shard tiebreak is untested here")
	}

	t.Run("shard count separates profiles sharing ClickHouse resources", func(t *testing.T) {
		got, ok := inferSizeProfile(large.ClickHouse, large.CHShards)
		if !ok || got.Name != "Large" {
			t.Errorf("inferSizeProfile(Large CH, %d shards) = %q/%v, want Large/true", large.CHShards, got.Name, ok)
		}
		got, ok = inferSizeProfile(xlarge.ClickHouse, xlarge.CHShards)
		if !ok || got.Name != "X-Large" {
			t.Errorf("inferSizeProfile(X-Large CH, %d shards) = %q/%v, want X-Large/true", xlarge.CHShards, got.Name, ok)
		}
	})

	t.Run("hand-tuned resources match nothing", func(t *testing.T) {
		if got, ok := inferSizeProfile(ResourceProfile{"7", "9", "13Gi", "27Gi"}, 3); ok {
			t.Errorf("inferSizeProfile matched %q for hand-tuned resources; no profile may be assumed", got.Name)
		}
	})

	t.Run("an unreadable ClickHouse block matches nothing", func(t *testing.T) {
		if _, ok := inferSizeProfile(ResourceProfile{}, 3); ok {
			t.Error("inferSizeProfile matched on empty resources")
		}
	})

	t.Run("a mismatched shard count does not match", func(t *testing.T) {
		if got, ok := inferSizeProfile(large.ClickHouse, 99); ok {
			t.Errorf("inferSizeProfile matched %q with an unknown shard count", got.Name)
		}
	})
}

// archiverProfileFor's only job is the never-downsize floor.
func TestArchiverProfileFor(t *testing.T) {
	small := ResourceProfile{"250m", "1", "512Mi", "1Gi"}
	big := ResourceProfile{"1", "4", "3Gi", "8Gi"}

	if got := archiverProfileFor(small, big); got != big {
		t.Errorf("chose %+v over the larger existing %+v; upgrades must not downsize", got, big)
	}
	if got := archiverProfileFor(big, small); got != big {
		t.Errorf("chose %+v; a larger chosen profile should win", got)
	}
	if got := archiverProfileFor(big, ResourceProfile{}); got != big {
		t.Errorf("chose %+v; with nothing parsed the chosen profile applies", got)
	}
}

// Seeding on upgrade is what makes the roll-threshold fix reach clusters that
// already exist; without it item 6 only ever helps fresh installs. The guards
// matter as much as the seeding: an operator's explicit value must win, and an
// install whose profile cannot be identified must not be handed Dev's
// thresholds, which are SMALLER than the code default they would replace.
func TestUpgradeSeedsRollThresholds(t *testing.T) {
	large, _ := lookupSizeProfile("Large")

	// seed reproduces buildK8sConfigFromExisting's decision for a given install.
	seed := func(ch ResourceProfile, shards int) (roll, pending int64) {
		if p, ok := inferSizeProfile(ch, shards); ok {
			return p.ArchiveRollBytes, p.ArchiveMaxPendingBytes
		}
		return 0, 0
	}

	t.Run("a recognised install adopts its profile's thresholds", func(t *testing.T) {
		roll, pending := seed(large.ClickHouse, large.CHShards)
		secrets := map[string]string{"ARCHIVE_ROLL_BYTES": "", "ARCHIVE_MAX_PENDING_BYTES": ""}
		defaultUserSecretBytes(secrets, "ARCHIVE_ROLL_BYTES", roll)
		defaultUserSecretBytes(secrets, "ARCHIVE_MAX_PENDING_BYTES", pending)

		if got, want := secrets["ARCHIVE_ROLL_BYTES"], strconv.FormatInt(large.ArchiveRollBytes, 10); got != want {
			t.Errorf("ARCHIVE_ROLL_BYTES = %q, want %q", got, want)
		}
		if got, want := secrets["ARCHIVE_MAX_PENDING_BYTES"], strconv.FormatInt(large.ArchiveMaxPendingBytes, 10); got != want {
			t.Errorf("ARCHIVE_MAX_PENDING_BYTES = %q, want %q", got, want)
		}
	})

	t.Run("an operator's explicit value is never overwritten", func(t *testing.T) {
		roll, pending := seed(large.ClickHouse, large.CHShards)
		secrets := map[string]string{"ARCHIVE_ROLL_BYTES": "1395864371", "ARCHIVE_MAX_PENDING_BYTES": "4294967296"}
		defaultUserSecretBytes(secrets, "ARCHIVE_ROLL_BYTES", roll)
		defaultUserSecretBytes(secrets, "ARCHIVE_MAX_PENDING_BYTES", pending)

		if secrets["ARCHIVE_ROLL_BYTES"] != "1395864371" {
			t.Errorf("ARCHIVE_ROLL_BYTES = %q, want the operator's value", secrets["ARCHIVE_ROLL_BYTES"])
		}
		if secrets["ARCHIVE_MAX_PENDING_BYTES"] != "4294967296" {
			t.Errorf("ARCHIVE_MAX_PENDING_BYTES = %q, want the operator's value", secrets["ARCHIVE_MAX_PENDING_BYTES"])
		}
	})

	t.Run("an unidentifiable install is left on the code defaults", func(t *testing.T) {
		roll, pending := seed(ResourceProfile{"7", "9", "13Gi", "27Gi"}, 3)
		if roll != 0 || pending != 0 {
			t.Fatalf("seeded %d/%d for a hand-tuned install; no profile may be assumed", roll, pending)
		}
		secrets := map[string]string{}
		defaultUserSecretBytes(secrets, "ARCHIVE_ROLL_BYTES", roll)
		if _, ok := secrets["ARCHIVE_ROLL_BYTES"]; ok {
			t.Error("wrote a threshold for an install whose profile is unknown")
		}
	})

	t.Run("every seeded threshold fits the archiver it ships with", func(t *testing.T) {
		for _, p := range sizeProfiles {
			roll, pending := seed(p.ClickHouse, p.CHShards)
			if roll == 0 {
				t.Errorf("%s: its own ClickHouse resources did not infer back to it", p.Name)
				continue
			}
			limit := parseK8sQuantityBytes(p.Archiver.MemLimit)
			if need := pending + 2*roll; limit < need {
				t.Errorf("%s: seeded %d pending + 2x%d roll needs %d bytes, archiver limit is %d",
					p.Name, pending, roll, need, limit)
			}
		}
	})
}

// The full upgrade pipeline against a manifest tree that looks like a pre-fix
// Large install: parse what is on disk, rebuild the config, re-render. Every
// other test here covers a component in isolation, which is how a working set of
// parts can still add up to an upgrade that under-provisions the archiver.
func TestUpgradePipelineOnPreFixLargeInstall(t *testing.T) {
	large, _ := lookupSizeProfile("Large")

	// A pre-fix install: the archiver sidecar rendered from the Bifract profile,
	// and no archive roll thresholds set anywhere.
	preFix := large
	preFix.Archiver = large.Bifract
	preFix.ArchiveRollBytes = 0
	preFix.ArchiveMaxPendingBytes = 0

	dir := t.TempDir()
	old := &K8sConfig{SizeProfile: preFix, CHShards: preFix.CHShards, OutputDir: dir}
	old.ImageTag = "v0.0.1"
	old.Domain = "example.test"
	old.PostgresPassword = "pw"
	old.ClickHousePassword = "pw"
	if err := writeK8sManifests(old); err != nil {
		t.Fatalf("render pre-fix install: %v", err)
	}
	if got := extractResources(readFile(t, dir, "bifract", "ingest-deployment.yaml"), "bifract-archiver"); got != large.Bifract {
		t.Fatalf("fixture is not a pre-fix install: archiver = %+v, want the Bifract profile %+v", got, large.Bifract)
	}

	// Now the upgrade: parse the tree, rebuild, re-render in place.
	settings, err := parseK8sSettings(dir)
	if err != nil {
		t.Fatalf("parseK8sSettings: %v", err)
	}
	secrets, err := parseK8sSecrets(filepath.Join(dir, "bifract", "secrets.yaml"))
	if err != nil {
		t.Fatalf("parseK8sSecrets: %v", err)
	}
	cfg := buildK8sConfigFromExisting(dir, secrets, settings)
	cfg.OutputDir = dir
	cfg.ImageTag = "v0.0.2"
	if err := writeK8sManifests(cfg); err != nil {
		t.Fatalf("re-render on upgrade: %v", err)
	}

	t.Run("archiver adopts its own profile instead of the app tier's", func(t *testing.T) {
		got := extractResources(readFile(t, dir, "bifract", "ingest-deployment.yaml"), "bifract-archiver")
		if got != large.Archiver {
			t.Errorf("archiver = %+v, want the Large Archiver profile %+v", got, large.Archiver)
		}
	})

	t.Run("roll thresholds are seeded from the inferred profile", func(t *testing.T) {
		sec := readFile(t, dir, "bifract", "secrets.yaml")
		for key, want := range map[string]int64{
			"ARCHIVE_ROLL_BYTES":        large.ArchiveRollBytes,
			"ARCHIVE_MAX_PENDING_BYTES": large.ArchiveMaxPendingBytes,
		} {
			if !strings.Contains(sec, key+": \""+strconv.FormatInt(want, 10)+"\"") {
				t.Errorf("secrets missing %s = %d", key, want)
			}
		}
	})

	t.Run("the archiver can hold the buffer it is now configured for", func(t *testing.T) {
		got := extractResources(readFile(t, dir, "bifract", "ingest-deployment.yaml"), "bifract-archiver")
		limit := parseK8sQuantityBytes(got.MemLimit)
		if need := large.ArchiveMaxPendingBytes + 2*large.ArchiveRollBytes; limit < need {
			t.Errorf("archiver limit %d < %d bytes needed for the seeded thresholds", limit, need)
		}
	})
}

func readFile(t *testing.T, parts ...string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(parts...))
	if err != nil {
		t.Fatalf("read %v: %v", parts, err)
	}
	return string(b)
}
