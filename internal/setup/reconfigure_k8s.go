package setup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

// lookupSizeProfile finds a size profile by name (case-insensitive).
func lookupSizeProfile(name string) (SizeProfile, bool) {
	normalized := strings.ToLower(strings.TrimSpace(name))
	for _, p := range sizeProfiles {
		if strings.ToLower(p.Name) == normalized {
			return p, true
		}
	}
	return SizeProfile{}, false
}

// K8sReconfigureOpts holds optional overrides for reconfiguration.
type K8sReconfigureOpts struct {
	Domain      string
	IPAccess    string
	AllowedIPs  string
	SizeProfile string
	Shards      int
}

// RunReconfigureK8s re-renders K8s manifests from existing secrets and settings.
// Useful when changing IP access mode, domain, or picking up new template changes
// without a version bump. All secrets and resource profiles are preserved.
func RunReconfigureK8s(dir string, opts K8sReconfigureOpts) error {
	defer abandonStep() // clean up any in-progress spinner on an early error return
	PrintBanner()

	fmt.Println(TitleStyle.Render("  Reconfiguring Kubernetes Manifests"))
	fmt.Println()

	// Validate existing installation
	secretsPath := filepath.Join(dir, "bifract", "secrets.yaml")
	deployPath := filepath.Join(dir, "bifract", "deployment.yaml")
	if _, err := os.Stat(secretsPath); os.IsNotExist(err) {
		return fmt.Errorf("no K8s installation found at %s (bifract/secrets.yaml missing)", dir)
	}
	if _, err := os.Stat(deployPath); os.IsNotExist(err) {
		return fmt.Errorf("no K8s installation found at %s (bifract/deployment.yaml missing)", dir)
	}

	// Parse existing secrets
	printStep("Reading existing secrets...")
	secrets, err := parseK8sSecrets(secretsPath)
	if err != nil {
		return fmt.Errorf("parse secrets: %w", err)
	}
	if secrets["POSTGRES_PASSWORD"] == "" || secrets["CLICKHOUSE_PASSWORD"] == "" {
		return fmt.Errorf("missing POSTGRES_PASSWORD or CLICKHOUSE_PASSWORD in %s\n  Cannot reconfigure without database credentials", secretsPath)
	}
	printDone("Secrets loaded")

	// Merge any values added via "kubectl edit" that are not in secrets.yaml.
	printStep("Checking live cluster secrets...")
	live, liveErr := tryReadLiveSecrets("bifract", "bifract-secrets")
	if live != nil {
		merged := 0
		for k, v := range live {
			if v != "" && !coreSecretKeys[k] {
				if secrets[k] != v {
					merged++
				}
				secrets[k] = v
			}
		}
		if merged > 0 {
			printDone(fmt.Sprintf("Synced %d user secret(s) from live cluster", merged))
		} else {
			printDone("Live cluster secrets match on-disk")
		}
	} else if liveErr != "" {
		printWarn(fmt.Sprintf("Could not read live cluster secrets: %s", liveErr))
		printWarn("kubectl edit values will not be preserved — secrets.yaml is the source of truth")
	}

	// Parse existing settings from manifests
	printStep("Reading existing settings...")
	settings, err := parseK8sSettings(dir)
	if err != nil {
		return fmt.Errorf("parse settings: %w", err)
	}
	printDone("Settings loaded")

	// Backup existing manifests
	timestamp := time.Now().Format("20060102-150405")
	backupDir := filepath.Join(dir, ".backups", timestamp)
	printStep("Backing up manifests...")
	if err := backupK8sManifests(dir, backupDir); err != nil {
		return fmt.Errorf("backup: %w", err)
	}
	printDone("Backed up to " + backupDir)

	// Build K8sConfig with preserved values
	cfg := buildK8sConfigFromExisting(dir, secrets, settings)

	// Keep the current image tag (no version change)
	cfg.ImageTag = settings.imageTag
	if cfg.ImageTag == "" {
		cfg.ImageTag = Version
	}

	// Apply overrides
	changes := []string{}
	if opts.SizeProfile != "" {
		profile, ok := lookupSizeProfile(opts.SizeProfile)
		if !ok {
			names := make([]string, len(sizeProfiles))
			for i, p := range sizeProfiles {
				names[i] = strings.ToLower(p.Name)
			}
			return fmt.Errorf("unknown size profile %q (available: %s)", opts.SizeProfile, strings.Join(names, ", "))
		}
		changes = append(changes, fmt.Sprintf("Size Profile: %s -> %s", cfg.SizeProfile.Name, profile.Name))
		cfg.SizeProfile = profile
		cfg.CHShards = profile.CHShards
	}
	if opts.Shards > 0 && opts.Shards != cfg.CHShards {
		changes = append(changes, fmt.Sprintf("CH Shards: %d -> %d", cfg.CHShards, opts.Shards))
		cfg.CHShards = opts.Shards
	}
	if opts.Domain != "" && opts.Domain != cfg.Domain {
		changes = append(changes, fmt.Sprintf("Domain: %s -> %s", cfg.Domain, opts.Domain))
		cfg.Domain = opts.Domain
	}
	if opts.IPAccess != "" {
		newMode := IPAccessMode(opts.IPAccess)
		if newMode != cfg.IPAccess {
			changes = append(changes, fmt.Sprintf("IP Access: %s -> %s", cfg.IPAccess, newMode))
			cfg.IPAccess = newMode
			if newMode == IPAccessMTLSApp {
				cfg.MTLSEnabled = true
			} else {
				cfg.MTLSEnabled = false
			}
		}
	}
	if opts.AllowedIPs != "" {
		cfg.ParseAllowedIPs(opts.AllowedIPs)
	}

	// Generate mTLS CA if switching to mTLS and no existing CA
	if cfg.MTLSEnabled && cfg.MTLSCACert == "" {
		printStep("Generating mTLS CA...")
		caCert, caKey, err := GenerateClientCAPEM()
		if err != nil {
			return fmt.Errorf("generate mTLS CA: %w", err)
		}
		cfg.MTLSCACert = caCert
		cfg.MTLSCAKey = caKey
		printDone("mTLS CA generated")
	}

	// Regenerate manifests
	printStep("Writing updated manifests...")
	if err := writeK8sManifests(cfg); err != nil {
		return fmt.Errorf("write manifests: %w", err)
	}
	if cfg.MTLSEnabled && cfg.MTLSCACert != "" {
		caDir := filepath.Join(dir, "client-ca")
		os.MkdirAll(caDir, 0700)
		os.WriteFile(filepath.Join(caDir, "ca.pem"), []byte(cfg.MTLSCACert), 0644)
		os.WriteFile(filepath.Join(caDir, "ca-key.pem"), []byte(cfg.MTLSCAKey), 0600)
	}
	printDone("Manifests updated")

	// Summary
	fmt.Println()
	summaryLines := fmt.Sprintf(
		"%s  %s\n%s  %s",
		PromptStyle.Render("Version:"), ValueStyle.Render(cfg.ImageTag),
		PromptStyle.Render("Backup: "), DimStyle.Render(backupDir),
	)
	if len(changes) > 0 {
		summaryLines += "\n"
		for _, c := range changes {
			summaryLines += fmt.Sprintf("\n%s  %s", SuccessStyle.Render("*"), DimStyle.Render(c))
		}
	}
	summary := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(Green).
		Padding(0, 2).
		Render(summaryLines)
	fmt.Println(summary)
	fmt.Println()
	fmt.Println(DimStyle.Render("  Apply with:"))
	fmt.Println(DimStyle.Render("    kubectl apply -k " + dir))
	fmt.Println()
	printDone("Reconfiguration complete")
	fmt.Println()

	return nil
}

// resourceProfileEmpty returns true if all fields in a ResourceProfile are empty.
func resourceProfileEmpty(p ResourceProfile) bool {
	return p.CPURequest == "" && p.CPULimit == "" && p.MemRequest == "" && p.MemLimit == ""
}

// fallbackProfile returns the profile value if non-empty, otherwise the fallback.
func fallbackProfile(parsed, fallback ResourceProfile) ResourceProfile {
	if resourceProfileEmpty(parsed) {
		return fallback
	}
	return parsed
}

// inferSizeProfile identifies which shipped size profile an existing install was
// created from, by matching its parsed ClickHouse resources and shard count.
//
// This exists because the fallback for every unparsed field used to be
// sizeProfiles[0] (Dev) regardless of how large the install actually is. For
// resources that is merely wrong; for the archiver it is dangerous, because the
// sidecar's memory limit has to cover the buffer its ARCHIVE_MAX_PENDING_BYTES
// tells it to accumulate, and handing a Large install Dev's 1Gi limit against a
// 4GiB buffer is an OOMKill loop rather than a smaller deployment.
//
// ClickHouse resources are the discriminator because they are distinct per
// profile, and the shard count separates Large from X-Large, which share them.
// No match means hand-tuned resources, where no profile may be assumed.
func inferSizeProfile(chResources ResourceProfile, chShards int) (SizeProfile, bool) {
	if resourceProfileEmpty(chResources) {
		return SizeProfile{}, false
	}
	for _, p := range sizeProfiles {
		if p.ClickHouse == chResources && p.CHShards == chShards {
			return p, true
		}
	}
	return SizeProfile{}, false
}

// archiverProfileFor picks the archiver sidecar's resources on regeneration,
// with one hard rule: never downsize. The sidecar's memory limit must cover the
// buffer it is configured to accumulate, so when the chosen profile's limit is
// below what the install already had, the existing value wins. That keeps a
// wrongly-inferred (or unavailable) profile from turning an upgrade into an
// OOMKill loop; too much headroom only wastes a reservation.
func archiverProfileFor(chosen, parsed ResourceProfile) ResourceProfile {
	if resourceProfileEmpty(parsed) {
		return chosen
	}
	if parseK8sQuantityBytes(chosen.MemLimit) < parseK8sQuantityBytes(parsed.MemLimit) {
		return parsed
	}
	return chosen
}

// fallbackInt returns a if non-zero, otherwise b.
func fallbackInt(a, b int) int {
	if a > 0 {
		return a
	}
	return b
}

// fallbackInt64 returns a if non-zero, otherwise b.
func fallbackInt64(a, b int64) int64 {
	if a > 0 {
		return a
	}
	return b
}

// buildK8sConfigFromExisting constructs a K8sConfig from parsed secrets and settings.
// Shared between upgrade and reconfigure flows.
func buildK8sConfigFromExisting(dir string, secrets map[string]string, settings *k8sSettings) *K8sConfig {
	// Fallback for anything that could not be parsed from the manifests: the
	// profile this install was actually created from, not Dev. See
	// inferSizeProfile -- a Dev-sized fallback on a large install is not a
	// conservative guess, it is an under-provision.
	fb := sizeProfiles[0]
	inferred, profileKnown := inferSizeProfile(settings.chResources, settings.chShards)
	if profileKnown {
		fb = inferred
	}

	// Archive roll thresholds are seeded from the inferred profile so an existing
	// install adopts sizing matched to its scale instead of staying on the
	// archiver's flat code defaults, which are Dev-sized and leave anything larger
	// writing Parquet below compaction's optimal-file floor forever. An operator's
	// explicit secret still wins (see defaultUserSecretBytes).
	//
	// Only when the profile is actually known. Guessing here would seed Dev's
	// thresholds onto a hand-tuned large install, which is smaller than the code
	// default it replaced -- the exact regression this is meant to fix.
	var rollBytes, maxPendingBytes int64
	if profileKnown {
		rollBytes, maxPendingBytes = fb.ArchiveRollBytes, fb.ArchiveMaxPendingBytes
	}

	// Warn for each component falling back to defaults.
	type componentCheck struct {
		name   string
		parsed ResourceProfile
	}
	for _, c := range []componentCheck{
		{"ClickHouse", settings.chResources},
		{"ClickHouse Keeper", settings.chKeeperResources},
		{"Bifract", settings.bifractResources},
		{"PostgreSQL", settings.postgresResources},
		{"Caddy", settings.caddyResources},
		{"Caddy Log Shipper", settings.caddyShipperResources},
		{"LiteLLM", settings.litellmResources},
	} {
		if resourceProfileEmpty(c.parsed) {
			printWarn(fmt.Sprintf("%s resources not found in manifests, using Dev defaults", c.name))
		}
	}

	cfg := &K8sConfig{
		SizeProfile: SizeProfile{
			Name:            "custom",
			ClickHouse:      fallbackProfile(settings.chResources, fb.ClickHouse),
			CHKeeper:        fallbackProfile(settings.chKeeperResources, fb.CHKeeper),
			Bifract:         fallbackProfile(settings.bifractResources, fb.Bifract),
			ArchiveMaintain: fallbackProfile(settings.archiveMaintainResources, fb.ArchiveMaintain),
			Archiver:        archiverProfileFor(fallbackProfile(settings.archiverResources, fb.Archiver), settings.archiverResourcesRaw),
			Postgres:        fallbackProfile(settings.postgresResources, fb.Postgres),
			Caddy:           fallbackProfile(settings.caddyResources, fb.Caddy),
			CaddyShipper:    fallbackProfile(settings.caddyShipperResources, fb.CaddyShipper),
			LiteLLM:         fallbackProfile(settings.litellmResources, fb.LiteLLM),
			IngestQueueSize: fallbackInt(settings.ingestQueueSize, fb.IngestQueueSize),
			IngestWorkers:   fallbackInt(settings.ingestWorkers, fb.IngestWorkers),
			SpoolPVCSizeGB:  fallbackInt(settings.spoolPVCSizeGB, fb.SpoolPVCSizeGB),

			ArchiveRollBytes:       rollBytes,
			ArchiveMaxPendingBytes: maxPendingBytes,
		},
		CHShards:            settings.chShards,
		CHStorageGB:         settings.chStorageGB,
		OutputDir:           dir,
		MTLSEnabled:         settings.mtlsEnabled,
		ImagePullSecrets:    settings.imagePullSecrets,
		DashboardTick:       settings.dashboardTick,
		DashboardMinRefresh: settings.dashboardMinRefresh,
		DashboardWorkers:    settings.dashboardWorkers,
		IngestReplicas:      settings.ingestReplicas,

		ArchiveMaintainByteBudget:    settings.archiveMaintainByteBudget,
		ArchiveMaintainCommitRetries: settings.archiveMaintainCommitRetries,
	}

	// Where ClickHouse lives. Recovered from the manifests rather than defaulted,
	// so an upgrade of an external install does not re-render a bundled
	// ClickHouseInstallation the operator never asked for.
	cfg.CH = settings.ch

	// Core secrets
	cfg.PostgresPassword = secrets["POSTGRES_PASSWORD"]
	cfg.ClickHousePassword = secrets["CLICKHOUSE_PASSWORD"]
	// Least-privilege ingest CH user password: preserve if present, else generate for
	// installs that predate the split (the app provisions/rotates the user to match).
	// Rotate a password that predates the complexity policy; see
	// ClickHousePasswordCompliant.
	cfg.IngestClickHousePassword = secrets["INGEST_CLICKHOUSE_PASSWORD"]
	if !ClickHousePasswordCompliant(cfg.IngestClickHousePassword) {
		if pw, err := GenerateClickHousePassword(24); err == nil {
			cfg.IngestClickHousePassword = pw
		}
	}
	cfg.IngestPostgresPassword = secrets["INGEST_POSTGRES_PASSWORD"]
	if cfg.IngestPostgresPassword == "" {
		if pw, err := GenerateAlphanumeric(24); err == nil {
			cfg.IngestPostgresPassword = pw
		}
	}
	cfg.PasswordPepper = secrets["PASSWORD_PEPPER"]
	cfg.AdminPasswordHash = secrets["ADMIN_PASSWORD_HASH"]
	cfg.LiteLLMMasterKey = secrets["LITELLM_MASTER_KEY"]

	// Encryption keys (generate if missing from older installs)
	cfg.FeedEncryptionKey = secrets["FEED_ENCRYPTION_KEY"]
	if cfg.FeedEncryptionKey == "" {
		key, err := GenerateHexKey(32)
		if err == nil {
			cfg.FeedEncryptionKey = key
		}
	}
	cfg.BackupEncryptionKey = secrets["BACKUP_ENCRYPTION_KEY"]
	if cfg.BackupEncryptionKey == "" {
		key, err := GenerateHexKey(32)
		if err == nil {
			cfg.BackupEncryptionKey = key
		}
	}

	// Domain and network settings
	cfg.Domain = settings.domain
	cfg.IPAccess = settings.ipAccess
	cfg.AllowedIPs = settings.allowedIPs

	// User-configured secrets
	cfg.UserSecrets = map[string]string{
		"LITELLM_API_KEY":           secrets["LITELLM_API_KEY"],
		"S3_ENDPOINT":               secrets["S3_ENDPOINT"],
		"S3_BUCKET":                 secrets["S3_BUCKET"],
		"S3_ACCESS_KEY":             secrets["S3_ACCESS_KEY"],
		"S3_SECRET_KEY":             secrets["S3_SECRET_KEY"],
		"S3_REGION":                 secrets["S3_REGION"],
		"ARCHIVE_ENABLED":           secrets["ARCHIVE_ENABLED"],
		"ARCHIVE_BACKEND":           secrets["ARCHIVE_BACKEND"],
		"ARCHIVE_PREFIX":            secrets["ARCHIVE_PREFIX"],
		"ARCHIVE_SPOOL_MAX_BYTES":   secrets["ARCHIVE_SPOOL_MAX_BYTES"],
		"ARCHIVE_ROLL_BYTES":        secrets["ARCHIVE_ROLL_BYTES"],
		"ARCHIVE_MAX_PENDING_BYTES": secrets["ARCHIVE_MAX_PENDING_BYTES"],
		"ARCHIVE_ROLL_INTERVAL":     secrets["ARCHIVE_ROLL_INTERVAL"],
		"ARCHIVE_S3_ENDPOINT":       secrets["ARCHIVE_S3_ENDPOINT"],
		"ARCHIVE_S3_BUCKET":         secrets["ARCHIVE_S3_BUCKET"],
		"ARCHIVE_S3_REGION":         secrets["ARCHIVE_S3_REGION"],
		"ARCHIVE_S3_ACCESS_KEY":     secrets["ARCHIVE_S3_ACCESS_KEY"],
		"ARCHIVE_S3_SECRET_KEY":     secrets["ARCHIVE_S3_SECRET_KEY"],
		"ARCHIVE_AZURE_ACCOUNT":     secrets["ARCHIVE_AZURE_ACCOUNT"],
		"ARCHIVE_AZURE_KEY":         secrets["ARCHIVE_AZURE_KEY"],
		"ARCHIVE_AZURE_CONTAINER":   secrets["ARCHIVE_AZURE_CONTAINER"],
		"ARCHIVE_AZURE_ENDPOINT":    secrets["ARCHIVE_AZURE_ENDPOINT"],
		"MAXMIND_LICENSE_KEY":       secrets["MAXMIND_LICENSE_KEY"],
		"MAXMIND_ACCOUNT_ID":        secrets["MAXMIND_ACCOUNT_ID"],
		"OIDC_ISSUER_URL":           secrets["OIDC_ISSUER_URL"],
		"OIDC_CLIENT_ID":            secrets["OIDC_CLIENT_ID"],
		"OIDC_CLIENT_SECRET":        secrets["OIDC_CLIENT_SECRET"],
		"OIDC_REDIRECT_URL":         secrets["OIDC_REDIRECT_URL"],
		"OIDC_SCOPES":               secrets["OIDC_SCOPES"],
		"OIDC_DEFAULT_ROLE":         secrets["OIDC_DEFAULT_ROLE"],
		"OIDC_ALLOWED_DOMAINS":      secrets["OIDC_ALLOWED_DOMAINS"],
		"OIDC_BUTTON_TEXT":          secrets["OIDC_BUTTON_TEXT"],
	}

	// Preserve user-customized maxmind PVC settings
	cfg.MaxmindPVCAccessMode = settings.maxmindPVCAccessMode
	cfg.MaxmindPVCStorageClass = settings.maxmindPVCStorageClass

	// Preserve mTLS CA if it exists
	if cfg.MTLSEnabled {
		mtlsCAPath := filepath.Join(dir, "client-ca", "ca.pem")
		if data, err := os.ReadFile(mtlsCAPath); err == nil {
			cfg.MTLSCACert = string(data)
		}
		mtlsKeyPath := filepath.Join(dir, "client-ca", "ca-key.pem")
		if data, err := os.ReadFile(mtlsKeyPath); err == nil {
			cfg.MTLSCAKey = string(data)
		}
	}

	return cfg
}
