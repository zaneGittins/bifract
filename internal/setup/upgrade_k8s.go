package setup

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

// RunUpgradeK8s upgrades existing K8s manifests, preserving all secrets,
// settings, and resource profiles. PVCs are safe to re-apply (K8s does
// not modify existing PVCs or StatefulSet volumeClaimTemplates).
// K8sUpgradeOpts holds optional overrides for upgrades.
type K8sUpgradeOpts struct {
	SizeProfile string
}

func RunUpgradeK8s(dir string, opts K8sUpgradeOpts) error {
	PrintBanner()

	fmt.Println(TitleStyle.Render("  Upgrading Kubernetes Manifests"))
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
		return fmt.Errorf("missing POSTGRES_PASSWORD or CLICKHOUSE_PASSWORD in %s\n  Cannot upgrade without database credentials", secretsPath)
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

	if settings.domain == "" {
		return fmt.Errorf("could not detect domain from existing manifests.\n  Use --reconfigure-k8s --domain <your-domain> to set it")
	}

	// Detect current version
	currentVersion := settings.imageTag
	if currentVersion == "" {
		currentVersion = "unknown"
	}

	// Version info box
	versionInfo := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(Dim).
		Padding(0, 2).
		Render(fmt.Sprintf(
			"%s  %s  %s  %s\n%s  %s",
			PromptStyle.Render("Current:"), ValueStyle.Render(currentVersion),
			PromptStyle.Render("New:"), ValueStyle.Render(Version),
			PromptStyle.Render("Directory:"), DimStyle.Render(dir),
		))
	fmt.Println(versionInfo)
	fmt.Println()

	if currentVersion != "unknown" && currentVersion != "dev" && Version != "dev" && CompareVersions(currentVersion, Version) >= 0 {
		printDone("Already up to date")
		return nil
	}

	// Check image availability (warn but continue if not yet public)
	if Version != "dev" {
		printStep("Checking image availability...")
		if err := CheckImageAvailable(BifractImage(Version)); err != nil {
			printWarn(fmt.Sprintf("Image %s not yet available. The release may still be building or the image may be private.", BifractImage(Version)))
			printWarn("Manifests will be generated. The image must be available before applying.")
		} else {
			printDone("Image available")
		}
	}

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

	// Apply size profile override if specified
	if opts.SizeProfile != "" {
		profile, ok := lookupSizeProfile(opts.SizeProfile)
		if !ok {
			names := make([]string, len(sizeProfiles))
			for i, p := range sizeProfiles {
				names[i] = strings.ToLower(p.Name)
			}
			return fmt.Errorf("unknown size profile %q (available: %s)", opts.SizeProfile, strings.Join(names, ", "))
		}
		printStep(fmt.Sprintf("Applying size profile: %s", profile.Name))
		cfg.SizeProfile = profile
		cfg.CHShards = profile.CHShards
		cfg.CHReplicas = profile.CHReplicas
	}

	// Regenerate manifests with new version into a staging directory so that
	// a partial template failure does not overwrite the real manifests (which
	// would poison the version check on retry).
	stagingDir := filepath.Join(dir, ".staging")
	os.RemoveAll(stagingDir)
	cfg.ImageTag = Version
	cfg.OutputDir = stagingDir

	printStep("Writing updated manifests...")
	if err := writeK8sManifests(cfg); err != nil {
		os.RemoveAll(stagingDir)
		return fmt.Errorf("write manifests: %w", err)
	}
	if cfg.MTLSEnabled && cfg.MTLSCACert != "" {
		caDir := filepath.Join(stagingDir, "client-ca")
		if err := os.MkdirAll(caDir, 0700); err != nil {
			os.RemoveAll(stagingDir)
			return fmt.Errorf("create client-ca dir: %w", err)
		}
		if err := os.WriteFile(filepath.Join(caDir, "ca.pem"), []byte(cfg.MTLSCACert), 0644); err != nil {
			os.RemoveAll(stagingDir)
			return fmt.Errorf("write CA cert: %w", err)
		}
		if err := os.WriteFile(filepath.Join(caDir, "ca-key.pem"), []byte(cfg.MTLSCAKey), 0600); err != nil {
			os.RemoveAll(stagingDir)
			return fmt.Errorf("write CA key: %w", err)
		}
	}

	// All renders succeeded - copy staged files to real output directory.
	if err := copyStagedManifests(stagingDir, dir); err != nil {
		return fmt.Errorf("apply staged manifests: %w", err)
	}
	os.RemoveAll(stagingDir)
	printDone("Manifests updated")

	// Summary
	fmt.Println()
	summary := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(Green).
		Padding(0, 2).
		Render(fmt.Sprintf(
			"%s  %s %s %s\n%s  %s",
			PromptStyle.Render("Version:"), DimStyle.Render(currentVersion),
			SuccessStyle.Render("->"),
			ValueStyle.Render(Version),
			PromptStyle.Render("Backup: "), DimStyle.Render(backupDir),
		))
	fmt.Println(summary)
	fmt.Println()
	fmt.Println(DimStyle.Render("  Apply with:"))
	fmt.Println(DimStyle.Render("    kubectl apply -k " + dir))
	fmt.Println()
	printDone("Upgrade complete")
	fmt.Println()

	return nil
}

// k8sSettings holds non-secret configuration extracted from existing manifests.
type k8sSettings struct {
	imageTag    string
	domain      string
	chShards    int
	chReplicas  int
	chStorageGB int
	ipAccess    IPAccessMode
	allowedIPs  []string
	mtlsEnabled bool

	// Resource profiles
	chResources           ResourceProfile
	chKeeperResources     ResourceProfile
	bifractResources      ResourceProfile
	postgresResources     ResourceProfile
	caddyResources        ResourceProfile
	caddyShipperResources ResourceProfile
	litellmResources      ResourceProfile

	// imagePullSecrets preserves manually-added pull secret names from the
	// bifract deployment (e.g. for private GHCR images).
	imagePullSecrets []string

	// maxmindPVCAccessMode and maxmindPVCStorageClass preserve user-customized
	// PVC settings (e.g. ReadWriteMany + azurefile-csi for Azure) across upgrades.
	maxmindPVCAccessMode  string
	maxmindPVCStorageClass string
}

// coreSecretKeys are generated by bifract during install and must not be
// overwritten by live cluster values. Everything else is user-configurable
// and will be merged from the live cluster when present.
var coreSecretKeys = map[string]bool{
	"POSTGRES_PASSWORD":        true,
	"CLICKHOUSE_PASSWORD":      true,
	"CLICKHOUSE_PASSWORD_HASH": true,
	"PASSWORD_PEPPER":          true,
	"ADMIN_PASSWORD_HASH":      true,
	"FEED_ENCRYPTION_KEY":      true,
	"BACKUP_ENCRYPTION_KEY":    true,
	"LITELLM_MASTER_KEY":       true,
}

// tryReadLiveSecrets reads the deployed Secret from the cluster using kubectl.
// Returns (nil, reason) if kubectl is unavailable, not configured, or the
// secret does not exist. reason is empty on success.
func tryReadLiveSecrets(namespace, secretName string) (map[string]string, string) {
	if _, err := exec.LookPath("kubectl"); err != nil {
		return nil, "kubectl not found in PATH"
	}
	cmd := exec.Command("kubectl", "get", "secret", secretName,
		"-n", namespace, "-o", "jsonpath={.data}")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, msg
	}
	trimmed := strings.TrimSpace(string(out))
	if trimmed == "" || trimmed == "{}" {
		return nil, ""
	}
	var raw map[string]string
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		return nil, fmt.Sprintf("unexpected kubectl output: %s", trimmed)
	}
	result := make(map[string]string, len(raw))
	for k, v := range raw {
		if dec, err := base64.StdEncoding.DecodeString(v); err == nil {
			result[k] = strings.TrimRight(string(dec), "\n")
		}
	}
	return result, ""
}

// parseK8sSecrets reads stringData key-value pairs from a generated secrets.yaml.
func parseK8sSecrets(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	secrets := make(map[string]string)
	inStringData := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if trimmed == "stringData:" {
			inStringData = true
			continue
		}
		if !inStringData {
			continue
		}
		// End of stringData block (next top-level key or end of file)
		if len(line) > 0 && line[0] != ' ' && line[0] != '\t' {
			break
		}

		// Parse "  KEY: VALUE" or "  KEY: "VALUE""
		parts := strings.SplitN(trimmed, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		val = strings.Trim(val, "\"")
		secrets[key] = val
	}

	return secrets, scanner.Err()
}

// parseK8sSettings extracts configuration from existing rendered manifests.
func parseK8sSettings(dir string) (*k8sSettings, error) {
	s := &k8sSettings{
		// Sensible defaults in case files are missing or unparseable
		chShards:    1,
		chReplicas:  2,
		chStorageGB: 100,
	}

	// Parse image tag, domain, imagePullSecrets, and PVC settings from bifract deployment
	if data, err := os.ReadFile(filepath.Join(dir, "bifract", "deployment.yaml")); err == nil {
		content := string(data)
		s.imageTag = extractValue(content, `image: ghcr.io/zanegittins/bifract:(.+)`)
		s.domain = strings.TrimSpace(extractValue(content, `BIFRACT_DOMAIN\s*\n\s*value:\s*"?([^"\n]+)"?`))
		s.bifractResources = extractResources(content, "bifract")
		s.imagePullSecrets = extractImagePullSecrets(content)

		// Preserve user-customized bifract-maxmind PVC settings across upgrades.
		// Split by document separator so regex stays within the PVC document.
		for _, doc := range strings.Split(content, "\n---") {
			if strings.Contains(doc, "kind: PersistentVolumeClaim") && strings.Contains(doc, "name: bifract-maxmind") {
				s.maxmindPVCAccessMode = extractValue(doc, `accessModes:\s*\n\s*-\s*(\S+)`)
				s.maxmindPVCStorageClass = extractValue(doc, `storageClassName:\s*(\S+)`)
				break
			}
		}
	}

	// Fallback: extract domain from Caddy configmap if not found in deployment
	if s.domain == "" {
		if data, err := os.ReadFile(filepath.Join(dir, "caddy", "configmap.yaml")); err == nil {
			s.domain = strings.TrimSpace(extractValue(string(data), `(?m)^\s*(\S+\.+\S+)\s*\{`))
		}
	}

	// Parse CH shards, replicas, storage from clickhouse installation
	if data, err := os.ReadFile(filepath.Join(dir, "clickhouse", "clickhouse-installation.yaml")); err == nil {
		content := string(data)
		if v := extractValue(content, `(?m)^\s*shards:\s*(\d+)`); v != "" {
			s.chShards, _ = strconv.Atoi(v)
		}
		if v := extractValue(content, `(?m)^\s*replicas:\s*(\d+)`); v != "" {
			s.chReplicas, _ = strconv.Atoi(v)
		}
		if v := extractValue(content, `storage:\s*(\d+)Gi`); v != "" {
			s.chStorageGB, _ = strconv.Atoi(v)
		}
		s.chResources = extractResources(content, "clickhouse-server")
		s.chKeeperResources = extractResources(content, "clickhouse-keeper")
	}

	// Parse resources from other deployments
	if data, err := os.ReadFile(filepath.Join(dir, "postgres", "statefulset.yaml")); err == nil {
		s.postgresResources = extractResources(string(data), "postgres")
	}
	if data, err := os.ReadFile(filepath.Join(dir, "caddy", "deployment.yaml")); err == nil {
		content := string(data)
		s.caddyResources = extractResources(content, "caddy")
		s.caddyShipperResources = extractResources(content, "log-shipper")
	}
	if data, err := os.ReadFile(filepath.Join(dir, "litellm", "deployment.yaml")); err == nil {
		s.litellmResources = extractResources(string(data), "litellm")
	}

	// Detect mTLS
	kustomPath := filepath.Join(dir, "kustomization.yaml")
	if data, err := os.ReadFile(kustomPath); err == nil {
		if strings.Contains(string(data), "mtls-ca.yaml") {
			s.mtlsEnabled = true
		}
	}

	// Detect IP access from caddy configmap
	if data, err := os.ReadFile(filepath.Join(dir, "caddy", "configmap.yaml")); err == nil {
		content := string(data)
		if s.mtlsEnabled {
			s.ipAccess = IPAccessMTLSApp
		} else if strings.Contains(content, "@blocked_ingest not remote_ip") {
			s.ipAccess = IPAccessRestrictAll
			s.allowedIPs = extractAllowedIPs(content, "@blocked not remote_ip")
		} else if strings.Contains(content, "@blocked not remote_ip") {
			s.ipAccess = IPAccessRestrictApp
			s.allowedIPs = extractAllowedIPs(content, "@blocked not remote_ip")
		} else {
			s.ipAccess = IPAccessAll
		}
	}

	return s, nil
}

// extractValue returns the first capture group match for a regex in content.
func extractValue(content, pattern string) string {
	re := regexp.MustCompile(pattern)
	m := re.FindStringSubmatch(content)
	if len(m) > 1 {
		return m[1]
	}
	return ""
}

// extractResources parses CPU/memory requests and limits from the resources
// block following a container image line containing the given image name.
// Falls back to zero values if parsing fails.
func extractResources(content, imageName string) ResourceProfile {
	// Find the resources block after the image reference.
	// The templates produce a consistent format we can rely on.
	var p ResourceProfile

	// Split into lines for scanning
	lines := strings.Split(content, "\n")
	inContainer := false
	inResources := false
	inRequests := false
	inLimits := false
	resourceIndent := 0

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		indent := len(line) - len(strings.TrimLeft(line, " "))

		// Detect the container by image reference or repository field
		// Standard deployments: image: ghcr.io/foo/bar:tag
		// ClickHouse operator CRDs: repository: clickhouse/clickhouse-server
		if (strings.Contains(trimmed, "image:") || strings.Contains(trimmed, "repository:")) && strings.Contains(trimmed, imageName) {
			inContainer = true
			continue
		}
		if inContainer && trimmed == "resources:" {
			inResources = true
			resourceIndent = indent
			continue
		}
		if inResources {
			// End of resources block
			if indent <= resourceIndent && trimmed != "" {
				break
			}
			if trimmed == "requests:" {
				inRequests = true
				inLimits = false
				continue
			}
			if trimmed == "limits:" {
				inLimits = true
				inRequests = false
				continue
			}
			if inRequests {
				if strings.HasPrefix(trimmed, "cpu:") {
					p.CPURequest = strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "cpu:")), "\"")
				}
				if strings.HasPrefix(trimmed, "memory:") {
					p.MemRequest = strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "memory:")), "\"")
				}
			}
			if inLimits {
				if strings.HasPrefix(trimmed, "cpu:") {
					p.CPULimit = strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "cpu:")), "\"")
				}
				if strings.HasPrefix(trimmed, "memory:") {
					p.MemLimit = strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "memory:")), "\"")
				}
			}
		}
	}

	return p
}

// extractImagePullSecrets parses imagePullSecrets names from a deployment manifest.
func extractImagePullSecrets(content string) []string {
	var secrets []string
	re := regexp.MustCompile(`(?m)^\s*imagePullSecrets:\s*\n((?:\s*-\s*name:\s*.+\n)*)`)
	m := re.FindStringSubmatch(content)
	if len(m) < 2 {
		return nil
	}
	nameRe := regexp.MustCompile(`name:\s*(\S+)`)
	for _, match := range nameRe.FindAllStringSubmatch(m[1], -1) {
		if len(match) > 1 {
			secrets = append(secrets, match[1])
		}
	}
	return secrets
}

// extractAllowedIPs parses IP addresses from a Caddy @blocked directive.
func extractAllowedIPs(content, directive string) []string {
	idx := strings.Index(content, directive)
	if idx < 0 {
		return nil
	}
	// The directive is: @blocked not remote_ip 10.0.0.0/8 192.168.0.0/16
	line := content[idx:]
	if nl := strings.Index(line, "\n"); nl >= 0 {
		line = line[:nl]
	}
	parts := strings.Fields(line)
	// Skip "@blocked", "not", "remote_ip"
	if len(parts) > 3 {
		return parts[3:]
	}
	return nil
}

// copyStagedManifests copies all files from a staging directory into the
// real output directory, preserving subdirectory structure. This ensures
// manifests are only updated after all templates render successfully.
func copyStagedManifests(stagingDir, destDir string) error {
	return filepath.Walk(stagingDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(stagingDir, path)
		dst := filepath.Join(destDir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, data, info.Mode())
	})
}

// backupK8sManifests copies all YAML files from the output directory
// into a timestamped backup directory, preserving subdirectory structure.
func backupK8sManifests(srcDir, backupDir string) error {
	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Skip backup directories and non-YAML files
		rel, _ := filepath.Rel(srcDir, path)
		if strings.HasPrefix(rel, ".backups") {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".yaml") && !strings.HasSuffix(info.Name(), ".yml") {
			return nil
		}

		dst := filepath.Join(backupDir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			return err
		}
		return copyFile(path, dst)
	})
}
