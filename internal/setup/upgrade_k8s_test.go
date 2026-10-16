package setup

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// tunedK8sConfig is an install an operator has customised in every way the
// upgrade path promises to preserve.
func tunedK8sConfig(dir string) *K8sConfig {
	medium, _ := lookupSizeProfile("Medium")
	cfg := freshK8sConfig(medium, dir)
	cfg.CHShards = 3
	cfg.CHStorageGB = 2048
	// Compliant, so the upgrade has no reason to rotate it.
	cfg.IngestClickHousePassword = "Ingest-Pw-123"
	cfg.IPAccess = IPAccessRestrictAll
	cfg.AllowedIPs = []string{"10.0.0.0/8", "203.0.113.7"}
	cfg.ImagePullSecrets = []string{"ghcr-pull", "mirror-pull"}
	cfg.MaxmindPVCAccessMode = "ReadWriteMany"
	cfg.MaxmindPVCStorageClass = "azurefile-csi"
	cfg.DashboardTick = 7
	cfg.DashboardMinRefresh = 30
	cfg.DashboardWorkers = 9
	cfg.IngestReplicas = 4
	cfg.SizeProfile.SpoolPVCSizeGB = 2048
	cfg.ArchiveMaintainByteBudget = 12 << 30
	cfg.ArchiveMaintainCommitRetries = 11
	cfg.UserSecrets = map[string]string{
		"OIDC_ISSUER_URL":    "https://login.example.com/realms/soc",
		"OIDC_CLIENT_SECRET": "s3cr3t:with:colons",
		"S3_BUCKET":          "bifract-backups",
		"LITELLM_API_KEY":    "sk-live-123",
	}
	return cfg
}

// An upgrade rebuilds everything from what it can read back out of the rendered
// manifests. Every setting the operator chose has to survive that round trip, or
// the upgrade silently reverts it.
func TestK8sSettingsSurviveRender(t *testing.T) {
	dir := t.TempDir()
	cfg := tunedK8sConfig(dir)
	if err := writeK8sManifests(cfg); err != nil {
		t.Fatal(err)
	}

	s, err := parseK8sSettings(dir)
	if err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"image tag", s.imageTag, cfg.ImageTag},
		{"domain", s.domain, cfg.Domain},
		{"shards", s.chShards, 3},
		{"storage (Ti)", s.chStorageGB, 2048},
		{"ip access", s.ipAccess, IPAccessRestrictAll},
		{"allowed ips", strings.Join(s.allowedIPs, ","), "10.0.0.0/8,203.0.113.7"},
		{"pull secrets", strings.Join(s.imagePullSecrets, ","), "ghcr-pull,mirror-pull"},
		{"maxmind access mode", s.maxmindPVCAccessMode, "ReadWriteMany"},
		{"maxmind storage class", s.maxmindPVCStorageClass, "azurefile-csi"},
		{"dashboard tick", s.dashboardTick, 7},
		{"dashboard min refresh", s.dashboardMinRefresh, 30},
		{"dashboard workers", s.dashboardWorkers, 9},
		{"ingest queue", s.ingestQueueSize, cfg.SizeProfile.IngestQueueSize},
		{"ingest workers", s.ingestWorkers, cfg.SizeProfile.IngestWorkers},
		{"ingest replicas", s.ingestReplicas, 4},
		{"spool size", s.spoolPVCSizeGB, 2048},
		{"maintain budget", s.archiveMaintainByteBudget, int64(12 << 30)},
		{"maintain retries", s.archiveMaintainCommitRetries, 11},
		{"mtls", s.mtlsEnabled, false},
		{"clickhouse resources", s.chResources, cfg.SizeProfile.ClickHouse},
		{"keeper resources", s.chKeeperResources, cfg.SizeProfile.CHKeeper},
		{"bifract resources", s.bifractResources, cfg.SizeProfile.Bifract},
		{"postgres resources", s.postgresResources, cfg.SizeProfile.Postgres},
		{"caddy resources", s.caddyResources, cfg.SizeProfile.Caddy},
		{"shipper resources", s.caddyShipperResources, cfg.SizeProfile.CaddyShipper},
		{"litellm resources", s.litellmResources, cfg.SizeProfile.LiteLLM},
		{"bundled clickhouse", s.ch.Bundled(), true},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: parsed %v, rendered %v", c.name, c.got, c.want)
		}
	}

	secrets, err := parseK8sSecrets(filepath.Join(dir, "bifract", "secrets.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]string{
		"POSTGRES_PASSWORD":          cfg.PostgresPassword,
		"CLICKHOUSE_PASSWORD":        cfg.ClickHousePassword,
		"INGEST_CLICKHOUSE_PASSWORD": cfg.IngestClickHousePassword,
		"INGEST_POSTGRES_PASSWORD":   cfg.IngestPostgresPassword,
		"PASSWORD_PEPPER":            cfg.PasswordPepper,
		"ADMIN_PASSWORD_HASH":        cfg.AdminPasswordHash,
		"FEED_ENCRYPTION_KEY":        cfg.FeedEncryptionKey,
		"BACKUP_ENCRYPTION_KEY":      cfg.BackupEncryptionKey,
		"LITELLM_MASTER_KEY":         cfg.LiteLLMMasterKey,
		"OIDC_ISSUER_URL":            "https://login.example.com/realms/soc",
		"OIDC_CLIENT_SECRET":         "s3cr3t:with:colons",
		"S3_BUCKET":                  "bifract-backups",
		"LITELLM_API_KEY":            "sk-live-123",
		"OIDC_SCOPES":                "openid,profile,email",
	} {
		if secrets[k] != want {
			t.Errorf("secret %s = %q, want %q", k, secrets[k], want)
		}
	}
}

func TestK8sSettingsAccessModes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*K8sConfig)
		want   IPAccessMode
		ips    string
	}{
		{"all", func(c *K8sConfig) {}, IPAccessAll, ""},
		{"restrict-app", func(c *K8sConfig) {
			c.IPAccess, c.AllowedIPs = IPAccessRestrictApp, []string{"192.0.2.0/24"}
		}, IPAccessRestrictApp, "192.0.2.0/24"},
		{"mtls", func(c *K8sConfig) {
			c.IPAccess, c.MTLSEnabled = IPAccessMTLSApp, true
			c.MTLSCACert, c.MTLSCAKey = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n", "k"
		}, IPAccessMTLSApp, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := freshK8sConfig(sizeProfiles[0], dir)
			tt.mutate(cfg)
			if err := writeK8sManifests(cfg); err != nil {
				t.Fatal(err)
			}
			s, err := parseK8sSettings(dir)
			if err != nil {
				t.Fatal(err)
			}
			if s.ipAccess != tt.want || strings.Join(s.allowedIPs, ",") != tt.ips {
				t.Errorf("parsed %q %v, want %q %q", s.ipAccess, s.allowedIPs, tt.want, tt.ips)
			}
			if s.mtlsEnabled != (tt.want == IPAccessMTLSApp) {
				t.Errorf("mtls = %v", s.mtlsEnabled)
			}
		})
	}
}

// No ClickHouseInstallation on disk is what marks an external install, and the
// target comes back from the app's rendered environment. Re-rendering a bundled
// ClickHouse for it would deploy a server the operator never asked for.
func TestK8sSettingsRecoverExternalClickHouse(t *testing.T) {
	dir := t.TempDir()
	cfg := freshK8sConfig(sizeProfiles[0], dir)
	cfg.CH = ClickHouseTarget{Backend: CHBackendExternal, Deployment: "cloud", Host: "abc.clickhouse.cloud", User: "bifract_admin"}
	cfg.CH.Normalize()
	if err := writeK8sManifests(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "clickhouse", "clickhouse-installation.yaml")); !os.IsNotExist(err) {
		t.Fatal("external install rendered a ClickHouseInstallation")
	}
	s, err := parseK8sSettings(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s.ch != cfg.CH {
		t.Errorf("recovered target %+v, want %+v", s.ch, cfg.CH)
	}
}

func TestTargetFromManifestEnv(t *testing.T) {
	if got := targetFromManifestEnv("kind: Deployment\nspec: {}\n"); !got.Bundled() {
		t.Errorf("no CLICKHOUSE_* env read as %+v", got)
	}
	manifest := `
          env:
            - name: CLICKHOUSE_HOST
              value: "ch.internal"
            - name: CLICKHOUSE_PORT
              value: "19000"
            - name: CLICKHOUSE_PASSWORD
              valueFrom:
                secretKeyRef: {name: bifract-secrets, key: CLICKHOUSE_PASSWORD}
            - name: CLICKHOUSE_SECURE
              value: "true"
`
	got := targetFromManifestEnv(manifest)
	want := ClickHouseTarget{Backend: CHBackendExternal, Deployment: "single-node", Host: "ch.internal",
		Port: 19000, Secure: true, Database: "logs", User: "default"}
	if got != want {
		t.Errorf("targetFromManifestEnv = %+v, want %+v", got, want)
	}
}

func TestParseK8sSecretsEdges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secrets.yaml")
	content := `apiVersion: v1
kind: Secret
metadata:
  name: bifract-secrets
stringData:
  PLAIN: unquoted
  QUOTED: "with spaces"
  URL: "https://h:8443/x"
  EMPTY: ""
  # a comment without a colon is skipped
---
apiVersion: v1
kind: Secret
stringData:
  OTHER_DOC: "must not be read"
`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := parseK8sSecrets(path)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"PLAIN": "unquoted", "QUOTED": "with spaces", "URL": "https://h:8443/x", "EMPTY": ""}
	if len(got) != len(want) {
		t.Errorf("parsed %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	if _, err := parseK8sSecrets(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Error("a missing secrets file parsed")
	}
}

func TestExtractHelpers(t *testing.T) {
	t.Run("allowed ips", func(t *testing.T) {
		caddy := "      @blocked not remote_ip 10.0.0.0/8 192.0.2.1\n      respond @blocked 403\n"
		if got := extractAllowedIPs(caddy, "@blocked not remote_ip"); strings.Join(got, " ") != "10.0.0.0/8 192.0.2.1" {
			t.Errorf("got %v", got)
		}
		if got := extractAllowedIPs(caddy, "@blocked_ingest not remote_ip"); got != nil {
			t.Errorf("absent directive gave %v", got)
		}
		if got := extractAllowedIPs("@blocked not remote_ip\n", "@blocked not remote_ip"); got != nil {
			t.Errorf("empty directive gave %v", got)
		}
	})
	t.Run("pull secrets", func(t *testing.T) {
		if got := extractImagePullSecrets("spec:\n  containers: []\n"); got != nil {
			t.Errorf("no pull secrets gave %v", got)
		}
	})
	t.Run("manifest kind", func(t *testing.T) {
		dir := t.TempDir()
		p := filepath.Join(dir, "m.yaml")
		os.WriteFile(p, []byte("# header\napiVersion: apps/v1\nkind: StatefulSet\nmetadata: {}\n---\nkind: Service\n"), 0600)
		if got := manifestKind(p); got != "StatefulSet" {
			t.Errorf("manifestKind = %q", got)
		}
		if got := manifestKind(filepath.Join(dir, "missing.yaml")); got != "" {
			t.Errorf("missing file kind = %q", got)
		}
	})
}

// Backups exist to recover a bad upgrade, so they must include every manifest
// (secrets included) and nothing else, and must not be any easier to read than
// the originals.
func TestBackupK8sManifests(t *testing.T) {
	src := t.TempDir()
	files := map[string]os.FileMode{
		"kustomization.yaml":           0644,
		"bifract/secrets.yaml":         0600,
		"bifract/deployment.yml":       0644,
		"client-ca/ca-key.pem":         0600,
		"notes.txt":                    0644,
		".backups/old/bifract/x.yaml":  0644,
		".staging/bifract/stale.yaml":  0644,
		"caddy/configmap.yaml":         0644,
		"bifract/admin-password":       0600,
		"bifract/nested/dir/deep.yaml": 0644,
	}
	for rel, mode := range files {
		p := filepath.Join(src, rel)
		os.MkdirAll(filepath.Dir(p), 0755)
		if err := os.WriteFile(p, []byte(rel), mode); err != nil {
			t.Fatal(err)
		}
	}
	backup := filepath.Join(src, ".backups", "now")
	if err := backupK8sManifests(src, backup); err != nil {
		t.Fatal(err)
	}

	for _, rel := range []string{"kustomization.yaml", "bifract/secrets.yaml", "bifract/deployment.yml",
		"caddy/configmap.yaml", "bifract/nested/dir/deep.yaml"} {
		b, err := os.ReadFile(filepath.Join(backup, rel))
		if err != nil || string(b) != rel {
			t.Errorf("%s not backed up: %v", rel, err)
		}
	}
	for _, rel := range []string{"notes.txt", "client-ca/ca-key.pem", "bifract/admin-password", ".backups/old/bifract/x.yaml"} {
		if _, err := os.Stat(filepath.Join(backup, rel)); err == nil {
			t.Errorf("%s was backed up", rel)
		}
	}
	assertMode(t, filepath.Join(backup, "bifract", "secrets.yaml"), 0600)
}

func TestCopyStagedManifests(t *testing.T) {
	staging, dest := t.TempDir(), t.TempDir()
	os.MkdirAll(filepath.Join(staging, "bifract"), 0755)
	os.WriteFile(filepath.Join(staging, "bifract", "secrets.yaml"), []byte("new"), 0600)
	os.WriteFile(filepath.Join(staging, "kustomization.yaml"), []byte("k"), 0644)
	os.MkdirAll(filepath.Join(dest, "bifract"), 0755)
	os.WriteFile(filepath.Join(dest, "bifract", "secrets.yaml"), []byte("old"), 0600)
	os.WriteFile(filepath.Join(dest, "bifract", "keep.yaml"), []byte("untouched"), 0644)

	if err := copyStagedManifests(staging, dest); err != nil {
		t.Fatal(err)
	}
	for rel, want := range map[string]string{"bifract/secrets.yaml": "new", "kustomization.yaml": "k", "bifract/keep.yaml": "untouched"} {
		if b, _ := os.ReadFile(filepath.Join(dest, rel)); string(b) != want {
			t.Errorf("%s = %q, want %q", rel, b, want)
		}
	}
	if err := copyStagedManifests(filepath.Join(staging, "missing"), dest); err == nil {
		t.Error("a missing staging dir copied without error")
	}
}

func TestCopyFilePreservesMode(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, ".env")
	os.WriteFile(src, []byte("POSTGRES_PASSWORD=x\n"), 0600)
	dst := filepath.Join(dir, "backup.env")
	if err := copyFile(src, dst); err != nil {
		t.Fatal(err)
	}
	assertMode(t, dst, 0600)
	if err := copyFile(filepath.Join(dir, "missing"), dst); !os.IsNotExist(err) {
		t.Errorf("missing source: err = %v, want not-exist (callers skip absent files on it)", err)
	}
}

// writeK8sInstall renders a complete install tree as --install-k8s would leave it.
func writeK8sInstall(t *testing.T, cfg *K8sConfig) {
	t.Helper()
	if err := writeK8sOutput(cfg); err != nil {
		t.Fatal(err)
	}
}

func setVersion(t *testing.T, v string) {
	t.Helper()
	old := Version
	Version = v
	t.Cleanup(func() { Version = old })
}

func TestRunUpgradeK8s(t *testing.T) {
	withoutExternalCommands(t)
	setVersion(t, "v1.5.0")

	dir := t.TempDir()
	cfg := tunedK8sConfig(dir)
	cfg.ImageTag = "v1.4.0"
	writeK8sInstall(t, cfg)
	before, _ := parseK8sSecrets(filepath.Join(dir, "bifract", "secrets.yaml"))
	// Leftovers of the pre-StatefulSet era the upgrade has to clean up after.
	os.WriteFile(filepath.Join(dir, "bifract", "archive-maintain-cronjob.yaml"), []byte("kind: CronJob\n"), 0600)
	ingest := readFile(t, dir, "bifract", "ingest-deployment.yaml")
	os.WriteFile(filepath.Join(dir, "bifract", "ingest-deployment.yaml"),
		[]byte(strings.Replace(ingest, "kind: StatefulSet", "kind: Deployment", 1)), 0600)

	var err error
	out := captureStdout(t, func() { err = RunUpgradeK8s(dir, K8sUpgradeOpts{}) })
	if err != nil {
		t.Fatalf("RunUpgradeK8s: %v\n%s", err, out)
	}

	s, err := parseK8sSettings(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s.imageTag != "v1.5.0" {
		t.Errorf("image tag = %q, want the new version", s.imageTag)
	}
	if s.domain != cfg.Domain || s.ipAccess != IPAccessRestrictAll || s.chShards != 3 || s.ingestReplicas != 4 ||
		s.dashboardWorkers != 9 || strings.Join(s.imagePullSecrets, ",") != "ghcr-pull,mirror-pull" {
		t.Errorf("settings changed across the upgrade: %+v", s)
	}
	after, _ := parseK8sSecrets(filepath.Join(dir, "bifract", "secrets.yaml"))
	for k, v := range before {
		// Seeded thresholds are the one sanctioned change; every credential stays.
		if strings.HasPrefix(k, "ARCHIVE_") {
			continue
		}
		if after[k] != v {
			t.Errorf("secret %s changed across the upgrade: %q -> %q", k, v, after[k])
		}
	}

	if _, err := os.Stat(filepath.Join(dir, "bifract", "archive-maintain-cronjob.yaml")); !os.IsNotExist(err) {
		t.Error("the obsolete archive-maintain CronJob manifest was left in the tree")
	}
	if !strings.Contains(out, "kubectl delete deployment bifract-ingest") {
		t.Error("no notice to delete the old ingest Deployment")
	}
	if _, err := os.Stat(filepath.Join(dir, ".staging")); !os.IsNotExist(err) {
		t.Error("staging directory left behind")
	}
	backups, _ := filepath.Glob(filepath.Join(dir, ".backups", "*", "bifract", "secrets.yaml"))
	if len(backups) != 1 {
		t.Fatalf("found %d secrets backups, want 1", len(backups))
	}
	if b, _ := parseK8sSecrets(backups[0]); b["POSTGRES_PASSWORD"] != before["POSTGRES_PASSWORD"] {
		t.Error("backup does not hold the pre-upgrade secrets")
	}
	assertMode(t, backups[0], 0600)
}

func TestRunUpgradeK8sSkipsWhenCurrent(t *testing.T) {
	withoutExternalCommands(t)
	for _, installed := range []string{"v1.5.0", "v1.6.0"} {
		t.Run(installed, func(t *testing.T) {
			setVersion(t, "v1.5.0")
			dir := t.TempDir()
			cfg := freshK8sConfig(sizeProfiles[0], dir)
			cfg.ImageTag = installed
			writeK8sInstall(t, cfg)
			deploy := readFile(t, dir, "bifract", "deployment.yaml")

			var err error
			captureStdout(t, func() { err = RunUpgradeK8s(dir, K8sUpgradeOpts{}) })
			if err != nil {
				t.Fatal(err)
			}
			if readFile(t, dir, "bifract", "deployment.yaml") != deploy {
				t.Error("manifests rewritten although the install is not older than this binary")
			}
			if _, err := os.Stat(filepath.Join(dir, ".backups")); !os.IsNotExist(err) {
				t.Error("backed up although nothing was upgraded")
			}
		})
	}
}

func TestRunUpgradeK8sSizeProfile(t *testing.T) {
	withoutExternalCommands(t)
	dir := t.TempDir()
	writeK8sInstall(t, freshK8sConfig(sizeProfiles[0], dir))

	var err error
	captureStdout(t, func() { err = RunUpgradeK8s(dir, K8sUpgradeOpts{SizeProfile: "large"}) })
	if err != nil {
		t.Fatal(err)
	}
	large, _ := lookupSizeProfile("Large")
	s, _ := parseK8sSettings(dir)
	if s.chShards != large.CHShards || s.chResources != large.ClickHouse {
		t.Errorf("profile not applied: %d shards, %+v", s.chShards, s.chResources)
	}

	deploy := readFile(t, dir, "bifract", "deployment.yaml")
	captureStdout(t, func() { err = RunUpgradeK8s(dir, K8sUpgradeOpts{SizeProfile: "huge"}) })
	if err == nil || !strings.Contains(err.Error(), "unknown size profile") {
		t.Errorf("err = %v", err)
	}
	if readFile(t, dir, "bifract", "deployment.yaml") != deploy {
		t.Error("an unknown profile still rewrote the manifests")
	}
}

func TestRunUpgradeK8sRefuses(t *testing.T) {
	withoutExternalCommands(t)
	tests := []struct {
		name    string
		setup   func(dir string)
		wantErr string
	}{
		{"no install", func(dir string) {}, "secrets.yaml missing"},
		{"no deployment", func(dir string) {
			os.MkdirAll(filepath.Join(dir, "bifract"), 0755)
			os.WriteFile(filepath.Join(dir, "bifract", "secrets.yaml"), []byte("stringData:\n"), 0600)
		}, "deployment.yaml missing"},
		{"no credentials", func(dir string) {
			os.MkdirAll(filepath.Join(dir, "bifract"), 0755)
			os.WriteFile(filepath.Join(dir, "bifract", "secrets.yaml"), []byte("stringData:\n  POSTGRES_PASSWORD: \"x\"\n"), 0600)
			os.WriteFile(filepath.Join(dir, "bifract", "deployment.yaml"), []byte("kind: Deployment\n"), 0600)
		}, "missing POSTGRES_PASSWORD or CLICKHOUSE_PASSWORD"},
		{"no domain", func(dir string) {
			os.MkdirAll(filepath.Join(dir, "bifract"), 0755)
			os.WriteFile(filepath.Join(dir, "bifract", "secrets.yaml"),
				[]byte("stringData:\n  POSTGRES_PASSWORD: \"x\"\n  CLICKHOUSE_PASSWORD: \"y\"\n"), 0600)
			os.WriteFile(filepath.Join(dir, "bifract", "deployment.yaml"), []byte("kind: Deployment\n"), 0600)
		}, "could not detect domain"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			tt.setup(dir)
			var err error
			captureStdout(t, func() { err = RunUpgradeK8s(dir, K8sUpgradeOpts{}) })
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err = %v, want %q", err, tt.wantErr)
			}
			if _, statErr := os.Stat(filepath.Join(dir, ".backups")); statErr == nil {
				t.Error("touched the install before refusing")
			}
		})
	}
}

// User-configured secrets added with kubectl edit live only in the cluster; the
// upgrade merges them in, but never lets the cluster override a core credential.
func TestRunUpgradeK8sMergesLiveSecrets(t *testing.T) {
	dir := t.TempDir()
	cfg := freshK8sConfig(sizeProfiles[0], dir)
	writeK8sInstall(t, cfg)

	live := `{"POSTGRES_PASSWORD":"` + b64("from-cluster") + `","OIDC_CLIENT_ID":"` + b64("bifract-sso\n") + `","S3_BUCKET":"` + b64("") + `"}`
	newFakeExec(t, fakeRule{Cmd: "kubectl", Match: []string{"get secret bifract-secrets"}, Stdout: []byte(live)})

	var err error
	captureStdout(t, func() { err = RunUpgradeK8s(dir, K8sUpgradeOpts{}) })
	if err != nil {
		t.Fatal(err)
	}
	secrets, _ := parseK8sSecrets(filepath.Join(dir, "bifract", "secrets.yaml"))
	if secrets["OIDC_CLIENT_ID"] != "bifract-sso" {
		t.Errorf("live user secret not merged: %q", secrets["OIDC_CLIENT_ID"])
	}
	if secrets["POSTGRES_PASSWORD"] != cfg.PostgresPassword {
		t.Errorf("live cluster overrode a core credential: %q", secrets["POSTGRES_PASSWORD"])
	}
}

func TestTryReadLiveSecrets(t *testing.T) {
	t.Run("no kubectl", func(t *testing.T) {
		withoutExternalCommands(t)
		got, reason := tryReadLiveSecrets("bifract", "bifract-secrets")
		if got != nil || !strings.Contains(reason, "kubectl not found") {
			t.Errorf("got %v, %q", got, reason)
		}
	})
	t.Run("kubectl error", func(t *testing.T) {
		newFakeExec(t, fakeRule{Cmd: "kubectl", Stdout: []byte("forbidden"), Exit: 1})
		if got, reason := tryReadLiveSecrets("bifract", "bifract-secrets"); got != nil || reason == "" {
			t.Errorf("got %v, %q", got, reason)
		}
	})
	t.Run("no secret data", func(t *testing.T) {
		newFakeExec(t, fakeRule{Cmd: "kubectl", Stdout: []byte("{}")})
		if got, reason := tryReadLiveSecrets("bifract", "bifract-secrets"); got != nil || reason != "" {
			t.Errorf("got %v, %q", got, reason)
		}
	})
	t.Run("garbage", func(t *testing.T) {
		newFakeExec(t, fakeRule{Cmd: "kubectl", Stdout: []byte("not json")})
		if _, reason := tryReadLiveSecrets("bifract", "bifract-secrets"); !strings.Contains(reason, "unexpected kubectl output") {
			t.Errorf("reason = %q", reason)
		}
	})
	t.Run("decodes", func(t *testing.T) {
		fake := newFakeExec(t, fakeRule{Cmd: "kubectl", Stdout: []byte(`{"A":"` + b64("one\n") + `","B":"!!notbase64"}`)})
		got, reason := tryReadLiveSecrets("bifract", "bifract-secrets")
		if reason != "" || got["A"] != "one" {
			t.Errorf("got %v, %q", got, reason)
		}
		if _, ok := got["B"]; ok {
			t.Error("an undecodable value was kept")
		}
		if c := fake.calls(); len(c) != 1 || c[0].joined() != "get secret bifract-secrets -n bifract -o jsonpath={.data}" {
			t.Errorf("calls = %+v", c)
		}
	})
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
