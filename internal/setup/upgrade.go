package setup

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/charmbracelet/lipgloss"
)

func RunUpgrade(dir string) error {
	PrintBanner()

	fmt.Println(TitleStyle.Render("  Upgrading Bifract"))
	fmt.Println()

	// Verify existing installation
	composePath := filepath.Join(dir, "docker-compose.yml")
	envPath := filepath.Join(dir, ".env")

	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("no installation found at %s (docker-compose.yml missing)", dir)
	}

	// Read existing config
	var existingEnv map[string]string
	if _, statErr := os.Stat(envPath); statErr == nil {
		var readErr error
		existingEnv, readErr = ReadEnvFile(envPath)
		if readErr != nil {
			return fmt.Errorf("read .env: %w", readErr)
		}
	} else {
		return fmt.Errorf("no .env file found at %s\n  Cannot upgrade without .env. Database passwords would be lost.\n  If this is a manual install, create a .env with your POSTGRES_PASSWORD and CLICKHOUSE_PASSWORD first", envPath)
	}

	// Verify critical credentials exist
	if existingEnv["POSTGRES_PASSWORD"] == "" || existingEnv["CLICKHOUSE_PASSWORD"] == "" {
		return fmt.Errorf("missing POSTGRES_PASSWORD or CLICKHOUSE_PASSWORD in %s\n  Cannot upgrade without database credentials", envPath)
	}

	currentVersion := existingEnv["BIFRACT_VERSION"]
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

	if currentVersion != "unknown" && CompareVersions(currentVersion, Version) >= 0 {
		printDone("Already up to date")
		return nil
	}

	// Verify the Bifract image is available on the registry before proceeding
	if Version != "dev" {
		printStep("Checking image availability...")
		if err := CheckImageAvailable(BifractImage(Version)); err != nil {
			return fmt.Errorf("bifract %s is not yet published. The release build may still be in progress or may have failed.\n  Try again shortly, or check https://github.com/zaneGittins/bifract/actions for build status", Version)
		}
		printDone("Image available")
	}

	// Backup existing config
	timestamp := time.Now().Format("20060102-150405")
	backupDir := filepath.Join(dir, ".backups", timestamp)
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("create backup dir: %w", err)
	}

	printStep("Backing up configuration...")
	backupFiles := []struct{ src, name string }{
		{envPath, ".env"},
		{composePath, "docker-compose.yml"},
		{filepath.Join(dir, "caddy", "Caddyfile"), "Caddyfile"},
	}
	for _, bf := range backupFiles {
		if err := copyFile(bf.src, filepath.Join(backupDir, bf.name)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("backup %s: %w", bf.name, err)
		}
	}
	printDone("Backed up to " + backupDir)

	// Build merged config
	cfg := DefaultConfig()
	cfg.InstallDir = dir
	cfg.ImageTag = Version

	// Preserve user values
	if v, ok := existingEnv["POSTGRES_PASSWORD"]; ok {
		cfg.PostgresPassword = v
	}
	if v, ok := existingEnv["CLICKHOUSE_PASSWORD"]; ok {
		cfg.ClickHousePassword = v
	}
	if v, ok := existingEnv["LITELLM_MASTER_KEY"]; ok {
		cfg.LiteLLMMasterKey = v
	}
	if v, ok := existingEnv["BIFRACT_DOMAIN"]; ok {
		cfg.Domain = v
	}
	if v, ok := existingEnv["LITELLM_MODEL"]; ok {
		cfg.LiteLLMModel = v
	}
	if v, ok := existingEnv["BIFRACT_CORS_ORIGINS"]; ok {
		cfg.CORSOrigins = v
	}
	if v, ok := existingEnv["BIFRACT_SSL_MODE"]; ok && v != "" {
		cfg.SSLMode = SSLMode(v)
	}
	if v, ok := existingEnv["BIFRACT_SSL_EMAIL"]; ok {
		cfg.SSLEmail = v
	}
	if v, ok := existingEnv["BIFRACT_CERT_PATH"]; ok {
		cfg.CertPath = v
	}
	if v, ok := existingEnv["BIFRACT_KEY_PATH"]; ok {
		cfg.KeyPath = v
	}
	if v, ok := existingEnv["BIFRACT_SECURE_COOKIES"]; ok && v == "true" {
		cfg.SecureCookies = true
	}
	// Always preserve the existing pepper. Generating a new one would invalidate
	// all stored password hashes. If absent (pre-pepper install), leave empty so
	// verifyPassword falls back to plain bcrypt -- changing it later requires a
	// full password reset for all users.
	if v, ok := existingEnv["BIFRACT_PASSWORD_PEPPER"]; ok {
		cfg.PasswordPepper = v
	}
	if v, ok := existingEnv["BIFRACT_FEED_ENCRYPTION_KEY"]; ok && v != "" {
		cfg.FeedEncryptionKey = v
	} else {
		// Generate for existing installations that predate this feature
		key, err := GenerateHexKey(32)
		if err != nil {
			return fmt.Errorf("generate feed encryption key: %w", err)
		}
		cfg.FeedEncryptionKey = key
		printDone("Generated feed encryption key")
	}
	if v, ok := existingEnv["BIFRACT_BACKUP_ENCRYPTION_KEY"]; ok && v != "" {
		cfg.BackupEncryptionKey = v
	} else {
		key, err := GenerateHexKey(32)
		if err != nil {
			return fmt.Errorf("generate backup encryption key: %w", err)
		}
		cfg.BackupEncryptionKey = key
		printDone("Generated backup encryption key")
	}

	// Preserve IP access control settings
	if v, ok := existingEnv["BIFRACT_IP_ACCESS"]; ok && v != "" {
		cfg.IPAccess = IPAccessMode(v)
	}
	if v, ok := existingEnv["BIFRACT_ALLOWED_IPS"]; ok && v != "" {
		cfg.ParseAllowedIPs(v)
	}

	// Preserve S3 backup storage config
	if v, ok := existingEnv["BIFRACT_S3_ENDPOINT"]; ok {
		cfg.S3Endpoint = v
	}
	if v, ok := existingEnv["BIFRACT_S3_BUCKET"]; ok {
		cfg.S3Bucket = v
	}
	if v, ok := existingEnv["BIFRACT_S3_ACCESS_KEY"]; ok {
		cfg.S3AccessKey = v
	}
	if v, ok := existingEnv["BIFRACT_S3_SECRET_KEY"]; ok {
		cfg.S3SecretKey = v
	}
	if v, ok := existingEnv["BIFRACT_S3_REGION"]; ok {
		cfg.S3Region = v
	}

	// Re-generate config files with merged values
	printStep("Updating configuration files...")

	compose, err := RenderDockerCompose(cfg)
	if err != nil {
		return fmt.Errorf("render docker-compose: %w", err)
	}
	if err := os.WriteFile(composePath, []byte(compose), 0644); err != nil {
		return fmt.Errorf("write docker-compose: %w", err)
	}

	envContent := RenderEnvFile(cfg)
	// Preserve manually-configured keys not managed by the setup wizard.
	preserveKeys := []string{
		"OPENAI_API_KEY", "ANTHROPIC_API_KEY",
		"MAXMIND_LICENSE_KEY", "MAXMIND_ACCOUNT_ID", "MAXMIND_EDITION_IDS",
		"BIFRACT_BASE_URL",
		"BIFRACT_OIDC_ISSUER_URL", "BIFRACT_OIDC_CLIENT_ID", "BIFRACT_OIDC_CLIENT_SECRET",
		"BIFRACT_OIDC_REDIRECT_URL", "BIFRACT_OIDC_SCOPES", "BIFRACT_OIDC_DEFAULT_ROLE",
		"BIFRACT_OIDC_ALLOWED_DOMAINS", "BIFRACT_OIDC_BUTTON_TEXT",
	}
	for _, key := range preserveKeys {
		if v, ok := existingEnv[key]; ok && v != "" {
			envContent += fmt.Sprintf("%s=%s\n", key, v)
		}
	}
	if err := os.WriteFile(envPath, []byte(envContent), 0600); err != nil {
		return fmt.Errorf("write .env: %w", err)
	}

	caddyfile, err := RenderCaddyfile(cfg)
	if err != nil {
		return fmt.Errorf("render caddyfile: %w", err)
	}
	caddyDir := filepath.Join(dir, "caddy")
	os.MkdirAll(caddyDir, 0755)
	if err := os.WriteFile(filepath.Join(caddyDir, "Caddyfile"), []byte(caddyfile), 0644); err != nil {
		return fmt.Errorf("write caddyfile: %w", err)
	}

	// Generate or copy TLS certificates for self-signed and custom modes.
	certsDir := filepath.Join(caddyDir, "certs")
	switch cfg.SSLMode {
	case SSLSelfSigned:
		if err := GenerateSelfSignedCert(cfg.Domain, certsDir); err != nil {
			printWarn(fmt.Sprintf("self-signed cert: %v", err))
		}
	case SSLCustom:
		if cfg.CertPath != "" && cfg.KeyPath != "" {
			if err := copyCertsToDir(cfg.CertPath, cfg.KeyPath, certsDir); err != nil {
				printWarn(fmt.Sprintf("copy custom certs: %v", err))
			}
		}
	}

	// Generate client CA if mTLS mode is enabled (idempotent, no-op if exists).
	if cfg.IPAccess == IPAccessMTLSApp {
		if err := GenerateClientCA(filepath.Join(caddyDir, "client-ca")); err != nil {
			printWarn(fmt.Sprintf("client CA: %v", err))
		}
	}

	if err := CopyEmbeddedFile("templates/entrypoint.sh", filepath.Join(caddyDir, "entrypoint.sh")); err != nil {
		printWarn(fmt.Sprintf("entrypoint.sh: %v", err))
	}
	if err := CopyEmbeddedFile("templates/ship-logs.sh", filepath.Join(caddyDir, "ship-logs.sh")); err != nil {
		printWarn(fmt.Sprintf("ship-logs.sh: %v", err))
	}
	if err := CopyEmbeddedFile("templates/litellm-config.yaml", filepath.Join(dir, "litellm-config.yaml")); err != nil {
		printWarn(fmt.Sprintf("litellm-config.yaml: %v", err))
	}
	printDone("Configuration updated")

	docker := &DockerOps{Dir: dir}

	// Run migrations if containers are running
	if docker.IsRunning() {
		printStep("Running database migrations...")
		pgApplied, err := RunPostgresMigrations(docker, "bifract", "bifract")
		if err != nil {
			printWarn(fmt.Sprintf("Postgres migration: %v", err))
		} else if pgApplied > 0 {
			printDone(fmt.Sprintf("Applied %d Postgres migration(s)", pgApplied))
		}

		chPass := cfg.ClickHousePassword
		chApplied, err := RunClickHouseMigrations(docker, "default", chPass)
		if err != nil {
			printWarn(fmt.Sprintf("ClickHouse migration: %v", err))
		} else if chApplied > 0 {
			printDone(fmt.Sprintf("Applied %d ClickHouse migration(s)", chApplied))
		}

		if pgApplied == 0 && chApplied == 0 {
			printDone("No new migrations to apply")
		}
	} else {
		printWarn("Containers not running, skipping migrations")
	}

	// Pull new images
	printStep("Pulling updated images...")
	if err := docker.Pull(); err != nil {
		return fmt.Errorf("docker compose pull: %w", err)
	}
	printDone("Images updated")

	// Restart
	printStep("Restarting services...")
	if _, err := docker.Down(); err != nil {
		printWarn("docker compose down had issues, continuing...")
	}
	if out, err := docker.Up(); err != nil {
		fmt.Println(DimStyle.Render(out))
		return fmt.Errorf("docker compose up: %w", err)
	}
	printDone("Services restarted")

	// Health check (via docker exec, avoids SSL/domain issues)
	printStep("Waiting for health check...")
	if err := docker.HealthCheck(120 * time.Second); err != nil {
		printWarn("Health check timed out, services may still be starting")
	} else {
		printDone("Bifract is healthy")
	}

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
	printDone("Upgrade complete")
	fmt.Println()

	return nil
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0644)
}
