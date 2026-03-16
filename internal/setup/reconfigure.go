package setup

import (
	"fmt"
	"os"
	"path/filepath"
)

// RunReconfigure re-renders configuration files (Caddyfile, docker-compose.yml, .env)
// from the existing .env values without requiring a version change. Useful for applying
// setting changes like IP access controls without a full upgrade cycle.
func RunReconfigure(dir string) error {
	PrintBanner()

	fmt.Println(TitleStyle.Render("  Reconfiguring Bifract"))
	fmt.Println()

	envPath := filepath.Join(dir, ".env")
	composePath := filepath.Join(dir, "docker-compose.yml")

	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("no installation found at %s (docker-compose.yml missing)", dir)
	}

	var existingEnv map[string]string
	if _, statErr := os.Stat(envPath); statErr == nil {
		var readErr error
		existingEnv, readErr = ReadEnvFile(envPath)
		if readErr != nil {
			return fmt.Errorf("read .env: %w", readErr)
		}
	} else {
		return fmt.Errorf("no .env file found at %s", envPath)
	}

	if existingEnv["POSTGRES_PASSWORD"] == "" || existingEnv["CLICKHOUSE_PASSWORD"] == "" {
		return fmt.Errorf("missing POSTGRES_PASSWORD or CLICKHOUSE_PASSWORD in %s", envPath)
	}

	// Build config from existing .env (same merge logic as upgrade)
	cfg := DefaultConfig()
	cfg.InstallDir = dir
	if v, ok := existingEnv["BIFRACT_VERSION"]; ok {
		cfg.ImageTag = v
	}
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
	if v, ok := existingEnv["BIFRACT_PASSWORD_PEPPER"]; ok {
		cfg.PasswordPepper = v
	}
	if v, ok := existingEnv["BIFRACT_FEED_ENCRYPTION_KEY"]; ok && v != "" {
		cfg.FeedEncryptionKey = v
	}
	if v, ok := existingEnv["BIFRACT_BACKUP_ENCRYPTION_KEY"]; ok && v != "" {
		cfg.BackupEncryptionKey = v
	}
	if v, ok := existingEnv["BIFRACT_IP_ACCESS"]; ok && v != "" {
		cfg.IPAccess = IPAccessMode(v)
	}
	if v, ok := existingEnv["BIFRACT_ALLOWED_IPS"]; ok && v != "" {
		cfg.ParseAllowedIPs(v)
	}
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

	// Re-render config files
	printStep("Regenerating configuration files...")

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
		"LITELLM_API_KEY",
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

	caddyDir := filepath.Join(dir, "caddy")
	os.MkdirAll(caddyDir, 0755)
	caddyfile, err := RenderCaddyfile(cfg)
	if err != nil {
		return fmt.Errorf("render caddyfile: %w", err)
	}
	if err := os.WriteFile(filepath.Join(caddyDir, "Caddyfile"), []byte(caddyfile), 0644); err != nil {
		return fmt.Errorf("write caddyfile: %w", err)
	}

	// Generate client CA if switching to mTLS (idempotent, no-op if exists).
	if cfg.IPAccess == IPAccessMTLSApp {
		caDir := filepath.Join(dir, "caddy", "client-ca")
		if err := GenerateClientCA(caDir); err != nil {
			printWarn(fmt.Sprintf("client CA: %v", err))
		} else {
			printDone("Client CA ready")
		}
	}

	printDone("Configuration files updated")

	// Recreate containers to pick up compose changes (e.g. new volume mounts for mTLS)
	docker := &DockerOps{Dir: dir}
	if docker.IsRunning() {
		printStep("Applying configuration changes...")
		if out, err := docker.Up(); err != nil {
			printWarn(fmt.Sprintf("docker compose up: %s", out))
		} else {
			printDone("Configuration applied")
		}
	} else {
		printWarn("Containers not running, changes will apply on next start")
	}

	fmt.Println()
	printDone("Reconfiguration complete")
	fmt.Println()

	return nil
}
