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
	defer abandonStep() // clean up any in-progress spinner on an early error return
	PrintBanner()

	fmt.Println(TitleStyle.Render("  Reconfiguring Bifract"))
	fmt.Println()

	envPath := filepath.Join(dir, ".env")
	composePath := filepath.Join(dir, "docker-compose.yml")

	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("no installation found at %s (docker-compose.yml missing)", dir)
	}
	if _, err := os.Stat(filepath.Join(dir, legacyLiteLLMConfigFile)); err == nil {
		return errLegacyLiteLLM
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
	cfg.CH = TargetFromEnv(existingEnv)
	// Least-privilege ingest DB passwords: preserve if present, else generate strong
	// ones for installs that predate the split. Dropping these writes empty values to
	// .env, which silently strands the ingest tier: the app skips provisioning on an
	// empty password and bifract-ingest then fails auth as bifract_ingest.
	// Rotate a password that predates the complexity policy: preserving it would
	// carry a credential a managed ClickHouse refuses, leaving the ingest tier
	// unable to authenticate. See ClickHousePasswordCompliant.
	if v, ok := existingEnv["BIFRACT_INGEST_CLICKHOUSE_PASSWORD"]; ok && ClickHousePasswordCompliant(v) {
		cfg.IngestClickHousePassword = v
	} else {
		pw, err := GenerateClickHousePassword(24)
		if err != nil {
			return fmt.Errorf("generate ingest clickhouse password: %w", err)
		}
		cfg.IngestClickHousePassword = pw
		printDone("Generated least-privilege ingest ClickHouse password")
	}
	if v, ok := existingEnv["BIFRACT_INGEST_POSTGRES_PASSWORD"]; ok && v != "" {
		cfg.IngestPostgresPassword = v
	} else {
		pw, err := GenerateAlphanumeric(24)
		if err != nil {
			return fmt.Errorf("generate ingest postgres password: %w", err)
		}
		cfg.IngestPostgresPassword = pw
		printDone("Generated least-privilege ingest Postgres password")
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

	envContent := withCarriedEnv(RenderEnvFile(cfg), existingEnv)
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
