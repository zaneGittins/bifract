package setup

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/charmbracelet/lipgloss"
)

func RunInstall() error {
	cfg := DefaultConfig()

	cfg, err := RunWizard(cfg)
	if err != nil {
		return err
	}

	// Post-wizard: show banner and progress in normal terminal
	PrintBanner()
	fmt.Println(TitleStyle.Render("  Installing Bifract"))
	fmt.Println()

	// Create install directory
	printStep("Creating install directory...")
	if err := os.MkdirAll(cfg.InstallDir, 0755); err != nil {
		return fmt.Errorf("create install dir: %w", err)
	}
	printDone("Directory created")

	// Check for existing .env to prevent accidental credential loss
	envPath := filepath.Join(cfg.InstallDir, ".env")
	if _, err := os.Stat(envPath); err == nil {
		return fmt.Errorf("existing installation found at %s (.env exists)\n  Use --upgrade to update, or remove %s to reinstall", cfg.InstallDir, envPath)
	}

	// Verify the Bifract image is available on the registry
	if cfg.ImageTag != "dev" {
		printStep("Checking image availability...")
		if err := CheckImageAvailable(BifractImage(cfg.ImageTag)); err != nil {
			return fmt.Errorf("bifract %s is not yet published. The release build may still be in progress or may have failed.\n  Try again shortly, or check https://github.com/zaneGittins/bifract/actions for build status", cfg.ImageTag)
		}
		printDone("Image available")
	}

	// Generate all config files
	printStep("Generating configuration files...")
	if err := WriteAllFiles(cfg); err != nil {
		return fmt.Errorf("write files: %w", err)
	}
	printDone("Configuration written to " + cfg.InstallDir)

	docker := &DockerOps{Dir: cfg.InstallDir}

	// Pull images
	printStep("Pulling Docker images (this may take a moment)...")
	if err := docker.Pull(); err != nil {
		return fmt.Errorf("docker compose pull: %w", err)
	}
	printDone("Images pulled")

	// Start services
	printStep("Starting services...")
	if out, err := docker.Up(); err != nil {
		fmt.Println(DimStyle.Render(out))
		return fmt.Errorf("docker compose up: %w", err)
	}
	printDone("Services started")

	// Health check (via docker exec, avoids SSL/domain issues)
	printStep("Waiting for Bifract to become healthy...")
	if err := docker.HealthCheck(120 * time.Second); err != nil {
		printWarn("Health check timed out, but services may still be starting")
		fmt.Println(DimStyle.Render("    Check: docker compose -f " + filepath.Join(cfg.InstallDir, "docker-compose.yml") + " logs bifract"))
	} else {
		printDone("Bifract is healthy")
	}

	// Set migration baseline
	printStep("Setting migration baseline...")
	if err := SetMigrationBaseline(docker, "bifract", "bifract", "default", cfg.ClickHousePassword); err != nil {
		printWarn("Could not set baseline (not critical)")
	} else {
		printDone("Migration baseline set")
	}

	// Run any migrations beyond the baseline
	printStep("Running database migrations...")
	pgApplied, err := RunPostgresMigrations(docker, "bifract", "bifract")
	if err != nil {
		printWarn(fmt.Sprintf("Postgres migration: %v", err))
	} else if pgApplied > 0 {
		printDone(fmt.Sprintf("Applied %d Postgres migration(s)", pgApplied))
	}

	chApplied, err := RunClickHouseMigrations(docker, "default", cfg.ClickHousePassword)
	if err != nil {
		printWarn(fmt.Sprintf("ClickHouse migration: %v", err))
	} else if chApplied > 0 {
		printDone(fmt.Sprintf("Applied %d ClickHouse migration(s)", chApplied))
	}

	if pgApplied == 0 && chApplied == 0 {
		printDone("No additional migrations needed")
	}

	// Generate initial client cert for mTLS
	var clientCertPath string
	if cfg.IPAccess == IPAccessMTLSApp {
		printStep("Generating initial client certificate...")
		caDir := filepath.Join(cfg.InstallDir, "caddy", "client-ca")
		clientCertPath = filepath.Join(cfg.InstallDir, "caddy", "client-ca", "admin.p12")
		if err := GenerateClientCert(caDir, "admin", cfg.AdminPassword, clientCertPath); err != nil {
			printWarn(fmt.Sprintf("client cert: %v", err))
			clientCertPath = ""
		} else {
			printDone("Client certificate generated")
		}
	}

	// Final summary
	fmt.Println()
	fmt.Println(TitleStyle.Render("  Installation Complete"))
	fmt.Println()

	url := "https://" + cfg.Domain

	summaryText := fmt.Sprintf(
		"%s  %s\n%s  %s\n%s  %s\n\n%s  %s",
		PromptStyle.Render("URL:      "), ValueStyle.Render(url),
		PromptStyle.Render("Username: "), ValueStyle.Render("admin"),
		PromptStyle.Render("Password: "), lipgloss.NewStyle().Foreground(White).Bold(true).Render(cfg.AdminPassword),
		PromptStyle.Render("Config:   "), DimStyle.Render(cfg.InstallDir),
	)
	if clientCertPath != "" {
		summaryText += fmt.Sprintf("\n\n%s  %s\n%s  %s",
			PromptStyle.Render("Client cert:"), DimStyle.Render(clientCertPath),
			PromptStyle.Render("Cert password:"), lipgloss.NewStyle().Foreground(White).Bold(true).Render(cfg.AdminPassword),
		)
	}

	summary := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(Green).
		Padding(1, 3).
		Render(summaryText)
	fmt.Println(summary)
	fmt.Println()
	fmt.Println(WarningStyle.Render("  Save the admin password above. It will not be shown again."))
	if clientCertPath != "" {
		fmt.Println(WarningStyle.Render("  Import the .p12 file into your browser to access Bifract."))
	}
	fmt.Println()

	return nil
}

// RunGenClientCert generates a new client certificate signed by the existing CA.
func RunGenClientCert(dir, name, password string) error {
	PrintBanner()

	// Look for CA in Docker layout (caddy/client-ca/) or K8s layout (client-ca/)
	caDir := filepath.Join(dir, "caddy", "client-ca")
	if !fileExists(filepath.Join(caDir, "ca.pem")) || !fileExists(filepath.Join(caDir, "ca-key.pem")) {
		caDir = filepath.Join(dir, "client-ca")
	}
	if !fileExists(filepath.Join(caDir, "ca.pem")) || !fileExists(filepath.Join(caDir, "ca-key.pem")) {
		return fmt.Errorf("client CA not found\n  Searched: %s and %s\n  mTLS must be enabled first (set BIFRACT_IP_ACCESS=mtls-app and run --reconfigure, or use --install-k8s with mTLS)",
			filepath.Join(dir, "caddy", "client-ca"), filepath.Join(dir, "client-ca"))
	}

	outputPath := filepath.Join(caDir, name+".p12")
	printStep(fmt.Sprintf("Generating client certificate for %q...", name))
	if err := GenerateClientCert(caDir, name, password, outputPath); err != nil {
		return fmt.Errorf("generate client cert: %w", err)
	}

	printDone("Client certificate generated")
	fmt.Println()

	summary := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(Green).
		Padding(0, 2).
		Render(fmt.Sprintf(
			"%s  %s\n%s  %s",
			PromptStyle.Render("File:    "), DimStyle.Render(outputPath),
			PromptStyle.Render("Password:"), ValueStyle.Render(password),
		))
	fmt.Println(summary)
	fmt.Println()
	fmt.Println(DimStyle.Render("  Import the .p12 file into your browser to authenticate."))
	fmt.Println()

	return nil
}

func printStep(msg string) {
	fmt.Printf("[%s] %s\n", DimStyle.Render("~"), msg)
}

func printDone(msg string) {
	fmt.Printf("[%s] %s\n", SuccessStyle.Render("+"), msg)
}

func printWarn(msg string) {
	fmt.Printf("[%s] %s\n", ErrorStyle.Render("-"), msg)
}

