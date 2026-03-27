package setup

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// RunStart starts the Bifract stack via docker compose up -d.
func RunStart(dir string) error {
	PrintBanner()

	composePath := filepath.Join(dir, "docker-compose.yml")
	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("no installation found at %s (docker-compose.yml missing)", dir)
	}

	docker := &DockerOps{Dir: dir}

	if docker.IsRunning() {
		printDone("Bifract is already running")
		return nil
	}

	printStep("Starting Bifract...")
	if out, err := docker.Up(); err != nil {
		return fmt.Errorf("docker compose up: %s", strings.TrimSpace(out))
	}

	printStep("Waiting for health check...")
	if err := docker.HealthCheck(120 * time.Second); err != nil {
		printWarn("Health check timed out, services may still be starting")
	} else {
		printDone("Bifract is healthy")
	}

	fmt.Println()
	printDone("Bifract started")
	fmt.Println()
	return nil
}

// RunStop stops the Bifract stack via docker compose down.
func RunStop(dir string) error {
	PrintBanner()

	composePath := filepath.Join(dir, "docker-compose.yml")
	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("no installation found at %s (docker-compose.yml missing)", dir)
	}

	docker := &DockerOps{Dir: dir}

	if !docker.IsRunning() {
		printDone("Bifract is already stopped")
		return nil
	}

	printStep("Stopping Bifract...")
	if out, err := docker.Down(); err != nil {
		return fmt.Errorf("docker compose down: %s", strings.TrimSpace(out))
	}

	fmt.Println()
	printDone("Bifract stopped")
	fmt.Println()
	return nil
}

// RunStatus shows the status of the Bifract deployment.
func RunStatus(dir string) error {
	PrintBanner()

	composePath := filepath.Join(dir, "docker-compose.yml")
	if _, err := os.Stat(composePath); os.IsNotExist(err) {
		return fmt.Errorf("no installation found at %s (docker-compose.yml missing)", dir)
	}

	// Read version from .env if available
	envPath := filepath.Join(dir, ".env")
	version := "unknown"
	if env, err := ReadEnvFile(envPath); err == nil {
		if v, ok := env["BIFRACT_VERSION"]; ok && v != "" {
			version = v
		}
	}

	fmt.Printf("  %s  %s\n", PromptStyle.Render("Version:"), ValueStyle.Render(version))
	fmt.Printf("  %s  %s\n", PromptStyle.Render("Directory:"), DimStyle.Render(dir))
	fmt.Println()

	// Show container status
	docker := &DockerOps{Dir: dir}
	cmd := docker.compose("ps", "--format", "table {{.Name}}\t{{.Status}}\t{{.Ports}}")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		printWarn("Could not retrieve container status")
	}

	fmt.Println()

	// Health check
	if !docker.IsRunning() {
		printWarn("Bifract is not running")
		return nil
	}

	out, err := exec.Command("docker", "inspect", "--format", "{{.State.Health.Status}}", "bifract-app").Output()
	if err != nil {
		printWarn("Could not determine health status")
	} else {
		status := strings.TrimSpace(string(out))
		switch status {
		case "healthy":
			printDone("Bifract is healthy")
		case "starting":
			printStep("Bifract is starting...")
		default:
			printWarn("Bifract is " + status)
		}
	}

	fmt.Println()
	return nil
}
