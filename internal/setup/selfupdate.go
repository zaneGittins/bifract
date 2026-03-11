package setup

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type githubRelease struct {
	TagName string `json:"tag_name"`
}

// CheckLatestSetupVersion queries the GitHub API for the latest bifract release tag.
func CheckLatestSetupVersion() (string, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get("https://api.github.com/repos/zaneGittins/bifract/releases/latest")
	if err != nil {
		return "", fmt.Errorf("check latest version: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GitHub API returned %d", resp.StatusCode)
	}

	var release githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return "", fmt.Errorf("parse release: %w", err)
	}
	if release.TagName == "" {
		return "", fmt.Errorf("empty tag in release response")
	}
	return release.TagName, nil
}

// SelfUpdate checks for a newer bifractctl binary and re-execs if one is found.
// It appends --skip-self-update to the re-exec args to prevent recursion.
// If an update occurs, this function calls os.Exit and does not return.
// If no update is needed or the check fails, it returns normally.
func SelfUpdate(origArgs []string) {
	if Version == "dev" {
		return
	}

	printStep("Checking for bifractctl updates...")

	latest, err := CheckLatestSetupVersion()
	if err != nil {
		printWarn(fmt.Sprintf("Could not check for updates: %v", err))
		return
	}

	if CompareVersions(Version, latest) >= 0 {
		printDone(fmt.Sprintf("bifractctl is up to date (%s)", Version))
		return
	}

	fmt.Printf("[%s] %s %s -> %s\n",
		SuccessStyle.Render("+"),
		PromptStyle.Render("New bifractctl available:"),
		DimStyle.Render(Version),
		ValueStyle.Render(latest),
	)

	fmt.Printf("    Update bifractctl before upgrading? [Y/n] ")
	reader := bufio.NewReader(os.Stdin)
	answer, _ := reader.ReadString('\n')
	answer = strings.TrimSpace(strings.ToLower(answer))
	if answer != "" && answer != "y" && answer != "yes" {
		printDone(fmt.Sprintf("Continuing with bifractctl %s", Version))
		return
	}

	binaryURL := fmt.Sprintf(
		"https://github.com/zaneGittins/bifract/releases/download/%s/bifractctl-%s-%s",
		latest, runtime.GOOS, runtime.GOARCH,
	)

	printStep("Downloading bifractctl " + latest + "...")
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Get(binaryURL)
	if err != nil {
		printWarn(fmt.Sprintf("Download failed: %v; continuing with current version", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		printWarn(fmt.Sprintf("Download returned %d; continuing with current version", resp.StatusCode))
		return
	}

	execPath, err := os.Executable()
	if err != nil {
		printWarn(fmt.Sprintf("Cannot determine executable path: %v", err))
		return
	}
	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		printWarn(fmt.Sprintf("Cannot resolve executable path: %v", err))
		return
	}

	// Write to a temp file next to the current binary, then atomic rename.
	tmpPath := execPath + ".update"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		printWarn(fmt.Sprintf("Cannot write update: %v", err))
		return
	}

	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		printWarn(fmt.Sprintf("Download incomplete: %v; continuing with current version", err))
		return
	}
	tmp.Close()

	if err := os.Rename(tmpPath, execPath); err != nil {
		os.Remove(tmpPath)
		printWarn(fmt.Sprintf("Cannot replace binary: %v; continuing with current version", err))
		return
	}

	printDone("Updated bifractctl to " + latest)
	fmt.Println()

	// Re-exec the new binary with --skip-self-update to prevent recursion.
	args := make([]string, len(origArgs)-1, len(origArgs))
	copy(args, origArgs[1:])
	args = append(args, "--skip-self-update")
	cmd := exec.Command(execPath, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode())
		}
		os.Exit(1)
	}
	os.Exit(0)
}
