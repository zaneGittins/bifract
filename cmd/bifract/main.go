package main

import (
	"fmt"
	"os"
	"os/exec"

	"bifract/internal/ingestcli"
	"bifract/internal/setup"
)

func init() {
	// Sync version info so ingestcli uses the same build metadata.
	ingestcli.Version = setup.Version
	ingestcli.Commit = setup.Commit
	ingestcli.BuildDate = setup.BuildDate
}

func main() {
	args := os.Args[1:]

	// --ingest is handled separately: all args after it belong to the ingest subsystem.
	for i, arg := range args {
		if arg == "--ingest" {
			if err := ingestcli.RunIngest(args[i+1:]); err != nil {
				fmt.Fprintf(os.Stderr, "\n%s %v\n", ingestcli.ErrorStyle.Render("Error:"), err)
				os.Exit(1)
			}
			return
		}
	}

	var installMode, installK8sMode, upgradeMode, upgradeK8sMode, reconfigureMode, reconfigureK8sMode, showVersion, skipSelfUpdate bool
	var backupMode, restoreMode, listBackupsMode, nonInteractive, genClientCertMode bool
	var startMode, stopMode, statusMode bool
	var restoreFile, certName, certPassword string
	var ipAccess, allowedIPs, domain, sizeProfile string
	dir := "/opt/bifract"

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--install":
			installMode = true
		case "--install-k8s":
			installK8sMode = true
		case "--upgrade":
			upgradeMode = true
		case "--upgrade-k8s":
			upgradeK8sMode = true
		case "--backup":
			backupMode = true
		case "--restore":
			restoreMode = true
		case "--list-backups":
			listBackupsMode = true
		case "--reconfigure":
			reconfigureMode = true
		case "--reconfigure-k8s":
			reconfigureK8sMode = true
		case "--ip-access":
			if i+1 < len(args) {
				i++
				ipAccess = args[i]
			} else {
				fmt.Fprintln(os.Stderr, "Error: --ip-access requires a value (all, restrict-app, restrict-all, mtls-app)")
				os.Exit(1)
			}
		case "--allowed-ips":
			if i+1 < len(args) {
				i++
				allowedIPs = args[i]
			} else {
				fmt.Fprintln(os.Stderr, "Error: --allowed-ips requires a value")
				os.Exit(1)
			}
		case "--domain":
			if i+1 < len(args) {
				i++
				domain = args[i]
			} else {
				fmt.Fprintln(os.Stderr, "Error: --domain requires a value")
				os.Exit(1)
			}
		case "--size":
			if i+1 < len(args) {
				i++
				sizeProfile = args[i]
			} else {
				fmt.Fprintln(os.Stderr, "Error: --size requires a value (dev, x-small, small, medium, large, x-large)")
				os.Exit(1)
			}
		case "--gen-client-cert":
			genClientCertMode = true
		case "--start":
			startMode = true
		case "--stop":
			stopMode = true
		case "--status":
			statusMode = true
		case "--name":
			if i+1 < len(args) {
				i++
				certName = args[i]
			} else {
				fmt.Fprintln(os.Stderr, "Error: --name requires a value")
				os.Exit(1)
			}
		case "--password":
			if i+1 < len(args) {
				i++
				certPassword = args[i]
			} else {
				fmt.Fprintln(os.Stderr, "Error: --password requires a value")
				os.Exit(1)
			}
		case "--non-interactive":
			nonInteractive = true
		case "--restore-file":
			if i+1 < len(args) {
				i++
				restoreFile = args[i]
			} else {
				fmt.Fprintln(os.Stderr, "Error: --restore-file requires a path argument")
				os.Exit(1)
			}
		case "--dir":
			if i+1 < len(args) {
				i++
				dir = args[i]
			} else {
				fmt.Fprintln(os.Stderr, "Error: --dir requires a path argument")
				os.Exit(1)
			}
		case "--version":
			showVersion = true
		case "--skip-self-update":
			skipSelfUpdate = true
		case "--help", "-h":
			printUsage()
			os.Exit(0)
		default:
			fmt.Fprintf(os.Stderr, "Unknown flag: %s\n\n", args[i])
			printUsage()
			os.Exit(1)
		}
	}

	if showVersion {
		fmt.Printf("bifract %s (commit: %s, built: %s)\n", setup.Version, setup.Commit, setup.BuildDate)
		os.Exit(0)
	}

	// Count mutually exclusive modes
	modeCount := 0
	if installMode {
		modeCount++
	}
	if installK8sMode {
		modeCount++
	}
	if upgradeK8sMode {
		modeCount++
	}
	if upgradeMode {
		modeCount++
	}
	if backupMode {
		modeCount++
	}
	if restoreMode {
		modeCount++
	}
	if listBackupsMode {
		modeCount++
	}
	if reconfigureMode {
		modeCount++
	}
	if reconfigureK8sMode {
		modeCount++
	}
	if genClientCertMode {
		modeCount++
	}
	if startMode {
		modeCount++
	}
	if stopMode {
		modeCount++
	}
	if statusMode {
		modeCount++
	}

	if modeCount == 0 {
		printUsage()
		os.Exit(1)
	}
	if modeCount > 1 {
		fmt.Fprintln(os.Stderr, "Error: only one mode flag can be used at a time")
		os.Exit(1)
	}

	if restoreMode && restoreFile == "" {
		fmt.Fprintln(os.Stderr, "Error: --restore requires --restore-file")
		os.Exit(1)
	}
	if genClientCertMode && (certName == "" || certPassword == "") {
		fmt.Fprintln(os.Stderr, "Error: --gen-client-cert requires --name and --password")
		os.Exit(1)
	}

	// Self-update: check for newer bifract before upgrading
	if upgradeMode && !skipSelfUpdate {
		setup.SelfUpdate(os.Args)
	}

	// K8s install/upgrade and client cert generation do not require Docker
	if installK8sMode {
		if err := setup.RunInstallK8s(); err != nil {
			fmt.Fprintf(os.Stderr, "\n%s %v\n", setup.ErrorStyle.Render("Error:"), err)
			os.Exit(1)
		}
		return
	}
	if upgradeK8sMode {
		if err := setup.RunUpgradeK8s(dir, setup.K8sUpgradeOpts{SizeProfile: sizeProfile}); err != nil {
			fmt.Fprintf(os.Stderr, "\n%s %v\n", setup.ErrorStyle.Render("Error:"), err)
			os.Exit(1)
		}
		return
	}
	if reconfigureK8sMode {
		opts := setup.K8sReconfigureOpts{
			Domain:      domain,
			IPAccess:    ipAccess,
			AllowedIPs:  allowedIPs,
			SizeProfile: sizeProfile,
		}
		if err := setup.RunReconfigureK8s(dir, opts); err != nil {
			fmt.Fprintf(os.Stderr, "\n%s %v\n", setup.ErrorStyle.Render("Error:"), err)
			os.Exit(1)
		}
		return
	}
	if genClientCertMode {
		if err := setup.RunGenClientCert(dir, certName, certPassword); err != nil {
			fmt.Fprintf(os.Stderr, "\n%s %v\n", setup.ErrorStyle.Render("Error:"), err)
			os.Exit(1)
		}
		return
	}

	// Preflight: check Docker is installed and running
	if _, err := exec.LookPath("docker"); err != nil {
		fmt.Fprintf(os.Stderr, "%s Docker is not installed or not in PATH.\n  Install it from https://docs.docker.com/get-docker/\n", setup.ErrorStyle.Render("Error:"))
		os.Exit(1)
	}
	if out, err := exec.Command("docker", "compose", "version").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "%s Docker Compose is not available.\n  %s\n  Install it from https://docs.docker.com/compose/install/\n", setup.ErrorStyle.Render("Error:"), string(out))
		os.Exit(1)
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s Docker daemon is not running.\n  Start Docker and try again.\n", setup.ErrorStyle.Render("Error:"))
		os.Exit(1)
	}

	var err error
	switch {
	case installMode:
		err = setup.RunInstall()
	case upgradeMode:
		err = setup.RunUpgrade(dir)
	case backupMode:
		err = setup.RunBackup(dir, nonInteractive)
	case restoreMode:
		err = setup.RunRestore(dir, restoreFile, nonInteractive)
	case listBackupsMode:
		err = setup.RunListBackups(dir)
	case reconfigureMode:
		err = setup.RunReconfigure(dir)
	case genClientCertMode:
		err = setup.RunGenClientCert(dir, certName, certPassword)
	case startMode:
		err = setup.RunStart(dir)
	case stopMode:
		err = setup.RunStop(dir)
	case statusMode:
		err = setup.RunStatus(dir)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "\n%s %v\n", setup.ErrorStyle.Render("Error:"), err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: bifract <mode> [options]")
	fmt.Println()
	fmt.Println("Modes:")
	fmt.Println("  --install          Run fresh installation wizard (Docker Compose)")
	fmt.Println("  --install-k8s      Generate Kubernetes manifests with secure defaults")
	fmt.Println("  --upgrade          Upgrade an existing installation (Docker Compose)")
	fmt.Println("  --upgrade-k8s      Upgrade existing K8s manifests (preserves secrets/settings)")
	fmt.Println("  --reconfigure      Regenerate config files from .env (no version change)")
	fmt.Println("  --reconfigure-k8s  Re-render K8s manifests (preserves secrets, allows setting changes)")
	fmt.Println("  --start            Start Bifract")
	fmt.Println("  --stop             Stop Bifract")
	fmt.Println("  --status           Show deployment status and health")
	fmt.Println("  --backup           Back up PostgreSQL database (encrypted)")
	fmt.Println("  --restore          Restore PostgreSQL from backup")
	fmt.Println("  --list-backups     List available backups")
	fmt.Println("  --gen-client-cert  Generate a client certificate for mTLS")
	fmt.Println("  --ingest           Bulk log ingestion (see --ingest --help)")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  --dir PATH         Installation directory (default: /opt/bifract)")
	fmt.Println("  --restore-file F   Backup file to restore from (required with --restore)")
	fmt.Println("  --name NAME        Client certificate common name (required with --gen-client-cert)")
	fmt.Println("  --password PASS    Password for .p12 bundle (required with --gen-client-cert)")
	fmt.Println("  --domain DOMAIN    Override domain (with --reconfigure-k8s)")
	fmt.Println("  --size PROFILE     Override size profile: dev, x-small, small, medium, large, x-large")
	fmt.Println("  --ip-access MODE   Override IP access mode: all, restrict-app, restrict-all, mtls-app")
	fmt.Println("  --allowed-ips IPs  Override allowed IPs (comma-separated CIDRs)")
	fmt.Println("  --non-interactive  Skip confirmation prompts (for cron/scripts)")
	fmt.Println("  --version          Show version and exit")
	fmt.Println("  --help             Show this help")
}
