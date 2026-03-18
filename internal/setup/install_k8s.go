package setup

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ResourceProfile defines CPU and memory requests/limits for a component.
type ResourceProfile struct {
	CPURequest string
	CPULimit   string
	MemRequest string
	MemLimit   string
}

// SizeProfile defines resource allocations for all components at a given scale.
type SizeProfile struct {
	Name           string
	Description    string
	CHShards       int
	CHReplicas     int
	ClickHouse     ResourceProfile
	CHKeeper       ResourceProfile
	Bifract        ResourceProfile
	Postgres       ResourceProfile
	Caddy          ResourceProfile
	CaddyShipper   ResourceProfile
	LiteLLM        ResourceProfile
}

var sizeProfiles = []SizeProfile{
	{
		Name:        "X-Small",
		Description: "Development/staging, ~10-50 GB/day (3 nodes, 4 vCPU / 8GB each)",
		CHShards:    1,
		CHReplicas:  2,
		ClickHouse:  ResourceProfile{"2", "3", "4Gi", "5Gi"},
		CHKeeper:    ResourceProfile{"250m", "500m", "256Mi", "512Mi"},
		Bifract:     ResourceProfile{"500m", "1", "512Mi", "1Gi"},
		Postgres:    ResourceProfile{"500m", "1", "512Mi", "1Gi"},
		Caddy:       ResourceProfile{"100m", "500m", "128Mi", "256Mi"},
		CaddyShipper: ResourceProfile{"10m", "100m", "32Mi", "64Mi"},
		LiteLLM:     ResourceProfile{"100m", "500m", "512Mi", "1Gi"},
	},
	{
		Name:        "Small",
		Description: "Light production, ~50-200 GB/day (3 nodes, 8 vCPU / 16GB each)",
		CHShards:    1,
		CHReplicas:  2,
		ClickHouse:  ResourceProfile{"4", "6", "8Gi", "12Gi"},
		CHKeeper:    ResourceProfile{"250m", "1", "512Mi", "1Gi"},
		Bifract:     ResourceProfile{"1", "2", "1Gi", "2Gi"},
		Postgres:    ResourceProfile{"500m", "2", "1Gi", "2Gi"},
		Caddy:       ResourceProfile{"200m", "1", "256Mi", "512Mi"},
		CaddyShipper: ResourceProfile{"10m", "100m", "32Mi", "64Mi"},
		LiteLLM:     ResourceProfile{"100m", "500m", "512Mi", "1Gi"},
	},
	{
		Name:        "Medium",
		Description: "Production workloads, ~200-500 GB/day (3 nodes, 16 vCPU / 32GB each)",
		CHShards:    2,
		CHReplicas:  2,
		ClickHouse:  ResourceProfile{"3", "8", "6Gi", "16Gi"},
		CHKeeper:    ResourceProfile{"500m", "2", "1Gi", "2Gi"},
		Bifract:     ResourceProfile{"1", "4", "1Gi", "4Gi"},
		Postgres:    ResourceProfile{"500m", "2", "1Gi", "4Gi"},
		Caddy:       ResourceProfile{"250m", "1", "256Mi", "1Gi"},
		CaddyShipper: ResourceProfile{"10m", "100m", "32Mi", "64Mi"},
		LiteLLM:     ResourceProfile{"100m", "500m", "512Mi", "1Gi"},
	},
	{
		Name:        "Large",
		Description: "High-volume production, ~500 GB-2 TB/day (3 nodes, 32 vCPU / 64GB each)",
		CHShards:    3,
		CHReplicas:  2,
		ClickHouse:  ResourceProfile{"8", "16", "16Gi", "32Gi"},
		CHKeeper:    ResourceProfile{"500m", "2", "1Gi", "2Gi"},
		Bifract:     ResourceProfile{"2", "4", "2Gi", "8Gi"},
		Postgres:    ResourceProfile{"1", "4", "2Gi", "8Gi"},
		Caddy:       ResourceProfile{"500m", "2", "512Mi", "1Gi"},
		CaddyShipper: ResourceProfile{"10m", "100m", "32Mi", "64Mi"},
		LiteLLM:     ResourceProfile{"250m", "1", "512Mi", "1Gi"},
	},
	{
		Name:        "X-Large",
		Description: "Very high-volume production, ~2-10 TB/day (6 nodes, 32 vCPU / 64GB each)",
		CHShards:    6,
		CHReplicas:  2,
		ClickHouse:  ResourceProfile{"8", "16", "16Gi", "32Gi"},
		CHKeeper:    ResourceProfile{"1", "2", "2Gi", "4Gi"},
		Bifract:     ResourceProfile{"4", "8", "4Gi", "16Gi"},
		Postgres:    ResourceProfile{"2", "4", "4Gi", "16Gi"},
		Caddy:       ResourceProfile{"1", "4", "1Gi", "2Gi"},
		CaddyShipper: ResourceProfile{"10m", "200m", "32Mi", "128Mi"},
		LiteLLM:     ResourceProfile{"500m", "2", "1Gi", "2Gi"},
	},
}

// K8sConfig extends SetupConfig with Kubernetes-specific settings.
type K8sConfig struct {
	SetupConfig
	SizeProfile  SizeProfile
	CHShards     int
	CHReplicas   int
	CHStorageGB  int
	StorageClass string
	OutputDir    string
	MTLSEnabled  bool
	MTLSCACert   string // PEM-encoded CA cert for client verification
	MTLSCAKey    string // PEM-encoded CA key for signing client certs

	// UserSecrets holds optional user-configured secrets that are not
	// managed by the setup wizard (e.g. LITELLM_API_KEY, OIDC settings).
	// Preserved during upgrades, empty on fresh installs.
	UserSecrets map[string]string

	// ImagePullSecrets preserves manually-added pull secret names across upgrades.
	ImagePullSecrets []string
}

// K8s wizard steps
type k8sStep int

const (
	k8sStepWelcome k8sStep = iota
	k8sStepDomain
	k8sStepSSL
	k8sStepIPAccess
	k8sStepAllowedIPs
	k8sStepSizeProfile
	k8sStepCHShards
	k8sStepCHReplicas
	k8sStepCHStorage
	k8sStepOutputDir
	k8sStepConfirm
	k8sStepDone
)

type k8sWizardModel struct {
	step   k8sStep
	config *K8sConfig
	err    error

	domainInput     textinput.Model
	allowedIPsInput textinput.Model
	shardsInput     textinput.Model
	replicasInput   textinput.Model
	storageInput    textinput.Model
	outputDirInput  textinput.Model

	sslChoices      []string
	sslCursor       int
	ipChoices       []string
	ipCursor        int
	ipValidationErr string
	sizeCursor      int

	width  int
	height int
}

func newK8sWizardModel() k8sWizardModel {
	domain := textinput.New()
	domain.Placeholder = "bifract.example.com"
	domain.Focus()
	domain.Width = 40
	domain.PromptStyle = PromptStyle
	domain.TextStyle = lipgloss.NewStyle().Foreground(White)

	allowedIPs := textinput.New()
	allowedIPs.Placeholder = "10.0.0.0/8, 192.168.1.0/24"
	allowedIPs.Width = 50
	allowedIPs.PromptStyle = PromptStyle
	allowedIPs.TextStyle = lipgloss.NewStyle().Foreground(White)

	shards := textinput.New()
	shards.Placeholder = "1"
	shards.SetValue("1")
	shards.Width = 10
	shards.PromptStyle = PromptStyle
	shards.TextStyle = lipgloss.NewStyle().Foreground(White)

	replicas := textinput.New()
	replicas.Placeholder = "2"
	replicas.SetValue("2")
	replicas.Width = 10
	replicas.PromptStyle = PromptStyle
	replicas.TextStyle = lipgloss.NewStyle().Foreground(White)

	storage := textinput.New()
	storage.Placeholder = "100"
	storage.SetValue("100")
	storage.Width = 10
	storage.PromptStyle = PromptStyle
	storage.TextStyle = lipgloss.NewStyle().Foreground(White)

	outputDir := textinput.New()
	outputDir.Placeholder = "./bifract-k8s"
	outputDir.SetValue("./bifract-k8s")
	outputDir.Width = 40
	outputDir.PromptStyle = PromptStyle
	outputDir.TextStyle = lipgloss.NewStyle().Foreground(White)

	return k8sWizardModel{
		step: k8sStepWelcome,
		config: &K8sConfig{
			SetupConfig: SetupConfig{ImageTag: Version},
			SizeProfile: sizeProfiles[0],
			CHShards:    1,
			CHReplicas:  2,
			CHStorageGB: 100,
			OutputDir:   "./bifract-k8s",
		},
		domainInput:     domain,
		allowedIPsInput: allowedIPs,
		shardsInput:     shards,
		replicasInput:   replicas,
		storageInput:    storage,
		outputDirInput:  outputDir,
		sslChoices:      []string{"Let's Encrypt (automatic)", "Custom certificate"},
		sslCursor:       0,
		ipChoices:       []string{"Allow all traffic", "Restrict UI only (allow ingest)", "Restrict all traffic", "mTLS (mutual TLS for UI)"},
		ipCursor:        0,
	}
}

func (m k8sWizardModel) Init() tea.Cmd {
	return textinput.Blink
}

func (m k8sWizardModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.err = fmt.Errorf("cancelled")
			return m, tea.Quit
		case "enter":
			return m.handleEnter()
		case "up", "k":
			m.handleUp()
		case "down", "j":
			m.handleDown()
		}
	}

	var cmd tea.Cmd
	switch m.step {
	case k8sStepDomain:
		m.domainInput, cmd = m.domainInput.Update(msg)
	case k8sStepAllowedIPs:
		m.allowedIPsInput, cmd = m.allowedIPsInput.Update(msg)
	case k8sStepCHShards:
		m.shardsInput, cmd = m.shardsInput.Update(msg)
	case k8sStepCHReplicas:
		m.replicasInput, cmd = m.replicasInput.Update(msg)
	case k8sStepCHStorage:
		m.storageInput, cmd = m.storageInput.Update(msg)
	case k8sStepOutputDir:
		m.outputDirInput, cmd = m.outputDirInput.Update(msg)
	}
	return m, cmd
}

func (m *k8sWizardModel) handleUp() {
	switch m.step {
	case k8sStepSSL:
		if m.sslCursor > 0 {
			m.sslCursor--
		}
	case k8sStepIPAccess:
		if m.ipCursor > 0 {
			m.ipCursor--
		}
	case k8sStepSizeProfile:
		if m.sizeCursor > 0 {
			m.sizeCursor--
		}
	}
}

func (m *k8sWizardModel) handleDown() {
	switch m.step {
	case k8sStepSSL:
		if m.sslCursor < len(m.sslChoices)-1 {
			m.sslCursor++
		}
	case k8sStepIPAccess:
		if m.ipCursor < len(m.ipChoices)-1 {
			m.ipCursor++
		}
	case k8sStepSizeProfile:
		if m.sizeCursor < len(sizeProfiles)-1 {
			m.sizeCursor++
		}
	}
}

func (m k8sWizardModel) handleEnter() (tea.Model, tea.Cmd) {
	switch m.step {
	case k8sStepWelcome:
		m.step = k8sStepDomain
		m.domainInput.Focus()
		return m, textinput.Blink

	case k8sStepDomain:
		domain := strings.TrimSpace(m.domainInput.Value())
		if domain == "" {
			return m, nil
		}
		m.config.Domain = domain
		m.step = k8sStepSSL
		return m, nil

	case k8sStepSSL:
		switch m.sslCursor {
		case 0:
			m.config.SSLMode = SSLLetsEncrypt
		case 1:
			m.config.SSLMode = SSLCustom
		}
		m.step = k8sStepIPAccess
		return m, nil

	case k8sStepIPAccess:
		switch m.ipCursor {
		case 0:
			m.config.IPAccess = IPAccessAll
			m.step = k8sStepSizeProfile
			return m, nil
		case 1:
			m.config.IPAccess = IPAccessRestrictApp
		case 2:
			m.config.IPAccess = IPAccessRestrictAll
		case 3:
			m.config.IPAccess = IPAccessMTLSApp
			m.config.MTLSEnabled = true
			m.step = k8sStepSizeProfile
			return m, nil
		}
		m.step = k8sStepAllowedIPs
		m.allowedIPsInput.Focus()
		return m, textinput.Blink

	case k8sStepAllowedIPs:
		ips := strings.TrimSpace(m.allowedIPsInput.Value())
		if ips == "" {
			return m, nil
		}
		m.config.ParseAllowedIPs(ips)
		if err := m.config.ValidateAllowedIPs(); err != nil {
			m.ipValidationErr = err.Error()
			return m, nil
		}
		m.ipValidationErr = ""
		m.step = k8sStepSizeProfile
		return m, nil

	case k8sStepSizeProfile:
		profile := sizeProfiles[m.sizeCursor]
		m.config.SizeProfile = profile
		m.config.CHShards = profile.CHShards
		m.config.CHReplicas = profile.CHReplicas
		m.shardsInput.SetValue(fmt.Sprintf("%d", profile.CHShards))
		m.replicasInput.SetValue(fmt.Sprintf("%d", profile.CHReplicas))
		m.step = k8sStepCHShards
		m.shardsInput.Focus()
		return m, textinput.Blink

	case k8sStepCHShards:
		val := strings.TrimSpace(m.shardsInput.Value())
		if val == "" {
			val = "1"
		}
		n := 1
		fmt.Sscanf(val, "%d", &n)
		if n < 1 {
			n = 1
		}
		m.config.CHShards = n
		m.step = k8sStepCHReplicas
		m.replicasInput.Focus()
		return m, textinput.Blink

	case k8sStepCHReplicas:
		val := strings.TrimSpace(m.replicasInput.Value())
		if val == "" {
			val = "2"
		}
		n := 2
		fmt.Sscanf(val, "%d", &n)
		if n < 1 {
			n = 1
		}
		m.config.CHReplicas = n
		m.step = k8sStepCHStorage
		m.storageInput.Focus()
		return m, textinput.Blink

	case k8sStepCHStorage:
		val := strings.TrimSpace(m.storageInput.Value())
		if val == "" {
			val = "100"
		}
		n := 100
		fmt.Sscanf(val, "%d", &n)
		if n < 10 {
			n = 10
		}
		m.config.CHStorageGB = n
		m.step = k8sStepOutputDir
		m.outputDirInput.Focus()
		return m, textinput.Blink

	case k8sStepOutputDir:
		dir := strings.TrimSpace(m.outputDirInput.Value())
		if dir == "" {
			dir = "./bifract-k8s"
		}
		m.config.OutputDir = dir
		m.step = k8sStepConfirm
		return m, nil

	case k8sStepConfirm:
		m.step = k8sStepDone
		return m, tea.Quit
	}

	return m, nil
}

func (m k8sWizardModel) View() string {
	var b strings.Builder

	title := lipgloss.NewStyle().
		Bold(true).
		Foreground(Green).
		Render("  Bifract Kubernetes Setup")
	b.WriteString(title + "\n\n")

	switch m.step {
	case k8sStepWelcome:
		b.WriteString("  This wizard generates Kubernetes manifests with secure defaults.\n")
		b.WriteString("  You will need the official ClickHouse Operator and cert-manager installed.\n\n")
		b.WriteString(DimStyle.Render("  Press Enter to continue, q to quit"))

	case k8sStepDomain:
		b.WriteString(PromptStyle.Render("  Domain name: "))
		b.WriteString(m.domainInput.View())
		b.WriteString("\n\n")
		b.WriteString(DimStyle.Render("  e.g. bifract.example.com"))

	case k8sStepSSL:
		b.WriteString(PromptStyle.Render("  SSL mode:\n\n"))
		for i, choice := range m.sslChoices {
			cursor := "  "
			if i == m.sslCursor {
				cursor = "> "
				b.WriteString(lipgloss.NewStyle().Foreground(Green).Render(cursor+choice) + "\n")
			} else {
				b.WriteString(DimStyle.Render(cursor+choice) + "\n")
			}
		}

	case k8sStepIPAccess:
		b.WriteString(PromptStyle.Render("  IP access control:\n\n"))
		for i, choice := range m.ipChoices {
			cursor := "  "
			if i == m.ipCursor {
				cursor = "> "
				b.WriteString(lipgloss.NewStyle().Foreground(Green).Render(cursor+choice) + "\n")
			} else {
				b.WriteString(DimStyle.Render(cursor+choice) + "\n")
			}
		}

	case k8sStepAllowedIPs:
		b.WriteString(PromptStyle.Render("  Allowed IPs/CIDRs (comma-separated): "))
		b.WriteString(m.allowedIPsInput.View())
		if m.ipValidationErr != "" {
			b.WriteString("\n  " + ErrorStyle.Render(m.ipValidationErr))
		}

	case k8sStepSizeProfile:
		b.WriteString(PromptStyle.Render("  Resource profile:\n\n"))
		for i, profile := range sizeProfiles {
			cursor := "  "
			if i == m.sizeCursor {
				cursor = "> "
				b.WriteString(lipgloss.NewStyle().Foreground(Green).Render(cursor+profile.Name) + "\n")
				b.WriteString(lipgloss.NewStyle().Foreground(Green).Render("    "+profile.Description) + "\n")
			} else {
				b.WriteString(DimStyle.Render(cursor+profile.Name) + "\n")
				b.WriteString(DimStyle.Render("    "+profile.Description) + "\n")
			}
		}
		b.WriteString("\n")
		b.WriteString(DimStyle.Render("  Shard and replica counts can be adjusted in the next steps."))

	case k8sStepCHShards:
		b.WriteString(PromptStyle.Render("  ClickHouse shards: "))
		b.WriteString(m.shardsInput.View())
		b.WriteString("\n\n")
		b.WriteString(DimStyle.Render("  Shards distribute data horizontally. 1 is fine for most workloads."))

	case k8sStepCHReplicas:
		b.WriteString(PromptStyle.Render("  ClickHouse replicas per shard: "))
		b.WriteString(m.replicasInput.View())
		b.WriteString("\n\n")
		b.WriteString(DimStyle.Render("  Minimum 2 for HA"))

	case k8sStepCHStorage:
		b.WriteString(PromptStyle.Render("  ClickHouse storage per replica (GB): "))
		b.WriteString(m.storageInput.View())

	case k8sStepOutputDir:
		b.WriteString(PromptStyle.Render("  Output directory: "))
		b.WriteString(m.outputDirInput.View())

	case k8sStepConfirm:
		b.WriteString(PromptStyle.Render("  Configuration Summary\n\n"))
		b.WriteString(fmt.Sprintf("  Domain:             %s\n", ValueStyle.Render(m.config.Domain)))
		b.WriteString(fmt.Sprintf("  SSL:                %s\n", ValueStyle.Render(string(m.config.SSLMode))))
		b.WriteString(fmt.Sprintf("  IP Access:          %s\n", ValueStyle.Render(string(m.config.IPAccess))))
		b.WriteString(fmt.Sprintf("  Resource Profile:   %s\n", ValueStyle.Render(m.config.SizeProfile.Name)))
		b.WriteString(fmt.Sprintf("  CH Shards:          %s\n", ValueStyle.Render(fmt.Sprintf("%d", m.config.CHShards))))
		b.WriteString(fmt.Sprintf("  CH Replicas/Shard:  %s\n", ValueStyle.Render(fmt.Sprintf("%d", m.config.CHReplicas))))
		b.WriteString(fmt.Sprintf("  CH Storage:         %s\n", ValueStyle.Render(fmt.Sprintf("%dGi per replica", m.config.CHStorageGB))))
		b.WriteString(fmt.Sprintf("  Output:             %s\n", ValueStyle.Render(m.config.OutputDir)))
		b.WriteString("\n")
		b.WriteString(DimStyle.Render("  Press Enter to generate manifests"))
	}

	return b.String()
}

// RunInstallK8s runs the Kubernetes installation wizard and generates manifests.
func RunInstallK8s() error {
	PrintBanner()

	model := newK8sWizardModel()
	p := tea.NewProgram(model)
	finalModel, err := p.Run()
	if err != nil {
		return fmt.Errorf("wizard error: %w", err)
	}

	final := finalModel.(k8sWizardModel)
	if final.err != nil {
		return final.err
	}
	if final.step != k8sStepDone {
		return fmt.Errorf("wizard did not complete")
	}

	cfg := final.config

	// Generate secure credentials
	PrintBanner()
	fmt.Println(TitleStyle.Render("  Generating Kubernetes Manifests"))
	fmt.Println()

	printStep("Generating secure credentials...")
	if err := cfg.GeneratePasswords(); err != nil {
		return fmt.Errorf("generate passwords: %w", err)
	}
	if cfg.MTLSEnabled {
		caCert, caKey, err := GenerateClientCAPEM()
		if err != nil {
			return fmt.Errorf("generate mTLS CA: %w", err)
		}
		cfg.MTLSCACert = caCert
		cfg.MTLSCAKey = caKey
	}
	printDone("Credentials generated")

	// Create output directory
	printStep("Creating output directory...")
	dirs := []string{
		cfg.OutputDir,
		filepath.Join(cfg.OutputDir, "clickhouse"),
		filepath.Join(cfg.OutputDir, "postgres"),
		filepath.Join(cfg.OutputDir, "bifract"),
		filepath.Join(cfg.OutputDir, "caddy"),
		filepath.Join(cfg.OutputDir, "litellm"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			return fmt.Errorf("create directory %s: %w", d, err)
		}
	}
	printDone("Directories created")

	// Generate manifests
	printStep("Writing manifests...")
	if err := writeK8sManifests(cfg); err != nil {
		return fmt.Errorf("write manifests: %w", err)
	}
	if cfg.MTLSEnabled {
		caDir := filepath.Join(cfg.OutputDir, "client-ca")
		if err := os.MkdirAll(caDir, 0700); err != nil {
			return fmt.Errorf("create client-ca dir: %w", err)
		}
		if err := os.WriteFile(filepath.Join(caDir, "ca.pem"), []byte(cfg.MTLSCACert), 0644); err != nil {
			return fmt.Errorf("write CA cert: %w", err)
		}
		if err := os.WriteFile(filepath.Join(caDir, "ca-key.pem"), []byte(cfg.MTLSCAKey), 0600); err != nil {
			return fmt.Errorf("write CA key: %w", err)
		}
	}
	printDone("Manifests written to " + cfg.OutputDir)

	// Final summary
	fmt.Println()
	fmt.Println(TitleStyle.Render("  Kubernetes Manifests Ready"))
	fmt.Println()

	summaryText := fmt.Sprintf(
		"%s  %s\n%s  %s\n%s  %s\n\n%s  %s",
		PromptStyle.Render("Domain:   "), ValueStyle.Render(cfg.Domain),
		PromptStyle.Render("Username: "), ValueStyle.Render("admin"),
		PromptStyle.Render("Password: "), lipgloss.NewStyle().Foreground(White).Bold(true).Render(cfg.AdminPassword),
		PromptStyle.Render("Manifests:"), DimStyle.Render(cfg.OutputDir),
	)

	summary := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(Green).
		Padding(1, 3).
		Render(summaryText)
	fmt.Println(summary)
	fmt.Println()
	fmt.Println(WarningStyle.Render("  Save the admin password above. It will not be shown again."))
	if cfg.MTLSEnabled {
		fmt.Println()
		fmt.Println(WarningStyle.Render("  mTLS is enabled. CA files are in " + filepath.Join(cfg.OutputDir, "client-ca") + "/"))
		fmt.Println(DimStyle.Render("  Generate a client certificate with:"))
		fmt.Println(DimStyle.Render("    bifract --gen-client-cert --dir " + cfg.OutputDir + " --name \"user@example.com\" --password changeme"))
	}
	fmt.Println()
	fmt.Println(DimStyle.Render("  Deploy with:"))
	fmt.Println(DimStyle.Render("    1. Install cert-manager:"))
	fmt.Println(DimStyle.Render("       kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.17.2/cert-manager.yaml"))
	fmt.Println(DimStyle.Render("    2. Install the ClickHouse Operator:"))
	fmt.Println(DimStyle.Render("       helm install clickhouse-operator -n clickhouse-operator-system --create-namespace \\"))
	fmt.Println(DimStyle.Render("         oci://ghcr.io/clickhouse/clickhouse-operator-helm"))
	fmt.Println(DimStyle.Render("    3. Apply the manifests:"))
	fmt.Println(DimStyle.Render("       kubectl apply -k " + cfg.OutputDir))
	fmt.Println()

	return nil
}

// k8sTemplateData holds all values needed by the K8s manifest templates.
type k8sTemplateData struct {
	ImageTag            string
	Domain              string
	CHShards            int
	CHReplicas          int
	CHStorageGB         int
	CHPasswordHash      string
	CHHostsList         string
	PostgresPassword    string
	ClickHousePassword  string
	PasswordPepper      string
	AdminPasswordHash   string
	FeedEncryptionKey   string
	BackupEncryptionKey string
	LiteLLMMasterKey    string
	IPBlock             string
	IPBlockIngest       string
	MTLSEnabled         bool
	MTLSCACert          string

	// Resource profiles
	CH           ResourceProfile
	CHKeeper     ResourceProfile
	BifractRes   ResourceProfile
	PostgresRes  ResourceProfile
	CaddyRes     ResourceProfile
	CaddyShipper ResourceProfile
	LiteLLMRes   ResourceProfile

	// User-configured secrets (preserved during upgrades, empty on fresh install)
	UserSecrets map[string]string

	// ImagePullSecrets preserves manually-added pull secret names across upgrades.
	ImagePullSecrets []string
}

// k8sManifestFile maps an embedded template to its output path.
type k8sManifestFile struct {
	template string // path within TemplateFS
	output   string // path relative to output dir
}

var k8sManifests = []k8sManifestFile{
	{"templates/k8s/namespace.yaml.tmpl", "namespace.yaml"},
	{"templates/k8s/kustomization.yaml.tmpl", "kustomization.yaml"},
	{"templates/k8s/clickhouse-installation.yaml.tmpl", "clickhouse/clickhouse-installation.yaml"},
	{"templates/k8s/postgres-statefulset.yaml.tmpl", "postgres/statefulset.yaml"},
	{"templates/k8s/bifract-deployment.yaml.tmpl", "bifract/deployment.yaml"},
	{"templates/k8s/bifract-configmap.yaml.tmpl", "bifract/configmap.yaml"},
	{"templates/k8s/bifract-secrets.yaml.tmpl", "bifract/secrets.yaml"},
	{"templates/k8s/caddy-deployment.yaml.tmpl", "caddy/deployment.yaml"},
	{"templates/k8s/caddy-configmap.yaml.tmpl", "caddy/configmap.yaml"},
	{"templates/k8s/caddy-log-shipper.yaml.tmpl", "caddy/log-shipper.yaml"},
	{"templates/k8s/litellm-deployment.yaml.tmpl", "litellm/deployment.yaml"},
	{"templates/k8s/litellm-configmap.yaml.tmpl", "litellm/configmap.yaml"},
	{"templates/k8s/network-policies.yaml.tmpl", "network-policies.yaml"},
}

func writeK8sManifests(cfg *K8sConfig) error {
	if cfg.UserSecrets == nil {
		cfg.UserSecrets = make(map[string]string)
	}
	data := k8sTemplateData{
		ImageTag:            cfg.ImageTag,
		ImagePullSecrets:    cfg.ImagePullSecrets,
		Domain:              cfg.Domain,
		CHShards:            cfg.CHShards,
		CHReplicas:          cfg.CHReplicas,
		CHStorageGB:         cfg.CHStorageGB,
		CHPasswordHash:      fmt.Sprintf("%x", sha256.Sum256([]byte(cfg.ClickHousePassword))),
		CHHostsList:         buildCHHostsList(cfg.CHShards, cfg.CHReplicas),
		PostgresPassword:    cfg.PostgresPassword,
		ClickHousePassword:  cfg.ClickHousePassword,
		PasswordPepper:      cfg.PasswordPepper,
		AdminPasswordHash:   cfg.AdminPasswordHash,
		FeedEncryptionKey:   cfg.FeedEncryptionKey,
		BackupEncryptionKey: cfg.BackupEncryptionKey,
		LiteLLMMasterKey:    cfg.LiteLLMMasterKey,
		UserSecrets:         cfg.UserSecrets,
		IPBlock:             buildIPBlock(cfg),
		IPBlockIngest:       buildIPBlockIngest(cfg),
		MTLSEnabled:         cfg.MTLSEnabled,
		MTLSCACert:          indentPEM(cfg.MTLSCACert, "    "),
		CH:                  cfg.SizeProfile.ClickHouse,
		CHKeeper:            cfg.SizeProfile.CHKeeper,
		BifractRes:          cfg.SizeProfile.Bifract,
		PostgresRes:         cfg.SizeProfile.Postgres,
		CaddyRes:            cfg.SizeProfile.Caddy,
		CaddyShipper:        cfg.SizeProfile.CaddyShipper,
		LiteLLMRes:          cfg.SizeProfile.LiteLLM,
	}

	for _, m := range k8sManifests {
		content, err := renderK8sTemplate(m.template, data)
		if err != nil {
			return fmt.Errorf("render %s: %w", m.template, err)
		}
		outPath := filepath.Join(cfg.OutputDir, m.output)
		if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
			return fmt.Errorf("create dir for %s: %w", m.output, err)
		}
		if err := os.WriteFile(outPath, []byte(content), 0600); err != nil {
			return fmt.Errorf("write %s: %w", m.output, err)
		}
	}

	// Conditionally write mTLS CA secret.
	if cfg.MTLSEnabled {
		content, err := renderK8sTemplate("templates/k8s/caddy-mtls.yaml.tmpl", data)
		if err != nil {
			return fmt.Errorf("render mTLS template: %w", err)
		}
		outPath := filepath.Join(cfg.OutputDir, "caddy/mtls-ca.yaml")
		if err := os.WriteFile(outPath, []byte(content), 0600); err != nil {
			return fmt.Errorf("write mTLS CA secret: %w", err)
		}
	}

	return nil
}

func renderK8sTemplate(name string, data k8sTemplateData) (string, error) {
	content, err := TemplateFS.ReadFile(name)
	if err != nil {
		return "", fmt.Errorf("read template %s: %w", name, err)
	}
	tmpl, err := template.New(filepath.Base(name)).Parse(string(content))
	if err != nil {
		return "", fmt.Errorf("parse template %s: %w", name, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("execute template %s: %w", name, err)
	}
	return buf.String(), nil
}

// buildCHHostsList generates the comma-separated ClickHouse host list for the
// Bifract deployment env var based on the official operator's naming convention.
// Pods are named: bifract-ch-clickhouse-{shard}-{replica}-0
// The list includes all replicas across all shards.
func buildCHHostsList(shards, replicas int) string {
	hosts := make([]string, 0, shards*replicas)
	for s := 0; s < shards; s++ {
		for r := 0; r < replicas; r++ {
			hosts = append(hosts, fmt.Sprintf("bifract-ch-clickhouse-%d-%d-0.bifract-ch-clickhouse-headless", s, r))
		}
	}
	return strings.Join(hosts, ",")
}

// indentPEM prepends each line of a PEM string with the given prefix.
func indentPEM(pem, prefix string) string {
	lines := strings.Split(strings.TrimRight(pem, "\n"), "\n")
	for i, line := range lines {
		lines[i] = prefix + line
	}
	return strings.Join(lines, "\n")
}

// buildIPBlock generates the Caddy IP restriction block for the main site in the Caddyfile template.
func buildIPBlock(cfg *K8sConfig) string {
	if cfg.IPAccess != IPAccessRestrictApp && cfg.IPAccess != IPAccessRestrictAll {
		return ""
	}
	if len(cfg.AllowedIPs) == 0 {
		return ""
	}
	ipList := strings.Join(cfg.AllowedIPs, " ")
	return fmt.Sprintf("      @blocked not remote_ip %s\n      respond @blocked 403\n", ipList)
}

// buildIPBlockIngest generates the Caddy IP restriction block for the ingest port (8443).
// Only restrict-all mode restricts ingest; restrict-app leaves ingest open to all IPs.
func buildIPBlockIngest(cfg *K8sConfig) string {
	if cfg.IPAccess != IPAccessRestrictAll {
		return ""
	}
	if len(cfg.AllowedIPs) == 0 {
		return ""
	}
	ipList := strings.Join(cfg.AllowedIPs, " ")
	return fmt.Sprintf("      @blocked_ingest not remote_ip %s\n      respond @blocked_ingest 403\n", ipList)
}
