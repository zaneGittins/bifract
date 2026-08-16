package setup

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type WizardStep int

const (
	StepWelcome WizardStep = iota
	StepPrereqs
	StepInstallDir
	StepClickHouse
	StepClickHouseHost
	StepClickHouseAuth
	StepDomain
	StepSSL
	StepSSLEmail
	StepSSLCert
	StepIPAccess
	StepAllowedIPs
	StepConfirm
	StepDone
)

// Steps that show in the progress bar (excludes conditional sub-steps)
var stepLabels = []struct {
	step  WizardStep
	label string
}{
	{StepWelcome, "Welcome"},
	{StepPrereqs, "Prerequisites"},
	{StepInstallDir, "Directory"},
	{StepClickHouse, "ClickHouse"},
	{StepDomain, "Domain"},
	{StepSSL, "SSL"},
	{StepIPAccess, "IP Access"},
	{StepConfirm, "Confirm"},
}

type WizardModel struct {
	step   WizardStep
	config *SetupConfig
	prereq PrereqResult
	err    error

	// Input fields
	installDirInput textinput.Model
	domainInput     textinput.Model
	emailInput      textinput.Model
	certPathInput   textinput.Model
	keyPathInput    textinput.Model

	// ClickHouse backend selection
	chChoices       []string
	chCursor        int
	chHostInput     textinput.Model
	chUserInput     textinput.Model
	chPassInput     textinput.Model
	chValidationErr string

	// SSL selection
	sslChoices []string
	sslCursor  int

	// IP access selection
	ipAccessChoices []string
	ipAccessCursor  int
	allowedIPsInput textinput.Model
	ipValidationErr string

	// Spinner for async ops
	spinner spinner.Model

	width  int
	height int
}

func NewWizardModel(cfg *SetupConfig) WizardModel {
	installDir := textinput.New()
	installDir.Placeholder = "/opt/bifract"
	installDir.SetValue(cfg.InstallDir)
	installDir.Focus()
	installDir.Width = 40
	installDir.PromptStyle = PromptStyle
	installDir.TextStyle = lipgloss.NewStyle().Foreground(White)
	installDir.PlaceholderStyle = lipgloss.NewStyle().Foreground(Gray)

	domain := textinput.New()
	domain.Placeholder = "localhost"
	domain.SetValue(cfg.Domain)
	domain.Width = 40
	domain.PromptStyle = PromptStyle
	domain.TextStyle = lipgloss.NewStyle().Foreground(White)
	domain.PlaceholderStyle = lipgloss.NewStyle().Foreground(Gray)

	email := textinput.New()
	email.Placeholder = "admin@example.com"
	email.Width = 40
	email.PromptStyle = PromptStyle
	email.TextStyle = lipgloss.NewStyle().Foreground(White)
	email.PlaceholderStyle = lipgloss.NewStyle().Foreground(Gray)

	certPath := textinput.New()
	certPath.Placeholder = "/path/to/cert.pem"
	certPath.Width = 50
	certPath.PromptStyle = PromptStyle
	certPath.TextStyle = lipgloss.NewStyle().Foreground(White)
	certPath.PlaceholderStyle = lipgloss.NewStyle().Foreground(Gray)

	keyPath := textinput.New()
	keyPath.Placeholder = "/path/to/key.pem"
	keyPath.Width = 50
	keyPath.PromptStyle = PromptStyle
	keyPath.TextStyle = lipgloss.NewStyle().Foreground(White)
	keyPath.PlaceholderStyle = lipgloss.NewStyle().Foreground(Gray)

	allowedIPs := textinput.New()
	allowedIPs.Placeholder = "10.0.0.0/8, 192.168.1.0/24, 203.0.113.5"
	allowedIPs.Width = 60
	allowedIPs.PromptStyle = PromptStyle
	allowedIPs.TextStyle = lipgloss.NewStyle().Foreground(White)
	allowedIPs.PlaceholderStyle = lipgloss.NewStyle().Foreground(Gray)

	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(Purple)

	chUser := textinput.New()
	chUser.Placeholder = "default"
	chUser.Width = 32
	chUser.PromptStyle = PromptStyle
	chUser.TextStyle = lipgloss.NewStyle().Foreground(White)

	chPass := textinput.New()
	chPass.EchoMode = textinput.EchoPassword
	chPass.Width = 48
	chPass.PromptStyle = PromptStyle
	chPass.TextStyle = lipgloss.NewStyle().Foreground(White)

	chHost := textinput.New()
	chHost.Placeholder = "clickhouse.internal:9000"
	chHost.Width = 48
	chHost.PromptStyle = PromptStyle
	chHost.TextStyle = lipgloss.NewStyle().Foreground(White)

	return WizardModel{
		step:            StepWelcome,
		config:          cfg,
		installDirInput: installDir,
		domainInput:     domain,
		emailInput:      email,
		certPathInput:   certPath,
		keyPathInput:    keyPath,
		chChoices:       []string{"Bundled (recommended)", "External ClickHouse", "ClickHouse Cloud"},
		chCursor:        0,
		chHostInput:     chHost,
		chUserInput:     chUser,
		chPassInput:     chPass,
		sslChoices:      []string{"Self-signed (default)", "Let's Encrypt (automatic)", "Custom certificates"},
		sslCursor:       0,
		ipAccessChoices: []string{"Restrict app, allow ingest from all", "Restrict all", "mTLS client certificates (app only)", "Allow all (not recommended)"},
		ipAccessCursor:  0,
		allowedIPsInput: allowedIPs,
		spinner:         s,
	}
}

func (m WizardModel) Init() tea.Cmd {
	return textinput.Blink
}

func (m WizardModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		case "q":
			if m.step == StepWelcome || m.step == StepConfirm {
				return m, tea.Quit
			}
		case "esc":
			if m.step != StepWelcome && m.step != StepDone {
				m.step = m.prevStep()
				return m, nil
			}
		}
	}

	switch m.step {
	case StepWelcome:
		return m.updateWelcome(msg)
	case StepPrereqs:
		return m.updatePrereqs(msg)
	case StepInstallDir:
		return m.updateInstallDir(msg)
	case StepClickHouse:
		return m.updateClickHouse(msg)
	case StepClickHouseHost:
		return m.updateClickHouseHost(msg)
	case StepClickHouseAuth:
		return m.updateClickHouseAuth(msg)
	case StepDomain:
		return m.updateDomain(msg)
	case StepSSL:
		return m.updateSSL(msg)
	case StepSSLEmail:
		return m.updateSSLEmail(msg)
	case StepSSLCert:
		return m.updateSSLCert(msg)
	case StepIPAccess:
		return m.updateIPAccess(msg)
	case StepAllowedIPs:
		return m.updateAllowedIPs(msg)
	case StepConfirm:
		return m.updateConfirm(msg)
	case StepDone:
		return m, tea.Quit
	}
	return m, nil
}

// prevStep returns the previous logical step for Esc navigation
func (m WizardModel) prevStep() WizardStep {
	switch m.step {
	case StepPrereqs:
		return StepWelcome
	case StepInstallDir:
		return StepPrereqs
	case StepClickHouse:
		return StepInstallDir
	case StepClickHouseHost:
		return StepClickHouse
	case StepClickHouseAuth:
		return StepClickHouseHost
	case StepDomain:
		if !m.config.CH.Bundled() {
			return StepClickHouseAuth
		}
		return StepClickHouse
	case StepSSL:
		return StepDomain
	case StepSSLEmail, StepSSLCert:
		return StepSSL
	case StepIPAccess:
		switch m.config.SSLMode {
		case SSLLetsEncrypt:
			return StepSSLEmail
		case SSLCustom:
			return StepSSLCert
		default:
			return StepSSL
		}
	case StepAllowedIPs:
		return StepIPAccess
	case StepConfirm:
		if m.config.IPAccess == IPAccessRestrictApp || m.config.IPAccess == IPAccessRestrictAll {
			return StepAllowedIPs
		}
		return StepIPAccess
	}
	return m.step
}

// renderProgress builds the step progress bar
func (m WizardModel) renderProgress() string {
	var parts []string
	for i, sl := range stepLabels {
		var style lipgloss.Style
		var marker string
		if m.step == sl.step || (sl.step == StepSSL && (m.step == StepSSLEmail || m.step == StepSSLCert)) || (sl.step == StepIPAccess && m.step == StepAllowedIPs) {
			marker = ">"
			style = StepActiveStyle
		} else if m.step > sl.step {
			marker = "*"
			style = StepDoneStyle
		} else {
			marker = "."
			style = StepPendingStyle
		}
		label := fmt.Sprintf(" %s %s", marker, sl.label)
		parts = append(parts, style.Render(label))

		if i < len(stepLabels)-1 {
			parts = append(parts, StepPendingStyle.Render(" --"))
		}
	}
	return strings.Join(parts, "")
}

func (m WizardModel) View() string {
	var s strings.Builder

	// Banner at top of every step
	s.WriteString(TitleStyle.Render(bannerArt))
	s.WriteString("\n")
	s.WriteString(SubtitleStyle.Render("Log Management, Detection, and Collaboration"))
	s.WriteString("  ")
	s.WriteString(DimStyle.Render(Version))
	s.WriteString("\n")

	// Progress bar (skip on welcome and done)
	if m.step != StepWelcome && m.step != StepDone {
		s.WriteString("\n")
		s.WriteString(m.renderProgress())
		s.WriteString("\n")
	}

	// Step content wrapped in panel
	var content string
	var hint string

	switch m.step {
	case StepWelcome:
		content = m.viewWelcome()
		hint = "Enter to continue  |  q to quit"

	case StepPrereqs:
		content = m.viewPrereqs()
		if m.prereq.OK() {
			hint = "Enter to continue  |  Esc to go back"
		} else {
			hint = "q to quit"
		}

	case StepInstallDir:
		content = m.viewInstallDir()
		hint = "Enter to confirm  |  Esc to go back"

	case StepClickHouse:
		content = m.viewClickHouse()
		hint = "Up/Down to select  |  Enter to confirm  |  Esc to go back"

	case StepClickHouseHost:
		content = m.viewClickHouseHost()
		hint = "Enter to confirm  |  Esc to go back"

	case StepClickHouseAuth:
		content = m.viewClickHouseAuth()
		hint = "Tab to switch fields  |  Enter to confirm  |  Esc to go back"

	case StepDomain:
		content = m.viewDomain()
		hint = "Enter to confirm  |  Esc to go back"

	case StepSSL:
		content = m.viewSSL()
		hint = "Up/Down to select  |  Enter to confirm  |  Esc to go back"

	case StepSSLEmail:
		content = m.viewSSLEmail()
		hint = "Enter to confirm  |  Esc to go back"

	case StepSSLCert:
		content = m.viewSSLCert()
		hint = "Enter to advance  |  Tab to switch fields  |  Esc to go back"

	case StepIPAccess:
		content = m.viewIPAccess()
		hint = "Up/Down to select  |  Enter to confirm  |  Esc to go back"

	case StepAllowedIPs:
		content = m.viewAllowedIPs()
		hint = "Enter to confirm  |  Esc to go back"

	case StepConfirm:
		content = m.viewConfirm()
		hint = "Enter to install  |  Esc to go back  |  q to quit"

	case StepDone:
		content = SuccessStyle.Render("Configuration complete.")
	}

	if m.step != StepDone {
		s.WriteString(PanelStyle.Render(content))
		s.WriteString("\n")
		s.WriteString(HintStyle.Render("  " + hint))
	} else {
		s.WriteString("\n")
		s.WriteString(content)
	}

	s.WriteString("\n")
	return s.String()
}

func (m WizardModel) viewWelcome() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("Welcome to Bifract Setup"))
	s.WriteString("\n\n")
	s.WriteString("This wizard will walk you through setting up Bifract.\n\n")
	s.WriteString(DimStyle.Render("  What it does:") + "\n")
	s.WriteString(fmt.Sprintf("  %s  Generate secure random passwords\n", SuccessStyle.Render("*")))
	s.WriteString(fmt.Sprintf("  %s  Configure SSL/TLS certificates\n", SuccessStyle.Render("*")))
	s.WriteString(fmt.Sprintf("  %s  Set up Docker Compose services\n", SuccessStyle.Render("*")))
	s.WriteString(fmt.Sprintf("  %s  Initialize databases\n", SuccessStyle.Render("*")))
	s.WriteString(fmt.Sprintf("  %s  Start Bifract\n", SuccessStyle.Render("*")))
	return s.String()
}

func (m WizardModel) viewPrereqs() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("Checking Prerequisites"))
	s.WriteString("\n\n")

	if m.prereq.DockerOK {
		s.WriteString(fmt.Sprintf("  %s  Docker    %s\n", SuccessStyle.Render("*"), DimStyle.Render(m.prereq.DockerVer)))
	} else {
		s.WriteString(fmt.Sprintf("  %s  Docker    %s\n", ErrorStyle.Render("x"), ErrorStyle.Render("not found")))
	}
	if m.prereq.ComposeOK {
		s.WriteString(fmt.Sprintf("  %s  Compose   %s\n", SuccessStyle.Render("*"), DimStyle.Render(m.prereq.ComposeVer)))
	} else {
		s.WriteString(fmt.Sprintf("  %s  Compose   %s\n", ErrorStyle.Render("x"), ErrorStyle.Render("not found")))
	}

	s.WriteString("\n")
	if m.prereq.OK() {
		s.WriteString(SuccessStyle.Render("All prerequisites met."))
	} else {
		s.WriteString(ErrorStyle.Render("Missing prerequisites. Please install them first."))
		for _, e := range m.prereq.Errors {
			s.WriteString("\n")
			s.WriteString(DimStyle.Render("  " + e))
		}
	}
	return s.String()
}

func (m WizardModel) viewInstallDir() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("Install Directory"))
	s.WriteString("\n\n")
	s.WriteString("Where should Bifract configuration and data live?\n\n")
	s.WriteString(LabelStyle.Render("  Path"))
	s.WriteString("\n")
	s.WriteString("  " + m.installDirInput.View())
	return s.String()
}

func (m WizardModel) viewDomain() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("Domain Configuration"))
	s.WriteString("\n\n")
	s.WriteString("Enter your domain name. Use ")
	s.WriteString(HighlightStyle.Render("localhost"))
	s.WriteString(" for local development.\n\n")
	s.WriteString(LabelStyle.Render("  Domain"))
	s.WriteString("\n")
	s.WriteString("  " + m.domainInput.View())
	return s.String()
}

func (m WizardModel) viewSSL() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("SSL/TLS Configuration"))
	s.WriteString("\n\n")
	s.WriteString("How should Bifract handle HTTPS?\n\n")

	descriptions := []string{
		"Generates a self-signed certificate during setup. Good for localhost or internal use.",
		"Caddy obtains a trusted certificate from Let's Encrypt. Requires a public domain.",
		"Provide your own certificate and key files.",
	}

	s.WriteString(RenderOptionList(m.sslChoices, descriptions, m.sslCursor))
	return s.String()
}

func (m WizardModel) viewSSLEmail() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("Let's Encrypt Configuration"))
	s.WriteString("\n\n")
	s.WriteString("Let's Encrypt will send certificate expiry notifications to this address.\n\n")
	s.WriteString(LabelStyle.Render("  Email"))
	s.WriteString("\n")
	s.WriteString("  " + m.emailInput.View())
	return s.String()
}

func (m WizardModel) viewSSLCert() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("Custom Certificate Paths"))
	s.WriteString("\n\n")
	s.WriteString("Provide absolute paths to your TLS certificate and private key.\n\n")

	certLabel := "  Certificate"
	keyLabel := "  Private Key"
	if m.certPathInput.Focused() {
		certLabel = SelectedStyle.Render(certLabel)
	} else {
		certLabel = LabelStyle.Render(certLabel)
	}
	if m.keyPathInput.Focused() {
		keyLabel = SelectedStyle.Render(keyLabel)
	} else {
		keyLabel = LabelStyle.Render(keyLabel)
	}

	s.WriteString(certLabel)
	s.WriteString("\n")
	s.WriteString("  " + m.certPathInput.View())
	s.WriteString("\n\n")
	s.WriteString(keyLabel)
	s.WriteString("\n")
	s.WriteString("  " + m.keyPathInput.View())
	return s.String()
}

func (m WizardModel) viewConfirm() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("Ready to Install"))
	s.WriteString("\n\n")

	row := func(label, value string) string {
		return fmt.Sprintf("  %s  %s\n", PromptStyle.Render(label), ValueStyle.Render(value))
	}

	s.WriteString(row("Directory:", m.config.InstallDir))
	s.WriteString(row("Domain:   ", m.config.Domain))
	s.WriteString(row("SSL:      ", string(m.config.SSLMode)))
	s.WriteString(row("IP Access:", string(m.config.IPAccess)))
	if m.config.IPAccess != IPAccessAll && len(m.config.AllowedIPs) > 0 {
		s.WriteString(row("Allowed:  ", strings.Join(m.config.AllowedIPs, ", ")))
	}
	s.WriteString(row("Image:    ", m.config.ImageTag))
	s.WriteString("\n")
	s.WriteString(DimStyle.Render("  Press Enter to begin installation, q to quit."))
	return s.String()
}

// --- Update handlers (unchanged logic) ---

func (m WizardModel) updateWelcome(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok && key.String() == "enter" {
		m.step = StepPrereqs
		m.prereq = CheckPrereqs()
		return m, nil
	}
	return m, nil
}

func (m WizardModel) updatePrereqs(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok && key.String() == "enter" {
		if m.prereq.OK() {
			m.step = StepInstallDir
			m.installDirInput.Focus()
			return m, textinput.Blink
		}
	}
	return m, nil
}

func (m WizardModel) updateInstallDir(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok && key.String() == "enter" {
		val := m.installDirInput.Value()
		if val == "" {
			val = "/opt/bifract"
		}
		m.config.InstallDir = val
		m.step = StepClickHouse
		return m, nil
	}
	var cmd tea.Cmd
	m.installDirInput, cmd = m.installDirInput.Update(msg)
	return m, cmd
}

func (m WizardModel) updateDomain(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok && key.String() == "enter" {
		val := m.domainInput.Value()
		if val == "" {
			val = "localhost"
		}
		m.config.Domain = val
		if val != "localhost" {
			m.config.SecureCookies = true
			m.config.CORSOrigins = fmt.Sprintf("https://%s", val)
		}
		m.step = StepSSL
		return m, nil
	}
	var cmd tea.Cmd
	m.domainInput, cmd = m.domainInput.Update(msg)
	return m, cmd
}

func (m WizardModel) updateSSL(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok {
		switch key.String() {
		case "up", "k":
			if m.sslCursor > 0 {
				m.sslCursor--
			}
		case "down", "j":
			if m.sslCursor < len(m.sslChoices)-1 {
				m.sslCursor++
			}
		case "enter":
			switch m.sslCursor {
			case 0:
				m.config.SSLMode = SSLSelfSigned
				m.step = StepIPAccess
				return m, nil
			case 1:
				m.config.SSLMode = SSLLetsEncrypt
				m.step = StepSSLEmail
				m.emailInput.Focus()
				return m, textinput.Blink
			case 2:
				m.config.SSLMode = SSLCustom
				m.step = StepSSLCert
				m.certPathInput.Focus()
				return m, textinput.Blink
			}
		}
	}
	return m, nil
}

func (m WizardModel) updateSSLEmail(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok && key.String() == "enter" {
		m.config.SSLEmail = m.emailInput.Value()
		m.step = StepIPAccess
		return m, nil
	}
	var cmd tea.Cmd
	m.emailInput, cmd = m.emailInput.Update(msg)
	return m, cmd
}

func (m WizardModel) updateSSLCert(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok {
		switch key.String() {
		case "tab":
			if m.certPathInput.Focused() {
				m.certPathInput.Blur()
				m.keyPathInput.Focus()
			} else {
				m.keyPathInput.Blur()
				m.certPathInput.Focus()
			}
			return m, textinput.Blink
		case "enter":
			if m.certPathInput.Focused() {
				m.certPathInput.Blur()
				m.keyPathInput.Focus()
				return m, textinput.Blink
			}
			if m.keyPathInput.Focused() {
				m.config.CertPath = m.certPathInput.Value()
				m.config.KeyPath = m.keyPathInput.Value()
				m.step = StepIPAccess
				return m, nil
			}
		}
	}
	var cmd tea.Cmd
	if m.certPathInput.Focused() {
		m.certPathInput, cmd = m.certPathInput.Update(msg)
	} else {
		m.keyPathInput, cmd = m.keyPathInput.Update(msg)
	}
	return m, cmd
}

func (m WizardModel) viewIPAccess() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("IP Access Control"))
	s.WriteString("\n\n")
	s.WriteString("Restrict IP access to Bifract.\n")
	s.WriteString(DimStyle.Render("Non-allowed IPs are rejected by Caddy before reaching the application."))
	s.WriteString("\n\n")

	descriptions := []string{
		"Only allowed IPs can access the UI and API. Ingest endpoints remain open to all.",
		"Only allowed IPs can access anything, including ingest endpoints.",
		"Require client certificates for UI and API. Ingest endpoints remain open.",
		"No restrictions. All traffic is allowed through.",
	}

	s.WriteString(RenderOptionList(m.ipAccessChoices, descriptions, m.ipAccessCursor))
	return s.String()
}

func (m WizardModel) viewAllowedIPs() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("Allowed IP Addresses"))
	s.WriteString("\n\n")
	s.WriteString("Enter the IPs or CIDR ranges that should be allowed, separated by commas.\n")
	s.WriteString(DimStyle.Render("Example: 10.0.0.0/8, 192.168.1.0/24, 203.0.113.5"))
	s.WriteString("\n\n")
	s.WriteString(LabelStyle.Render("  Allowed IPs"))
	s.WriteString("\n")
	s.WriteString("  " + m.allowedIPsInput.View())
	if m.ipValidationErr != "" {
		s.WriteString("\n\n")
		s.WriteString(ErrorStyle.Render("  " + m.ipValidationErr))
	}
	return s.String()
}

func (m WizardModel) updateIPAccess(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok {
		switch key.String() {
		case "up", "k":
			if m.ipAccessCursor > 0 {
				m.ipAccessCursor--
			}
		case "down", "j":
			if m.ipAccessCursor < len(m.ipAccessChoices)-1 {
				m.ipAccessCursor++
			}
		case "enter":
			switch m.ipAccessCursor {
			case 0:
				m.config.IPAccess = IPAccessRestrictApp
				m.step = StepAllowedIPs
				m.allowedIPsInput.Focus()
				return m, textinput.Blink
			case 1:
				m.config.IPAccess = IPAccessRestrictAll
				m.step = StepAllowedIPs
				m.allowedIPsInput.Focus()
				return m, textinput.Blink
			case 2:
				m.config.IPAccess = IPAccessMTLSApp
				m.config.AllowedIPs = nil
				if err := m.config.GeneratePasswords(); err != nil {
					m.err = err
					return m, tea.Quit
				}
				m.step = StepConfirm
				return m, nil
			case 3:
				m.config.IPAccess = IPAccessAll
				m.config.AllowedIPs = nil
				if err := m.config.GeneratePasswords(); err != nil {
					m.err = err
					return m, tea.Quit
				}
				m.step = StepConfirm
				return m, nil
			}
		}
	}
	return m, nil
}

func (m WizardModel) updateAllowedIPs(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok && key.String() == "enter" {
		val := m.allowedIPsInput.Value()
		if val == "" {
			return m, nil
		}
		m.config.ParseAllowedIPs(val)
		if err := m.config.ValidateAllowedIPs(); err != nil {
			m.ipValidationErr = err.Error()
			return m, nil
		}
		m.ipValidationErr = ""
		if err := m.config.GeneratePasswords(); err != nil {
			m.err = err
			return m, tea.Quit
		}
		m.step = StepConfirm
		return m, nil
	}
	var cmd tea.Cmd
	m.allowedIPsInput, cmd = m.allowedIPsInput.Update(msg)
	return m, cmd
}

func (m WizardModel) updateConfirm(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok && key.String() == "enter" {
		m.step = StepDone
		return m, tea.Quit
	}
	return m, nil
}

// RunWizard executes the interactive wizard and returns the completed config.
func RunWizard(cfg *SetupConfig) (*SetupConfig, error) {
	model := NewWizardModel(cfg)
	p := tea.NewProgram(model, tea.WithAltScreen())
	finalModel, err := p.Run()
	if err != nil {
		return nil, err
	}

	final := finalModel.(WizardModel)
	if final.err != nil {
		return nil, final.err
	}
	if final.step != StepDone {
		return nil, fmt.Errorf("setup cancelled")
	}
	return final.config, nil
}

// ClickHouse backend selection. The deployment kind is chosen here rather than
// inferred from the address: a managed service exposes a cluster name for its
// table functions, and reading that as a self-managed cluster would wrongly turn
// on Distributed tables and ON CLUSTER against a server that has neither.
func (m WizardModel) updateClickHouse(msg tea.Msg) (tea.Model, tea.Cmd) {
	key, ok := msg.(tea.KeyMsg)
	if !ok {
		return m, nil
	}
	switch key.String() {
	case "up", "k":
		if m.chCursor > 0 {
			m.chCursor--
		}
	case "down", "j":
		if m.chCursor < len(m.chChoices)-1 {
			m.chCursor++
		}
	case "enter":
		switch m.chCursor {
		case 0:
			m.config.CH = ClickHouseTarget{}
			m.config.CH.Normalize()
			m.step = StepDomain
			m.domainInput.Focus()
			return m, textinput.Blink
		case 1:
			m.config.CH = ClickHouseTarget{Backend: CHBackendExternal}
			m.chHostInput.Placeholder = "clickhouse.internal:9000"
		case 2:
			m.config.CH = ClickHouseTarget{
				Backend:    CHBackendExternal,
				Deployment: "cloud",
				Secure:     true,
			}
			m.chHostInput.Placeholder = "your-service.clickhouse.cloud"
		}
		m.chValidationErr = ""
		m.step = StepClickHouseHost
		m.chHostInput.Focus()
		return m, textinput.Blink
	}
	return m, nil
}

func (m WizardModel) updateClickHouseHost(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok && key.String() == "enter" {
		val := strings.TrimSpace(m.chHostInput.Value())
		if val == "" {
			m.chValidationErr = "A host is required."
			return m, nil
		}
		host, port := val, 0
		if h, p, err := net.SplitHostPort(val); err == nil {
			host = h
			if n, convErr := strconv.Atoi(p); convErr == nil {
				port = n
			}
		}
		m.config.CH.Host = host
		m.config.CH.Port = port
		m.config.CH.Normalize()

		if err := m.config.CH.Validate(); err != nil {
			m.chValidationErr = err.Error()
			return m, nil
		}
		// Reachability now, while the operator can still fix a typo, rather than
		// after the containers come up and fail their health check.
		if err := CheckClickHouseReachable(m.config.CH); err != nil {
			m.chValidationErr = err.Error()
			return m, nil
		}
		m.chValidationErr = ""
		m.step = StepClickHouseAuth
		m.chUserInput.Focus()
		return m, textinput.Blink
	}
	var cmd tea.Cmd
	m.chHostInput, cmd = m.chHostInput.Update(msg)
	return m, cmd
}

// updateClickHouseAuth collects the credential for an external ClickHouse. It
// cannot be generated: the server already exists and owns its own users.
func (m WizardModel) updateClickHouseAuth(msg tea.Msg) (tea.Model, tea.Cmd) {
	if key, ok := msg.(tea.KeyMsg); ok {
		switch key.String() {
		case "tab", "shift+tab":
			if m.chUserInput.Focused() {
				m.chUserInput.Blur()
				m.chPassInput.Focus()
			} else {
				m.chPassInput.Blur()
				m.chUserInput.Focus()
			}
			return m, textinput.Blink
		case "enter":
			user := strings.TrimSpace(m.chUserInput.Value())
			if user == "" {
				user = "default"
			}
			pass := m.chPassInput.Value()
			if pass == "" {
				m.chValidationErr = "A password is required for an external ClickHouse."
				return m, nil
			}
			m.config.CH.User = user
			m.config.ClickHousePassword = pass
			m.chValidationErr = ""
			m.step = StepDomain
			m.domainInput.Focus()
			return m, textinput.Blink
		}
	}
	var cmd tea.Cmd
	if m.chUserInput.Focused() {
		m.chUserInput, cmd = m.chUserInput.Update(msg)
	} else {
		m.chPassInput, cmd = m.chPassInput.Update(msg)
	}
	return m, cmd
}

func (m WizardModel) viewClickHouseAuth() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("ClickHouse Credentials"))
	s.WriteString("\n\n")
	s.WriteString("The user Bifract connects as. It needs privileges to create the schema,\n")
	s.WriteString("and to create the least-privilege ingest user.\n\n")
	s.WriteString(LabelStyle.Render("  Username"))
	s.WriteString("\n")
	s.WriteString("  " + m.chUserInput.View())
	s.WriteString("\n\n")
	s.WriteString(LabelStyle.Render("  Password"))
	s.WriteString("\n")
	s.WriteString("  " + m.chPassInput.View())
	if m.chValidationErr != "" {
		s.WriteString("\n\n")
		s.WriteString(ErrorStyle.Render("  " + m.chValidationErr))
	}
	return s.String()
}

func (m WizardModel) viewClickHouse() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("ClickHouse Backend"))
	s.WriteString("\n\n")
	s.WriteString("Where should Bifract store log data?\n\n")
	s.WriteString(RenderOptionList(m.chChoices, []string{
		"Run ClickHouse in this Compose stack. Bifract manages its lifecycle and upgrades.",
		"Connect to a ClickHouse you already run. This installer will not manage it.",
		"Connect to a managed ClickHouse Cloud service over TLS.",
	}, m.chCursor))
	return s.String()
}

func (m WizardModel) viewClickHouseHost() string {
	var s strings.Builder
	s.WriteString(TitleStyle.Render("ClickHouse Endpoint"))
	s.WriteString("\n\n")
	if m.config.CH.Deployment == "cloud" {
		s.WriteString("Enter your service hostname. TLS is required and port ")
		s.WriteString(HighlightStyle.Render("9440"))
		s.WriteString(" is assumed.\n\n")
	} else {
		s.WriteString("Enter the native-protocol endpoint. Port ")
		s.WriteString(HighlightStyle.Render("9000"))
		s.WriteString(" is assumed if omitted.\n\n")
	}
	s.WriteString(LabelStyle.Render("  Host"))
	s.WriteString("\n")
	s.WriteString("  " + m.chHostInput.View())
	if m.chValidationErr != "" {
		s.WriteString("\n\n")
		s.WriteString(ErrorStyle.Render("  " + m.chValidationErr))
	}
	return s.String()
}
