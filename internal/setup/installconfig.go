package setup

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

// InstallFileVersion is the only install config schema version this build reads.
const InstallFileVersion = 1

// InstallFileName is where a wizard run saves its answers for replay.
const InstallFileName = "bifract-install.yaml"

// InstallFile is the non-interactive answer file for --install and
// --install-k8s. It never holds a secret value, only references to one, so it
// can be committed alongside the rest of an operator's configuration.
type InstallFile struct {
	Version    int            `yaml:"version"`
	Domain     string         `yaml:"domain"`
	Access     accessSpec     `yaml:"access"`
	ClickHouse clickhouseSpec `yaml:"clickhouse,omitempty"`
	Secrets    secretsSpec    `yaml:"secrets,omitempty"`
	Output     outputSpec     `yaml:"output,omitempty"`
	Compose    *composeSpec   `yaml:"compose,omitempty"`
	K8s        *k8sSpec       `yaml:"k8s,omitempty"`
}

type accessSpec struct {
	Mode       IPAccessMode `yaml:"mode"`
	AllowedIPs []string     `yaml:"allowed_ips,omitempty"`
}

type clickhouseSpec struct {
	Backend       string   `yaml:"backend,omitempty"`
	Deployment    string   `yaml:"deployment,omitempty"`
	Host          string   `yaml:"host,omitempty"`
	Hosts         []string `yaml:"hosts,omitempty"`
	Cluster       string   `yaml:"cluster,omitempty"`
	FanoutCluster string   `yaml:"fanout_cluster,omitempty"`
	Database      string   `yaml:"database,omitempty"`
	User          string   `yaml:"user,omitempty"`
	Secure        bool     `yaml:"secure,omitempty"`
	PasswordFile  string   `yaml:"password_file,omitempty"`
	PasswordEnv   string   `yaml:"password_env,omitempty"`
	// CheckReachable defaults to true: the same probe the wizard runs.
	CheckReachable *bool `yaml:"check_reachable,omitempty"`
}

type secretsSpec struct {
	AdminPasswordFile string `yaml:"admin_password_file,omitempty"`
	AdminPasswordEnv  string `yaml:"admin_password_env,omitempty"`
}

// AdminPasswordOutput is where a generated admin password is reported.
type AdminPasswordOutput string

const (
	AdminPasswordToFile   AdminPasswordOutput = "file"
	AdminPasswordToStdout AdminPasswordOutput = "stdout"
	AdminPasswordToNone   AdminPasswordOutput = "none"
)

// AdminPasswordFileName is the 0600 file a file-mode install writes the admin
// password to, inside the install or output directory.
const AdminPasswordFileName = "admin-password"

type outputSpec struct {
	AdminPassword AdminPasswordOutput `yaml:"admin_password,omitempty"`
}

type composeSpec struct {
	InstallDir string   `yaml:"install_dir,omitempty"`
	ImageTag   string   `yaml:"image_tag,omitempty"`
	TLS        *tlsSpec `yaml:"tls,omitempty"`
}

type tlsSpec struct {
	Mode     SSLMode `yaml:"mode"`
	Email    string  `yaml:"email,omitempty"`
	CertFile string  `yaml:"cert_file,omitempty"`
	KeyFile  string  `yaml:"key_file,omitempty"`
}

type k8sSpec struct {
	OutputDir   string `yaml:"output_dir,omitempty"`
	SizeProfile string `yaml:"size_profile"`
	CHShards    *int   `yaml:"ch_shards,omitempty"`
	CHStorageGB *int   `yaml:"ch_storage_gb,omitempty"`
}

// InstallOptions selects how an install gathers its answers.
type InstallOptions struct {
	// ConfigPath is an install config file; empty runs the interactive wizard.
	ConfigPath string
}

// installPlan is what an install needs beyond its config: whether it came
// from a file, and how to treat the admin password.
type installPlan struct {
	fromFile       bool
	adminPassword  string // operator-supplied; empty means generated
	passwordOutput AdminPasswordOutput
}

// requireTerminal fails fast when the wizard has no terminal to draw on,
// rather than hanging in cloud-init or CI.
func requireTerminal(flag string) error {
	if term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd())) {
		return nil
	}
	return fmt.Errorf("%s needs an interactive terminal; run it with --config <file> for a non-interactive install", flag)
}

// LoadInstallFile reads and strictly parses an install config file.
func LoadInstallFile(path string) (*InstallFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read install config: %w", err)
	}
	return parseInstallFile(b)
}

func parseInstallFile(b []byte) (*InstallFile, error) {
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	var f InstallFile
	if err := dec.Decode(&f); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("install config is empty")
		}
		return nil, fmt.Errorf("parse install config: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("install config must be a single YAML document")
	}
	if f.Version == 0 {
		return nil, fmt.Errorf("install config needs \"version: %d\"", InstallFileVersion)
	}
	if f.Version != InstallFileVersion {
		return nil, fmt.Errorf("install config version %d is not supported by this build (want %d)", f.Version, InstallFileVersion)
	}
	return &f, nil
}

// readSecretRef resolves a secret given as a file or an environment variable.
func readSecretRef(what, file, env string) (string, error) {
	switch {
	case file != "" && env != "":
		return "", fmt.Errorf("%s: set a file or an environment variable, not both", what)
	case file != "":
		b, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("%s: %w", what, err)
		}
		v := strings.TrimRight(string(b), "\r\n")
		if v == "" {
			return "", fmt.Errorf("%s: %s is empty", what, file)
		}
		return v, nil
	case env != "":
		v := os.Getenv(env)
		if v == "" {
			return "", fmt.Errorf("%s: environment variable %s is unset or empty", what, env)
		}
		return v, nil
	}
	return "", nil
}

// common builds the settings both install kinds share and the plan around them.
func (f *InstallFile) common(cfg *SetupConfig) (installPlan, error) {
	plan := installPlan{fromFile: true, passwordOutput: f.Output.AdminPassword}

	if err := ValidateDomain(f.Domain); err != nil {
		return plan, err
	}
	cfg.ApplyDomain(f.Domain)

	if f.Access.Mode == "" {
		return plan, fmt.Errorf("access.mode is required (all, restrict-app, restrict-all, mtls-app)")
	}
	cfg.IPAccess = f.Access.Mode
	cfg.AllowedIPs = nil
	for _, ip := range f.Access.AllowedIPs {
		if ip = strings.TrimSpace(ip); ip != "" {
			cfg.AllowedIPs = append(cfg.AllowedIPs, ip)
		}
	}

	ch, err := f.ClickHouse.target()
	if err != nil {
		return plan, err
	}
	cfg.CH = ch
	if !ch.Bundled() {
		if f.ClickHouse.PasswordFile == "" && f.ClickHouse.PasswordEnv == "" {
			return plan, fmt.Errorf("external ClickHouse needs clickhouse.password_file or clickhouse.password_env")
		}
		if cfg.ClickHousePassword, err = readSecretRef("clickhouse password", f.ClickHouse.PasswordFile, f.ClickHouse.PasswordEnv); err != nil {
			return plan, err
		}
	}

	if plan.adminPassword, err = readSecretRef("admin password", f.Secrets.AdminPasswordFile, f.Secrets.AdminPasswordEnv); err != nil {
		return plan, err
	}
	if plan.adminPassword != "" && len(plan.adminPassword) < MinPasswordLength {
		return plan, fmt.Errorf("admin password must be at least %d characters", MinPasswordLength)
	}

	switch plan.passwordOutput {
	case "":
		plan.passwordOutput = AdminPasswordToFile
	case AdminPasswordToFile, AdminPasswordToStdout:
	case AdminPasswordToNone:
		if plan.adminPassword == "" {
			return plan, fmt.Errorf("output.admin_password: none would lose the generated password; supply secrets.admin_password_file or use file")
		}
	default:
		return plan, fmt.Errorf("output.admin_password must be file, stdout or none, not %q", plan.passwordOutput)
	}
	return plan, nil
}

func (s clickhouseSpec) target() (ClickHouseTarget, error) {
	switch s.Backend {
	case "", "bundled":
		if s.Deployment != "" || s.Host != "" || len(s.Hosts) > 0 || s.Cluster != "" || s.FanoutCluster != "" ||
			s.Database != "" || s.User != "" || s.Secure || s.PasswordFile != "" || s.PasswordEnv != "" || s.CheckReachable != nil {
			return ClickHouseTarget{}, fmt.Errorf("clickhouse: connection settings only apply to backend: external")
		}
		var t ClickHouseTarget
		t.Normalize()
		return t, nil
	case string(CHBackendExternal):
	default:
		return ClickHouseTarget{}, fmt.Errorf("clickhouse.backend must be bundled or external, not %q", s.Backend)
	}

	t := ClickHouseTarget{
		Backend:       CHBackendExternal,
		Deployment:    s.Deployment,
		Hosts:         strings.Join(s.Hosts, ","),
		Cluster:       s.Cluster,
		FanoutCluster: s.FanoutCluster,
		Database:      s.Database,
		User:          s.User,
		Secure:        s.Secure,
	}
	if s.Host != "" && len(s.Hosts) > 0 {
		return t, fmt.Errorf("clickhouse: set host or hosts, not both")
	}
	if s.Host != "" {
		host, port, err := ParseCHEndpoint(s.Host)
		if err != nil {
			return t, err
		}
		t.Host, t.Port = host, port
	}
	t.Normalize()
	if err := t.Validate(); err != nil {
		return t, fmt.Errorf("clickhouse: %w", err)
	}
	if s.CheckReachable == nil || *s.CheckReachable {
		if err := CheckClickHouseReachable(t); err != nil {
			return t, fmt.Errorf("clickhouse: %w (set check_reachable: false to skip)", err)
		}
	}
	return t, nil
}

// ComposeConfig builds a validated docker install config from the file.
func (f *InstallFile) ComposeConfig() (*SetupConfig, installPlan, error) {
	if f.K8s != nil {
		return nil, installPlan{}, fmt.Errorf("the k8s section is only read by --install-k8s")
	}
	cfg := DefaultConfig()
	plan, err := f.common(cfg)
	if err != nil {
		return nil, plan, err
	}
	if c := f.Compose; c != nil {
		if c.InstallDir != "" {
			cfg.InstallDir = c.InstallDir
		}
		if c.ImageTag != "" {
			cfg.ImageTag = c.ImageTag
		}
		if c.TLS != nil {
			cfg.SSLMode = c.TLS.Mode
			if cfg.SSLMode == "" {
				return nil, plan, fmt.Errorf("compose.tls.mode is required (self-signed, letsencrypt, custom)")
			}
			if c.TLS.Email != "" && cfg.SSLMode != SSLLetsEncrypt {
				return nil, plan, fmt.Errorf("compose.tls.email only applies to letsencrypt")
			}
			if (c.TLS.CertFile != "" || c.TLS.KeyFile != "") && cfg.SSLMode != SSLCustom {
				return nil, plan, fmt.Errorf("compose.tls.cert_file and key_file only apply to custom")
			}
			cfg.SSLEmail = c.TLS.Email
			cfg.CertPath, cfg.KeyPath = c.TLS.CertFile, c.TLS.KeyFile
		}
	}
	if err := cfg.Validate(); err != nil {
		return nil, plan, err
	}
	return cfg, plan, nil
}

// K8sConfig builds a validated k8s install config from the file.
func (f *InstallFile) K8sConfig() (*K8sConfig, installPlan, error) {
	if f.Compose != nil {
		return nil, installPlan{}, fmt.Errorf("the compose section is only read by --install")
	}
	if f.K8s == nil || f.K8s.SizeProfile == "" {
		return nil, installPlan{}, fmt.Errorf("k8s.size_profile is required (%s)", sizeProfileNames())
	}
	profile, ok := lookupSizeProfile(f.K8s.SizeProfile)
	if !ok {
		return nil, installPlan{}, fmt.Errorf("unknown k8s.size_profile %q (%s)", f.K8s.SizeProfile, sizeProfileNames())
	}
	cfg := newK8sConfig()
	plan, err := f.common(&cfg.SetupConfig)
	if err != nil {
		return nil, plan, err
	}
	cfg.MTLSEnabled = cfg.IPAccess == IPAccessMTLSApp
	cfg.SizeProfile = profile
	cfg.CHShards = profile.CHShards
	if f.K8s.OutputDir != "" {
		cfg.OutputDir = f.K8s.OutputDir
	}
	if f.K8s.CHShards != nil || f.K8s.CHStorageGB != nil {
		if !cfg.CH.Bundled() {
			return nil, plan, fmt.Errorf("k8s.ch_shards and ch_storage_gb only apply to bundled ClickHouse")
		}
		if f.K8s.CHShards != nil {
			cfg.CHShards = *f.K8s.CHShards
		}
		if f.K8s.CHStorageGB != nil {
			cfg.CHStorageGB = *f.K8s.CHStorageGB
		}
	}
	if err := cfg.Validate(); err != nil {
		return nil, plan, err
	}
	return cfg, plan, nil
}

func sizeProfileNames() string {
	names := make([]string, len(sizeProfiles))
	for i, p := range sizeProfiles {
		names[i] = p.Name
	}
	return strings.Join(names, ", ")
}

// applyAdminPassword replaces the generated admin password with a supplied one.
func (p installPlan) applyAdminPassword(cfg *SetupConfig) error {
	if p.adminPassword == "" {
		return nil
	}
	hash, err := HashPassword(p.adminPassword, cfg.PasswordPepper)
	if err != nil {
		return err
	}
	cfg.AdminPassword, cfg.AdminPasswordHash = p.adminPassword, hash
	return nil
}

// reportAdminPassword delivers the admin password per the plan and returns the
// summary line value. Only stdout mode ever prints the password itself.
func (p installPlan) reportAdminPassword(dir, password string) (string, bool, error) {
	if p.adminPassword != "" {
		return "(the one you supplied)", false, nil
	}
	switch p.passwordOutput {
	case AdminPasswordToFile:
		path := filepath.Join(dir, AdminPasswordFileName)
		if err := createSecretFile(path, password+"\n"); err != nil {
			if errors.Is(err, fs.ErrExist) {
				return "", false, fmt.Errorf("%s already exists from an earlier install; remove it and run again", path)
			}
			return "", false, fmt.Errorf("write admin password: %w", err)
		}
		return "saved to " + path, false, nil
	default:
		return password, true, nil
	}
}

// createSecretFile creates path with 0600 permissions, refusing to follow or
// replace anything already there.
func createSecretFile(path, content string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// installFileFromConfig captures a wizard run as a replayable install file.
// Secret values are never written; an external ClickHouse password must be
// supplied by reference before replay.
func installFileFromConfig(cfg *SetupConfig) *InstallFile {
	f := &InstallFile{
		Version: InstallFileVersion,
		Domain:  cfg.Domain,
		Access:  accessSpec{Mode: cfg.IPAccess, AllowedIPs: cfg.AllowedIPs},
	}
	if !cfg.CH.Bundled() {
		t := cfg.CH
		f.ClickHouse = clickhouseSpec{
			Backend:       string(CHBackendExternal),
			Deployment:    t.Deployment,
			Cluster:       t.Cluster,
			FanoutCluster: t.FanoutCluster,
			Database:      t.Database,
			User:          t.User,
			Secure:        t.Secure,
		}
		if t.Hosts != "" {
			f.ClickHouse.Hosts = splitCSV(t.Hosts)
		} else {
			f.ClickHouse.Host = fmt.Sprintf("%s:%d", t.Host, t.Port)
		}
	}
	return f
}

func composeInstallFile(cfg *SetupConfig) *InstallFile {
	f := installFileFromConfig(cfg)
	f.Compose = &composeSpec{
		InstallDir: cfg.InstallDir,
		TLS:        &tlsSpec{Mode: cfg.SSLMode, Email: cfg.SSLEmail, CertFile: cfg.CertPath, KeyFile: cfg.KeyPath},
	}
	if cfg.ImageTag != Version {
		f.Compose.ImageTag = cfg.ImageTag
	}
	return f
}

func k8sInstallFile(cfg *K8sConfig) *InstallFile {
	f := installFileFromConfig(&cfg.SetupConfig)
	f.K8s = &k8sSpec{OutputDir: cfg.OutputDir, SizeProfile: cfg.SizeProfile.Name}
	if cfg.CH.Bundled() {
		shards, storage := cfg.CHShards, cfg.CHStorageGB
		f.K8s.CHShards, f.K8s.CHStorageGB = &shards, &storage
	}
	return f
}

// saveInstallFile writes a wizard run's answers into dir for later replay.
func saveInstallFile(dir string, f *InstallFile) (string, error) {
	body, err := yaml.Marshal(f)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("# Answers from an interactive install. Replay with --config.\n")
	if f.ClickHouse.Backend == string(CHBackendExternal) {
		b.WriteString("# Set clickhouse.password_file or clickhouse.password_env before replaying.\n")
	}
	b.Write(body)
	path := filepath.Join(dir, InstallFileName)
	if err := writeSecretFile(path, b.String()); err != nil {
		return "", err
	}
	return path, nil
}

// renderPasswordLine emphasizes the password only when it is printed in full.
func renderPasswordLine(line string, shown bool) string {
	if shown {
		return lipgloss.NewStyle().Foreground(White).Bold(true).Render(line)
	}
	return ValueStyle.Render(line)
}
