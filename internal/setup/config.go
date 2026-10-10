package setup

import (
	"fmt"
	"net"
	"net/mail"
	"os"
	"strconv"
	"strings"
)

// MinPasswordLength is the shortest password Bifract accepts for any user.
const MinPasswordLength = 12

// Minimums the installer enforces on bundled ClickHouse sizing.
const (
	MinCHShards    = 1
	MinCHStorageGB = 10
)

type SSLMode string

const (
	SSLSelfSigned  SSLMode = "self-signed"
	SSLLetsEncrypt SSLMode = "letsencrypt"
	SSLCustom      SSLMode = "custom"
)

type IPAccessMode string

const (
	IPAccessRestrictApp IPAccessMode = "restrict-app"
	IPAccessRestrictAll IPAccessMode = "restrict-all"
	IPAccessMTLSApp     IPAccessMode = "mtls-app"
	IPAccessAll         IPAccessMode = "all"
)

type SetupConfig struct {
	InstallDir string
	Domain     string
	SSLMode    SSLMode
	SSLEmail   string
	CertPath   string
	KeyPath    string

	PostgresPassword         string
	IngestPostgresPassword   string // least-privilege ingest PG role (bifract_ingest)
	ClickHousePassword       string
	IngestClickHousePassword string // least-privilege ingest CH user (bifract_ingest)
	LiteLLMMasterKey         string
	AdminPassword            string
	AdminPasswordHash        string
	PasswordPepper           string
	FeedEncryptionKey        string
	BackupEncryptionKey      string

	S3Endpoint  string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3Region    string

	ImageTag string

	SecureCookies bool
	CORSOrigins   string

	IPAccess   IPAccessMode
	AllowedIPs []string

	// CH is where ClickHouse comes from. Its zero value is the bundled
	// ClickHouse this installer renders, so an install that predates the field
	// keeps its existing shape through reconfigure and upgrade.
	CH ClickHouseTarget
}

// AllowedIPsString returns the allowed IPs as a comma-separated string for env persistence.
func (c *SetupConfig) AllowedIPsString() string {
	return strings.Join(c.AllowedIPs, ",")
}

// ParseAllowedIPs splits a comma-separated IP string into the AllowedIPs slice.
func (c *SetupConfig) ParseAllowedIPs(s string) {
	c.AllowedIPs = nil
	for _, ip := range strings.Split(s, ",") {
		ip = strings.TrimSpace(ip)
		if ip != "" {
			c.AllowedIPs = append(c.AllowedIPs, ip)
		}
	}
}

// ValidateAllowedIPs checks that each entry in AllowedIPs is a valid IP or CIDR.
func (c *SetupConfig) ValidateAllowedIPs() error {
	for _, entry := range c.AllowedIPs {
		if strings.Contains(entry, "/") {
			if _, _, err := net.ParseCIDR(entry); err != nil {
				return fmt.Errorf("invalid CIDR %q: %w", entry, err)
			}
		} else {
			if net.ParseIP(entry) == nil {
				return fmt.Errorf("invalid IP address %q", entry)
			}
		}
	}
	return nil
}

func DefaultConfig() *SetupConfig {
	return &SetupConfig{
		InstallDir:    "/opt/bifract",
		Domain:        "localhost",
		SSLMode:       SSLSelfSigned,
		ImageTag:      Version,
		SecureCookies: false,
		CORSOrigins:   "http://localhost:8080,http://127.0.0.1:8080",
		IPAccess:      IPAccessAll,
	}
}

func (c *SetupConfig) GeneratePasswords() error {
	var err error
	c.PostgresPassword, err = GenerateAlphanumeric(24)
	if err != nil {
		return err
	}
	c.IngestPostgresPassword, err = GenerateAlphanumeric(24)
	if err != nil {
		return err
	}
	// Only a ClickHouse this installer creates gets a generated password. An
	// external server already has its own credential, supplied by the operator;
	// generating over it would write a password that authenticates nowhere.
	if c.CH.Bundled() {
		c.ClickHousePassword, err = GenerateAlphanumeric(24)
		if err != nil {
			return err
		}
	}
	// Created on whichever ClickHouse the app connects to, including a managed
	// one that enforces a complexity policy.
	c.IngestClickHousePassword, err = GenerateClickHousePassword(24)
	if err != nil {
		return err
	}
	c.LiteLLMMasterKey, err = GenerateAlphanumeric(32)
	if err != nil {
		return err
	}
	c.LiteLLMMasterKey = "sk-" + c.LiteLLMMasterKey
	c.PasswordPepper, err = GenerateAlphanumeric(32)
	if err != nil {
		return err
	}
	c.FeedEncryptionKey, err = GenerateHexKey(32)
	if err != nil {
		return err
	}
	c.BackupEncryptionKey, err = GenerateHexKey(32)
	if err != nil {
		return err
	}
	c.AdminPassword, err = GenerateAlphanumeric(16)
	if err != nil {
		return err
	}
	c.AdminPasswordHash, err = HashPassword(c.AdminPassword, c.PasswordPepper)
	if err != nil {
		return err
	}
	return nil
}

// ValidateDomain accepts a hostname or IP address that can be written into a
// Caddy site address unmodified.
func ValidateDomain(d string) error {
	if d == "" {
		return fmt.Errorf("a domain is required")
	}
	if ip := net.ParseIP(d); ip != nil {
		// An IPv6 literal is not a valid Caddy site address or CORS origin unbracketed.
		if ip.To4() == nil {
			return fmt.Errorf("IPv6 address %q is not supported as the domain; use a hostname or IPv4 address", d)
		}
		return nil
	}
	if len(d) > 253 {
		return fmt.Errorf("domain %q is too long", d)
	}
	for _, label := range strings.Split(d, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return fmt.Errorf("invalid domain %q: use a bare hostname such as bifract.example.com", d)
		}
		for _, r := range label {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
				return fmt.Errorf("invalid domain %q: use a bare hostname such as bifract.example.com", d)
			}
		}
	}
	return nil
}

// ApplyDomain sets the domain and the cookie and CORS settings derived from it.
func (c *SetupConfig) ApplyDomain(d string) {
	c.Domain = d
	if d == "localhost" {
		c.SecureCookies = false
		c.CORSOrigins = DefaultConfig().CORSOrigins
		return
	}
	c.SecureCookies = true
	c.CORSOrigins = "https://" + d
}

// ValidateEmail checks a Let's Encrypt contact address.
func ValidateEmail(e string) error {
	if e == "" {
		return fmt.Errorf("an email address is required for Let's Encrypt")
	}
	if a, err := mail.ParseAddress(e); err != nil || a.Address != e {
		return fmt.Errorf("invalid email address %q", e)
	}
	return nil
}

// ValidateCertPair checks that a custom certificate and key are readable files.
func ValidateCertPair(certPath, keyPath string) error {
	for _, f := range []struct{ what, path string }{{"certificate", certPath}, {"private key", keyPath}} {
		if f.path == "" {
			return fmt.Errorf("a %s path is required for custom TLS", f.what)
		}
		st, err := os.Stat(f.path)
		if err != nil {
			return fmt.Errorf("%s: %w", f.what, err)
		}
		if st.IsDir() {
			return fmt.Errorf("%s %s is a directory", f.what, f.path)
		}
	}
	return nil
}

// ParseCHEndpoint splits "host[:port]"; a zero port means the default for the
// target's TLS mode.
func ParseCHEndpoint(v string) (string, int, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", 0, fmt.Errorf("a ClickHouse host is required")
	}
	if strings.Contains(v, "[") || strings.Count(v, ":") > 1 {
		return "", 0, fmt.Errorf("IPv6 ClickHouse address %q is not supported; use a hostname or IPv4 address", v)
	}
	h, p, err := net.SplitHostPort(v)
	if err != nil {
		return v, 0, nil
	}
	port, err := strconv.Atoi(p)
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("invalid ClickHouse port in %q", v)
	}
	return h, port, nil
}

// ParseMinInt parses a whole number no smaller than min.
func ParseMinInt(what, v string, min int) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("%s must be a whole number", what)
	}
	if n < min {
		return 0, fmt.Errorf("%s must be at least %d", what, min)
	}
	return n, nil
}

// validateAccess checks the IP access mode against its allow list.
func (c *SetupConfig) validateAccess() error {
	switch c.IPAccess {
	case IPAccessRestrictApp, IPAccessRestrictAll:
		if len(c.AllowedIPs) == 0 {
			return fmt.Errorf("IP access %q needs at least one allowed IP or CIDR", c.IPAccess)
		}
		return c.ValidateAllowedIPs()
	case IPAccessAll, IPAccessMTLSApp:
		if len(c.AllowedIPs) > 0 {
			return fmt.Errorf("allowed IPs only apply to restrict-app and restrict-all, not %q", c.IPAccess)
		}
		return nil
	default:
		return fmt.Errorf("unknown IP access mode %q", c.IPAccess)
	}
}

// validateCommon covers the settings the docker and k8s installs share.
func (c *SetupConfig) validateCommon() error {
	if err := ValidateDomain(c.Domain); err != nil {
		return err
	}
	if err := c.validateAccess(); err != nil {
		return err
	}
	if err := c.CH.Validate(); err != nil {
		return err
	}
	if !c.CH.Bundled() && c.ClickHousePassword == "" {
		return fmt.Errorf("a password is required for external ClickHouse")
	}
	return nil
}

// Validate is the single gate a docker install config passes, whether it came
// from the wizard or a config file.
func (c *SetupConfig) Validate() error {
	if c.InstallDir == "" {
		return fmt.Errorf("an install directory is required")
	}
	if err := c.validateCommon(); err != nil {
		return err
	}
	switch c.SSLMode {
	case SSLSelfSigned:
		return nil
	case SSLLetsEncrypt:
		return ValidateEmail(c.SSLEmail)
	case SSLCustom:
		return ValidateCertPair(c.CertPath, c.KeyPath)
	default:
		return fmt.Errorf("unknown TLS mode %q", c.SSLMode)
	}
}
