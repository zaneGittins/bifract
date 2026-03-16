package setup

import (
	"fmt"
	"net"
	"strings"
)

type SSLMode string

const (
	SSLSelfSigned  SSLMode = "self-signed"
	SSLLetsEncrypt SSLMode = "letsencrypt"
	SSLCustom      SSLMode = "custom"
)

type IPAccessMode string

const (
	IPAccessRestrictApp  IPAccessMode = "restrict-app"
	IPAccessRestrictAll  IPAccessMode = "restrict-all"
	IPAccessMTLSApp      IPAccessMode = "mtls-app"
	IPAccessAll          IPAccessMode = "all"
)

type SetupConfig struct {
	InstallDir string
	Domain     string
	SSLMode    SSLMode
	SSLEmail   string
	CertPath   string
	KeyPath    string

	PostgresPassword   string
	ClickHousePassword string
	LiteLLMMasterKey   string
	AdminPassword      string
	AdminPasswordHash  string
	PasswordPepper     string
	FeedEncryptionKey    string
	BackupEncryptionKey string

	S3Endpoint  string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3Region    string

	ImageTag     string

	SecureCookies bool
	CORSOrigins   string

	IPAccess   IPAccessMode
	AllowedIPs []string
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
	c.ClickHousePassword, err = GenerateAlphanumeric(24)
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
