package maxmind

import (
	"os"
	"strings"
)

// Config holds MaxMind GeoLite2 configuration loaded from environment variables.
type Config struct {
	LicenseKey string
	AccountID  string
	EditionIDs []string // e.g. ["GeoLite2-City-CSV", "GeoLite2-ASN-CSV"]
}

// LoadConfigFromEnv returns a MaxMind config if MAXMIND_LICENSE_KEY is set,
// or nil if the feature is not configured.
func LoadConfigFromEnv() *Config {
	licenseKey := os.Getenv("MAXMIND_LICENSE_KEY")
	if licenseKey == "" {
		return nil
	}

	accountID := os.Getenv("MAXMIND_ACCOUNT_ID")
	if accountID == "" {
		return nil
	}

	editionIDs := []string{"GeoLite2-City-CSV", "GeoLite2-ASN-CSV"}
	if v := os.Getenv("MAXMIND_EDITION_IDS"); v != "" {
		editionIDs = nil
		for _, id := range strings.Split(v, ",") {
			id = strings.TrimSpace(id)
			if id != "" {
				editionIDs = append(editionIDs, id)
			}
		}
	}

	return &Config{
		LicenseKey: licenseKey,
		AccountID:  accountID,
		EditionIDs: editionIDs,
	}
}
