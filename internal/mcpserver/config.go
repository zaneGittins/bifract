package mcpserver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultTimeout = 60 * time.Second

// Config is the resolved connection to a Bifract instance.
type Config struct {
	URL    string
	APIKey string
	// Scope is the X-Bifract-Scope value this session sends, or empty when the
	// key carries its own. A tenant-admin key (bifract_admin_...) belongs to no
	// fractal and names the one it means per request, so it needs this.
	Scope   string
	TLS     *tls.Config
	Timeout time.Duration
}

// FractalScope reports the fractal this session was configured for, if it named
// one rather than a prism.
func (c Config) FractalScope() string {
	id, ok := strings.CutPrefix(c.Scope, "fractal:")
	if !ok {
		return ""
	}
	return id
}

// APIBase is the versioned prefix every tool path is relative to.
func (c Config) APIBase() string { return c.URL + "/api/v1" }

// LoadConfig reads the environment. Every problem is reported at once, because a
// misconfigured client is usually missing more than one thing and a server that
// dies on the first is a slow way to find that out.
func LoadConfig() (Config, error) {
	var problems []string
	note := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	url := strings.TrimRight(strings.TrimSpace(os.Getenv("BIFRACT_URL")), "/")
	switch {
	case url == "":
		note("BIFRACT_URL is not set.")
	case !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://"):
		note("BIFRACT_URL must start with http:// or https:// (got %s).", url)
	}

	apiKey := strings.TrimSpace(os.Getenv("BIFRACT_API_KEY"))
	if apiKey == "" {
		note("BIFRACT_API_KEY is not set.")
	}

	cfg := Config{
		URL:     url,
		APIKey:  apiKey,
		Scope:   resolveScope(note),
		TLS:     resolveTLS(note),
		Timeout: resolveTimeout(note),
	}
	if len(problems) > 0 {
		return Config{}, fmt.Errorf("%s", strings.Join(problems, " "))
	}
	return cfg, nil
}

// resolveScope builds the scope header. Only a tenant-admin key needs it; any
// other already carries its scope. The id is shape-checked here because the
// server answers a malformed one with a 400 on every call.
func resolveScope(note func(string, ...any)) string {
	fractal := strings.TrimSpace(os.Getenv("BIFRACT_FRACTAL_ID"))
	prism := strings.TrimSpace(os.Getenv("BIFRACT_PRISM_ID"))
	switch {
	case fractal != "" && prism != "":
		note("Set BIFRACT_FRACTAL_ID or BIFRACT_PRISM_ID, not both: a session acts in one scope.")
		return ""
	case fractal != "":
		if !validScopeID(fractal) {
			note("BIFRACT_FRACTAL_ID is not an id: %s.", fractal)
			return ""
		}
		return "fractal:" + fractal
	case prism != "":
		if !validScopeID(prism) {
			note("BIFRACT_PRISM_ID is not an id: %s.", prism)
			return ""
		}
		return "prism:" + prism
	}
	return ""
}

// validScopeID mirrors what the server will accept in the scope header.
func validScopeID(id string) bool {
	if len(id) > 36 {
		return false
	}
	for _, c := range id {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-':
		default:
			return false
		}
	}
	return true
}

// resolveTLS builds the trust and client-identity halves of the connection. A CA
// bundle implies verification, so it wins over BIFRACT_VERIFY_SSL rather than
// being silently combined with it.
func resolveTLS(note func(string, ...any)) *tls.Config {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if bundle := strings.TrimSpace(os.Getenv("BIFRACT_CA_CERT")); bundle != "" {
		pem, err := os.ReadFile(bundle)
		if err != nil {
			note("BIFRACT_CA_CERT cannot be read: %v.", err)
		} else {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				note("BIFRACT_CA_CERT holds no certificates: %s.", bundle)
			} else {
				cfg.RootCAs = pool
			}
		}
	} else if falsey(os.Getenv("BIFRACT_VERIFY_SSL")) {
		// Opt-in only, and never reachable while a CA bundle is configured.
		cfg.InsecureSkipVerify = true
	}

	certPath := strings.TrimSpace(os.Getenv("BIFRACT_CLIENT_CERT"))
	if certPath == "" {
		return cfg
	}
	// A combined PEM carries its own key, so the key path is optional.
	keyPath := strings.TrimSpace(os.Getenv("BIFRACT_CLIENT_KEY"))
	if keyPath == "" {
		keyPath = certPath
	}
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		note("BIFRACT_CLIENT_CERT/BIFRACT_CLIENT_KEY cannot be loaded: %v.", err)
		return cfg
	}
	cfg.Certificates = []tls.Certificate{pair}
	return cfg
}

func resolveTimeout(note func(string, ...any)) time.Duration {
	raw := strings.TrimSpace(os.Getenv("BIFRACT_TIMEOUT"))
	if raw == "" {
		return defaultTimeout
	}
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		note("BIFRACT_TIMEOUT is not a number: %s.", raw)
		return defaultTimeout
	}
	if seconds <= 0 {
		note("BIFRACT_TIMEOUT must be positive: %s.", raw)
		return defaultTimeout
	}
	return time.Duration(seconds * float64(time.Second))
}

func falsey(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "0", "false", "no", "off":
		return true
	}
	return false
}
