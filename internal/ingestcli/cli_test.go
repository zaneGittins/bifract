package ingestcli

import (
	"net/url"
	"testing"
)

// The ingest port only speaks TLS, so plain HTTP to it can never work.
func TestDefaultURLUsesHTTPS(t *testing.T) {
	u, err := url.Parse(defaultURL)
	if err != nil {
		t.Fatal(err)
	}
	if u.Scheme != "https" || u.Port() != "8443" {
		t.Fatalf("defaultURL = %q, want https on port 8443", defaultURL)
	}
}
