package setup

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	"software.sslmate.com/src/go-pkcs12"
)

// GenerateSelfSignedCert creates a self-signed ECDSA P-256 certificate for
// the given domain. If domain is an IP address it is added as an IP SAN;
// otherwise as a DNS SAN. localhost and 127.0.0.1 are always included.
// Cert and key are written to certsDir/cert.pem and certsDir/key.pem.
// If both files already exist the call is a no-op (safe for upgrades).
func GenerateSelfSignedCert(domain, certsDir string) error {
	certPath := filepath.Join(certsDir, "cert.pem")
	keyPath := filepath.Join(certsDir, "key.pem")

	if fileExists(certPath) && fileExists(keyPath) {
		return nil
	}

	if err := os.MkdirAll(certsDir, 0755); err != nil {
		return fmt.Errorf("create certs directory: %w", err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate private key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate serial number: %w", err)
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{"Bifract"},
			CommonName:   domain,
		},
		NotBefore:             now,
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	if ip := net.ParseIP(domain); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	} else {
		tmpl.DNSNames = []string{domain}
	}

	// Always include localhost and loopback for convenience.
	if domain != "localhost" {
		tmpl.DNSNames = append(tmpl.DNSNames, "localhost")
	}
	loopback := net.ParseIP("127.0.0.1")
	hasLoopback := false
	for _, ip := range tmpl.IPAddresses {
		if ip.Equal(loopback) {
			hasLoopback = true
			break
		}
	}
	if !hasLoopback {
		tmpl.IPAddresses = append(tmpl.IPAddresses, loopback)
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		return fmt.Errorf("write certificate: %w", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return fmt.Errorf("write private key: %w", err)
	}

	return nil
}

// copyCertsToDir copies user-provided certificate and key files into certsDir
// with standard names so the Caddy container can access them via volume mount.
func copyCertsToDir(certSrc, keySrc, certsDir string) error {
	if err := os.MkdirAll(certsDir, 0755); err != nil {
		return err
	}

	certData, err := os.ReadFile(certSrc)
	if err != nil {
		return fmt.Errorf("read certificate %s: %w", certSrc, err)
	}
	if err := os.WriteFile(filepath.Join(certsDir, "cert.pem"), certData, 0644); err != nil {
		return fmt.Errorf("write certificate: %w", err)
	}

	keyData, err := os.ReadFile(keySrc)
	if err != nil {
		return fmt.Errorf("read key %s: %w", keySrc, err)
	}
	if err := os.WriteFile(filepath.Join(certsDir, "key.pem"), keyData, 0600); err != nil {
		return fmt.Errorf("write key: %w", err)
	}

	return nil
}

// GenerateClientCA creates an ECDSA P-256 CA certificate for signing client
// certificates. The CA is stored in caDir/ca.pem and caDir/ca-key.pem.
// If both files already exist the call is a no-op (safe for upgrades).
func GenerateClientCA(caDir string) error {
	certPath := filepath.Join(caDir, "ca.pem")
	keyPath := filepath.Join(caDir, "ca-key.pem")

	if fileExists(certPath) && fileExists(keyPath) {
		return nil
	}

	if err := os.MkdirAll(caDir, 0700); err != nil {
		return fmt.Errorf("create client-ca directory: %w", err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate CA key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate serial: %w", err)
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{"Bifract"},
			CommonName:   "Bifract Client CA",
		},
		NotBefore:             now,
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("create CA certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		return fmt.Errorf("write CA certificate: %w", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal CA key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return fmt.Errorf("write CA key: %w", err)
	}

	return nil
}

// GenerateClientCert creates a client certificate signed by the CA in caDir,
// and writes a PKCS#12 (.p12) bundle to outputPath for easy browser import.
// The .p12 is protected with the given password.
func GenerateClientCert(caDir, name, password, outputPath string) error {
	caCertPEM, err := os.ReadFile(filepath.Join(caDir, "ca.pem"))
	if err != nil {
		return fmt.Errorf("read CA cert: %w", err)
	}
	caKeyPEM, err := os.ReadFile(filepath.Join(caDir, "ca-key.pem"))
	if err != nil {
		return fmt.Errorf("read CA key: %w", err)
	}

	caCertBlock, _ := pem.Decode(caCertPEM)
	if caCertBlock == nil {
		return fmt.Errorf("failed to decode CA certificate PEM")
	}
	caCert, err := x509.ParseCertificate(caCertBlock.Bytes)
	if err != nil {
		return fmt.Errorf("parse CA cert: %w", err)
	}

	caKeyBlock, _ := pem.Decode(caKeyPEM)
	if caKeyBlock == nil {
		return fmt.Errorf("failed to decode CA key PEM")
	}
	caKey, err := x509.ParseECPrivateKey(caKeyBlock.Bytes)
	if err != nil {
		return fmt.Errorf("parse CA key: %w", err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate client key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("generate serial: %w", err)
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{"Bifract"},
			CommonName:   name,
		},
		NotBefore:             now,
		NotAfter:              now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	clientCertDER, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("sign client cert: %w", err)
	}

	clientCert, err := x509.ParseCertificate(clientCertDER)
	if err != nil {
		return fmt.Errorf("parse signed cert: %w", err)
	}

	p12Data, err := pkcs12.Modern.Encode(clientKey, clientCert, []*x509.Certificate{caCert}, password)
	if err != nil {
		return fmt.Errorf("create PKCS#12 bundle: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if err := os.WriteFile(outputPath, p12Data, 0600); err != nil {
		return fmt.Errorf("write .p12 file: %w", err)
	}

	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
