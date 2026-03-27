# Reverse Proxy (Caddy)

Bifract uses Caddy as its sole ingress point. All traffic flows through Caddy before reaching the Bifract backend.

## Production

Ports 80, 443, and 8443 are exposed in production. Port 443 serves the UI and API; port 8443 serves ingest endpoints exclusively. All internal services (LiteLLM, PostgreSQL, ClickHouse) are accessible only within the Docker network. Caddy supports three SSL modes.

### Let's Encrypt (Automatic)

Caddy automatically obtains and renews certificates from Let's Encrypt. Your server must be reachable on ports 80 and 443 from the internet.

```bash
BIFRACT_DOMAIN=bifract.example.com \
BIFRACT_CORS_ORIGINS=https://bifract.example.com \
BIFRACT_SECURE_COOKIES=true \
docker compose up -d
```

### Self-Signed

The setup wizard generates a self-signed certificate during installation. Useful for internal or air-gapped deployments where a public certificate is not possible. Browsers will show a certificate warning.

The generated certificate is stored in `caddy/certs/` and mounted into the Caddy container automatically. It supports both IP addresses and hostnames as SANs.

### Custom Certificates

Use your own certificate and key files (e.g. from an internal CA or purchased certificate). Mount the files into the Caddy container and reference them in the Caddyfile:

```
bifract.example.com {
    tls /etc/caddy/certs/cert.pem /etc/caddy/certs/key.pem
    ...
}
```

Mount the certificates in `docker-compose.yml`:

```yaml
caddy:
  volumes:
    - ./certs/cert.pem:/etc/caddy/certs/cert.pem:ro
    - ./certs/key.pem:/etc/caddy/certs/key.pem:ro
```

## Access Control

Bifract supports access restrictions via Caddy. Non-allowed IPs are rejected before reaching the application.

Four modes are available:

| Mode | Description |
|------|-------------|
| `restrict-app` | Only allowed IPs can access the UI and API. Any IP can hit ingestion endpoints. |
| `restrict-all` | Only allowed IPs can access anything, including ingest endpoints. |
| `mtls-app` | Require client certificates for UI and API. Ingestion endpoints do not require a client cert. |
| `all` | No access restrictions for UI, API, or ingestion (not recommended). |

### Configuration

IP access is configured during `bifract --install`, or by editing the `.env` file directly:

```bash
# Mode: restrict-app, restrict-all, mtls-app, all
BIFRACT_IP_ACCESS=restrict-app

# Comma-separated IPs or CIDR ranges (used by restrict-app and restrict-all)
BIFRACT_ALLOWED_IPS=10.0.0.0/8,192.168.1.0/24,203.0.113.5
```

To apply changes after installation, regenerate the config files and restart Caddy:

```bash
bifract --reconfigure --dir /opt/bifract
```

This re-renders the Caddyfile from `.env` and restarts Caddy without requiring a version upgrade. You can switch between any access mode at any time by editing `.env` and running `--reconfigure`.

### mTLS Client Certificates

When `mtls-app` is selected during installation, bifract automatically:
1. Generates a client CA (`caddy/client-ca/ca.pem`)
2. Creates an initial `admin.p12` client certificate
3. Configures Caddy to require client certs for non-ingest routes

To generate additional client certificates, use either the UI or the CLI:

**UI (recommended)**: Go to **Settings > Users** and click the **Cert** button next to any user. You will be prompted for a password to protect the `.p12` file, which is then downloaded directly in your browser. The Cert button only appears when `mtls-app` mode is active.

**CLI**:
```bash
bifract --gen-client-cert --name alice --password secretpass --dir /opt/bifract
```

Both methods create a PKCS#12 `.p12` file that can be imported into any browser. Each certificate is valid for 1 year.

## Access Logs

Caddy access logs are automatically shipped to the **system** fractal. Switch to the system fractal in the UI to view them.