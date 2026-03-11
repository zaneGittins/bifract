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
| `restrict-app` | Only allowed IPs can access the UI and API. Ingest endpoints are on port 8443 only. |
| `restrict-all` | Only allowed IPs can access anything, including ingest endpoints (port 8443). |
| `mtls-app` | Require client certificates for UI/API on port 443. Ingest on port 8443 does not require client certs. |
| `all` | No restrictions on port 443. Ingest on port 8443 (not recommended for production). |

### Configuration

IP access is configured during `bifractctl --install`, or by editing the `.env` file directly:

```bash
# Mode: restrict-app, restrict-all, mtls-app, all
BIFRACT_IP_ACCESS=restrict-app

# Comma-separated IPs or CIDR ranges (used by restrict-app and restrict-all)
BIFRACT_ALLOWED_IPS=10.0.0.0/8,192.168.1.0/24,203.0.113.5
```

To apply changes after installation, regenerate the config files and restart Caddy:

```bash
bifractctl --reconfigure --dir /opt/bifract
```

This re-renders the Caddyfile from `.env` and restarts Caddy without requiring a version upgrade. You can switch between any access mode at any time by editing `.env` and running `--reconfigure`.

### How it works

**IP restriction modes** use Caddy `remote_ip` matchers. In `restrict-app` mode, the generated Caddyfile contains:

```
@blocked_app {
    not path /api/v1/ingest* /_bulk
    not remote_ip 10.0.0.0/8 192.168.1.0/24 10.10.1.15
}
respond @blocked_app 403
```

In `restrict-all` mode, the path exclusion is removed and all routes are gated.

**mTLS mode** uses Caddy's `client_auth` with `require_and_verify` on port 443. Connections without a valid client certificate are rejected at the TLS layer. Ingest traffic on port 8443 does not require client certificates.

### mTLS Client Certificates

When `mtls-app` is selected during installation, bifractctl automatically:
1. Generates a client CA (`caddy/client-ca/ca.pem`)
2. Creates an initial `admin.p12` client certificate
3. Configures Caddy to require client certs for non-ingest routes

To generate additional client certificates:

```bash
bifractctl --gen-client-cert --name alice --password secretpass --dir /opt/bifract
```

This creates a `.p12` file in `caddy/client-ca/` that can be imported into any browser. Each certificate is valid for 1 year.

### Notes

- Both individual IPs (`10.10.1.15`) and CIDR ranges (`10.0.0.0/8`) are supported for IP restriction modes.
- If `BIFRACT_IP_ACCESS` is set to a restrict mode but `BIFRACT_ALLOWED_IPS` is empty, the restriction is silently skipped to prevent lockout.

## Development

In development, Caddy runs in HTTP-only mode on port 8080:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

**Note**: Database ports (PostgreSQL 5432, ClickHouse 8123/9000) are exposed for development. Do not use this configuration in production.

## Access Logs

Caddy access logs are automatically shipped to the **system** fractal. Switch to the system fractal in the UI to view them.