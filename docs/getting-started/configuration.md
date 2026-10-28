# Configuration

Bifract is configured with environment variables. The installer generates them, with random secrets, so most installs only change a few later (domain, SSO, archive). On Docker Compose they live in `/opt/bifract/.env`; on Kubernetes, in the `bifract-secrets` Secret (see [Post-deploy configuration](kubernetes.md#post-deploy-configuration)).

## Change a setting (Docker Compose)

1. Edit `/opt/bifract/.env`.
2. Apply it:
    - Domain, TLS or IP access: `sudo bifract --reconfigure` (regenerates the Caddyfile and Compose file, then restarts what changed).
    - Anything else: `cd /opt/bifract && sudo docker compose up -d`.

!!! note
    `--reconfigure` and `--upgrade` rewrite `.env`. Keys you added yourself are kept at the end of the file; comments are not.

## Variables

**Network and access** (apply with `--reconfigure`):

| Variable | Description |
|---|---|
| `BIFRACT_DOMAIN` | Hostname or IPv4 address Caddy serves. When you change it, update `BIFRACT_CORS_ORIGINS` to match. |
| `BIFRACT_CORS_ORIGINS` | Comma-separated allowed origins. The installer sets `https://<domain>`. |
| `BIFRACT_SSL_MODE` | `self-signed`, `letsencrypt` or `custom` |
| `BIFRACT_SSL_EMAIL` | Let's Encrypt contact email |
| `BIFRACT_CERT_PATH`, `BIFRACT_KEY_PATH` | Certificate and key for `custom` |
| `BIFRACT_IP_ACCESS` | `all`, `restrict-app`, `restrict-all` or `mtls-app`. See [Access control](../administration/reverse-proxy.md#access-control). |
| `BIFRACT_ALLOWED_IPS` | Comma-separated IPs or CIDRs for the `restrict-*` modes |
| `BIFRACT_SECURE_COOKIES` | `true` sets the `Secure` flag on session cookies. The installer enables it for any domain other than `localhost`. |

**Application** (apply with `docker compose up -d`):

| Variable | Default | Description |
|---|---|---|
| `BIFRACT_BASE_URL` | `https://<domain>` | Base URL used in alert links and as the OIDC redirect base |
| `BIFRACT_EXCHANGE_URL` | Bifract repository on GitHub | Index the [Exchange](../features/exchange.md) reads, for example a mirror for an offline install. `off` removes it |
| `BIFRACT_MEM_LIMIT` | `4g` | Memory limit of the app container |
| `BIFRACT_INGEST_MEM_LIMIT` | `4g` | Memory limit of the ingest container |
| `BIFRACT_LLM_PROVIDER`, `BIFRACT_LLM_API_KEY`, `BIFRACT_LLM_MODEL`, `BIFRACT_LLM_BASE_URL` | `anthropic`, empty | Model provider for [AI Chat](../features/ai-chat.md) |
| `BIFRACT_OIDC_*` | empty | Single sign-on. See [OIDC / SSO](../administration/oidc-sso.md). |
| `BIFRACT_S3_*` | empty | Off-host PostgreSQL backups. See [Backup & Restore](../administration/backup-restore.md). |
| `BIFRACT_ARCHIVE_*` | archive off | Iceberg archive. See [Iceberg Archive](../administration/backup-restore.md#iceberg-archive). |
| `MAXMIND_LICENSE_KEY`, `MAXMIND_ACCOUNT_ID` | empty | GeoIP enrichment |
| `BIFRACT_INGEST_*` | see tuning | Ingest queue, workers and rate limit. See [Ingestion Tuning](../ingestion/tuning.md). |

**Generated secrets** (set once by the installer; do not change):

| Variable | Why it must not change |
|---|---|
| `BIFRACT_PASSWORD_PEPPER` | Mixed into every password hash; changing it locks out all local users |
| `BIFRACT_FEED_ENCRYPTION_KEY` | Encrypts Git credentials for alert feeds |
| `BIFRACT_BACKUP_ENCRYPTION_KEY` | Encrypts PostgreSQL backups; without it they cannot be restored |
| `POSTGRES_PASSWORD`, `CLICKHOUSE_PASSWORD`, `BIFRACT_INGEST_*_PASSWORD` | Database and service credentials already stored in those services |

!!! note
    The full list of variables the server reads, with comments, is in [`.env.example`](https://github.com/zaneGittins/bifract/blob/main/.env.example). The installer's Compose file fixes some of them (database hosts, ports, `BIFRACT_MAX_QUERY_ROWS`), so setting those in `.env` has no effect on a wizard install.
