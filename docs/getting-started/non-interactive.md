# Non-interactive Install

Pass `--config <file>` to `--install` or `--install-k8s` to answer the wizard's questions from a YAML file. Use it for cloud-init, Ansible or CI, or to keep a Kubernetes install's settings in git.

```bash
sudo bifract --install --config bifract-install.yaml      # Docker Compose
bifract --install-k8s --config bifract-install.yaml       # Kubernetes manifests
```

Without `--config` and without a terminal, both commands exit with an error instead of waiting for input.

`sudo` clears the environment, so with a `*_env` reference run `sudo -E bifract ...` or use a `*_file` reference instead.

## Config file

The file holds references to secrets, never the values. A file has either a `compose` section (for `--install`) or a `k8s` section (for `--install-k8s`), not both.

Docker Compose with ClickHouse Cloud:

```yaml
version: 1                          # required
domain: bifract.example.com         # required

access:
  mode: restrict-app                # all | restrict-app | restrict-all | mtls-app
  allowed_ips: [10.0.0.0/8, 203.0.113.5]

clickhouse:                         # omit for the bundled ClickHouse
  backend: external                 # bundled | external
  deployment: cloud                 # single-node | cluster | cloud
  host: abc123.clickhouse.cloud     # host[:port]; or hosts: [a:9000, b:9000] with cluster
  user: default
  password_file: /run/secrets/clickhouse   # or password_env: CLICKHOUSE_PASSWORD

secrets:
  admin_password_file: /run/secrets/bifract-admin   # optional; or admin_password_env

output:
  admin_password: file              # file | stdout | none

compose:
  install_dir: /opt/bifract
  tls:
    mode: letsencrypt               # self-signed | letsencrypt | custom
    email: ops@example.com
```

Kubernetes with the bundled ClickHouse:

```yaml
version: 1
domain: bifract.example.com
access:
  mode: mtls-app

k8s:
  size_profile: Small               # Dev | X-Small | Small | Medium | Large | X-Large
  ch_shards: 2
  ch_storage_gb: 500
  output_dir: ./bifract-k8s
```

## Fields

| Field | Default | Notes |
|-------|---------|-------|
| `version` | none | Must be `1`. |
| `domain` | none | Bare hostname or IPv4 address, no scheme or path. |
| `access.mode` | none | `allowed_ips` is required for `restrict-app` and `restrict-all`, and rejected for the other modes. |
| `clickhouse.backend` | `bundled` | Connection fields are only accepted with `external`. |
| `clickhouse.deployment` | inferred | `cloud` implies TLS and port 9440. |
| `clickhouse.host`, `hosts` | none | Hostname or IPv4 address with an optional `:port`. |
| `clickhouse.secure` | `false` | TLS for a self-managed server. |
| `clickhouse.database` | `logs` | |
| `clickhouse.cluster`, `fanout_cluster` | none | Self-managed clusters only. |
| `clickhouse.check_reachable` | `true` | Connects to the server before writing anything, like the wizard. |
| `secrets.admin_password_*` | generated | At least 12 characters. |
| `output.admin_password` | `file` | See [Admin password](#admin-password). |
| `compose.install_dir` | `/opt/bifract` | |
| `compose.image_tag` | installer version | |
| `compose.tls.mode` | `self-signed` | `letsencrypt` needs `email`; `custom` needs `cert_file` and `key_file`. Kubernetes has no TLS setting: Caddy gets a Let's Encrypt certificate. |
| `k8s.size_profile` | none | Required. Case-insensitive. See the [Sizing Guide](sizing.md). |
| `k8s.ch_shards` | profile value | Bundled ClickHouse only, minimum 1. |
| `k8s.ch_storage_gb` | `100` | Bundled ClickHouse only, minimum 10. |
| `k8s.output_dir` | `./bifract-k8s` | |

Validation is strict:

- Unknown fields are errors, so a typo such as `allowed_ip` fails instead of being ignored. An inline secret such as `password:` is rejected the same way.
- Conflicting settings are errors, such as `ch_shards` with an external ClickHouse or a `k8s` section passed to `--install`.
- A `*_file` reference is read with trailing newlines removed. A `*_env` reference must name a variable that is set and not empty.

## Admin password

| `output.admin_password` | Behavior |
|-------------------------|----------|
| `file` | Writes the generated password to `admin-password` (mode 0600) in the install or output directory. Nothing is printed. |
| `stdout` | Prints it in the summary, as the wizard does. Avoid this where output is logged. |
| `none` | Writes and prints nothing. Only allowed with `secrets.admin_password_file` or `admin_password_env`, since a generated password would be lost. |

A supplied admin password is never written or printed. If `admin-password` already exists from an earlier install, the install stops before deploying anything; remove the file and run again.

## Replay a wizard install

1. Run the wizard once. It saves your answers as `bifract-install.yaml` in the install directory (Docker Compose) or output directory (Kubernetes).
2. For an external ClickHouse, add `clickhouse.password_file` or `password_env` to the file.
3. Run `bifract --install --config bifract-install.yaml` (or `--install-k8s`) on the next host.

Neither command overwrites an existing install. Use `--upgrade`, `--reconfigure`, `--upgrade-k8s` or `--reconfigure-k8s` to change one.
