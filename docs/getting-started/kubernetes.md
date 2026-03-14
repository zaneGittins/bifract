# Kubernetes Deployment

ClickHouse scales vertically on a single node exceptionally well, but when you need high availability or have outgrown the resources of a single machine, Bifract supports deploying across a Kubernetes cluster. This guide walks through deploying to a managed Kubernetes provider such as DigitalOcean DOKS, AWS EKS, or GKE.

Docker Compose remains the primary and simplest deployment method. See [Installation](installation.md) for the standard setup.

## Prerequisites

- A running Kubernetes cluster (1.28+)
- `kubectl` configured and connected to your cluster
- `helm` v3.0+ installed
- A domain name

## Architecture

```mermaid
graph TB
    users["Users / Browsers"]
    sources["Log Sources"]

    subgraph k8s ["Kubernetes Cluster (bifract namespace)"]
        caddy["Caddy (LoadBalancer)<br/><small>Reverse Proxy + TLS + Log Shipper</small>"]
        bifract["Bifract x2<br/><small>Stateless Replicas</small>"]
        litellm["LiteLLM<br/><small>AI Proxy</small>"]
        pg[("PostgreSQL<br/><small>StatefulSet</small>")]

        subgraph ch ["ClickHouse Cluster (Operator-managed)"]
            subgraph shard0 ["Shard 0"]
                ch0r0[("Replica 0")]
                ch0r1[("Replica 1")]
            end
            subgraph shard1 ["Shard 1"]
                ch1r0[("Replica 0")]
                ch1r1[("Replica 1")]
            end
        end

        keeper[("ClickHouse Keeper<br/><small>Coordinates replication</small>")]
    end

    users -->|"HTTPS :443"| caddy
    sources -->|"HTTPS :8443"| caddy
    caddy -->|":8080"| bifract
    bifract --> pg
    bifract -->|"Distributed table"| ch
    bifract --> litellm
    ch0r0 <-->|"replication"| ch0r1
    ch1r0 <-->|"replication"| ch1r1
    ch0r0 & ch0r1 & ch1r0 & ch1r1 --> keeper
```

Traffic flow is enforced by NetworkPolicies: only Caddy accepts external traffic, only Caddy can reach Bifract, only Bifract can reach the databases and LiteLLM. A log shipper sidecar in the Caddy pod ships access logs to the Bifract system fractal for audit visibility.

## Step 1: Install the ClickHouse Operator

Bifract uses the [official ClickHouse Kubernetes Operator](https://clickhouse.com/docs/clickhouse-operator) to manage ClickHouse and Keeper clusters.

First, install cert-manager (required by the operator):

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.17.2/cert-manager.yaml
```

Wait for cert-manager to be ready:

```bash
kubectl -n cert-manager wait --for=condition=ready pod -l app.kubernetes.io/instance=cert-manager --timeout=120s
```

Then install ClickHouse operator:

```bash
helm install clickhouse-operator -n clickhouse-operator-system --create-namespace \
  oci://ghcr.io/clickhouse/clickhouse-operator-helm
```

Verify it's running:

```bash
kubectl -n clickhouse-operator-system get pods
```

## Step 2: Generate Manifests

Run the Kubernetes install wizard:

```bash
bifract --install-k8s
```

The wizard will prompt for:

| Setting | Description | Example |
|---------|-------------|---------|
| Domain | Your domain name | `bifract.example.com` |
| SSL mode | Let's Encrypt or custom cert | Let's Encrypt |
| IP access | Traffic restriction mode (includes mTLS option) | Allow all |
| Resource profile | Cluster sizing preset (X-Small through X-Large) | Small |
| CH shards | ClickHouse shards for horizontal scaling | `1` |
| CH replicas | ClickHouse replicas per shard (2+ for HA) | `2` |
| CH storage | Storage per replica in GB | `100` |
| Output dir | Where to write manifests | `./bifract-k8s` |

The resource profile sets CPU and memory requests/limits for all components based on your expected workload. Shard and replica counts are pre-filled by the profile but can be adjusted. See [Sizing Guide](sizing.md) for details on each profile.

This generates a complete set of Kustomize manifests with secure credentials in the output directory. Save the admin password displayed at the end.

## Step 3: Deploy

```bash
kubectl apply -k ./bifract-k8s
```

Watch the pods come up:

```bash
kubectl -n bifract get pods -w
```

You should see:

- 1 PostgreSQL pod
- 1 ClickHouse Keeper pod (managed by the operator via `KeeperCluster`)
- 2 ClickHouse replica pods (managed by the operator via `ClickHouseCluster`)
- 2 Bifract pods
- 1 Caddy pod (with a log shipper sidecar)
- 1 LiteLLM pod

ClickHouse and Keeper pods may take a minute as the operator creates and configures them.

## Step 4: Configure DNS

Get the load balancer's external IP:

```bash
kubectl -n bifract get svc caddy
```

Create an A record for your domain pointing to the external IP of Caddy. Once DNS propagates, Caddy will automatically provision a Let's Encrypt certificate. You can then log in at `https://your-domain.com` with the admin credentials from Step 2.

## Verification

Check the cluster is healthy:

```bash
# Bifract health
curl https://bifract.example.com/api/v1/health

# Bifract pod logs
kubectl -n bifract logs -l app=bifract --tail=50

# ClickHouse cluster status
kubectl -n bifract exec -it bifract-ch-clickhouse-0-0-0 -- \
  clickhouse-client --query "SELECT * FROM system.clusters"

# Network policies
kubectl -n bifract get networkpolicies
```

## Scaling ClickHouse

The default deployment creates 1 shard with 2 replicas. Both shard and replica counts can be configured during `--install-k8s`. Replicas provide high availability within a shard. Shards distribute data across multiple nodes for increased storage capacity and query throughput.

**Adding replicas** (HA within a shard): edit the `ClickHouseCluster` resource and increase `replicas`. Then update the `CLICKHOUSE_HOSTS` env var in the Bifract deployment to include the new replica hostnames. Hostnames follow the pattern `bifract-ch-clickhouse-{shard}-{replica}-0.bifract-ch-clickhouse-headless`.

**Adding shards** (horizontal scaling): edit the `ClickHouseCluster` resource and increase `shards`:

```yaml
spec:
  shards: 2
  replicas: 2
```

This creates 2 shards with 2 replicas each (4 total ClickHouse pods). Update `CLICKHOUSE_HOSTS` to include all hosts:

```
bifract-ch-clickhouse-0-0-0.bifract-ch-clickhouse-headless,bifract-ch-clickhouse-0-1-0.bifract-ch-clickhouse-headless,bifract-ch-clickhouse-1-0-0.bifract-ch-clickhouse-headless,bifract-ch-clickhouse-1-1-0.bifract-ch-clickhouse-headless
```

Bifract's Distributed table automatically routes queries across all shards and distributes writes evenly using random sharding.

## Post-Deploy Configuration

Optional features are configured by editing the `bifract-secrets` Secret. The generated manifests include empty placeholders for all optional integrations. To enable a feature, populate the relevant keys and restart Bifract:

```bash
kubectl -n bifract edit secret bifract-secrets
kubectl rollout restart deployment bifract -n bifract
```

| Feature | Secret Keys | Docs |
|---------|------------|------|
| OIDC / SSO | `OIDC_ISSUER_URL`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET`, `OIDC_REDIRECT_URL`, `OIDC_SCOPES`, `OIDC_DEFAULT_ROLE`, `OIDC_ALLOWED_DOMAINS`, `OIDC_BUTTON_TEXT` | [OIDC/SSO](../administration/oidc-sso.md) |
| S3 Backups | `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION` | [Backup & Restore](../administration/backup-restore.md) |
| GeoIP Enrichment | `MAXMIND_LICENSE_KEY`, `MAXMIND_ACCOUNT_ID` | [Field Operations](../bql/field-operations.md) |
| AI Chat | `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` | [AI Chat](../features/ai-chat.md) |

## Updating Bifract

After pushing a new container image, restart the deployment to pull it:

```bash
kubectl rollout restart deployment bifract -n bifract
```

## Troubleshooting

**Bifract pods crash-looping:** Check logs with `kubectl -n bifract logs <pod>`. Usually means the databases are still starting. The pods will self-recover once PostgreSQL and ClickHouse are ready.

**ClickHouse pods stuck in Pending:** Run `kubectl -n bifract describe pod <pod>` and check for PVC/storage class issues. Most managed Kubernetes providers have a default storage class that works automatically.

**SSL errors / Let's Encrypt not issuing cert:** Verify DNS is pointing to the load balancer IP and that ports 80/443 are reachable. Check Caddy logs with `kubectl -n bifract logs -l app=caddy`. If Caddy attempted certificate issuance before DNS was ready, it may have cached a failed state. Restart the Caddy pod to retry:

```bash
kubectl rollout restart deployment caddy -n bifract
```

**ClickHouse `KEEPER_EXCEPTION` or connection timeouts:** Usually a network policy issue. Verify the policies are applied and that pod labels match:

```bash
kubectl -n bifract get networkpolicies
kubectl -n bifract get pods --show-labels
```

The ClickHouse pods should have `app.kubernetes.io/instance=bifract-ch-clickhouse` and Keeper pods should have `app.kubernetes.io/instance=bifract-keeper-keeper`.

**Client IPs showing as internal addresses:** The Caddy Service uses `externalTrafficPolicy: Local` to preserve client source IPs. If you see internal 10.x.x.x addresses in logs, verify this setting is present on the `caddy` Service. Note that `externalTrafficPolicy: Local` requires at least one Caddy pod running on a node that receives traffic from the load balancer.

**Password mismatch after regenerating manifests:** If you re-run `--install-k8s` (which generates new passwords) but the databases still have data from a previous deployment, the credentials will not match. Delete the database PVCs and reapply:

```bash
# PostgreSQL
kubectl -n bifract delete statefulset postgres
kubectl -n bifract delete pvc postgres-data-postgres-0
# ClickHouse
kubectl -n bifract delete clickhousecluster bifract-ch
kubectl -n bifract delete pvc -l app.kubernetes.io/instance=bifract-ch-clickhouse
# Keeper
kubectl -n bifract delete keepercluster bifract-keeper
kubectl -n bifract delete pvc -l app.kubernetes.io/instance=bifract-keeper-keeper
# Reapply
kubectl apply -k ./bifract-k8s
```

## Cleanup

```bash
kubectl delete -k ./bifract-k8s
```

This removes all Bifract resources but preserves PersistentVolumeClaims. To fully clean up storage:

```bash
kubectl -n bifract delete pvc --all
kubectl delete namespace bifract
```
