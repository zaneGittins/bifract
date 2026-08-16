# ClickHouse Cloud

Bifract can store its logs in a ClickHouse you already run instead of the one the installer deploys. That includes [ClickHouse Cloud](https://clickhouse.com/cloud), where ClickHouse operates the database for you and Bifract connects to it over TLS.

Everything else is unchanged: the app, ingest tier, Postgres and Caddy still run wherever you deploy them, on Docker Compose or Kubernetes. Only the log storage moves.

!!! warning "Experimental"
    External ClickHouse support is new. It has been verified end to end against a live ClickHouse Cloud service (schema provisioning, ingestion and search), but it has far less production exposure than the bundled ClickHouse.

## When this is worth it

Running ClickHouse yourself at scale means owning shard provisioning, merges, Keeper, and upgrades. A managed service removes that work, at the cost of a service bill and less control over the version you run.

The bundled ClickHouse remains the default and the best-tested path. Choose an external one when you would rather not operate the database.

## Requirements

- **Scale tier or higher.** The Basic tier caps storage at 1 TB and 500 tables, which a log platform reaches quickly.
- **Idling disabled.** Bifract ingests continuously; a service that scales to zero adds cold starts to the write path.
- **An IP access list** covering the egress addresses of wherever Bifract runs. Services are reachable over the public internet by default.

Private Link is optional and only applies when Bifract runs inside the same cloud provider as the service. Connecting from elsewhere (a DigitalOcean droplet to a service on AWS, for example) works fine over the public endpoint.

## Getting the connection details

In the ClickHouse Cloud console, select your service and click **Connect**. Take the hostname, username and password.

The console shows an HTTPS example on port 8443. Bifract uses the **native protocol on port 9440** instead, which is the same hostname and credentials on a different port. The installer applies port 9440 and TLS for you, so you only enter the hostname:

```
your-subdomain.us-west-2.aws.clickhouse.cloud
```

## Installing

Run the installer as usual. At the **ClickHouse Backend** step choose **ClickHouse Cloud**, then enter the hostname followed by the username and password.

```bash
sudo bifract --install       # Docker Compose
sudo bifract --install-k8s   # Kubernetes
```

The installer dials the endpoint before writing any files, so a wrong hostname, a closed port or a TLS mismatch is reported while you are still in the wizard.

No ClickHouse is deployed. On Docker Compose the `clickhouse` service and its volume are omitted; on Kubernetes no `ClickHouseInstallation`, load balancer service or ClickHouse network policy is rendered, and the ClickHouse operator is not required.

Bifract creates its own schema on first start, exactly as it does against a bundled ClickHouse.