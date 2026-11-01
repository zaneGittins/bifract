# ClickHouse Cloud

Store Bifract's logs in [ClickHouse Cloud](https://clickhouse.com/cloud) (or another ClickHouse you already run) instead of the bundled one. Use it when you would rather not operate ClickHouse yourself. Everything else (app, ingest, PostgreSQL, Caddy) still runs on your Docker Compose host or Kubernetes cluster.

!!! warning "Experimental"
    External ClickHouse has been verified end to end against a live ClickHouse Cloud service (schema, ingest and search), but has far less production use than the bundled ClickHouse, which stays the default.

## Requirements

- **ClickHouse 25.12 or later.** Bifract refuses to start against an older self-managed server.
- **Scale tier or higher.** The Basic tier caps storage at 1 TB and 500 tables.
- **Idling disabled.** Bifract writes continuously; a service that scales to zero adds cold starts to ingest.
- **An IP access list** that allows the egress addresses of your Bifract hosts.

Private Link is optional and only applies when Bifract runs in the same cloud provider as the service. The public endpoint works from anywhere.

## Install

1. In the ClickHouse Cloud console, select your service and click **Connect**. Note the hostname, username and password. Ignore the HTTPS port the console shows: Bifract uses the native protocol over TLS on port 9440, which the installer sets for you.
2. Run the installer:

    ```bash
    sudo bifract --install       # Docker Compose
    bifract --install-k8s        # Kubernetes
    ```

3. At **ClickHouse Backend**, choose **ClickHouse Cloud**.
4. At **ClickHouse Endpoint**, enter only the hostname, for example `abc123.us-west-2.aws.clickhouse.cloud`. The installer connects to it before writing anything, so a wrong hostname, closed port or TLS problem is reported here.
5. At **ClickHouse Credentials**, enter the username (default `default`) and password. The user must be able to create the schema and Bifract's least-privilege ingest user.
6. Finish the remaining steps as in [Installation](installation.md) or [Kubernetes](kubernetes.md).

Bifract creates its schema on first start. No ClickHouse is deployed: Docker Compose omits the `clickhouse` service and volume, and Kubernetes renders no ClickHouse cluster, load balancer or ClickHouse network policy, so the ClickHouse operator and cert-manager are not needed.

For a self-managed server, choose **External ClickHouse** instead and enter `host:port` (port 9000 if omitted). To script either case, see the `clickhouse` section in [Non-interactive Install](non-interactive.md).
