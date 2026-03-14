# Monitoring

Bifract exposes an optional Prometheus metrics endpoint for monitoring ingestion throughput, backpressure state, and alert engine health.

## Enabling Metrics

### Docker Compose

Add the environment variables to the `bifract` service in your `docker-compose.yml` and expose the metrics port:

```yaml
bifract:
  environment:
    BIFRACT_METRICS_ENABLED: "true"
    BIFRACT_METRICS_ADDR: ":9090"
  ports:
    - "9090:9090"    # metrics (omit to keep internal-only)
```

The metrics port is on the internal Docker network by default. Only publish it (`ports:`) if you need external access. Other containers on the same network (e.g. Prometheus) can reach it at `bifract:9090` without publishing.

To add a Prometheus scraper, add a service and a config file:

```yaml
# docker-compose.yml (add to services)
prometheus:
  image: prom/prometheus
  volumes:
    - ./prometheus.yml:/etc/prometheus/prometheus.yml
  ports:
    - "9091:9090"
  networks:
    - bifract-network
```

```yaml
# prometheus.yml
scrape_configs:
  - job_name: bifract
    static_configs:
      - targets: ["bifract:9090"]
```

### Kubernetes

Add the environment variables and a metrics port to the Bifract Deployment:

```yaml
containers:
  - name: bifract
    env:
      - name: BIFRACT_METRICS_ENABLED
        value: "true"
      - name: BIFRACT_METRICS_ADDR
        value: ":9090"
    ports:
      - containerPort: 8080
        name: http
      - containerPort: 9090
        name: metrics
```

Add the metrics port to the Service:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: bifract
  namespace: bifract
spec:
  ports:
    - port: 8080
      targetPort: 8080
      name: http
    - port: 9090
      targetPort: 9090
      name: metrics
  selector:
    app: bifract
```

If using the Prometheus Operator, create a `ServiceMonitor` to auto-discover and scrape:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: bifract
  namespace: bifract
  labels:
    release: prometheus    # match your Prometheus Operator's selector
spec:
  selector:
    matchLabels:
      app: bifract
  endpoints:
    - port: metrics
      interval: 15s
```

Alternatively, use pod annotations for annotation-based service discovery:

```yaml
# In the Deployment's pod template metadata
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "9090"
  prometheus.io/path: "/metrics"
```

To restrict metrics access to only the monitoring namespace, add a `NetworkPolicy`:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: bifract-metrics
  namespace: bifract
spec:
  podSelector:
    matchLabels:
      app: bifract
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: monitoring
      ports:
        - port: 9090
          protocol: TCP
```

!!! note "Security"
    The metrics endpoint exposes only operational counters and gauges. No log data, user information, credentials, or other secrets are included.

## Available Metrics

### Ingestion

| Metric | Type | Description |
|--------|------|-------------|
| `bifract_ingest_accepted_total` | counter | Total logs accepted into the ingestion queue |
| `bifract_ingest_inserted_total` | counter | Total logs successfully inserted into ClickHouse |
| `bifract_ingest_insert_errors_total` | counter | Total logs that failed to insert after all retries |
| `bifract_ingest_drops_total` | counter | Total logs dropped due to backpressure |
| `bifract_ingest_retries_total` | counter | Total retry attempts for failed batch inserts |
| `bifract_ingest_queue_depth` | gauge | Current number of pending batches in the queue |
| `bifract_ingest_healthy` | gauge | `1` if ingestion is healthy, `0` if under backpressure |
| `bifract_ingest_cpu_pressure` | gauge | `1` if ClickHouse CPU backpressure is active |
| `bifract_ingest_consecutive_failures` | gauge | Number of consecutive ClickHouse insert failures |

### Alerts

| Metric | Type | Description |
|--------|------|-------------|
| `bifract_alerts_cached_count` | gauge | Number of alerts currently cached by the engine |
| `bifract_alerts_evaluating` | gauge | `1` if an evaluation cycle is running |

### System

| Metric | Type | Description |
|--------|------|-------------|
| `bifract_build_info` | gauge | Always `1`; `version` label carries the build version |

## Example Alerts

Prometheus alerting rules for common scenarios:

```yaml
groups:
  - name: bifract
    rules:
      - alert: BifractIngestUnhealthy
        expr: bifract_ingest_healthy == 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Bifract ingestion is under backpressure"

      - alert: BifractHighDropRate
        expr: rate(bifract_ingest_drops_total[5m]) > 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Bifract is dropping logs due to backpressure"

      - alert: BifractInsertErrors
        expr: rate(bifract_ingest_insert_errors_total[5m]) > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "ClickHouse insert failures detected"
```
