# CEF over Syslog with Vector

Bifract has no CEF parser, and the **Key=Value** parser cannot read CEF: it skips the pipe-delimited header and cuts unquoted values at the first space (`UNIFIhost=UDM Pro` becomes `UDM`). Run [Vector](https://vector.dev) next to the device instead. It receives the syslog stream, parses CEF, and posts JSON to Bifract.

This page uses UniFi Network as the example. Any device that sends CEF over syslog works the same way.

```
UniFi --(syslog, UDP 5514)--> Vector --(HTTPS, NDJSON)--> Bifract :8443
```

## 1. Create an ingest token

On the fractal's **Ingest** page, click **Create token**, name it (for example `unifi`), and keep the **JSON** parser. See [Sending Logs](overview.md#create-a-token-for-a-new-source).

## 2. Configure Vector

Save as `vector.yaml`:

```yaml
sources:
  unifi:
    type: socket
    mode: udp
    address: 0.0.0.0:5514

transforms:
  unifi_cef:
    type: remap
    inputs: [unifi]
    drop_on_error: true
    source: |
      cef = parse_cef!(.message)
      . = merge(cef, {"timestamp": .timestamp, "observer_ip": .host, "message": .message})

sinks:
  bifract:
    type: http
    inputs: [unifi_cef]
    uri: https://bifract.example.com:8443/api/v1/ingest
    method: post
    auth:
      strategy: bearer
      token: "${BIFRACT_TOKEN}"
    encoding:
      codec: json
    framing:
      method: newline_delimited
    compression: gzip
```

What each part does:

- **`socket` source**: listens for raw syslog lines on UDP 5514. Port 514 needs root, so pick a high port unless Vector runs privileged.
- **`parse_cef!`**: skips the syslog header and returns the CEF header (`deviceVendor`, `deviceProduct`, `deviceEventClassId`, `name`, `severity`, ...) plus every extension key (`UNIFIcategory`, `src`, `msg`, ...) as top-level fields.
- **`merge`**: keeps the original line in `message`, the receive time in `timestamp`, and the sender's address in `observer_ip`.
- **`drop_on_error`**: drops lines that are not CEF instead of forwarding them unparsed.
- **`http` sink**: sends gzipped NDJSON with the token as a Bearer header. Vector retries `429` and `503` responses on its own; see [status codes](../api/ingestion.md#status-codes).

Run it with the token in the environment:

```bash
docker run -d --name vector --restart unless-stopped \
  -p 5514:5514/udp \
  -e BIFRACT_TOKEN=bifract_ingest_... \
  -v "$PWD/vector.yaml:/etc/vector/vector.yaml:ro" \
  timberio/vector:0.51.0-debian
```

## 3. Point UniFi at Vector

In UniFi Network, open **Settings > CyberSecure > Traffic Logging** (**Settings > System > Integrations** on older versions), enable **Activity Logging (Syslog)**, choose **SIEM Server**, and enter the Vector host's IP with port `5514`.

## Result

A UniFi line such as:

```
<14>Feb 27 10:22:11 UDM-Pro CEF:0|Ubiquiti|UniFi Network|9.0.114|544|Admin Accessed UniFi Network|1|UNIFIcategory=System UNIFIsubCategory=Admin UNIFIhost=UDM Pro UNIFIadmin=jdoe src=192.168.1.5 msg=jdoe accessed UniFi Network using the web.
```

arrives in Bifract as:

| Field | Value |
|---|---|
| `deviceVendor` | `Ubiquiti` |
| `deviceProduct` | `UniFi Network` |
| `deviceEventClassId` | `544` |
| `name` | `Admin Accessed UniFi Network` |
| `severity` | `1` |
| `UNIFIcategory` | `System` |
| `UNIFIhost` | `UDM Pro` |
| `UNIFIadmin` | `jdoe` |
| `src` | `192.168.1.5` |
| `msg` | `jdoe accessed UniFi Network using the web.` |

Search it like any other source:

```
deviceProduct="UniFi Network" UNIFIcategory=System
```

To rename fields (for example to snake_case), attach a [normalizer](../features/normalizers.md) to the token.

## Troubleshooting

- **Nothing arrives**: run `docker logs vector`. A `401` means a wrong or disabled token. Check that UDP 5514 is open between UniFi and the Vector host.
- **Some events missing**: lines that fail CEF parsing are dropped. Remove `drop_on_error: true` to see the errors in Vector's log.
- **Event time is the receive time**: the RFC 3164 syslog header has no year or time zone, so the example stamps events with Vector's receive time, which is normally within a second of the event.
