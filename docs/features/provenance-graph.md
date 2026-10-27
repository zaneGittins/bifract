# Provenance Graph

The provenance graph (`pgr()`) rebuilds what a suspicious process did, scores every file, network, DNS and injection action by how unusual it is, and prunes the everyday noise. Use it to triage a process: a handful of suspicious actions surface instead of thousands of raw events.

![Provenance graph of a process tree](../images/features/pgr-graph.png)

## Turn it on

Provenance needs baselines built from endpoint logs, so it is off by default. A tenant admin enables it:

1. Open **Settings** in the sidebar, then the **Platform** tab.
2. Under **Endpoint analytics**, turn on **Collect baselines**.

Bifract then maintains two baselines as logs arrive: process lineage (who spawned whom) and behavior frequency (how common each file, IP and domain is across hosts). They run on every ingested log, so leave this off if you do not use `pgr()`. Baselines start from the moment you enable it.

## Investigate a process

1. Open **Query** and set the time range to cover the whole investigation; lineage outside the range is not included.
2. Find the process and copy its `process_guid`, for example:
   ```
   bifract_category=process_creation image=~powershell
   ```
3. Run:
   ```
   pgr(start="{GUID}") | pgraph()
   ```
4. Use the **‹ ›** arrows beside **N rare** in the stats bar to step through the processes with rare activity.
5. Click a process to open its drawer and pivot from there.

For just the spawn tree, without scoring or file, network and DNS activity, use `ptg(start="{GUID}") | pgraph()`. It reads only the lineage table, so it is faster. See [Graph Traversal](../bql/graph-traversal.md) for all arguments.

## Required fields

Your [normalizer](normalizers.md) must set `bifract_category` and the fields below (under the `fields` column). Every event also needs `computer_name` and a `timestamp`. `process_creation` alone builds the tree; each extra category adds a layer.

| `bifract_category` | Adds | Required fields | Sysmon event |
|--------------------|------|-----------------|---|
| `process_creation` | The spawn tree (required) | `process_guid`, `parent_process_guid`, `image`, `parent_image`, `commandline` | 1 |
| `file_write` | Files a process wrote | `process_guid`, `image`, `target_file` | 11 |
| `network_connect` | Outbound destinations | `process_guid`, `image`, `dst_ip` | 3 |
| `dns_query` | Domains a process resolved | `process_guid`, `image`, `query` | 22 |
| `remote_thread` | Injection edges (opt-in) | `source_process_guid`, `target_process_guid`, `image`, `target_image` | 8 |
| `process_access` | Handle-access edges (opt-in) | `source_process_guid`, `target_process_guid`, `image`, `target_image` | 10 |

Optional: `user` on `process_creation` (shown in the drawer) and `query_results` on `dns_query` (lets reconnection match resolved IPs).

!!! note "Injection edges are opt-in"
    `remote_thread` and `process_access` are left out by default because they force an unindexed scan per branch. Ask for them explicitly:

    ```
    pgr(start="{GUID}", include="remote_thread,process_access") | pgraph()
    ```

## Read the graph

| Control | What it does |
|---------|--------------|
| **Graph / Table** icons | Toggle between the canvas and an indented table. The table's time column shows the gap since the parent. |
| **Show all** / **Focus suspicious** | Larger scored graphs open focused: only processes on a path to red activity, plus the seed and its ancestors. The rest folds into `+N hidden` markers; click one to expand it. |
| **N rare** with **‹ ›** | Steps through processes with rare activity, centering each and opening its details. |
| Canvas controls | Zoom, fit and minimap, bottom left. The minimap is off until you turn it on. |
| Process drawer | Click a process. **Search process**, **Search hash** and **Host ±5 min** open searches in a new tab; **Copy command** copies the command line; **Hide branch** hides it (undo from the hidden branches stat). |
| Keyboard (table) | `↑` `↓` or `j` `k` move, `Enter` opens the drawer, `←` `→` collapse and expand, `Esc` closes. |

## Anomaly scoring

Every action gets an `anomaly_score` from 0 (common everywhere) to 1 (never seen before). Scores also add up along the chain, so a sequence of individually common steps (living off the land) still stands out.

- **Host-days, not events.** An action counts once per host per day, so 10,000 beacons to one IP is one host-day.
- **Leave-one-out.** The tree being scored is subtracted from the baseline, so an attack never vouches for itself.
- **Known source.** For an image that ran on other hosts, the score is how rarely it made this move elsewhere: `winword.exe` starting its first-ever child scores 1.
- **New source.** For a binary never seen on another host, the score is how rarely anyone touched the target.

Each action shows a **Why** sentence and fact chips in the drawer and on edge hover, for example `cmd.exe → node.exe: never seen from cmd.exe on any other host` with **First seen**, **Target on**, **Other host-days**, **Own** and **Inherited**. Clicking **Target on** searches that IP, domain, file or image in a new tab.

??? note "Score columns"
    | Column | Meaning |
    |--------|---------|
    | `edge_score` | The action's own score; `anomaly_score` minus it is what the chain added |
    | `score_basis` | `transition` (known source), `new_source`, `no_source` (parent never logged), or `reconnect` |
    | `edge_host_days` | Other host-days on which this source made this exact move |
    | `source_host_days` | Other host-days on which this source made any move of this type |
    | `source_exec_host_days` | Other host-days on which this source binary ran |
    | `target_host_days` | Other host-days on which any process touched the target |
    | `target_hosts`, `total_hosts` | Hosts that touched the target (capped at 256) out of all hosts |
    | `first_seen` | First day the relationship was observed |

## Reconnection

`pgr()` also pulls in other process trees that share a **rare** artifact with the seeded one: a file this tree wrote that another tree executed, or the same rare IP or domain. This is how lateral spread shows up. Common artifacts (CDNs, resolvers, update servers) do not bridge.

- Peers appear in a **Reconnected** section below the seeded tree, grouped by the artifact that links them, strongest first. Click a peer to expand its tree.
- Up to 50 peers are admitted by default. Change it with `peers=` (max 500), or turn reconnection off with `reconnect=false`:

```
pgr(start="{GUID}", peers=200) | pgraph()
```
