# Graph Traversal

`ptg()` (Process Tree Graph) walks process lineage from a seed process, following parent-child links in either direction. It reads `proc_lineage`, a materialized view holding one row per process-create event, so each hop is a primary-key point lookup rather than a scan of `logs`.

`ptg()` starts a query rather than filtering one, so it is the first stage in the pipeline.

## Syntax

```
ptg(start="<process_guid>", depth=N, direction=forward|backward|both)
```

| Parameter | Required | Description |
|-----------|----------|-------------|
| `start` | Yes | `process_guid` of the seed node |
| `depth` | No | Max traversal depth (default: 10, max: 50) |
| `direction` | No | `forward` = descendants, `backward` = ancestors, `both` (default) |

Every row returns `process_guid`, `parent_guid`, `image`, `parent_image`, `commandline`, `computer_name`, `log_id`, plus two computed fields:

| Field | Description |
|-------|-------------|
| `_depth` | Distance from the seed (0 = start) |
| `_path` | Full path from the seed (guids joined by ` > `) |

Requires an EDR source normalized to `bifract_category='process_creation'`.

!!! warning
    The tree is scoped only by `start=`, the time range, and the fractal. A filter placed before `ptg()` is ignored, and ancestors or descendants outside the selected time range are silently omitted, so set the range to cover the whole investigation window.

## Examples

Full tree around a process, drawn as a process map:
```
ptg(start="{GUID}") | pgraph()
```

Ancestors only, 20 levels up:
```
ptg(start="{GUID}", direction=backward, depth=20)
```

Descendants as a table, pruned to three levels:
```
ptg(start="{GUID}", direction=forward)
| _depth <= 3
| table(process_guid, image, commandline, _depth)
```

## Beyond the spawn tree

`ptg()` returns process creation only. For file, network and DNS activity around the tree, anomaly scoring and cross-tree reconnection, use `pgr()`: see [Provenance Graph](../features/provenance-graph.md).
