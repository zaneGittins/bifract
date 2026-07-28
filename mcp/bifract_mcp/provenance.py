"""Shape pgr() edge rows into a process tree a model can read.

pgr() returns a flat edge list (parent, child, label, event_type, anomaly_score, ...).
Handing hundreds of those rows to a model wastes context and buries the structure, so
this rebuilds the spawn tree, renders it as an outline, and reports the non-spawn
activity and cross-tree bridges separately, highest anomaly first.
"""

SPAWN = "spawn"
RECONNECT_PREFIX = "reconnect_"

MAX_COMMAND_LINE = 200
MAX_TREE_DEPTH = 64


def _f(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _s(value) -> str:
    return value if isinstance(value, str) else ("" if value is None else str(value))


def _truncate(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


class Process:
    """A node in the spawn tree, built from the spawn edge that created it."""

    __slots__ = ("guid", "image", "command_line", "host", "user", "timestamp", "anomaly", "log_id")

    def __init__(self, guid: str, row: dict):
        self.guid = guid
        self.image = _s(row.get("label"))
        self.command_line = _s(row.get("command_line"))
        self.host = _s(row.get("host"))
        self.user = _s(row.get("proc_user"))
        self.timestamp = _s(row.get("timestamp"))
        self.anomaly = _f(row.get("anomaly_score"))
        self.log_id = _s(row.get("log_id"))

    def headline(self) -> str:
        parts = [self.image or self.guid, f"guid={self.guid}", f"anomaly={self.anomaly:.2f}"]
        if self.log_id:
            parts.append(f"log={self.log_id}")
        if self.user:
            parts.append(f"user={self.user}")
        return "  ".join(parts)


def _partition(rows: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    spawns, activity, bridges = [], [], []
    for row in rows:
        event_type = _s(row.get("event_type"))
        if event_type == SPAWN:
            spawns.append(row)
        elif event_type.startswith(RECONNECT_PREFIX):
            bridges.append(row)
        elif event_type:
            activity.append(row)
    return spawns, activity, bridges


def _build_tree(spawns: list[dict]) -> tuple[dict[str, Process], dict[str, list[str]], list[str]]:
    """Return processes by guid, the parent -> children map, and the root guids.

    A root is a process whose parent was not itself returned: either the walk reached
    the edge of the time window, or it is a reconnected peer's own tree.
    """
    processes: dict[str, Process] = {}
    children: dict[str, list[str]] = {}
    parents: dict[str, str] = {}

    for row in spawns:
        child = _s(row.get("child"))
        if not child or child in processes:
            continue
        parent = _s(row.get("parent"))
        processes[child] = Process(child, row)
        parents[child] = parent
        children.setdefault(parent, []).append(child)

    roots = [guid for guid in processes if parents[guid] not in processes]
    return processes, children, roots


def _render(
    processes: dict[str, Process],
    children: dict[str, list[str]],
    roots: list[str],
) -> str:
    """Draw the spawn tree as an indented outline, most anomalous sibling first."""
    lines: list[str] = []
    visited: set[str] = set()

    def sorted_children(guid: str) -> list[str]:
        kids = [k for k in children.get(guid, []) if k in processes]
        return sorted(kids, key=lambda k: -processes[k].anomaly)

    def walk(guid: str, prefix: str, depth: int) -> None:
        if guid in visited or depth > MAX_TREE_DEPTH:
            return
        visited.add(guid)
        kids = sorted_children(guid)
        for index, kid in enumerate(kids):
            last = index == len(kids) - 1
            child = processes[kid]
            lines.append(f"{prefix}{'└─ ' if last else '├─ '}{child.headline()}")
            child_prefix = prefix + ("   " if last else "│  ")
            if child.command_line and child.command_line != child.image:
                lines.append(f"{child_prefix}cmd: {_truncate(child.command_line, MAX_COMMAND_LINE)}")
            walk(kid, child_prefix, depth + 1)

    for root in sorted(roots, key=lambda g: -processes[g].anomaly):
        if root in visited:
            continue
        proc = processes[root]
        host = f"[{proc.host}] " if proc.host else ""
        lines.append(f"{host}{proc.headline()}")
        if proc.command_line and proc.command_line != proc.image:
            lines.append(f"cmd: {_truncate(proc.command_line, MAX_COMMAND_LINE)}")
        walk(root, "", 0)
        lines.append("")

    # Any process not reachable from a root (a cycle in lineage data) still gets listed.
    for guid, proc in processes.items():
        if guid not in visited:
            lines.append(f"(detached) {proc.headline()}")

    return "\n".join(lines).strip()


def _activity(rows: list[dict], processes: dict[str, Process], limit: int) -> list[dict]:
    """Non-spawn edges (file/network/DNS/injection), most anomalous first."""
    entries = []
    for row in rows:
        parent = _s(row.get("parent"))
        proc = processes.get(parent)
        entry = {
            "type": _s(row.get("event_type")),
            "target": _s(row.get("label")),
            "anomaly": round(_f(row.get("anomaly_score")), 4),
            "process": parent,
            "image": proc.image if proc else "",
            "host": _s(row.get("host")) or (proc.host if proc else ""),
            "timestamp": _s(row.get("timestamp")),
            "log_id": _s(row.get("log_id")),
        }
        entries.append({k: v for k, v in entry.items() if v != ""})
    entries.sort(key=lambda e: -e.get("anomaly", 0))
    return entries[:limit]


def _bridges(rows: list[dict], limit: int) -> list[dict]:
    """Cross-tree reconnections.

    For net/dns both trees converge on the shared artifact, so `target` is that
    artifact node. For file it is the writer process -> the process that executed it.
    """
    entries = []
    for row in rows:
        entry = {
            "type": _s(row.get("event_type")).removeprefix(RECONNECT_PREFIX),
            "source": _s(row.get("parent")),
            "target": _s(row.get("child")),
            "shared_artifact": _s(row.get("label")),
            "anomaly": round(_f(row.get("anomaly_score")), 4),
            "host": _s(row.get("host")),
            "log_id": _s(row.get("log_id")),
        }
        entries.append({k: v for k, v in entry.items() if v != ""})
    entries.sort(key=lambda e: -e.get("anomaly", 0))
    return entries[:limit]


def summarize(rows: list[dict], *, max_activity: int = 40, max_bridges: int = 25) -> dict:
    """Turn a pgr() result set into a tree, its notable activity, and its bridges."""
    spawns, activity, bridges = _partition(rows)
    processes, children, roots = _build_tree(spawns)

    counts: dict[str, int] = {}
    for row in rows:
        event_type = _s(row.get("event_type")) or "unknown"
        counts[event_type] = counts.get(event_type, 0) + 1

    hosts = sorted({p.host for p in processes.values() if p.host})
    shown_activity = _activity(activity, processes, max_activity)
    shown_bridges = _bridges(bridges, max_bridges)

    return {
        "processes": len(processes),
        "roots": len(roots),
        "hosts": hosts,
        "edge_counts": counts,
        "process_tree": _render(processes, children, roots),
        "notable_activity": shown_activity,
        "activity_omitted": max(0, len(activity) - len(shown_activity)),
        "reconnections": shown_bridges,
        "reconnections_omitted": max(0, len(bridges) - len(shown_bridges)),
    }
