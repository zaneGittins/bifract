"""Tools for endpoint process provenance: find a process, then expand it."""

from .. import http, provenance
from ..app import as_json, tool
from ..bql import BQLValueError, contains, equals, literal, time_window

EDGE_TYPES = {"file_write", "net_connect", "dns_query", "remote_thread", "process_access"}
DIRECTIONS = {"forward", "backward", "both"}


def _edge_list(value: str, *, argument: str) -> list[str]:
    """Validate a comma-separated edge-type list against what pgr() generates."""
    types = [t.strip().lower() for t in value.split(",") if t.strip()]
    if unknown := [t for t in types if t not in EDGE_TYPES]:
        raise BQLValueError(
            f"{argument} has unknown edge type(s) {', '.join(unknown)}. "
            f"Valid types: {', '.join(sorted(EDGE_TYPES))}."
        )
    return types


@tool
async def find_processes(
    image: str = "",
    host: str = "",
    user: str = "",
    commandline: str = "",
    start: str = "",
    end: str = "",
    limit: int = 20,
) -> str:
    """
    Find process-creation events and return their process GUIDs.

    This is the step before provenance_graph, which needs a GUID to seed on. All
    filters are optional and combine with AND; the text ones are case-insensitive
    substring matches.

    Args:
        image: Substring of the executable path (e.g. 'powershell', 'rundll32').
        host: Substring of the hostname.
        user: Substring of the account the process ran as.
        commandline: Substring of the command line (e.g. '-enc', 'Invoke-').
        start: Optional start time, RFC3339.
        end: Optional end time, RFC3339.
        limit: Max processes to return (1-100, default 20).

    Returns:
        Matching processes with process_guid, image, command line, host, user, and time.
    """
    try:
        conditions = [equals("bifract_category", "process_creation")]
        for field, value in (
            ("image", image),
            ("computer_name", host),
            ("user", user),
            ("commandline", commandline),
        ):
            if value.strip():
                conditions.append(contains(field, value.strip()))
    except BQLValueError as exc:
        return f"Error: {exc}"

    limit = max(1, min(limit, 100))
    query = (
        f"{' AND '.join(conditions)} | table(timestamp, computer_name, image, "
        f"process_guid, parent_process_guid, commandline, user) | head({limit})"
    )

    result = await http.post("/query", {"query": query, **time_window(start, end)})
    rows = result.get("results", [])
    if not rows:
        return as_json(
            {
                "query": query,
                "count": 0,
                "hint": (
                    "No process_creation events matched. Widen the time range, loosen the "
                    "filters, or confirm this fractal receives endpoint/EDR data."
                ),
            }
        )

    return as_json({"query": query, "count": len(rows), "processes": rows})


@tool
async def provenance_graph(
    guid: str,
    depth: int = 10,
    direction: str = "both",
    threshold: float = 0.7,
    include: str = "",
    exclude: str = "",
    reconnect: bool = True,
    diffuse: bool = True,
    peers: int = 50,
    limit: int = 500,
    start: str = "",
    end: str = "",
    max_activity: int = 40,
) -> str:
    """
    Build the provenance graph for a process: what it descends from and what it did.

    Runs pgr() on the seed GUID. It walks the spawn tree in both directions, scores
    every file write, network connection, DNS query, and injection by how unusual it
    is across the fleet (0 = ubiquitous, 1 = never seen before), prunes the everyday
    noise, and pulls in other process trees that share a rare artifact with this one.

    The result is returned as a rendered process tree plus the notable activity and
    cross-tree bridges, ranked by anomaly, rather than a raw edge list.

    Get a GUID from find_processes. Set the time range to cover the whole
    investigation window: lineage outside it is not included.

    Requires an admin to have enabled endpoint behavioral analytics, and endpoint/EDR
    data (for example Sysmon) normalized to bifract_category process_creation.

    Args:
        guid: The process_guid to seed on.
        depth: Tree hops to walk from the seed (1-50, default 10).
        direction: 'both' (default), 'forward' for descendants, 'backward' for ancestors.
        threshold: Drop non-spawn edges scoring below this, 0.0-1.0 (default 0.7).
                   Lower it to see more of what the tree did.
        include: Comma-separated edge types to generate instead of the default set.
                 Default is file_write,net_connect,dns_query. remote_thread and
                 process_access are opt-in: they force an unindexed scan, so request
                 them only when hunting injection.
        exclude: Comma-separated edge types to drop.
        reconnect: Pull in other trees sharing a rare file, IP, or domain (default true).
        diffuse: Propagate anomaly down the tree so a quiet step under a suspicious
                 chain still surfaces (default true).
        peers: Max reconnected peer processes admitted (1-500, default 50).
        limit: Max edges pgr() returns (1-20000, default 500).
        start: Optional start time, RFC3339.
        end: Optional end time, RFC3339.
        max_activity: Max non-spawn actions to list, highest anomaly first (default 40).

    Returns:
        The process tree as text, the notable activity, and any cross-tree reconnections.
    """
    direction = direction.strip().lower() or "both"
    if direction not in DIRECTIONS:
        return f"Error: direction must be one of {', '.join(sorted(DIRECTIONS))}."
    if not 0.0 <= threshold <= 1.0:
        return "Error: threshold must be between 0.0 and 1.0."

    try:
        args = [
            f"start={literal(guid.strip(), field='guid')}",
            f"depth={max(1, min(depth, 50))}",
            f'direction="{direction}"',
            f"threshold={threshold}",
            f"reconnect={'true' if reconnect else 'false'}",
            f"diffuse={'true' if diffuse else 'false'}",
            f"peers={max(1, min(peers, 500))}",
            f"limit={max(1, min(limit, 20000))}",
        ]
        if include.strip():
            args.append(f'include="{",".join(_edge_list(include, argument="include"))}"')
        if exclude.strip():
            args.append(f'exclude="{",".join(_edge_list(exclude, argument="exclude"))}"')
    except BQLValueError as exc:
        return f"Error: {exc}"

    query = f"pgr({', '.join(args)})"
    result = await http.post("/query", {"query": query, **time_window(start, end)})
    rows = result.get("results", [])
    if not rows:
        return as_json(
            {
                "query": query,
                "seed": guid,
                "processes": 0,
                "hint": (
                    "No provenance edges. The GUID may be outside the time range, the "
                    "fractal may not carry process_creation events, or endpoint "
                    "behavioral analytics may be disabled (Admin > Settings > Features)."
                ),
            }
        )

    graph = provenance.summarize(rows, max_activity=max(1, max_activity))
    return as_json(
        {
            "query": query,
            "seed": guid,
            "edges_returned": len(rows),
            "edges_truncated": len(rows) >= max(1, min(limit, 20000)),
            "execution_ms": result.get("execution_ms", 0),
            **graph,
        }
    )
