"""Tests for shaping pgr() edge rows into a readable graph."""

from bifract_mcp import provenance


def spawn(parent, child, label, anomaly=0.5, **extra):
    return {
        "parent": parent,
        "child": child,
        "label": label,
        "event_type": "spawn",
        "anomaly_score": anomaly,
        "log_id": f"log-{child}",
        "host": "HOST-1",
        "command_line": label,
        "proc_user": "",
        "timestamp": "2026-07-07 14:00:00.000",
        **extra,
    }


def leaf(parent, target, event_type, anomaly):
    return {
        "parent": parent,
        "child": f"{event_type}:{target}",
        "label": target,
        "event_type": event_type,
        "anomaly_score": anomaly,
        "log_id": f"log-{target}",
        "host": "HOST-1",
        "command_line": "",
        "proc_user": "",
        "timestamp": "2026-07-07 14:01:00.000",
    }


def test_builds_tree_from_spawn_edges():
    rows = [
        spawn("ROOT", "A", "explorer.exe", 0.6),
        spawn("A", "B", "powershell.exe", 0.9),
        spawn("B", "C", "cmd.exe", 0.2),
    ]
    graph = provenance.summarize(rows)

    assert graph["processes"] == 3
    assert graph["roots"] == 1
    assert graph["hosts"] == ["HOST-1"]
    assert graph["edge_counts"] == {"spawn": 3}

    tree = graph["process_tree"]
    assert "explorer.exe" in tree
    assert tree.index("explorer.exe") < tree.index("powershell.exe") < tree.index("cmd.exe")
    assert "guid=A" in tree


def test_orders_siblings_by_anomaly():
    rows = [
        spawn("ROOT", "A", "explorer.exe"),
        spawn("A", "quiet", "notepad.exe", 0.1),
        spawn("A", "loud", "rundll32.exe", 0.95),
    ]
    tree = provenance.summarize(rows)["process_tree"]
    assert tree.index("rundll32.exe") < tree.index("notepad.exe")


def test_separate_roots_for_disconnected_trees():
    rows = [
        spawn("OUT1", "A", "explorer.exe"),
        spawn("OUT2", "X", "svchost.exe"),
        spawn("X", "Y", "cmd.exe"),
    ]
    graph = provenance.summarize(rows)
    assert graph["roots"] == 2
    assert graph["processes"] == 3


def test_command_line_shown_only_when_it_adds_information():
    rows = [
        spawn("ROOT", "A", "explorer.exe", command_line="explorer.exe"),
        spawn("A", "B", "powershell.exe", command_line="powershell -enc SQBFAFgA"),
    ]
    tree = provenance.summarize(rows)["process_tree"]
    assert "cmd: powershell -enc SQBFAFgA" in tree
    assert "cmd: explorer.exe" not in tree


def test_long_command_line_is_truncated():
    rows = [spawn("ROOT", "A", "cmd.exe", command_line="x" * 500)]
    tree = provenance.summarize(rows)["process_tree"]
    assert "..." in tree
    assert len(max(tree.splitlines(), key=len)) < 400


def test_lineage_cycle_does_not_hang():
    rows = [
        spawn("OUT", "A", "a.exe"),
        spawn("A", "B", "b.exe"),
        spawn("B", "A", "a.exe"),
    ]
    graph = provenance.summarize(rows)
    assert graph["processes"] == 2
    assert "a.exe" in graph["process_tree"]


def test_every_process_appears_even_when_all_are_in_a_cycle():
    rows = [spawn("B", "A", "a.exe"), spawn("A", "B", "b.exe")]
    graph = provenance.summarize(rows)
    assert graph["roots"] == 0
    tree = graph["process_tree"]
    assert "a.exe" in tree and "b.exe" in tree


def test_activity_ranked_by_anomaly_and_capped():
    rows = [spawn("ROOT", "A", "powershell.exe")]
    rows += [leaf("A", f"host{i}.example.com", "dns_query", i / 100) for i in range(60)]

    graph = provenance.summarize(rows, max_activity=10)
    assert len(graph["notable_activity"]) == 10
    assert graph["activity_omitted"] == 50
    anomalies = [a["anomaly"] for a in graph["notable_activity"]]
    assert anomalies == sorted(anomalies, reverse=True)
    assert graph["notable_activity"][0]["image"] == "powershell.exe"


def test_activity_from_a_process_outside_the_spawn_set_is_kept():
    rows = [spawn("ROOT", "A", "chrome.exe"), leaf("GHOST", "8.8.8.8", "net_connect", 0.99)]
    graph = provenance.summarize(rows)
    entry = graph["notable_activity"][0]
    assert entry["process"] == "GHOST"
    assert "image" not in entry


def test_reconnections_are_separated_from_activity():
    rows = [
        spawn("ROOT", "A", "powershell.exe"),
        leaf("A", "1.2.3.4", "net_connect", 0.8),
        {
            "parent": "A",
            "child": "net:203.0.113.9",
            "label": "203.0.113.9",
            "event_type": "reconnect_net",
            "anomaly_score": 0.97,
            "log_id": "",
            "host": "HOST-2",
            "command_line": "",
            "proc_user": "",
            "timestamp": "",
        },
    ]
    graph = provenance.summarize(rows)

    assert len(graph["notable_activity"]) == 1
    assert graph["notable_activity"][0]["type"] == "net_connect"

    assert len(graph["reconnections"]) == 1
    bridge = graph["reconnections"][0]
    assert bridge["type"] == "net"
    assert bridge["source"] == "A"
    assert bridge["shared_artifact"] == "203.0.113.9"
    assert graph["edge_counts"]["reconnect_net"] == 1


def test_empty_input():
    graph = provenance.summarize([])
    assert graph["processes"] == 0
    assert graph["process_tree"] == ""
    assert graph["notable_activity"] == []


def test_non_numeric_anomaly_does_not_crash():
    row = spawn("ROOT", "A", "cmd.exe")
    row["anomaly_score"] = None
    graph = provenance.summarize([row])
    assert "anomaly=0.00" in graph["process_tree"]


def test_missing_fields_are_tolerated():
    graph = provenance.summarize([{"parent": "R", "child": "A", "event_type": "spawn"}])
    assert graph["processes"] == 1
    assert "guid=A" in graph["process_tree"]
