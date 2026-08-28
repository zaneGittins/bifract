"""Tools for reading detection coverage against MITRE ATT&CK."""

from .. import http
from ..app import as_json, tool

MAX_TECHNIQUES = 60
MAX_GAPS = 40


@tool
async def get_attack_coverage(tactic: str = "", limit: int = 40) -> str:
    """
    Report which ATT&CK techniques this fractal's detections cover.

    Coverage is derived from the attack.* labels on the alerts that exist, so it
    describes what is configured, not what has fired. Use it to answer "are we
    watching for this technique" before writing a detection that already exists.

    Args:
        tactic: Restrict to one tactic, by ATT&CK id or name (e.g. 'TA0006' or
                'credential-access'). Empty covers every tactic.
        limit: How many covered techniques to list, capped at 60. The summary
               counts are always for the whole matrix, not just the listed page.

    Returns:
        The coverage summary, and the covered techniques with the number of rules
        behind each.
    """
    payload = await http.get("/attack/coverage")
    if not isinstance(payload, dict):
        return as_json(payload)

    techniques = [t for t in payload.get("techniques") or [] if isinstance(t, dict)]
    if needle := tactic.strip().lower():
        techniques = [
            t
            for t in techniques
            if needle in str(t.get("tactic", "")).lower()
            or needle in str(t.get("tactic_id", "")).lower()
        ]

    covered = sorted(
        (t for t in techniques if t.get("rule_count")),
        key=lambda t: -int(t.get("rule_count") or 0),
    )
    shown = covered[: max(1, min(limit, MAX_TECHNIQUES))]
    return as_json(
        {
            "summary": payload.get("summary"),
            "covered_techniques": len(covered),
            "showing": len(shown),
            "techniques": shown,
            "note": "Coverage counts configured detections, not detections that have fired.",
        }
    )


@tool
async def get_attack_gaps(limit: int = 20) -> str:
    """
    List uncovered ATT&CK techniques, ranked by what could be covered today.

    The ranking accounts for whether rules exist that would detect the technique
    against the fields this fractal actually ingests, so the top entries are the
    ones worth acting on rather than the ones needing new telemetry.

    Args:
        limit: How many gaps to return, capped at 40.

    Returns:
        The uncovered techniques with candidate rule counts, and the total number
        of gaps so a short list is not mistaken for the whole picture.
    """
    count = max(1, min(limit, MAX_GAPS))
    payload = await http.get("/attack/gaps", {"limit": count})
    if not isinstance(payload, dict):
        return as_json(payload)

    if not payload.get("catalog_populated"):
        return as_json(
            {
                "gaps": [],
                "note": (
                    "No rule catalog is loaded, so candidate rules cannot be ranked. "
                    "Sync a detection feed first; get_attack_coverage still works."
                ),
            }
        )
    return as_json(
        {
            "uncovered_total": payload.get("uncovered_total"),
            "returned": payload.get("returned"),
            "gaps": payload.get("gaps"),
        }
    )
