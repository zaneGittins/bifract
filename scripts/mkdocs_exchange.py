"""MkDocs hook: renders exchange/index.yaml into the Exchange page, so the docs
list exactly what is merged. The docs workflow rebuilds on any change to exchange/."""

import html
import os

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXCHANGE = os.path.join(ROOT, "exchange")
MARKER = "<!-- exchange:catalog -->"
BLOB = "https://github.com/zaneGittins/bifract/blob/main/exchange/"
SECTIONS = [
    ("lookup", "Lookups"),
    ("detection", "Detections"),
    ("normalizer", "Normalizers"),
    ("policy", "Alert policies"),
]


def on_page_markdown(markdown, page, **kwargs):
    if MARKER not in markdown:
        return markdown
    return markdown.replace(MARKER, render())


def render():
    with open(os.path.join(EXCHANGE, "index.yaml"), encoding="utf-8") as f:
        # BaseLoader keeps every scalar a string, as the Go reader sees it: a tag
        # such as "on" stays "on" rather than becoming True.
        items = yaml.load(f, Loader=yaml.BaseLoader)["items"]
    out = []
    for kind, title in SECTIONS:
        group = [it for it in items if it["kind"] == kind]
        if not group:
            continue
        out.append(f"### {title}\n")
        out.extend(render_item(it) for it in group)
    return "\n".join(out)


def every(minutes):
    # Unset or 0 means the server's default of an hour.
    minutes = int(minutes or 0) or 60
    for size, unit in ((1440, "day"), (60, "hour"), (1, "minute")):
        if minutes % size == 0:
            n = minutes // size
            return f"{n} {unit}" + ("s" if n != 1 else "")


def indent(text):
    return "\n".join(("    " + line) if line else "" for line in text.splitlines())


def text(value):
    """Index text as literal markdown, never HTML."""
    return html.escape(str(value), quote=False)


def code(value):
    return "`" + str(value).replace("`", "") + "`"


def cell(value):
    """A table cell, where a | would otherwise end the cell."""
    return value.replace("|", "\\|")


def render_item(it):
    name = text(it["name"]).replace('"', "'")
    body = [text(it.get("description", "").strip())]

    meta = []
    if it.get("author"):
        author = text(it["author"])
        meta.append(f"From [{author}](<{it['homepage']}>)" if it.get("homepage") else f"From {author}")
    if it.get("tags"):
        meta.append(" ".join(code(t) for t in it["tags"]))
    if meta:
        body.append(". ".join(meta))

    if it["kind"] == "lookup":
        src = it["lookup"]["source"]
        rows = [
            ("Lookup name", code(it["id"])),
            ("Kind", it["lookup"].get("list_kind", "value")),
            ("URL", f"<{src['url']}>"),
            ("Key column", code(src["key_field"])),
            ("Other columns", ", ".join(code(c) for c in src.get("columns", [])) or "all"),
            ("Split key cells on", code(src["split_key"]) if src.get("split_key") else ""),
            ("Keys are globs", "yes" if src.get("key_format") == "glob" else ""),
            ("Refresh every", every(src.get("refresh_minutes"))),
        ]
        body.append("| Setting | Value |\n|---|---|\n" + "\n".join(f"| {k} | {cell(v)} |" for k, v in rows if v))
    else:
        with open(os.path.join(EXCHANGE, it["path"]), encoding="utf-8") as f:
            content = f.read().rstrip("\n")
        body.append(f"[`exchange/{it['path']}`]({BLOB}{it['path']})")
        body.append(f'```yaml title="{os.path.basename(it["path"])}"\n{content}\n```')

    return f'??? abstract "{name}"\n' + indent("\n\n".join(body)) + "\n"
