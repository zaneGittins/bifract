"""MkDocs hook: builds the Exchange section from exchange/index.yaml, so the docs
list exactly what is merged. It fills the catalog on exchange/index.md and adds a
page per item. The docs workflow rebuilds on any change to exchange/."""

import html
import os

import yaml
from mkdocs.structure.files import File

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXCHANGE = os.path.join(ROOT, "exchange")
MARKER = "<!-- exchange:catalog -->"
BLOB = "https://github.com/zaneGittins/bifract/blob/main/exchange/"
SECTION = "exchange"

# Kind, label, and what adding one does. Bundles lead, as in the app.
KINDS = {
    "bundle": ("Bundle", "Adds each of its items that is not there yet, requirements first."),
    "lookup": ("Lookup", "Adds a lookup filled from the URL below, refreshed on its schedule. Needs a tenant admin."),
    "model": ("Model", "Adds an analytics model to the fractal. It builds from new logs; run a backfill under Analytics for stored ones. Needs an analyst."),
    "detection": ("Detection", "Adds an alert to the fractal, or a draft in your name when the scope reviews alert changes."),
    "search": ("Saved search", "Saves the search for everyone in the fractal."),
    "dashboard": ("Dashboard", "Adds the dashboard to the fractal. Needs an analyst."),
    "normalizer": ("Normalizer", "Adds an instance-wide normalizer. Needs a tenant admin."),
    "policy": ("Policy", "Appends its rules to the scope's alert policies, skipping any already there. Needs a fractal admin."),
}


def load_items():
    with open(os.path.join(EXCHANGE, "index.yaml"), encoding="utf-8") as f:
        # BaseLoader keeps every scalar a string, as the Go reader sees it: a tag
        # such as "on" stays "on" rather than becoming True.
        items = yaml.load(f, Loader=yaml.BaseLoader)["items"]
    order = list(KINDS)
    return sorted(items, key=lambda it: order.index(it["kind"]))


def on_files(files, config):
    items = load_items()
    taken = {f.src_uri for f in files}
    for it in items:
        uri = f"{SECTION}/{it['id']}.md"
        if uri in taken:
            raise ValueError(f"exchange item {it['id']} clashes with docs page {uri}")
        files.append(File.generated(config, uri, content=item_page(it, items)))
    return files


def on_page_markdown(markdown, page, **kwargs):
    if MARKER not in markdown:
        return markdown
    return markdown.replace(MARKER, catalog(load_items()))


def text(value):
    """Index text as literal markdown, never HTML."""
    return html.escape(str(value), quote=False)


def attr(value):
    return html.escape(str(value), quote=True)


def code(value):
    return "`" + str(value).replace("`", "") + "`"


def cell(value):
    """A table cell, where a | would otherwise end the cell."""
    return value.replace("|", "\\|")


def every(minutes):
    # Unset or 0 means the server's default of an hour.
    minutes = int(minutes or 0) or 60
    for size, unit in ((1440, "day"), (60, "hour"), (1, "minute")):
        if minutes % size == 0:
            n = minutes // size
            return f"{n} {unit}" + ("s" if n != 1 else "")


def catalog(items):
    """The searchable list on the catalog page. It is plain HTML, so every item
    shows without JavaScript; exchange-catalog.js adds the search and filters."""
    kinds = [k for k in KINDS if any(it["kind"] == k for it in items)]
    buttons = "".join(
        f'<button type="button" class="exchange-catalog__kind" data-kind="{k}" aria-pressed="false">{KINDS[k][0]}</button>'
        for k in kinds
    )
    cards = []
    for it in items:
        label = KINDS[it["kind"]][0]
        tags = " ".join(f'<span class="exchange-tag">{attr(t)}</span>' for t in it.get("tags", []))
        haystack = " ".join([it["name"], it.get("description", ""), it.get("author", ""), label, *it.get("tags", [])])
        cards.append(
            f'<a class="exchange-card exchange-card--{it["kind"]}" href="{attr(it["id"])}/" '
            f'data-kind="{it["kind"]}" data-text="{attr(haystack.lower())}">'
            f'<span class="exchange-kind exchange-kind--{it["kind"]}">{label}</span>'
            f'<span class="exchange-card__name">{attr(it["name"])}</span>'
            f'<span class="exchange-card__desc">{attr(it.get("description", "").strip())}</span>'
            f'<span class="exchange-card__meta">{attr(it.get("author", "Bifract"))} {tags}</span>'
            "</a>"
        )
    return (
        '<div class="exchange-catalog">'
        '<div class="exchange-catalog__controls">'
        '<input type="search" class="exchange-catalog__search" placeholder="Search the Exchange" aria-label="Search the Exchange">'
        f'<div class="exchange-catalog__kinds" role="group" aria-label="Kind">{buttons}</div>'
        "</div>"
        f'<p class="exchange-catalog__count" aria-live="polite">{len(items)} items</p>'
        f'<div class="exchange-catalog__list">{"".join(cards)}</div>'
        '<p class="exchange-catalog__empty" hidden>No items match.</p>'
        "</div>"
    )


def links(ids, items):
    by_id = {it["id"]: it for it in items}
    return ", ".join(f"[{text(by_id[i]['name'])}]({i}.md)" for i in ids if i in by_id)


def item_page(it, items):
    label, adds = KINDS[it["kind"]]
    out = [f"# {text(it['name'])}", ""]
    badges = [f'<span class="exchange-kind exchange-kind--{it["kind"]}">{label}</span>']
    badges += [f'<span class="exchange-tag">{attr(t)}</span>' for t in it.get("tags", [])]
    out += [f'<p class="exchange-badges">{" ".join(badges)}</p>', ""]
    out += [text(it.get("description", "").strip()), ""]

    facts = []
    if it.get("author"):
        author = text(it["author"])
        facts.append(f"**From:** [{author}](<{it['homepage']}>)" if it.get("homepage") else f"**From:** {author}")
    bundles = [b["id"] for b in items if b["kind"] == "bundle" and it["id"] in b.get("items", [])]
    if bundles:
        facts.append(f"**Part of:** {links(bundles, items)}")
    if it.get("requires"):
        facts.append(f"**Requires:** {links(it['requires'], items)}")
    needed_by = [o["id"] for o in items if it["id"] in o.get("requires", [])]
    if needed_by:
        facts.append(f"**Required by:** {links(needed_by, items)}")
    if facts:
        out += ["  \n".join(facts), ""]

    out += ["## Add it", "", f"In Bifract, open **Exchange**, find **{text(it['name'])}** and click **Add**. {adds}", ""]

    if it["kind"] == "bundle":
        by_id = {i["id"]: i for i in items}
        rows = [
            f"| {KINDS[by_id[m]['kind']][0]} | [{text(by_id[m]['name'])}]({m}.md) | {cell(text(by_id[m].get('description', '').strip()))} |"
            for m in it["items"]
            if m in by_id
        ]
        out += ["## Contents", "", "| Kind | Item | What it is |", "|---|---|---|", *rows, ""]
    elif it["kind"] == "lookup":
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
        out += ["## Settings", "", "| Setting | Value |", "|---|---|"]
        out += [f"| {k} | {cell(v)} |" for k, v in rows if v]
        out += [""]
    else:
        with open(os.path.join(EXCHANGE, it["path"]), encoding="utf-8") as f:
            content = f.read().rstrip("\n")
        out += [
            "## File",
            "",
            f"[`exchange/{it['path']}`]({BLOB}{it['path']})",
            "",
            f'```yaml title="{os.path.basename(it["path"])}"',
            content,
            "```",
            "",
        ]
    return "\n".join(out)
