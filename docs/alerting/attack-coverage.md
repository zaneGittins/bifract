# MITRE ATT&CK Coverage

Sigma rules carry ATT&CK tags, and Bifract stores them verbatim on the alert:

```yaml
tags:
    - attack.persistence
    - attack.privilege-escalation
    - attack.t1543.003
    - attack.t1068
```

**Alerts > Coverage** reads those tags back against an embedded copy of the ATT&CK
Enterprise matrix and renders the familiar tactic-column grid, heat-mapped by how
many of your rules cover each technique. Everything on the page is scoped to the
fractal or prism you are currently in.

## Reading the map

Each column is a tactic in kill-chain order. Each cell is a technique, filled in
proportion to how many rules map to it and badged with the count. **Uncovered
techniques are drawn with a dashed border and no fill**, so a gap never reads as
merely low coverage.

Cells with a chevron have sub-techniques; expand them individually or turn on
**Sub-techniques** to expand every group.

## How coverage is counted

Two numbers, and the difference between them matters:

**Coverage** (the headline, and the large number in each column header) counts
**detectable units**: every sub-technique, plus every technique that has none.
Nothing is inherited. A rule tagged `attack.t1547` covers the *parent* but zero of
its 14 autostart sub-techniques, because the tag does not say which mechanism the
rule actually catches. This is the number that does not flatter.

**Techniques touched** (the small line under each column header) counts top-level
techniques with at least one rule mapped to them **or to any one of their
sub-techniques**. A technique with 14 sub-techniques and 1 covered scores a full
point here.

The gap between them is usually large and is itself informative:

| Tactic | Coverage | Techniques touched |
|---|---|---|
| Privilege Escalation | 36/85 (42%) | 12/13 (92%) |
| Persistence | 42/99 (42%) | 17/22 (77%) |
| Credential Access | 26/58 (45%) | 14/17 (82%) |

Reading the right-hand column as your coverage would overstate it by roughly a
factor of two. Use "techniques touched" for breadth ("have we looked at this at
all?") and "Coverage" for depth ("can we actually catch it?").

Three further counting rules, chosen for the same reason:

- A rule tagged with a sub-technique (`attack.t1543.003`) counts as **direct**
  coverage of that sub-technique and **inherited** coverage of its parent. The
  drawer and tooltip report the two separately.
- A rule tagged with **only a tactic** (`attack.persistence`, no technique) is not
  coverage of anything on the grid. It is counted under "rules mapped" as
  unmapped, because it does not say which technique it detects.
- A rule tagged with a **retired** technique ID resolves to its replacement where
  ATT&CK defines one (`attack.t1086` counts toward T1059.001), silently and
  automatically. An ID with **no** replacement, usually a typo or a technique MITRE
  removed outright, cannot resolve to anything, so the rule is invisible to this
  map; those are counted in the **Broken ATT&CK tags** card with the offending IDs
  listed, so you can go fix the rule.

Deprecated techniques are excluded from every denominator.

## Controls

| Control | Effect |
|---|---|
| Search | Dims non-matching cells and expands any sub-technique group holding a match. Matches technique IDs too, which are not printed on the cells |
| All techniques / Gaps only / Covered only | Narrows the grid to what you are working on |
| Colour | Rule count, enabled rules, or highest severity. "Enabled rules" is how you find coverage that exists but is switched off |
| Filters | Severity, platform, source and enabled-only, folded into one control with a badge showing how many are active. The platform filter narrows the technique universe too, so the percentage stays honest |
| Sub-techniques | Expands every sub-technique group at once |
| Export layer | Downloads your coverage as an ATT&CK Navigator layer (see below) |

## Export layer

**Layer** is ATT&CK Navigator's term for a coverage overlay: a small JSON file of
`{techniqueID, score, comment}` entries that MITRE's own viewer paints onto the
matrix. It is the interchange format the whole ATT&CK ecosystem speaks, so
exporting one lets you:

- Open Bifract's coverage in [MITRE's Navigator](https://mitre-attack.github.io/attack-navigator/)
  and hand the file to people who have no Bifract access
- **Diff it against another source** -- Navigator can subtract one layer from
  another, so you can overlay your EDR vendor's claimed coverage, a red team's
  layer, or a threat group's technique set (`attack.mitre.org` publishes those) and
  see precisely where you are exposed
- Keep a dated snapshot to show coverage growth over time

The export honours whatever filters are active, so you can export "Windows only"
or "critical severity only" as its own layer. Scores are rule counts, and each
entry's comment records the direct/inherited split.

Clicking any cell opens a drawer with the technique's tactics, the rules covering
it (click one to open it in the alert editor), its platforms, the telemetry MITRE
expects it to be detectable in, and a link to attack.mitre.org.

## Top gaps

Below the grid, uncovered techniques are ranked by what you can do about them
**today**. A gap is far more actionable when a rule for it already exists in a
feed you have configured but was never imported, so the list cross-references the
feed rule catalog and states the reason each candidate is not running:

- `below the feed severity threshold` -- the rule's `level` is under the feed's **Min Level**
- `below the feed maturity threshold` -- the rule's `status` is under the feed's **Min Status**
- `cannot be translated to BQL` -- the rule parsed but Bifract cannot express its detection logic
- `failed to import` -- the translation produced a query Bifract's own parser rejected

The first two are a threshold you chose: lower the feed's Min Level or Min Status
to pull those rules in. Bifract will not import them behind your back. The last
two are translator work, and their counts are the clearest signal of which Sigma
constructs to support next.

Techniques with nothing available are shown as **Needs a new rule**, with MITRE's
expected telemetry as the starting point.

## The rule catalog

The gap list is backed by `feed_rule_catalog`, which records every rule a feed's
repository offers, imported or not. It is populated on **every feed sync**, so a
freshly upgraded install shows no candidates until its feeds sync again. Trigger a
sync from **Alerts > Feeds** to populate it immediately.

Metadata for the catalog is read before translation is attempted, which is the
whole point: a rule Bifract cannot translate still has ATT&CK tags, and those are
exactly the gaps worth knowing about.

## Updating the ATT&CK matrix

The Enterprise matrix is embedded in the binary (gzipped, ~17 KB) rather than
fetched at runtime, so air-gapped installs work and startup has no dependency on
GitHub. Regenerate it when MITRE publishes a new version:

```sh
go run ./cmd/bifract-attackgen -out pkg/attack/data/enterprise-attack.json.gz
```

That downloads MITRE's STIX bundle, slims it, validates the result, and writes the
embedded file; pass `-in enterprise-attack.json` to use a local copy instead.
Commit the regenerated file. `go test ./pkg/attack/...` verifies the result loads,
that every tactic column is populated in kill-chain order, and that no
sub-technique has a dangling parent.

ATT&CK renames tactics between versions (v19 renamed Defense Evasion to Stealth)
while rule sets keep emitting the old slug for years. Bifract resolves both, so
`attack.defense-evasion` keeps working regardless of which version is embedded.
