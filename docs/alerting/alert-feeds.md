# Alert Feeds

Alert feeds sync detection rules from Git repositories. Feeds support both native Bifract YAML alert definitions and Sigma rules, which are automatically translated and normalized.

Admins manage feeds from **Alerts > Feeds** within a fractal.

## How Feeds Work

1. Bifract clones the configured Git repository on each sync cycle.
2. YAML files in the specified path are parsed as Sigma rules or Bifract alert definitions.
3. New rules are created as alerts. Changed rules are updated. Removed rules are deleted.
4. A normalizer (explicit or default) maps Sigma field names to your log schema.

Each feed alert is linked to its source file. Editing a feed alert creates a manual copy, leaving the original feed-managed version intact.

## Feed Configuration

| Field | Description |
|-------|-------------|
| Name | Display name for the feed |
| Repository URL | Git HTTPS URL (e.g. `https://github.com/SigmaHQ/sigma`) |
| Branch | Git branch to sync from (default: `main`) |
| Path | Subdirectory within the repo containing rules (e.g. `rules/windows`) |
| Auth Token | Personal access token for private repositories (encrypted at rest) |
| Normalizer | Field mapping normalizer for Sigma rule translation |
| Sync Schedule | `hourly`, `daily`, `weekly`, `monthly`, or `never` |
| Min Severity | Minimum Sigma severity level to import (`informational`, `low`, `medium`, `high`, `critical`) |
| Min Status | Minimum Sigma maturity status to import (`unsupported`, `deprecated`, `experimental`, `test`, `stable`) |

## Severity Hierarchy

Rules below the configured minimum severity are skipped during sync.

| Level | Order |
|-------|-------|
| informational | 1 (lowest) |
| low | 2 |
| medium | 3 |
| high | 4 |
| critical | 5 (highest) |

## Status Hierarchy

Rules below the configured minimum status are skipped during sync.

| Status | Order |
|--------|-------|
| unsupported | 1 (lowest) |
| deprecated | 2 |
| experimental | 3 |
| test | 4 |
| stable | 5 (highest) |

## Recommended Community Feeds

These public Sigma rule repositories work well as starting points. Add them from **Alerts > Alert Feeds**. This allows you to rapidly onboard detection rules that are normalized to your field names.

| Name | Repository URL | Path | Min Severity | Min Status | Schedule |
|------|---------------|------|-------------|------------|----------|
| SigmaHQ Windows | `https://github.com/SigmaHQ/sigma` | `rules/windows` | high | stable | daily |
| Hayabusa Sysmon | `https://github.com/Yamato-Security/hayabusa-rules` | `sigma/sysmon` | medium | stable | daily |