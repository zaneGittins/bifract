# Fractals

A fractal is an isolated log container. Each fractal has its own log data in ClickHouse and its own set of API keys, alerts, and comments.

Admins manage fractals from the top-level **Fractals** tab.

- **Create fractal**: Provide a name and optional description.
- **Delete fractal**: From **[Fractal] > Manage > Danger Zone**. Removes all associated log data, API keys, and configuration. This is **irreversible**.

Each fractal's own settings live under its **Manage** tab, which has four sections:

| Sub-tab | Contents |
|---------|----------|
| **Overview** | Name, description, and fractal statistics |
| **Access** | Per-fractal user and group permissions, plus API keys |
| **Lifecycle** | Retention, archive retention, and [disk quota](disk-quotas.md) |
| **Danger Zone** | Clear logs, delete fractal |

## Clearing Logs

Admins can delete all logs for a fractal from **[Fractal] > Manage > Danger Zone > Clear Logs**. The fractal itself, its API keys, and its configuration remain. This is **irreversible**.

## Disk Quotas

Each fractal can have a configurable disk quota that caps raw log storage. See [Disk Quotas](disk-quotas.md) for configuration details and enforcement modes.

## Prisms

Prisms are views of multiple fractals. They do not store data themselves. Create alerts and attach alert feeds to prisms so the member fractals can change as your deployment grows. Admins can create Prisms from the main Fractal listing and add member Fractals on the Prism's manage page.
