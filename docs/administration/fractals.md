# Fractals

A fractal is an isolated log container. Each fractal has its own log data in ClickHouse and its own set of API keys, alerts, and comments.

Admins manage fractals from **Settings > Fractals**.

- **Create fractal**: Provide a name and optional description.
- **Delete fractal**: Removes all associated log data, API keys, and configuration. This is **irreversible**.

## Clearing Logs

Admins can clear all logs for the currently selected fractal from **Settings > Danger Zone**. This is **irreversible**.

## Disk Quotas

Each fractal can have a configurable disk quota that caps raw log storage. See [Disk Quotas](disk-quotas.md) for configuration details and enforcement modes.

## Prisms

Prisms are views of multiple fractals. They do not store data themselves. It is best practice to create alerts and attach alert feeds to Prisms so you can adjust the backend as your deployment grows. Admins can create Prisms from the main Fractal listing and add member Fractals on the Prism's manage page.
