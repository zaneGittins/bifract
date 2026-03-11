# Prisms

A prism is a virtual view across multiple fractals. Prisms do not store data themselves; they query all member fractals as a unified dataset.

## When to Use Prisms

- Query logs from multiple teams or environments in a single view
- Build dashboards and notebooks that span fractal boundaries
- Attach [alert feeds](../alerting/alert-feeds.md) so detection rules stay consistent as your fractal layout evolves

## Creating a Prism

Admins create prisms from **Settings > Fractals**. Provide a name and optional description, then add member fractals from the prism's manage page.

## Managing Members

Add or remove fractals from a prism at any time. Changes take effect immediately for all users.

## Access

Users can see a prism if they have access to at least one of its member fractals. Switching to a prism works the same as switching fractals via the selector in the UI.

## Scope

Dictionaries, dashboards, and notebooks can be scoped to a prism instead of a single fractal. Alerts remain per-fractal.
