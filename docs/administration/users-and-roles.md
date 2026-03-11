# Users & Roles

## Tenant Roles

Users are created with one of two tenant-level roles:

| Role | Capabilities |
|------|-------------|
| `admin` (Tenant Admin) | Full access: manage users, fractals, API keys, delete logs |
| `user` | Access fractals based on per-fractal permissions |

The first user is created during initial setup as an admin. Additional users are created by admins through the Settings page.

## Fractal Roles

Each user can be assigned a per-fractal role controlling what they can do within that fractal:

| Role | Capabilities |
|------|-------------|
| `admin` | Manage fractal settings, keys, and alerts |
| `analyst` | Search logs, write comments, view alerts and notebooks |
| `viewer` | Search logs only |

Tenant admins have full access to all fractals regardless of per-fractal roles.

## User Management

Admins manage users from **Settings > Users**.

- **Create user**: Provide a username, display name, password, and role (`admin` or `user`). Defaults to `user`.
- **Delete user**: Remove a user account. Admins cannot delete their own account.
