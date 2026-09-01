# Users & Roles

## Tenant Roles

Users are created with one of two tenant-level roles:

| Role | Capabilities |
|------|-------------|
| `admin` (Tenant Admin) | Full access: manage users, fractals, API keys, delete logs |
| `user` | Access fractals based on per-fractal permissions |

The first user is created during initial setup as an admin. Additional users are invited by admins from the **Admin** tab.

## Fractal Roles

Each user can be assigned a per-fractal role controlling what they can do within that fractal:

| Role | Capabilities |
|------|-------------|
| `admin` | Manage fractal settings, keys, and alerts |
| `analyst` | Search logs, write comments, view alerts and notebooks |
| `viewer` | Search logs only |

Tenant admins have full access to all fractals regardless of per-fractal roles.

## User Management

Admins manage users from **Admin > Users**.

- **Create user**: Provide a username, optional display name, and role (`admin` or `user`, defaults to `user`). Admins never set a password. Bifract generates a **one-time invite link** that the user opens to choose their own password. Invite links expire after 7 days; use **Reset invite** to issue a new one.
- **Enable / disable user**: Suspend an account without deleting it. A disabled user cannot log in but their comments, notebooks, and dashboards are preserved.
- **Reset password**: Admins can issue a password reset for a local account.
- **Delete user**: Remove a user account. Admins cannot delete their own account.
- **Reset two-factor**: Clear a user's authenticator enrollment when they have lost both their device and their recovery codes. They are signed out and enroll again at their next sign in.
- **Download mTLS certificate**: When running in `mtls-app` mode, a **Cert** button appears next to each user. Click it to generate and download a `.p12` client certificate for that user. See [mTLS Client Certificates](reverse-proxy.md#mtls-client-certificates) for details.

OIDC-provisioned users have no password and no invite; see [OIDC / SSO](oidc-sso.md).

## Two-Factor Authentication

Bifract uses standard TOTP, which works with Google Authenticator, 1Password, Authy, Bitwarden, and similar.

### Turning it on

Individual users can enroll at any time from the user menu by selecting **Two-Factor Authentication**. Nothing needs to be enabled first.

To require it for everyone, turn on **Require two-factor authentication** under **Admin > Settings > Security**. Users are required to enroll at their next sign in.

Two exemptions:

- **SSO accounts are exempt.** Their identity provider is responsible for MFA. See [OIDC / SSO](oidc-sso.md).
- **API keys are unaffected.** They are not interactive logins and carry their own scoping and revocation. An API key granted tenant administration therefore reaches the API without a code, which is a reason to scope keys narrowly. See [API Keys](ingest-tokens.md).

### Recovery codes

Enrollment hands the user ten single-use recovery codes, shown once. Each one substitutes for an authenticator code at sign in, which is what gets someone back in after a lost or wiped phone. Users can generate a fresh set from the same screen at any time, which invalidates the previous set.

### Requirements

Enrollment secrets are encrypted at rest with a key derived from `BIFRACT_PASSWORD_PEPPER`. The installer generates a pepper for you and carries it across upgrades.

If a deployment has no pepper set, two-factor authentication is unavailable and the settings toggle is disabled rather than storing secrets in plaintext. Rotating the pepper invalidates enrollments along with every password hash, so treat it as a one-time value.

## Groups

Groups let you grant fractal and prism permissions to several users at once instead of managing each user individually. Manage them from **Admin > Groups**.

- Create a group, then add users as members.
- Grant the group a role on a fractal or prism from that fractal's **Manage > Access** tab, exactly as you would for a single user.
- A user's effective role is the strongest role granted to them directly or through any group they belong to.