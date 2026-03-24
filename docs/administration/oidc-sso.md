# OIDC / SSO Authentication

Bifract supports generic OpenID Connect (OIDC) for single sign-on. This works with any OIDC-compliant provider: Entra ID, Okta, Keycloak, Google Workspace, Auth0, etc. OIDC is **disabled by default** and coexists with local username/password authentication.

## Enabling OIDC

Set these environment variables (in `.env` for Docker Compose, or in the `bifract-secrets` Secret for Kubernetes):

```bash
BIFRACT_OIDC_ISSUER_URL=https://login.microsoftonline.com/{tenant-id}/v2.0
BIFRACT_OIDC_CLIENT_ID=your-client-id
BIFRACT_OIDC_CLIENT_SECRET=your-client-secret
```

Restart Bifract. The login page will show a "Sign in with SSO" button.

For Kubernetes deployments, see [Post-Deploy Configuration](../getting-started/kubernetes.md#post-deploy-configuration).

## Provider Setup

Register Bifract as an application in your identity provider with these settings:

- **Redirect URI**: `https://your-domain.com/api/v1/auth/oidc/callback`
- **Scopes**: `openid`, `profile`, `email`
- **Grant type**: Authorization Code

## User Provisioning

When a user signs in via OIDC for the first time, Bifract automatically creates their account. OIDC-provisioned users cannot sign in with a password.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BIFRACT_OIDC_ISSUER_URL` | _(disabled)_ | OIDC issuer discovery URL |
| `BIFRACT_OIDC_CLIENT_ID` | _(disabled)_ | OAuth2 client ID |
| `BIFRACT_OIDC_CLIENT_SECRET` | _(empty)_ | OAuth2 client secret |
| `BIFRACT_OIDC_REDIRECT_URL` | auto-derived | Callback URL (derived from `BIFRACT_BASE_URL` or `BIFRACT_DOMAIN`) |
| `BIFRACT_OIDC_SCOPES` | `openid,profile,email` | Comma-separated OIDC scopes |
| `BIFRACT_OIDC_DEFAULT_ROLE` | `user` | Role assigned to auto-provisioned users (`user` or `admin`) |
| `BIFRACT_OIDC_ALLOWED_DOMAINS` | _(all)_ | Comma-separated email domain allowlist (e.g. `example.com,corp.co`) |
| `BIFRACT_OIDC_BUTTON_TEXT` | `Sign in with SSO` | Text displayed on the SSO button |

## Domain Restriction

To limit OIDC access to specific email domains:

```bash
BIFRACT_OIDC_ALLOWED_DOMAINS=example.com,subsidiary.example.com
```

Users with email addresses outside these domains will be rejected at login.

## Provider Examples

**Entra ID (Azure AD)**
```bash
BIFRACT_OIDC_ISSUER_URL=https://login.microsoftonline.com/{tenant-id}/v2.0
BIFRACT_OIDC_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
BIFRACT_OIDC_CLIENT_SECRET=your-secret
```

**Google Workspace**
```bash
BIFRACT_OIDC_ISSUER_URL=https://accounts.google.com
BIFRACT_OIDC_CLIENT_ID=xxxx.apps.googleusercontent.com
BIFRACT_OIDC_CLIENT_SECRET=your-secret
BIFRACT_OIDC_ALLOWED_DOMAINS=yourcompany.com
```

**Keycloak**
```bash
BIFRACT_OIDC_ISSUER_URL=https://keycloak.example.com/realms/bifract
BIFRACT_OIDC_CLIENT_ID=bifract
BIFRACT_OIDC_CLIENT_SECRET=your-secret
```
